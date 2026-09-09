use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::Context;
use relaygate_sdk::{DestinationId, ErrorCode, PeerObservation, Pipe, Relay};
use tokio::time::{sleep, timeout};

pub(crate) async fn dial(
    relay: &Relay,
    destination: &str,
    wait: Duration,
    admission_rejections: &AtomicU64,
) -> anyhow::Result<Pipe> {
    let destination: DestinationId = destination.parse()?;
    timeout(wait, async {
        loop {
            match relay.dial(destination).await {
                Ok(pipe) => return Ok(pipe),
                Err(error) if retryable(error.code(), error.observation()) => {
                    if error.code() == ErrorCode::ResourceExhausted {
                        admission_rejections.fetch_add(1, Ordering::Relaxed);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await
    .context("soak dial did not recover before its deadline")?
}

const fn retryable(code: ErrorCode, observation: PeerObservation) -> bool {
    matches!(observation, PeerObservation::NotObserved)
        && matches!(
            code,
            ErrorCode::NotFound | ErrorCode::Unavailable | ErrorCode::ResourceExhausted
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soak_retries_only_known_unobserved_transient_failures() {
        for code in [
            ErrorCode::NotFound,
            ErrorCode::Unavailable,
            ErrorCode::ResourceExhausted,
        ] {
            assert!(retryable(code, PeerObservation::NotObserved));
            assert!(!retryable(code, PeerObservation::MaybeObserved));
            assert!(!retryable(code, PeerObservation::Observed));
        }
        for code in [
            ErrorCode::PermissionDenied,
            ErrorCode::Unauthenticated,
            ErrorCode::ProtocolError,
        ] {
            assert!(!retryable(code, PeerObservation::NotObserved));
        }
    }
}
