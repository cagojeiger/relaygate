use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::Context;
use relaygate_sdk::{AccessTokenSource, Destination, ErrorCode, PeerObservation, Pipe, Relay};
use tokio::time::{sleep, timeout};

pub(crate) async fn dial(
    relay: &Relay,
    destination: &str,
    access_token_source: &AccessTokenSource,
    wait: Duration,
    admission_rejections: &AtomicU64,
) -> anyhow::Result<Pipe> {
    let destination: Destination = destination.parse()?;
    retry_until_available(wait, admission_rejections, || async {
        match relay
            .dial(destination.clone(), access_token_source.clone())
            .await
        {
            Ok(pipe) => Attempt::Complete(Ok(pipe)),
            Err(error) if retryable(error.code(), error.observation()) => Attempt::Retry {
                admission_rejected: error.code() == ErrorCode::ResourceExhausted,
            },
            Err(error) => Attempt::Complete(Err(error.into())),
        }
    })
    .await
}

enum Attempt<T> {
    Complete(anyhow::Result<T>),
    Retry { admission_rejected: bool },
}

async fn retry_until_available<T, F, Fut>(
    wait: Duration,
    admission_rejections: &AtomicU64,
    mut attempt: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Attempt<T>>,
{
    timeout(wait, async {
        loop {
            match attempt().await {
                Attempt::Complete(result) => return result,
                Attempt::Retry { admission_rejected } => {
                    if admission_rejected {
                        admission_rejections.fetch_add(1, Ordering::Relaxed);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
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
mod tests;
