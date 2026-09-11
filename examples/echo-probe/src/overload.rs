use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, ensure};
use relaygate_sdk::{ErrorCode, PeerObservation, RouteAddress};
use tokio::task::JoinSet;

use crate::{
    config::{
        access_token_source, environment, overload_duration, overload_sessions, overload_workers,
        route_address,
    },
    probe::{assert_echo, connect},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct OverloadAccounting {
    attempted: u64,
    succeeded: u64,
    resource_exhausted_not_observed: u64,
    other_failures: u64,
}

impl OverloadAccounting {
    fn add(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.succeeded += other.succeeded;
        self.resource_exhausted_not_observed += other.resource_exhausted_not_observed;
        self.other_failures += other.other_failures;
    }

    const fn is_exact(self) -> bool {
        self.attempted
            == self.succeeded + self.resource_exhausted_not_observed + self.other_failures
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let address = environment("RELAYGATE_ADDR", "gateway-a:27420");
    let route_address = route_address()?;
    let route_address: RouteAddress = route_address
        .parse()
        .with_context(|| format!("invalid RouteAddress {route_address:?}"))?;
    let access_token_source = access_token_source()?;
    let duration = overload_duration()?;
    let worker_count = overload_workers()?;
    let session_count = overload_sessions()?;

    let mut relays = Vec::with_capacity(session_count);
    for _ in 0..session_count {
        relays.push(connect(&address).await?);
    }

    let deadline = Instant::now() + duration;
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = JoinSet::new();
    for worker in 0..worker_count {
        let relay = relays[worker % relays.len()].clone();
        let route_address = route_address.clone();
        let access_token_source = access_token_source.clone();
        let stop = Arc::clone(&stop);
        workers.spawn(async move {
            let mut accounting = OverloadAccounting::default();
            let mut first_unexpected = None;
            let mut sequence = 0_u64;
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                accounting.attempted += 1;
                match relay
                    .dial(route_address.clone(), access_token_source.clone())
                    .await
                {
                    Ok(pipe) => {
                        let payload =
                            format!("relaygate overload worker={worker} sequence={sequence}");
                        if let Err(error) = assert_echo(pipe, payload.as_bytes()).await {
                            accounting.other_failures += 1;
                            first_unexpected = Some(format!(
                                "worker={worker} sequence={sequence}: echo failed: {error:#}"
                            ));
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
                        accounting.succeeded += 1;
                    }
                    Err(error) if is_expected_overload(error.code(), error.observation()) => {
                        accounting.resource_exhausted_not_observed += 1;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => {
                        accounting.other_failures += 1;
                        first_unexpected = Some(format!(
                            "worker={worker} sequence={sequence}: dial failed: {error}"
                        ));
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                sequence += 1;
            }
            (accounting, first_unexpected)
        });
    }

    let mut total = OverloadAccounting::default();
    let mut first_unexpected = None;
    while let Some(result) = workers.join_next().await {
        let (accounting, unexpected) = result.context("overload worker failed to join")?;
        total.add(accounting);
        if first_unexpected.is_none() {
            first_unexpected = unexpected;
        }
    }
    for relay in relays {
        relay.close();
    }

    println!(
        "relaygate overload result workers={worker_count} sessions={session_count} attempted={} succeeded={} resource_exhausted_not_observed={} other_failures={} duration_seconds={}",
        total.attempted,
        total.succeeded,
        total.resource_exhausted_not_observed,
        total.other_failures,
        duration.as_secs(),
    );
    ensure!(total.is_exact(), "overload completion accounting mismatch");
    ensure!(total.attempted > 0, "overload probe made no attempts");
    ensure!(total.succeeded > 0, "overload probe completed no Pipes");
    if let Some(error) = first_unexpected {
        anyhow::bail!("unexpected overload failure: {error}");
    }
    Ok(())
}

const fn is_expected_overload(code: ErrorCode, observation: PeerObservation) -> bool {
    matches!(code, ErrorCode::ResourceExhausted)
        && matches!(observation, PeerObservation::NotObserved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_accounting_is_exact_only_when_every_attempt_is_terminal() {
        let exact = OverloadAccounting {
            attempted: 10,
            succeeded: 6,
            resource_exhausted_not_observed: 3,
            other_failures: 1,
        };
        assert!(exact.is_exact());
        assert!(
            !OverloadAccounting {
                attempted: 11,
                ..exact
            }
            .is_exact()
        );
    }

    #[test]
    fn overload_accounting_accepts_only_resource_exhausted_not_observed() {
        assert!(is_expected_overload(
            ErrorCode::ResourceExhausted,
            PeerObservation::NotObserved
        ));
        assert!(!is_expected_overload(
            ErrorCode::ResourceExhausted,
            PeerObservation::MaybeObserved
        ));
        assert!(!is_expected_overload(
            ErrorCode::Unavailable,
            PeerObservation::NotObserved
        ));
    }
}
