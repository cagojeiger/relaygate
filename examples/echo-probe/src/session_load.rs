use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, ensure};
use relaygate_sdk::{Config, DestinationId, Relay};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::{
    config::{DESTINATION_IDS, environment, sdk_config},
    probe::assert_echo,
};

const DEFAULT_ADDRESSES: &str = "gateway-a:27420,gateway-b:27420,gateway-c:27420";
const DEFAULT_SESSIONS: usize = 100;
const DEFAULT_RAMP_PER_SECOND: usize = 100;
const DEFAULT_HOLD_SECONDS: u64 = 60;
const DEFAULT_ACTIVE_PERCENT: u8 = 0;
const DEFAULT_PAYLOAD_BYTES: usize = 1024;
const DEFAULT_PAYLOAD_INTERVAL_MS: u64 = 1000;

#[derive(Clone, Debug)]
struct Settings {
    addresses: Vec<String>,
    sessions: usize,
    ramp_per_second: usize,
    hold: Duration,
    active_percent: u8,
    payload_bytes: usize,
    payload_interval: Duration,
    destination_id: DestinationId,
}

#[derive(Clone, Copy, Debug, Default)]
struct TrafficAccounting {
    attempts: u64,
    succeeded: u64,
    failed: u64,
    payload_bytes: u64,
}

impl TrafficAccounting {
    fn add(&mut self, other: Self) {
        self.attempts += other.attempts;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.payload_bytes += other.payload_bytes;
    }

    const fn is_exact(self) -> bool {
        self.attempts == self.succeeded + self.failed
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env()?;
    let configs = Arc::new(load_configs(&settings.addresses)?);
    let started = Instant::now();
    let relays = connect_ramped(&settings, configs).await?;
    let connect_millis = started.elapsed().as_millis();
    let active_sessions = active_session_count(settings.sessions, settings.active_percent);

    println!(
        "relaygate session load ready sessions={} addresses={} ramp_per_second={} connect_millis={} hold_seconds={} active_sessions={} payload_bytes={} payload_interval_ms={}",
        relays.len(),
        settings.addresses.len(),
        settings.ramp_per_second,
        connect_millis,
        settings.hold.as_secs(),
        active_sessions,
        settings.payload_bytes,
        settings.payload_interval.as_millis(),
    );

    let accounting = hold_and_generate_traffic(&settings, &relays, active_sessions).await;
    for relay in &relays {
        relay.close();
    }
    let accounting = accounting?;

    println!(
        "relaygate session load result sessions={} active_sessions={} attempted={} succeeded={} failed={} payload_bytes={} hold_seconds={} connect_millis={}",
        relays.len(),
        active_sessions,
        accounting.attempts,
        accounting.succeeded,
        accounting.failed,
        accounting.payload_bytes,
        settings.hold.as_secs(),
        connect_millis,
    );
    ensure!(accounting.is_exact(), "session load accounting mismatch");
    ensure!(
        accounting.failed == 0,
        "session load observed failed traffic"
    );
    if active_sessions > 0 {
        ensure!(
            accounting.succeeded > 0,
            "active session load completed no Pipes"
        );
    }
    Ok(())
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        let addresses = address_list(&environment(
            "RELAYGATE_SESSION_LOAD_GATEWAYS",
            DEFAULT_ADDRESSES,
        ))?;
        let sessions = positive_usize("RELAYGATE_SESSION_LOAD_SESSIONS", DEFAULT_SESSIONS)?;
        let ramp_per_second = positive_usize(
            "RELAYGATE_SESSION_LOAD_RAMP_PER_SECOND",
            DEFAULT_RAMP_PER_SECOND,
        )?;
        let hold = Duration::from_secs(positive_u64(
            "RELAYGATE_SESSION_LOAD_HOLD_SECS",
            DEFAULT_HOLD_SECONDS,
        )?);
        let active_percent = percentage(
            "RELAYGATE_SESSION_LOAD_ACTIVE_PERCENT",
            DEFAULT_ACTIVE_PERCENT,
        )?;
        let payload_bytes = positive_usize(
            "RELAYGATE_SESSION_LOAD_PAYLOAD_BYTES",
            DEFAULT_PAYLOAD_BYTES,
        )?;
        let payload_interval = Duration::from_millis(positive_u64(
            "RELAYGATE_SESSION_LOAD_PAYLOAD_INTERVAL_MS",
            DEFAULT_PAYLOAD_INTERVAL_MS,
        )?);
        let destination_id =
            environment("RELAYGATE_SESSION_LOAD_DESTINATION_ID", DESTINATION_IDS[0])
                .parse()
                .context("RELAYGATE_SESSION_LOAD_DESTINATION_ID must be a UUIDv4 DestinationId")?;
        Ok(Self {
            addresses,
            sessions,
            ramp_per_second,
            hold,
            active_percent,
            payload_bytes,
            payload_interval,
            destination_id,
        })
    }
}

fn load_configs(addresses: &[String]) -> anyhow::Result<Vec<Config>> {
    addresses
        .iter()
        .map(|address| {
            Ok(sdk_config(address)?
                .with_connect_timeout(Duration::from_secs(30))
                .with_operation_timeout(Duration::from_secs(30)))
        })
        .collect()
}

async fn connect_ramped(
    settings: &Settings,
    configs: Arc<Vec<Config>>,
) -> anyhow::Result<Vec<Relay>> {
    let mut relays: Vec<Relay> = Vec::with_capacity(settings.sessions);
    let mut next = 0_usize;
    while next < settings.sessions {
        let batch_started = Instant::now();
        let end = next
            .saturating_add(settings.ramp_per_second)
            .min(settings.sessions);
        let mut operations = JoinSet::new();
        for index in next..end {
            let config = configs[index % configs.len()].clone();
            operations.spawn(async move {
                Relay::connect(config)
                    .await
                    .map(|relay| (index, relay))
                    .map_err(anyhow::Error::from)
            });
        }

        let mut batch: Vec<(usize, Relay)> = Vec::with_capacity(end - next);
        while let Some(result) = operations.join_next().await {
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    operations.abort_all();
                    while operations.join_next().await.is_some() {}
                    for (_, relay) in batch {
                        relay.close();
                    }
                    for relay in relays {
                        relay.close();
                    }
                    return Err(error).context("session load connect task failed to join");
                }
            };
            match result {
                Ok(relay) => batch.push(relay),
                Err(error) => {
                    operations.abort_all();
                    while operations.join_next().await.is_some() {}
                    for (_, relay) in batch {
                        relay.close();
                    }
                    for relay in relays {
                        relay.close();
                    }
                    return Err(error.context(format!(
                        "session load failed while connecting batch {next}..{end}"
                    )));
                }
            }
        }
        batch.sort_unstable_by_key(|(index, _)| *index);
        relays.extend(batch.into_iter().map(|(_, relay)| relay));
        next = end;
        println!(
            "relaygate session load ramp connected={}/{}",
            relays.len(),
            settings.sessions
        );
        if next < settings.sessions {
            sleep_until(batch_started + Duration::from_secs(1)).await;
        }
    }
    Ok(relays)
}

async fn hold_and_generate_traffic(
    settings: &Settings,
    relays: &[Relay],
    active_sessions: usize,
) -> anyhow::Result<TrafficAccounting> {
    if active_sessions == 0 {
        sleep(settings.hold).await;
        return Ok(TrafficAccounting::default());
    }

    let deadline = Instant::now() + settings.hold;
    let payload = Arc::new(load_payload(settings.payload_bytes));
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = JoinSet::new();
    for (index, relay) in relays.iter().take(active_sessions).cloned().enumerate() {
        let payload = Arc::clone(&payload);
        let stop = Arc::clone(&stop);
        let destination_id = settings.destination_id;
        let interval = settings.payload_interval;
        workers.spawn(async move {
            let mut accounting = TrafficAccounting::default();
            let mut first_error = None;
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                accounting.attempts += 1;
                match relay.dial(destination_id).await {
                    Ok(pipe) => match assert_echo(pipe, &payload).await {
                        Ok(()) => {
                            accounting.succeeded += 1;
                            accounting.payload_bytes += payload.len() as u64;
                        }
                        Err(error) => {
                            accounting.failed += 1;
                            first_error = Some(format!(
                                "session load echo failed for active session {index}: {error:#}"
                            ));
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
                    },
                    Err(error) => {
                        accounting.failed += 1;
                        first_error = Some(format!(
                            "session load dial failed for active session {index}: {error}"
                        ));
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                sleep(interval).await;
            }
            (accounting, first_error)
        });
    }

    let mut total = TrafficAccounting::default();
    let mut first_error = None;
    while let Some(result) = workers.join_next().await {
        let (accounting, error) = result.context("session load traffic task failed to join")?;
        total.add(accounting);
        if first_error.is_none() {
            first_error = error;
        }
    }
    if let Some(error) = first_error {
        anyhow::bail!(error);
    }
    Ok(total)
}

fn address_list(value: &str) -> anyhow::Result<Vec<String>> {
    let addresses = value
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    ensure!(
        !addresses.is_empty(),
        "RELAYGATE_SESSION_LOAD_GATEWAYS must contain at least one address"
    );
    Ok(addresses)
}

fn positive_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(value > 0, "{name} must be greater than zero");
    Ok(value)
}

fn positive_u64(name: &str, default: u64) -> anyhow::Result<u64> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(value > 0, "{name} must be greater than zero");
    Ok(value)
}

fn percentage(name: &str, default: u8) -> anyhow::Result<u8> {
    let raw = env::var(name).unwrap_or_else(|_| default.to_string());
    let value = raw
        .parse::<u8>()
        .with_context(|| format!("{name} must be an integer in 0..=100"))?;
    ensure!(value <= 100, "{name} must be in 0..=100");
    Ok(value)
}

fn active_session_count(sessions: usize, percentage: u8) -> usize {
    if percentage == 0 {
        return 0;
    }
    sessions
        .saturating_mul(usize::from(percentage))
        .div_ceil(100)
}

fn load_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(17)) % 256) as u8)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_accept_one_or_more_non_empty_entries() -> anyhow::Result<()> {
        assert_eq!(address_list("gw-a:1")?, ["gw-a:1"]);
        assert_eq!(address_list("gw-a:1, gw-b:2")?, ["gw-a:1", "gw-b:2"]);
        assert!(address_list(" , ").is_err());
        Ok(())
    }

    #[test]
    fn active_count_rounds_up_without_exceeding_sessions() {
        assert_eq!(active_session_count(1_000, 0), 0);
        assert_eq!(active_session_count(1_000, 1), 10);
        assert_eq!(active_session_count(101, 1), 2);
        assert_eq!(active_session_count(10, 100), 10);
    }

    #[test]
    fn traffic_accounting_requires_one_terminal_result_per_attempt() {
        assert!(
            TrafficAccounting {
                attempts: 3,
                succeeded: 2,
                failed: 1,
                payload_bytes: 2_048,
            }
            .is_exact()
        );
        assert!(
            !TrafficAccounting {
                attempts: 4,
                succeeded: 2,
                failed: 1,
                payload_bytes: 2_048,
            }
            .is_exact()
        );
    }
}
