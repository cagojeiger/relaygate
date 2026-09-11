use std::{env, time::Instant};

use anyhow::{Context, ensure};
use relaygate_sdk::{Destination, Pipe};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use crate::{
    config::{
        DESTINATION_WAIT, DESTINATIONS, ECHO_DEADLINE, access_token_source, destination,
        gateway_addresses,
    },
    probe::{connect, dial_when_available},
};

pub(crate) async fn run() -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let warmup = setting("RELAYGATE_LATENCY_WARMUP", 100, 0, 10_000)?;
    let samples = setting("RELAYGATE_LATENCY_SAMPLES", 1_000, 1, 100_000)?;
    let payload_bytes = setting("RELAYGATE_LATENCY_PAYLOAD_BYTES", 64, 1, 65_536)?;
    let single = env::var("RELAYGATE_ADDR").ok();
    let addresses = match &single {
        Some(address) => vec![address.clone()],
        None => gateway_addresses()?,
    };
    let destinations = if single.is_some() {
        vec![destination()?]
    } else {
        DESTINATIONS
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    };
    let mut cases = Vec::new();
    let mut failed = false;
    for (entry, address) in addresses.iter().enumerate() {
        let setup = Instant::now();
        let relay = connect(address).await?;
        let setup_seconds = setup.elapsed().as_secs_f64();
        for (owner, destination) in destinations.iter().enumerate() {
            // Registration convergence is a preflight, outside the timed dial and DATA samples.
            drop(
                dial_when_available(&relay, destination, &access_token_source, DESTINATION_WAIT)
                    .await?,
            );
            let dial_started = Instant::now();
            let pipe = relay
                .dial(
                    destination.parse::<Destination>()?,
                    access_token_source.clone(),
                )
                .await?;
            let dial_seconds = dial_started.elapsed().as_secs_f64();
            let mut result = measure(pipe, warmup, samples, payload_bytes).await?;
            failed |= result["errors"].as_u64().unwrap_or(1) != 0;
            result["path"] = json!(if single.is_some() {
                "single_target"
            } else if entry == owner {
                "local"
            } else {
                "one_hop"
            });
            result["entry_index"] = json!(entry);
            result["owner_index"] = json!(owner);
            result["session_connect_seconds"] = json!(setup_seconds);
            result["dial_seconds"] = json!(dial_seconds);
            cases.push(result);
        }
        relay.close();
    }
    println!(
        "{}",
        json!({"probe": "established_pipe_rtt", "concurrency": 1, "cases": cases})
    );
    ensure!(
        !failed,
        "DATA latency probe had failed exchanges; see JSON results"
    );
    Ok(())
}

async fn measure(
    mut pipe: Pipe,
    warmup: usize,
    samples: usize,
    bytes: usize,
) -> anyhow::Result<Value> {
    let mut payload = vec![0x5a; bytes];
    let mut received = vec![0; bytes];
    for _ in 0..warmup {
        exchange(&mut pipe, &payload, &mut received).await?;
    }
    let started = Instant::now();
    let mut times = Vec::with_capacity(samples);
    let mut errors = 0;
    for sequence in 0..samples {
        // Different sequence content detects a stale echo, not just the byte count.
        for (index, byte) in payload.iter_mut().take(8).enumerate() {
            *byte = (sequence as u64).to_le_bytes()[index];
        }
        let round_trip = Instant::now();
        if exchange(&mut pipe, &payload, &mut received).await.is_err() {
            errors += 1;
            break;
        }
        times.push(round_trip.elapsed().as_secs_f64());
    }
    let elapsed = started.elapsed().as_secs_f64();
    times.sort_by(f64::total_cmp);
    Ok(json!({
        "payload_bytes": bytes, "warmup": warmup, "requested_samples": samples,
        "completed_samples": times.len(), "errors": errors,
        "rtt_seconds": {"p50": percentile(&times, 50), "p95": percentile(&times, 95), "p99": percentile(&times, 99), "max": times.last()},
        "measurement_seconds": elapsed,
        "echo_payload_bytes": times.len() * bytes * 2,
        "echo_goodput_bytes_per_second": (times.len() * bytes * 2) as f64 / elapsed,
    }))
}

async fn exchange(pipe: &mut Pipe, payload: &[u8], received: &mut [u8]) -> anyhow::Result<()> {
    timeout(ECHO_DEADLINE, async {
        pipe.write_all(payload).await?;
        pipe.read_exact(received).await?;
        ensure!(received == payload, "echo payload mismatch");
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("DATA RTT exchange timed out")?
}

fn percentile(sorted: &[f64], percent: usize) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    Some(sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)])
}

fn setting(name: &str, default: usize, min: usize, max: usize) -> anyhow::Result<usize> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        (min..=max).contains(&value),
        "{name} must be in {min}..={max}"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_percentiles_use_nearest_rank_and_preserve_empty_results() {
        let samples: Vec<_> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&samples, 50), Some(50.0));
        assert_eq!(percentile(&samples, 95), Some(95.0));
        assert_eq!(percentile(&samples, 99), Some(99.0));
        assert_eq!(percentile(&[], 99), None);
        assert_eq!(percentile(&[0.25], 99), Some(0.25));
    }
}
