use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use relaygate_sdk::{AccessTokenSource, Destination, ErrorCode, PeerObservation, Pipe, Relay};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
    time::timeout,
};

use crate::config::{
    CONCURRENT_PIPES_PER_PATH, DESTINATION_WAIT, DESTINATIONS, ECHO_DEADLINE, SHARED_DESTINATION,
    access_token_source, destination, environment, gateway_addresses, sdk_config, soak_concurrency,
    soak_duration, storm_pause, storm_sessions,
};

const STORM_PIPE_BATCH_SIZE: usize = 32;

pub(crate) async fn run_single() -> anyhow::Result<()> {
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let destination = destination()?;
    let access_token_source = access_token_source()?;
    let dialer = connect(&address).await?;

    assert_echo(
        dial_when_available(
            &dialer,
            &destination,
            &access_token_source,
            DESTINATION_WAIT,
        )
        .await?,
        b"hello relaygate",
    )
    .await?;

    let binary = deterministic_payload(65_537, 0);
    assert_echo(
        dial_when_available(
            &dialer,
            &destination,
            &access_token_source,
            Duration::from_secs(3),
        )
        .await?,
        &binary,
    )
    .await?;

    assert_concurrent_path(&dialer, &destination, &access_token_source, 0, 0).await?;
    dialer.close();
    println!("relaygate single-Gateway echo verified");
    Ok(())
}

pub(crate) async fn run_matrix() -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let addresses = gateway_addresses()?;
    let dialers = connect_all(&addresses).await?;

    let mut cross_dial = JoinSet::new();
    for (entry, dialer) in dialers.iter().enumerate() {
        for (owner, destination) in DESTINATIONS.iter().enumerate() {
            if entry == owner {
                continue;
            }
            let payload = matrix_payload(entry, owner, 0);
            let context = format!(
                "phase=cross-dial entry={entry} owner={owner} destination={destination} sequence=0 payload_len={}",
                payload.len()
            );
            spawn_echo(
                &mut cross_dial,
                dialer.clone(),
                (*destination).to_owned(),
                access_token_source.clone(),
                payload,
                context,
            );
        }
    }
    join_all(&mut cross_dial).await?;

    for index in 0..dialers.len() {
        let payload = matrix_payload(index, index, 0);
        assert_echo(
            dial_when_available(
                &dialers[index],
                DESTINATIONS[index],
                &access_token_source,
                DESTINATION_WAIT,
            )
            .await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "phase=local entry={index} owner={index} destination={} sequence=0 payload_len={}",
                DESTINATIONS[index],
                payload.len()
            )
        })?;
    }

    for (entry, dialer) in dialers.iter().enumerate() {
        let payload = matrix_payload(entry, DESTINATIONS.len(), 0);
        assert_echo(
            dial_when_available(
                dialer,
                SHARED_DESTINATION,
                &access_token_source,
                DESTINATION_WAIT,
            )
            .await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "phase=shared entry={entry} destination={SHARED_DESTINATION} sequence=0 payload_len={}",
                payload.len()
            )
        })?;
    }

    for (entry, dialer) in dialers.iter().enumerate() {
        for (owner, destination) in DESTINATIONS.iter().enumerate() {
            let boundary = deterministic_payload(65_537, entry * 100 + owner);
            assert_echo(
                dial_when_available(
                    dialer,
                    destination,
                    &access_token_source,
                    DESTINATION_WAIT,
                )
                .await?,
                &boundary,
            )
            .await
            .with_context(|| {
                format!(
                    "phase=boundary entry={entry} owner={owner} destination={destination} sequence=0 payload_len={}",
                    boundary.len()
                )
            })?;
            assert_concurrent_path(dialer, destination, &access_token_source, entry, owner).await?;
        }
    }

    for dialer in dialers {
        dialer.close();
    }
    println!(
        "relaygate GW3 matrix verified: 3 local, 6 directed remote, N:M shared, {} concurrent Pipes per path",
        CONCURRENT_PIPES_PER_PATH
    );
    Ok(())
}

pub(crate) async fn run_soak() -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let addresses = gateway_addresses()?;
    let dialers = connect_all(&addresses).await?;
    let duration = soak_duration()?;
    let concurrency = soak_concurrency()?;
    let deadline = Instant::now() + duration;
    let completed = Arc::new(AtomicU64::new(0));
    let admission_rejections = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();

    for worker in 0..concurrency {
        let dialer = dialers[worker % dialers.len()].clone();
        let access_token_source = access_token_source.clone();
        let completed = Arc::clone(&completed);
        let admission_rejections = Arc::clone(&admission_rejections);
        workers.spawn(async move {
            let mut sequence = 0_u64;
            while Instant::now() < deadline {
                let target = (worker + sequence as usize) % (DESTINATIONS.len() + 1);
                let destination = if target == DESTINATIONS.len() {
                    SHARED_DESTINATION
                } else {
                    DESTINATIONS[target]
                };
                let payload = format!(
                    "relaygate soak worker={worker} sequence={sequence} destination={destination}"
                )
                .into_bytes();
                assert_echo(
                    crate::soak_dial::dial(
                        &dialer,
                        destination,
                        &access_token_source,
                        DESTINATION_WAIT,
                        &admission_rejections,
                    )
                        .await
                        .with_context(|| {
                            format!(
                                "phase=soak worker={worker} sequence={sequence} destination={destination}: dial failed"
                            )
                        })?,
                    &payload,
                )
                .await
                .with_context(|| {
                    format!(
                        "phase=soak worker={worker} sequence={sequence} destination={destination}: echo failed"
                    )
                })?;
                completed.fetch_add(1, Ordering::Relaxed);
                sequence += 1;
            }
            ensure!(sequence > 0, "soak worker={worker} made no progress");
            Ok::<_, anyhow::Error>(sequence)
        });
    }

    let progress = Arc::clone(&completed);
    let reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.tick().await;
        while Instant::now() < deadline {
            interval.tick().await;
            println!(
                "relaygate soak progress: {} Pipes completed",
                progress.load(Ordering::Relaxed)
            );
        }
    });

    let mut worker_total = 0_u64;
    let mut failure = None;
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(count)) => worker_total += count,
            Ok(Err(error)) => {
                failure = Some(error);
                break;
            }
            Err(error) => {
                failure = Some(anyhow::anyhow!("soak worker failed to join: {error}"));
                break;
            }
        }
    }

    if failure.is_some() {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    reporter.abort();
    let _ = reporter.await;
    for dialer in dialers {
        dialer.close();
    }
    println!(
        "relaygate soak admission_rejections={}",
        admission_rejections.load(Ordering::Relaxed)
    );
    if let Some(error) = failure {
        return Err(error);
    }

    ensure!(
        worker_total == completed.load(Ordering::Relaxed),
        "soak completion accounting mismatch"
    );
    println!(
        "relaygate soak verified: {worker_total} Pipes completed in {}s with {concurrency} workers over {} long-lived Relay sessions",
        duration.as_secs(),
        addresses.len()
    );
    Ok(())
}

pub(crate) async fn run_reconnect_storm() -> anyhow::Result<()> {
    let default_address = gateway_addresses()?
        .into_iter()
        .next()
        .context("at least one Gateway address is required")?;
    let address = environment("RELAYGATE_ADDR", &default_address);
    let destination = destination()?;
    let access_token_source = access_token_source()?;
    let session_count = storm_sessions()?;
    let pause = storm_pause()?;
    let dialers = connect_many(&address, session_count).await?;
    let marker_pipes = open_marker_pipes(&dialers, &destination, &access_token_source).await?;

    println!(
        "relaygate reconnect storm ready: {session_count} Relay sessions and marker Pipes; interrupt and restore the Gateway path within {}s",
        pause.as_secs()
    );
    await_marker_pipes_closed(marker_pipes, pause).await?;
    println!("relaygate reconnect storm observed all original Relay sessions close");

    let result = verify_dialers_in_batches(&dialers, &destination, &access_token_source).await;
    for dialer in dialers {
        dialer.close();
    }
    result?;

    println!(
        "relaygate reconnect storm verified: {session_count} Relay sessions opened a new Pipe after the recovery window"
    );
    Ok(())
}

pub(crate) async fn wait_destination_available(destination: &str) -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let dialers = connect_all(&gateway_addresses()?).await?;
    for (entry, dialer) in dialers.iter().enumerate() {
        let payload = format!("relaygate wait-destination entry={entry} destination={destination}")
            .into_bytes();
        assert_echo(
            dial_when_available(dialer, destination, &access_token_source, DESTINATION_WAIT)
                .await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!("destination {destination:?} did not converge from gateway entry {entry}")
        })?;
    }
    for dialer in dialers {
        dialer.close();
    }
    println!("relaygate destination {destination:?} converged from all Gateway entries");
    Ok(())
}

pub(crate) async fn expect_shard_isolation(
    unavailable_destination: &str,
    local_owner_index: usize,
    available_destination: &str,
) -> anyhow::Result<()> {
    let access_token_source = access_token_source()?;
    let addresses = gateway_addresses()?;
    let dialers = connect_all(&addresses).await?;
    ensure!(
        local_owner_index < dialers.len(),
        "local owner index {local_owner_index} is outside the configured Gateway range 0..{}",
        dialers.len()
    );

    let local_payload = format!(
        "relaygate shard-isolation local owner={local_owner_index} destination={unavailable_destination}"
    )
    .into_bytes();
    assert_echo(
        dial_when_available(
            &dialers[local_owner_index],
            unavailable_destination,
            &access_token_source,
            DESTINATION_WAIT,
        )
        .await?,
        &local_payload,
    )
    .await
    .with_context(|| {
        format!(
            "local owner path for {unavailable_destination:?} failed at Gateway index {local_owner_index}"
        )
    })?;

    for (entry, dialer) in dialers.iter().enumerate() {
        if entry != local_owner_index {
            assert_new_remote_open_unavailable(
                dialer,
                unavailable_destination,
                &access_token_source,
            )
                .await
                .with_context(|| {
                    format!(
                        "remote path entry={entry} destination={unavailable_destination:?} did not fail at the unavailable shard boundary"
                    )
                })?;
        }
    }

    for (entry, dialer) in dialers.iter().enumerate() {
        let payload = format!(
            "relaygate shard-isolation healthy entry={entry} destination={available_destination}"
        )
        .into_bytes();
        assert_echo(
            dial_when_available(
                dialer,
                available_destination,
                &access_token_source,
                DESTINATION_WAIT,
            )
            .await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "healthy shard path failed from Gateway index {entry} to {available_destination:?}"
            )
        })?;
    }

    for dialer in dialers {
        dialer.close();
    }
    println!(
        "RouteTable shard isolation verified: destination {unavailable_destination:?} stayed local-only at Gateway index {local_owner_index}; destination {available_destination:?} remained reachable from all Gateways"
    );
    Ok(())
}

pub(crate) async fn connect(address: &str) -> anyhow::Result<Relay> {
    Relay::connect(
        sdk_config(address)?
            .with_connect_timeout(Duration::from_secs(2))
            .with_operation_timeout(Duration::from_secs(3)),
    )
    .await
    .with_context(|| format!("failed to connect Relay SDK to {address}"))
}

pub(crate) async fn dial_when_available(
    dialer: &Relay,
    destination: &str,
    access_token_source: &AccessTokenSource,
    wait: Duration,
) -> anyhow::Result<Pipe> {
    let destination: Destination = destination
        .parse()
        .with_context(|| format!("invalid Destination {destination:?}"))?;
    let deadline = Instant::now() + wait;
    loop {
        match dialer
            .dial(destination.clone(), access_token_source.clone())
            .await
        {
            Ok(pipe) => return Ok(pipe),
            Err(error)
                if Instant::now() < deadline
                    && error.observation() == PeerObservation::NotObserved
                    && matches!(error.code(), ErrorCode::NotFound | ErrorCode::Unavailable) =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn connect_all(addresses: &[String]) -> anyhow::Result<Vec<Relay>> {
    let mut dialers = Vec::with_capacity(addresses.len());
    for address in addresses {
        dialers.push(connect(address).await?);
    }
    Ok(dialers)
}

async fn connect_many(address: &str, count: usize) -> anyhow::Result<Vec<Relay>> {
    let mut operations = JoinSet::new();
    for _ in 0..count {
        let address = address.to_owned();
        operations.spawn(async move {
            Relay::connect(
                sdk_config(&address)?
                    .with_connect_timeout(Duration::from_secs(30))
                    .with_operation_timeout(Duration::from_secs(30)),
            )
            .await
            .with_context(|| format!("failed to connect Relay SDK to {address}"))
        });
    }

    let mut dialers = Vec::with_capacity(count);
    while let Some(result) = operations.join_next().await {
        match result {
            Ok(Ok(dialer)) => dialers.push(dialer),
            Ok(Err(error)) => {
                operations.abort_all();
                while operations.join_next().await.is_some() {}
                for dialer in dialers {
                    dialer.close();
                }
                return Err(error);
            }
            Err(error) => {
                operations.abort_all();
                while operations.join_next().await.is_some() {}
                for dialer in dialers {
                    dialer.close();
                }
                return Err(anyhow::anyhow!("Relay task failed to join: {error}"));
            }
        }
    }
    Ok(dialers)
}

async fn open_marker_pipes(
    dialers: &[Relay],
    destination: &str,
    access_token_source: &AccessTokenSource,
) -> anyhow::Result<Vec<Pipe>> {
    let mut pipes = Vec::with_capacity(dialers.len());
    for (batch, dialers) in dialers.chunks(STORM_PIPE_BATCH_SIZE).enumerate() {
        let mut operations = JoinSet::new();
        for (offset, dialer) in dialers.iter().enumerate() {
            let index = batch * STORM_PIPE_BATCH_SIZE + offset;
            let dialer = dialer.clone();
            let destination = destination.to_owned();
            let access_token_source = access_token_source.clone();
            operations.spawn(async move {
                let mut pipe = dial_when_available(
                    &dialer,
                    &destination,
                    &access_token_source,
                    DESTINATION_WAIT,
                )
                .await?;
                let marker = format!("relaygate reconnect marker session={index}").into_bytes();
                timeout(ECHO_DEADLINE, async {
                    pipe.write_all(&marker).await?;
                    let mut echoed = vec![0_u8; marker.len()];
                    pipe.read_exact(&mut echoed).await?;
                    ensure!(echoed == marker, "marker echo mismatch for session {index}");
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .with_context(|| format!("marker echo timed out for session {index}"))??;
                Ok::<_, anyhow::Error>(pipe)
            });
        }
        while let Some(result) = operations.join_next().await {
            pipes.push(result.context("marker Pipe task failed to join")??);
        }
    }
    Ok(pipes)
}

async fn await_marker_pipes_closed(pipes: Vec<Pipe>, deadline: Duration) -> anyhow::Result<()> {
    let mut operations = JoinSet::new();
    for (index, mut pipe) in pipes.into_iter().enumerate() {
        operations.spawn(async move {
            timeout(deadline, async {
                let mut unexpected = [0_u8; 1];
                match pipe.read(&mut unexpected).await {
                    Ok(0) | Err(_) => Ok(()),
                    Ok(count) => bail!(
                        "marker Pipe {index} received {count} unexpected bytes before disconnect"
                    ),
                }
            })
            .await
            .with_context(|| {
                format!("marker Pipe {index} did not close before the outage deadline")
            })?
        });
    }
    join_all(&mut operations).await
}

async fn verify_dialers_in_batches(
    dialers: &[Relay],
    destination: &str,
    access_token_source: &AccessTokenSource,
) -> anyhow::Result<()> {
    for (batch, dialers) in dialers.chunks(STORM_PIPE_BATCH_SIZE).enumerate() {
        let mut operations = JoinSet::new();
        for (offset, dialer) in dialers.iter().enumerate() {
            let index = batch * STORM_PIPE_BATCH_SIZE + offset;
            let payload = format!("relaygate reconnect storm session={index}").into_bytes();
            spawn_echo(
                &mut operations,
                dialer.clone(),
                destination.to_owned(),
                access_token_source.clone(),
                payload,
                format!("phase=reconnect-storm session={index} destination={destination}"),
            );
        }
        join_all(&mut operations).await?;
    }
    Ok(())
}

fn spawn_echo(
    operations: &mut JoinSet<anyhow::Result<()>>,
    dialer: Relay,
    destination: String,
    access_token_source: AccessTokenSource,
    payload: Vec<u8>,
    context: String,
) {
    operations.spawn(async move {
        let pipe = dial_when_available(
            &dialer,
            &destination,
            &access_token_source,
            DESTINATION_WAIT,
        )
        .await
        .with_context(|| format!("{context}: dial failed"))?;
        assert_echo(pipe, &payload)
            .await
            .with_context(|| format!("{context}: echo failed"))
    });
}

async fn join_all(operations: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    while let Some(result) = operations.join_next().await {
        result.context("echo task failed to join")??;
    }
    Ok(())
}

async fn assert_concurrent_path(
    dialer: &Relay,
    destination: &str,
    access_token_source: &AccessTokenSource,
    entry: usize,
    owner: usize,
) -> anyhow::Result<()> {
    let mut operations = JoinSet::new();
    for sequence in 0..CONCURRENT_PIPES_PER_PATH {
        let payload = deterministic_payload(
            4096 + sequence * 257,
            entry * 10_000 + owner * 100 + sequence,
        );
        let context = format!(
            "phase=concurrent entry={entry} owner={owner} destination={destination} sequence={sequence} payload_len={}",
            payload.len()
        );
        spawn_echo(
            &mut operations,
            dialer.clone(),
            destination.to_owned(),
            access_token_source.clone(),
            payload,
            context,
        );
    }
    join_all(&mut operations).await
}

async fn assert_new_remote_open_unavailable(
    dialer: &Relay,
    destination: &str,
    access_token_source: &AccessTokenSource,
) -> anyhow::Result<()> {
    let destination: Destination = destination
        .parse()
        .with_context(|| format!("invalid Destination {destination:?}"))?;
    match dialer.dial(destination, access_token_source.clone()).await {
        Ok(mut pipe) => {
            let _ = pipe.close().await;
            bail!("new remote open unexpectedly succeeded")
        }
        Err(error) => {
            ensure!(
                error.code() == ErrorCode::Unavailable
                    && error.observation() == PeerObservation::NotObserved,
                "expected UNAVAILABLE/NOT_OBSERVED, got {:?}/{:?}: {}",
                error.code(),
                error.observation(),
                error
            );
            Ok(())
        }
    }
}

pub(crate) async fn assert_echo(mut pipe: Pipe, payload: &[u8]) -> anyhow::Result<()> {
    timeout(ECHO_DEADLINE, async {
        pipe.write_all(payload).await?;
        pipe.shutdown().await?;

        let mut received = Vec::with_capacity(payload.len());
        let mut buffer = [0_u8; 8192];
        loop {
            let count = pipe.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            received.extend_from_slice(&buffer[..count]);
            if received.len() > payload.len() {
                bail!("echo returned more bytes than sent");
            }
        }
        ensure!(
            received == payload,
            "echo mismatch: sent {} bytes, received {} bytes",
            payload.len(),
            received.len()
        );
        pipe.close().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("echo operation timed out")?
}

fn matrix_payload(entry: usize, owner: usize, sequence: usize) -> Vec<u8> {
    format!("relaygate matrix entry={entry} owner={owner} sequence={sequence}").into_bytes()
}

fn deterministic_payload(length: usize, seed: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index.wrapping_add(seed).wrapping_mul(31).wrapping_add(17)) % 256) as u8)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_payload_changes_with_seed() {
        assert_ne!(deterministic_payload(128, 1), deterministic_payload(128, 2));
    }
}
