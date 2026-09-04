use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation, Pipe};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
    time::timeout,
};

use crate::config::{
    CLIENT_IDS, CONCURRENT_PIPES_PER_PATH, ECHO_DEADLINE, ROUTE_WAIT, SHARED_CLIENT_ID,
    environment, gateway_addresses, soak_concurrency, soak_duration, storm_pause, storm_sessions,
};

const STORM_PIPE_BATCH_SIZE: usize = 64;

pub(crate) async fn run_single() -> anyhow::Result<()> {
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let client_id = environment("RELAYGATE_CLIENT_ID", "echo.alpha");
    let connector = connect(&address).await?;

    assert_echo(
        open_when_registered(&connector, &client_id, ROUTE_WAIT).await?,
        b"hello relaygate",
    )
    .await?;

    let binary = deterministic_payload(65_537, 0);
    assert_echo(
        open_when_registered(&connector, &client_id, Duration::from_secs(3)).await?,
        &binary,
    )
    .await?;

    assert_concurrent_path(&connector, &client_id, 0, 0).await?;
    connector.close();
    println!("relaygate single-Gateway echo verified");
    Ok(())
}

pub(crate) async fn run_matrix() -> anyhow::Result<()> {
    let addresses = gateway_addresses()?;
    let connectors = connect_all(&addresses).await?;

    let mut cross_dial = JoinSet::new();
    for (entry, connector) in connectors.iter().enumerate() {
        for (owner, client_id) in CLIENT_IDS.iter().enumerate() {
            if entry == owner {
                continue;
            }
            let payload = matrix_payload(entry, owner, 0);
            let context = format!(
                "phase=cross-dial entry={entry} owner={owner} client_id={client_id} sequence=0 payload_len={}",
                payload.len()
            );
            spawn_echo(
                &mut cross_dial,
                connector.clone(),
                (*client_id).to_owned(),
                payload,
                context,
            );
        }
    }
    join_all(&mut cross_dial).await?;

    for index in 0..connectors.len() {
        let payload = matrix_payload(index, index, 0);
        assert_echo(
            open_when_registered(&connectors[index], CLIENT_IDS[index], ROUTE_WAIT).await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "phase=local entry={index} owner={index} client_id={} sequence=0 payload_len={}",
                CLIENT_IDS[index],
                payload.len()
            )
        })?;
    }

    for (entry, connector) in connectors.iter().enumerate() {
        let payload = matrix_payload(entry, CLIENT_IDS.len(), 0);
        assert_echo(
            open_when_registered(connector, SHARED_CLIENT_ID, ROUTE_WAIT).await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "phase=shared entry={entry} client_id={SHARED_CLIENT_ID} sequence=0 payload_len={}",
                payload.len()
            )
        })?;
    }

    for (entry, connector) in connectors.iter().enumerate() {
        for (owner, client_id) in CLIENT_IDS.iter().enumerate() {
            let boundary = deterministic_payload(65_537, entry * 100 + owner);
            assert_echo(
                open_when_registered(connector, client_id, ROUTE_WAIT).await?,
                &boundary,
            )
            .await
            .with_context(|| {
                format!(
                    "phase=boundary entry={entry} owner={owner} client_id={client_id} sequence=0 payload_len={}",
                    boundary.len()
                )
            })?;
            assert_concurrent_path(connector, client_id, entry, owner).await?;
        }
    }

    for connector in connectors {
        connector.close();
    }
    println!(
        "relaygate GW3 matrix verified: 3 local, 6 directed remote, N:M shared, {} concurrent Pipes per path",
        CONCURRENT_PIPES_PER_PATH
    );
    Ok(())
}

pub(crate) async fn run_soak() -> anyhow::Result<()> {
    let addresses = gateway_addresses()?;
    let connectors = connect_all(&addresses).await?;
    let duration = soak_duration()?;
    let concurrency = soak_concurrency()?;
    let deadline = Instant::now() + duration;
    let completed = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();

    for worker in 0..concurrency {
        let connector = connectors[worker % connectors.len()].clone();
        let completed = Arc::clone(&completed);
        workers.spawn(async move {
            let mut sequence = 0_u64;
            while Instant::now() < deadline {
                let target = (worker + sequence as usize) % (CLIENT_IDS.len() + 1);
                let client_id = if target == CLIENT_IDS.len() {
                    SHARED_CLIENT_ID
                } else {
                    CLIENT_IDS[target]
                };
                let payload = format!(
                    "relaygate soak worker={worker} sequence={sequence} client={client_id}"
                )
                .into_bytes();
                assert_echo(
                    open_when_registered(&connector, client_id, ROUTE_WAIT)
                        .await
                        .with_context(|| {
                            format!(
                                "phase=soak worker={worker} sequence={sequence} client_id={client_id}: OPEN failed"
                            )
                        })?,
                    &payload,
                )
                .await
                .with_context(|| {
                    format!(
                        "phase=soak worker={worker} sequence={sequence} client_id={client_id}: echo failed"
                    )
                })?;
                completed.fetch_add(1, Ordering::Relaxed);
                sequence += 1;
            }
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
    for connector in connectors {
        connector.close();
    }
    if let Some(error) = failure {
        return Err(error);
    }

    ensure!(
        worker_total == completed.load(Ordering::Relaxed),
        "soak completion accounting mismatch"
    );
    println!(
        "relaygate soak verified: {worker_total} Pipes completed in {}s with {concurrency} workers over {} long-lived Connector sessions",
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
    let client_id = environment("RELAYGATE_CLIENT_ID", CLIENT_IDS[0]);
    let session_count = storm_sessions()?;
    let pause = storm_pause()?;
    let connectors = connect_many(&address, session_count).await?;
    let marker_pipes = open_marker_pipes(&connectors, &client_id).await?;

    println!(
        "relaygate reconnect storm ready: {session_count} Connector sessions and marker Pipes; interrupt and restore the Gateway path within {}s",
        pause.as_secs()
    );
    await_marker_pipes_closed(marker_pipes, pause).await?;
    println!("relaygate reconnect storm observed all original Connector sessions close");

    let result = verify_connectors_in_batches(&connectors, &client_id).await;
    for connector in connectors {
        connector.close();
    }
    result?;

    println!(
        "relaygate reconnect storm verified: {session_count} Connector sessions opened a new Pipe after the recovery window"
    );
    Ok(())
}

pub(crate) async fn wait_client_registered(client_id: &str) -> anyhow::Result<()> {
    let connectors = connect_all(&gateway_addresses()?).await?;
    for (entry, connector) in connectors.iter().enumerate() {
        let payload =
            format!("relaygate wait-client entry={entry} client={client_id}").into_bytes();
        assert_echo(
            open_when_registered(connector, client_id, ROUTE_WAIT).await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!("client {client_id:?} did not converge from gateway entry {entry}")
        })?;
    }
    for connector in connectors {
        connector.close();
    }
    println!("relaygate client {client_id:?} converged from all Gateway entries");
    Ok(())
}

pub(crate) async fn expect_shard_isolation(
    unavailable_client_id: &str,
    local_owner_index: usize,
    available_client_id: &str,
) -> anyhow::Result<()> {
    let addresses = gateway_addresses()?;
    let connectors = connect_all(&addresses).await?;
    ensure!(
        local_owner_index < connectors.len(),
        "local owner index {local_owner_index} is outside the configured Gateway range 0..{}",
        connectors.len()
    );

    let local_payload = format!(
        "relaygate shard-isolation local owner={local_owner_index} client={unavailable_client_id}"
    )
    .into_bytes();
    assert_echo(
        open_when_registered(
            &connectors[local_owner_index],
            unavailable_client_id,
            ROUTE_WAIT,
        )
        .await?,
        &local_payload,
    )
    .await
    .with_context(|| {
        format!(
            "local owner path for {unavailable_client_id:?} failed at Gateway index {local_owner_index}"
        )
    })?;

    for (entry, connector) in connectors.iter().enumerate() {
        if entry != local_owner_index {
            assert_new_remote_open_unavailable(connector, unavailable_client_id)
                .await
                .with_context(|| {
                    format!(
                        "remote path entry={entry} client={unavailable_client_id:?} did not fail at the unavailable shard boundary"
                    )
                })?;
        }
    }

    for (entry, connector) in connectors.iter().enumerate() {
        let payload =
            format!("relaygate shard-isolation healthy entry={entry} client={available_client_id}")
                .into_bytes();
        assert_echo(
            open_when_registered(connector, available_client_id, ROUTE_WAIT).await?,
            &payload,
        )
        .await
        .with_context(|| {
            format!(
                "healthy shard path failed from Gateway index {entry} to {available_client_id:?}"
            )
        })?;
    }

    for connector in connectors {
        connector.close();
    }
    println!(
        "RouteTable shard isolation verified: client {unavailable_client_id:?} stayed local-only at Gateway index {local_owner_index}; client {available_client_id:?} remained reachable from all Gateways"
    );
    Ok(())
}

pub(crate) async fn connect(address: &str) -> anyhow::Result<Connector> {
    Connector::connect(
        Config::new(address)
            .with_connect_timeout(Duration::from_secs(2))
            .with_operation_timeout(Duration::from_secs(3)),
    )
    .await
    .with_context(|| format!("failed to connect Connector SDK to {address}"))
}

pub(crate) async fn open_when_registered(
    connector: &Connector,
    client_id: &str,
    wait: Duration,
) -> relaygate_sdk::Result<Pipe> {
    let deadline = Instant::now() + wait;
    loop {
        match connector.open(client_id).await {
            Ok(pipe) => return Ok(pipe),
            Err(error)
                if Instant::now() < deadline
                    && error.observation() == PeerObservation::NotObserved
                    && matches!(error.code(), ErrorCode::NotFound | ErrorCode::Unavailable) =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn connect_all(addresses: &[String]) -> anyhow::Result<Vec<Connector>> {
    let mut connectors = Vec::with_capacity(addresses.len());
    for address in addresses {
        connectors.push(connect(address).await?);
    }
    Ok(connectors)
}

async fn connect_many(address: &str, count: usize) -> anyhow::Result<Vec<Connector>> {
    let mut operations = JoinSet::new();
    for _ in 0..count {
        let address = address.to_owned();
        operations.spawn(async move {
            Connector::connect(
                Config::new(&address)
                    .with_connect_timeout(Duration::from_secs(30))
                    .with_operation_timeout(Duration::from_secs(30)),
            )
            .await
            .with_context(|| format!("failed to connect Connector SDK to {address}"))
        });
    }

    let mut connectors = Vec::with_capacity(count);
    while let Some(result) = operations.join_next().await {
        match result {
            Ok(Ok(connector)) => connectors.push(connector),
            Ok(Err(error)) => {
                operations.abort_all();
                while operations.join_next().await.is_some() {}
                for connector in connectors {
                    connector.close();
                }
                return Err(error);
            }
            Err(error) => {
                operations.abort_all();
                while operations.join_next().await.is_some() {}
                for connector in connectors {
                    connector.close();
                }
                return Err(anyhow::anyhow!("Connector task failed to join: {error}"));
            }
        }
    }
    Ok(connectors)
}

async fn open_marker_pipes(connectors: &[Connector], client_id: &str) -> anyhow::Result<Vec<Pipe>> {
    let mut pipes = Vec::with_capacity(connectors.len());
    for (batch, connectors) in connectors.chunks(STORM_PIPE_BATCH_SIZE).enumerate() {
        let mut operations = JoinSet::new();
        for (offset, connector) in connectors.iter().enumerate() {
            let index = batch * STORM_PIPE_BATCH_SIZE + offset;
            let connector = connector.clone();
            let client_id = client_id.to_owned();
            operations.spawn(async move {
                let mut pipe = open_when_registered(&connector, &client_id, ROUTE_WAIT).await?;
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

async fn verify_connectors_in_batches(
    connectors: &[Connector],
    client_id: &str,
) -> anyhow::Result<()> {
    for (batch, connectors) in connectors.chunks(STORM_PIPE_BATCH_SIZE).enumerate() {
        let mut operations = JoinSet::new();
        for (offset, connector) in connectors.iter().enumerate() {
            let index = batch * STORM_PIPE_BATCH_SIZE + offset;
            let payload = format!("relaygate reconnect storm session={index}").into_bytes();
            spawn_echo(
                &mut operations,
                connector.clone(),
                client_id.to_owned(),
                payload,
                format!("phase=reconnect-storm session={index} client_id={client_id}"),
            );
        }
        join_all(&mut operations).await?;
    }
    Ok(())
}

fn spawn_echo(
    operations: &mut JoinSet<anyhow::Result<()>>,
    connector: Connector,
    client_id: String,
    payload: Vec<u8>,
    context: String,
) {
    operations.spawn(async move {
        let pipe = open_when_registered(&connector, &client_id, ROUTE_WAIT)
            .await
            .with_context(|| format!("{context}: OPEN failed"))?;
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
    connector: &Connector,
    client_id: &str,
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
            "phase=concurrent entry={entry} owner={owner} client_id={client_id} sequence={sequence} payload_len={}",
            payload.len()
        );
        spawn_echo(
            &mut operations,
            connector.clone(),
            client_id.to_owned(),
            payload,
            context,
        );
    }
    join_all(&mut operations).await
}

async fn assert_new_remote_open_unavailable(
    connector: &Connector,
    client_id: &str,
) -> anyhow::Result<()> {
    match connector.open(client_id).await {
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

async fn assert_echo(mut pipe: Pipe, payload: &[u8]) -> anyhow::Result<()> {
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
