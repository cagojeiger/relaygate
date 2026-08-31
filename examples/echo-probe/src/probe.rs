use std::time::{Duration, Instant};

use anyhow::{Context, bail, ensure};
use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation, Pipe};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
    time::timeout,
};

use crate::config::{
    CLIENT_IDS, CONCURRENT_PIPES_PER_PATH, ECHO_DEADLINE, ROUTE_WAIT, SHARED_CLIENT_ID,
    environment, gateway_addresses,
};

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
            spawn_echo(
                &mut cross_dial,
                connector.clone(),
                (*client_id).to_owned(),
                matrix_payload(entry, owner, 0),
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
        .await?;
    }

    for (entry, connector) in connectors.iter().enumerate() {
        let payload = matrix_payload(entry, CLIENT_IDS.len(), 0);
        assert_echo(
            open_when_registered(connector, SHARED_CLIENT_ID, ROUTE_WAIT).await?,
            &payload,
        )
        .await?;
    }

    for (entry, connector) in connectors.iter().enumerate() {
        for (owner, client_id) in CLIENT_IDS.iter().enumerate() {
            let boundary = deterministic_payload(65_537, entry * 100 + owner);
            assert_echo(
                open_when_registered(connector, client_id, ROUTE_WAIT).await?,
                &boundary,
            )
            .await?;
            assert_concurrent_path(connector, client_id, entry, owner).await?;
        }
    }

    for connector in connectors {
        connector.close();
    }
    println!(
        "relaygate RT1/GW3 matrix verified: 3 local, 6 directed remote, N:M shared, {} concurrent Pipes per path",
        CONCURRENT_PIPES_PER_PATH
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

pub(crate) async fn expect_route_table_unavailable() -> anyhow::Result<()> {
    let addresses = gateway_addresses()?;
    let connectors = connect_all(&addresses).await?;

    for index in 0..connectors.len() {
        let payload = matrix_payload(index, index, 1);
        assert_echo(
            open_when_registered(&connectors[index], CLIENT_IDS[index], ROUTE_WAIT).await?,
            &payload,
        )
        .await
        .with_context(|| format!("local path {index}->{index} failed while RouteTable was down"))?;

        for (owner, client_id) in CLIENT_IDS.iter().enumerate() {
            if owner != index {
                assert_new_remote_open_unavailable(&connectors[index], client_id)
                    .await
                    .with_context(|| {
                        format!("remote path {index}->{owner} did not fail at the RT boundary")
                    })?;
            }
        }
    }

    for connector in connectors {
        connector.close();
    }
    println!("RouteTable outage verified: local paths live, 6 new remote opens unavailable");
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

fn spawn_echo(
    operations: &mut JoinSet<anyhow::Result<()>>,
    connector: Connector,
    client_id: String,
    payload: Vec<u8>,
) {
    operations.spawn(async move {
        let pipe = open_when_registered(&connector, &client_id, ROUTE_WAIT).await?;
        assert_echo(pipe, &payload).await
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
        spawn_echo(
            &mut operations,
            connector.clone(),
            client_id.to_owned(),
            deterministic_payload(
                4096 + sequence * 257,
                entry * 10_000 + owner * 100 + sequence,
            ),
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
