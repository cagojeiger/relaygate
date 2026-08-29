use std::time::{Duration, Instant};

use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation, Pipe};
use tokio::{task::JoinSet, time::timeout};

const CONCURRENT_PIPES: usize = 32;
const ECHO_DEADLINE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let client_id = environment("RELAYGATE_CLIENT_ID", "echo.alpha");
    let connector = Connector::connect(
        Config::new(address)
            .with_connect_timeout(Duration::from_secs(2))
            .with_operation_timeout(Duration::from_secs(3)),
    )
    .await?;

    timeout(
        ECHO_DEADLINE,
        assert_echo(
            open_when_registered(&connector, &client_id, Duration::from_secs(10)).await?,
            b"hello relaygate",
        ),
    )
    .await??;

    let binary = deterministic_payload(65_537);
    timeout(
        ECHO_DEADLINE,
        assert_echo(
            open_when_registered(&connector, &client_id, Duration::from_secs(3)).await?,
            &binary,
        ),
    )
    .await??;

    assert_concurrent_echo(&connector, &client_id).await?;

    connector.close();
    println!("relaygate single-Gateway echo verified");
    Ok(())
}

async fn assert_concurrent_echo(connector: &Connector, client_id: &str) -> anyhow::Result<()> {
    let mut operations = JoinSet::new();
    for index in 0..CONCURRENT_PIPES {
        let connector = connector.clone();
        let client_id = client_id.to_owned();
        operations.spawn(async move {
            let mut payload = deterministic_payload(4096 + index * 257);
            payload[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let pipe = open_when_registered(&connector, &client_id, Duration::from_secs(3)).await?;
            timeout(ECHO_DEADLINE, assert_echo(pipe, &payload)).await??;
            Ok::<_, anyhow::Error>(())
        });
    }

    while let Some(result) = operations.join_next().await {
        result??;
    }
    Ok(())
}

async fn open_when_registered(
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

async fn assert_echo(mut pipe: Pipe, payload: &[u8]) -> anyhow::Result<()> {
    pipe.write_all(payload).await?;
    pipe.shutdown_write().await?;

    let mut received = Vec::with_capacity(payload.len());
    let mut buffer = [0_u8; 8192];
    loop {
        let count = pipe.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        received.extend_from_slice(&buffer[..count]);
        if received.len() > payload.len() {
            anyhow::bail!("echo returned more bytes than sent");
        }
    }
    if received != payload {
        anyhow::bail!(
            "echo mismatch: sent {} bytes, received {} bytes",
            payload.len(),
            received.len()
        );
    }
    pipe.close().await?;
    Ok(())
}

fn deterministic_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(17)) % 256) as u8)
        .collect()
}

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
