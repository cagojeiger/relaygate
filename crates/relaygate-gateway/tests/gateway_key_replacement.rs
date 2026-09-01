#[allow(dead_code)]
mod support;

use std::{io, net::SocketAddr, time::Duration};

use relaygate_gateway::GatewayConfig;
use relaygate_sdk::{Config, Connector, ErrorCode, Listener, ListenerRuntime, ListenerStatus};
use tokio::time::{Instant, sleep, timeout};

use support::{TestGateway, TestResult};

const ALPHA_ID: &str = "echo.alpha";
const BETA_ID: &str = "echo.beta";
const OLD_KEY: &str = "old-key";
const NEW_KEY: &str = "new-key";
const STABLE_KEY: &str = "stable-key";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_gateway_key_change_blocks_only_affected_listener() -> TestResult {
    timeout(Duration::from_secs(6), replacement_key_case()).await??;
    Ok(())
}

async fn replacement_key_case() -> TestResult {
    let first = TestGateway::start(&[(ALPHA_ID, OLD_KEY), (BETA_ID, STABLE_KEY)]).await?;
    let address = first.address;
    let runtime = ListenerRuntime::connect(sdk_config(address)).await?;
    let alpha = runtime.listen(ALPHA_ID, OLD_KEY).await?;
    let beta = runtime.listen(BETA_ID, STABLE_KEY).await?;
    wait_until("first Gateway owns both Listener bindings", || {
        let snapshot = first.snapshot();
        alpha.status() == ListenerStatus::Active
            && beta.status() == ListenerStatus::Active
            && snapshot.listener_sessions == 1
            && snapshot.listener_bindings == 2
    })
    .await?;

    let first_connector = connector(address).await?;
    assert_round_trip(&first_connector, &alpha, ALPHA_ID).await?;
    assert_round_trip(&first_connector, &beta, BETA_ID).await?;
    first_connector.close();
    first.stop().await?;
    wait_until("returned Listeners observe the stopped Gateway", || {
        alpha.status() == ListenerStatus::Suspended && beta.status() == ListenerStatus::Suspended
    })
    .await?;

    let replacement = TestGateway::start_on(
        GatewayConfig::new([
            (ALPHA_ID.to_owned(), NEW_KEY.to_owned()),
            (BETA_ID.to_owned(), STABLE_KEY.to_owned()),
        ]),
        address,
    )
    .await?;
    wait_until("only the stale-key Listener is blocked", || {
        let snapshot = replacement.snapshot();
        alpha.status() == ListenerStatus::Blocked
            && beta.status() == ListenerStatus::Active
            && snapshot.listener_sessions == 1
            && snapshot.listener_bindings == 1
    })
    .await?;

    let alpha_error = alpha
        .accept()
        .await
        .err()
        .ok_or_else(|| io::Error::other("stale-key Listener unexpectedly accepted a Pipe"))?;
    assert_eq!(alpha_error.code(), ErrorCode::Unauthenticated);

    sleep(Duration::from_millis(100)).await;
    let stable_snapshot = replacement.snapshot();
    assert_eq!(alpha.status(), ListenerStatus::Blocked);
    assert_eq!(beta.status(), ListenerStatus::Active);
    assert_eq!(stable_snapshot.listener_sessions, 1);
    assert_eq!(stable_snapshot.listener_bindings, 1);

    let replacement_connector = connector(address).await?;
    let missing_alpha = replacement_connector
        .open(ALPHA_ID)
        .await
        .err()
        .ok_or_else(|| io::Error::other("blocked Listener unexpectedly created a binding"))?;
    assert_eq!(missing_alpha.code(), ErrorCode::NotFound);
    assert_round_trip(&replacement_connector, &beta, BETA_ID).await?;

    alpha.close().await?;
    assert_eq!(alpha.status(), ListenerStatus::Closed);
    let new_alpha = runtime.listen(ALPHA_ID, NEW_KEY).await?;
    wait_until(
        "application installs alpha with the replacement key",
        || {
            let snapshot = replacement.snapshot();
            new_alpha.status() == ListenerStatus::Active
                && beta.status() == ListenerStatus::Active
                && snapshot.listener_sessions == 1
                && snapshot.listener_bindings == 2
        },
    )
    .await?;
    assert_round_trip(&replacement_connector, &new_alpha, ALPHA_ID).await?;
    assert_round_trip(&replacement_connector, &beta, BETA_ID).await?;

    replacement_connector.close();
    runtime.close();
    wait_until("replacement Gateway releases the ListenerSession", || {
        let snapshot = replacement.snapshot();
        snapshot.listener_sessions == 0
            && snapshot.listener_bindings == 0
            && snapshot.live_pipes == 0
    })
    .await?;
    replacement.stop().await?;
    Ok(())
}

fn sdk_config(address: SocketAddr) -> Config {
    Config::new(address.to_string())
        .with_connect_timeout(Duration::from_millis(200))
        .with_operation_timeout(Duration::from_millis(500))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
        .with_heartbeat(Duration::from_secs(5), Duration::from_secs(5))
}

async fn connector(address: SocketAddr) -> TestResult<Connector> {
    Ok(Connector::connect(sdk_config(address)).await?)
}

async fn assert_round_trip(
    connector: &Connector,
    listener: &Listener,
    client_id: &str,
) -> TestResult {
    let mut opened = connector.open(client_id).await?;
    let mut accepted = timeout(Duration::from_secs(1), listener.accept()).await??;
    opened.write_all_bytes(b"round-trip").await?;
    let mut payload = [0_u8; 10];
    accepted.read_into(&mut payload).await?;
    assert_eq!(&payload, b"round-trip");
    opened.close().await?;
    accepted.close().await?;
    Ok(())
}

async fn wait_until(label: &'static str, mut condition: impl FnMut() -> bool) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {label}").into());
        }
        sleep(Duration::from_millis(10)).await;
    }
}
