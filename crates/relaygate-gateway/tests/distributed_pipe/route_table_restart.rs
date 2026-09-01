use std::{sync::Arc, time::Duration};

use relaygate_sdk::{
    Connector, ErrorCode as SdkErrorCode, ListenerRuntime, PeerObservation as SdkPeerObservation,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

use super::{
    CLIENT_ID, CLIENT_KEY, GATEWAY_A, GATEWAY_A_KEY, GATEWAY_B, GATEWAY_B_KEY, RunningGateway,
    RunningRouteTable, TestResult, one_shard_directory, sdk_config, wait_until,
};

#[path = "route_table_restart/proxy.rs"]
mod proxy;

use proxy::UpdateGateProxy;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_table_restart_window_preserves_local_and_existing_pipes_until_remote_republish()
-> TestResult {
    tokio::time::timeout(Duration::from_secs(10), route_table_restart_case()).await??;
    Ok(())
}

async fn route_table_restart_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = UpdateGateProxy::start(route_endpoint, GATEWAY_B).await?;
    let directory = relaygate_route_table::ShardDirectory::from_json_bytes(one_shard_directory(
        proxy.endpoint(),
    ))?;
    let restart_directory = directory.clone();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let mut gateway_a = RunningGateway::start(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory.clone(),
    )
    .await?;
    let mut gateway_b = RunningGateway::start(
        GATEWAY_B,
        GATEWAY_B_KEY,
        GATEWAY_A,
        GATEWAY_A_KEY,
        directory,
    )
    .await?;

    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener = Arc::new(listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?);
    wait_until(Duration::from_secs(2), || {
        gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;
    let remote_connector = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let local_connector = Connector::connect(sdk_config(gateway_b.sdk_address)).await?;

    let (mut existing_connector_pipe, mut existing_listener_pipe) =
        tokio::try_join!(remote_connector.open(CLIENT_ID), listener.accept(),)?;
    existing_connector_pipe.write_all(b"before restart").await?;
    let mut before_restart = [0_u8; 14];
    existing_listener_pipe
        .read_exact(&mut before_restart)
        .await?;
    assert_eq!(&before_restart, b"before restart");

    route_table.stop().await?;
    wait_until(Duration::from_secs(2), || {
        let snapshot = gateway_b.gateway.snapshot();
        snapshot.route_registrations_synced == 0 && snapshot.route_registrations_unsynced == 1
    })
    .await?;

    existing_listener_pipe.write_all(b"during loss").await?;
    let mut during_loss = [0_u8; 11];
    existing_connector_pipe.read_exact(&mut during_loss).await?;
    assert_eq!(&during_loss, b"during loss");

    proxy.arm();
    let restarted_listener = TcpListener::bind(route_endpoint).await?;
    let restarted_route_table = RunningRouteTable::start(restarted_listener, restart_directory)?;
    proxy.wait_until_update_blocked().await?;
    assert_eq!(proxy.blocked_updates(), 1);
    let blocked = gateway_b.gateway.snapshot();
    assert_eq!(blocked.route_registrations_synced, 0);
    assert_eq!(blocked.route_registrations_unsynced, 1);

    let (mut local_connector_pipe, mut local_listener_pipe) =
        tokio::try_join!(local_connector.open(CLIENT_ID), listener.accept(),)?;
    local_connector_pipe.write_all(b"local survives").await?;
    let mut local_payload = [0_u8; 14];
    local_listener_pipe.read_exact(&mut local_payload).await?;
    assert_eq!(&local_payload, b"local survives");
    local_connector_pipe.close().await?;
    local_listener_pipe.close().await?;

    wait_for_not_found(&remote_connector).await?;

    existing_connector_pipe
        .write_all(b"during empty rt")
        .await?;
    let mut during_empty = [0_u8; 15];
    existing_listener_pipe.read_exact(&mut during_empty).await?;
    assert_eq!(&during_empty, b"during empty rt");

    proxy.release();
    wait_until(Duration::from_secs(2), || {
        let snapshot = gateway_b.gateway.snapshot();
        snapshot.route_registrations_synced == 1 && snapshot.route_registrations_unsynced == 0
    })
    .await?;

    let (mut fresh_connector_pipe, mut fresh_listener_pipe) =
        tokio::try_join!(remote_connector.open(CLIENT_ID), listener.accept(),)?;
    fresh_connector_pipe.write_all(b"after republish").await?;
    let mut after_republish = [0_u8; 15];
    fresh_listener_pipe.read_exact(&mut after_republish).await?;
    assert_eq!(&after_republish, b"after republish");
    fresh_connector_pipe.close().await?;
    fresh_listener_pipe.close().await?;
    existing_connector_pipe.close().await?;
    existing_listener_pipe.close().await?;

    assert_eq!(proxy.blocked_updates(), 1);
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    remote_connector.close();
    local_connector.close();
    listener_runtime.close();
    restarted_route_table.stop().await?;
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    proxy.stop().await?;
    Ok(())
}

async fn wait_for_not_found(connector: &Connector) -> TestResult {
    let expires = Instant::now() + Duration::from_secs(2);
    loop {
        match connector.open(CLIENT_ID).await {
            Err(error)
                if error.code() == SdkErrorCode::NotFound
                    && error.observation() == SdkPeerObservation::NotObserved =>
            {
                return Ok(());
            }
            Err(error)
                if error.code() == SdkErrorCode::Unavailable
                    && error.observation() == SdkPeerObservation::NotObserved
                    && Instant::now() < expires =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
            Ok(mut pipe) => {
                pipe.close().await?;
                return Err(
                    "remote OPEN unexpectedly succeeded before RouteTable republish".into(),
                );
            }
        }
    }
}
