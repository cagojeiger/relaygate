use std::time::Duration;

use relaygate_route_table::{ClientId, GatewayLocator, ShardDirectoryGeneration};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig,
    RouteTableServiceConfig,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    registry::Binding,
    routing::{GatewayRoutingConfig, RoutingRuntime},
};

use super::{
    TestResult, client_for_shard, clients_by_shard, connect_lease_client, gateway,
    protocol_binding, protocol_session, spawn_service, two_live_shard_directory, wait_for_counts,
    wait_for_resolve,
};

mod proxy;

use proxy::KeepAliveResponsePartition;

#[tokio::test]
async fn keep_alive_response_loss_isolated_to_shard_and_recovers_current_desired_state()
-> TestResult {
    timeout(Duration::from_secs(8), keep_alive_partition_case()).await??;
    Ok(())
}

async fn keep_alive_partition_case() -> TestResult {
    let listener_0 = TcpListener::bind("127.0.0.1:0").await?;
    let target_0 = listener_0.local_addr()?;
    let proxy = KeepAliveResponsePartition::start(target_0).await?;
    let listener_1 = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint_1 = listener_1.local_addr()?;
    let directory = two_live_shard_directory(proxy.endpoint(), endpoint_1)?;
    let gateway_id = gateway(4_000);
    let gateway_name = GatewayName::new("gw-keep-alive-partition")?;
    let gateway_key = InternalGatewayKey::new("keep-alive-partition-key")?;
    let service_config =
        RouteTableServiceConfig::new(32, 32, 8, 256 * 1024, Duration::from_secs(1))?;
    let lease_ttl = Duration::from_secs(1);

    let shutdown_0 = CancellationToken::new();
    let shutdown_1 = CancellationToken::new();
    let task_0 = spawn_service(
        listener_0,
        directory.clone(),
        "rt-0",
        lease_ttl,
        gateway_name.clone(),
        gateway_key.clone(),
        service_config,
        shutdown_0.clone(),
    )?;
    let task_1 = spawn_service(
        listener_1,
        directory.clone(),
        "rt-1",
        lease_ttl,
        gateway_name.clone(),
        gateway_key.clone(),
        service_config,
        shutdown_1.clone(),
    )?;

    let client_config = RouteTableClientConfig::new(
        32,
        256 * 1024,
        Duration::from_millis(100),
        Duration::from_millis(100),
        Duration::from_millis(100),
    )?;
    let routing_shutdown = CancellationToken::new();
    let runtime = RoutingRuntime::start(
        GatewayRoutingConfig::new(
            directory.clone(),
            gateway_name.clone(),
            gateway_key.clone(),
            GatewayLocator::new("gw-keep-alive-partition.internal:27431")?,
            client_config,
        )
        .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_desired_scan_interval(Duration::from_millis(5))
        .with_shutdown_timeout(Duration::from_millis(100)),
        gateway_id,
        routing_shutdown.clone(),
    )?;
    let handle = runtime.handle();
    let session_id = protocol_session(4_001);
    let unrelated_session_id = protocol_session(4_004);
    let clients = clients_by_shard(&directory)?;
    let client_0 = clients.get("rt-0").ok_or("missing rt-0 client")?;
    let client_1 = clients.get("rt-1").ok_or("missing rt-1 client")?;
    let unrelated_client_0 = client_for_shard(&directory, "rt-0", "unrelated")?;

    handle.publish_session(
        session_id,
        vec![
            Binding {
                id: protocol_binding(4_002),
                client_id: client_0.as_str().to_owned(),
                session_id,
            },
            Binding {
                id: protocol_binding(4_003),
                client_id: client_1.as_str().to_owned(),
                session_id,
            },
        ],
    )?;
    handle.publish_session(
        unrelated_session_id,
        vec![Binding {
            id: protocol_binding(4_005),
            client_id: unrelated_client_0.as_str().to_owned(),
            session_id: unrelated_session_id,
        }],
    )?;
    wait_for_resolve(&handle, client_0.clone()).await?;
    wait_for_resolve(&handle, client_1.clone()).await?;
    wait_for_resolve(&handle, unrelated_client_0.clone()).await?;
    wait_for_counts(&handle, 3, 0).await?;

    proxy.arm();
    proxy.wait_until_response_dropped().await?;
    wait_for_counts(&handle, 1, 2).await?;
    wait_for_resolve(&handle, client_1.clone()).await?;

    let direct_rt_0 = connect_lease_client(
        target_0,
        gateway_name,
        gateway_id,
        gateway_key,
        client_config,
    )
    .await?;
    assert_eq!(
        direct_rt_0
            .resolve(directory.generation(), client_0)
            .await?
            .entries()
            .len(),
        1
    );
    assert_eq!(
        direct_rt_0
            .resolve(directory.generation(), &unrelated_client_0)
            .await?
            .entries()
            .len(),
        1
    );
    wait_for_direct_not_found(&direct_rt_0, directory.generation(), client_0).await?;
    wait_for_direct_not_found(&direct_rt_0, directory.generation(), &unrelated_client_0).await?;

    proxy.release();
    wait_for_counts(&handle, 3, 0).await?;
    wait_for_resolve(&handle, client_0.clone()).await?;
    wait_for_resolve(&handle, client_1.clone()).await?;
    wait_for_resolve(&handle, unrelated_client_0).await?;
    assert_eq!(proxy.dropped_responses(), 1);

    routing_shutdown.cancel();
    timeout(Duration::from_secs(2), runtime.wait()).await??;
    proxy.stop().await?;
    shutdown_0.cancel();
    shutdown_1.cancel();
    timeout(Duration::from_secs(2), task_0).await???;
    timeout(Duration::from_secs(2), task_1).await???;
    Ok(())
}

async fn wait_for_direct_not_found(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &ClientId,
) -> TestResult {
    for _ in 0..400 {
        match client.resolve(generation, client_id).await {
            Err(error) if error.code() == ErrorCode::NotFound => return Ok(()),
            Ok(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(error) => return Err(error.into()),
        }
    }
    Err(format!(
        "{} did not expire during the held partition",
        client_id.as_str()
    )
    .into())
}
