use std::{collections::BTreeMap, time::Duration};

use relaygate_route_table::{
    DestinationId, GatewayLocator, RegistrationKey, RegistrationRevision, RouteTableConfig,
    RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TrustedGatewayKeys,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{RouteDependencyHealth, registry::Binding, routing::GatewayRoutingConfig};

use super::{
    TestResult, gateway, protocol_binding, protocol_session, wait_for_not_found, wait_for_resolve,
};
use crate::routing::{RoutingHandle, RoutingRuntime, projection::project_session_id};

mod keep_alive_partition;

#[tokio::test]
async fn one_session_keeps_independent_lease_lifecycles_across_two_shards() -> TestResult {
    let listener_0 = TcpListener::bind("127.0.0.1:0").await?;
    let listener_1 = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint_0 = listener_0.local_addr()?;
    let endpoint_1 = listener_1.local_addr()?;
    let directory = two_live_shard_directory(endpoint_0, endpoint_1)?;
    let generation = directory.generation();
    let gateway_id = gateway(1_000);
    let gateway_name = GatewayName::new("gw-multi-shard")?;
    let gateway_key = InternalGatewayKey::new("multi-shard-key")?;
    let service_config =
        RouteTableServiceConfig::new(32, 32, 8, 256 * 1024, Duration::from_secs(1))?;
    let lease_ttl = Duration::from_secs(3);

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
        Duration::from_millis(200),
    )?;
    let routing_shutdown = CancellationToken::new();
    let runtime = RoutingRuntime::start(
        GatewayRoutingConfig::new(
            directory.clone(),
            gateway_name.clone(),
            gateway_key.clone(),
            GatewayLocator::new("gw-multi-shard.internal:27431")?,
            client_config,
        )
        .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_desired_scan_interval(Duration::from_millis(5))
        .with_shutdown_timeout(Duration::from_millis(200)),
        gateway_id,
        routing_shutdown.clone(),
    )?;
    let handle = runtime.handle();
    let session_id = protocol_session(2_000);
    let clients = clients_by_shard(&directory)?;
    let client_0 = clients.get("rt-0").ok_or("missing rt-0 client")?;
    let client_1 = clients.get("rt-1").ok_or("missing rt-1 client")?;

    handle.publish_session(
        session_id,
        vec![
            Binding {
                id: protocol_binding(3_000),
                destination_id: client_0.as_str().parse()?,
                session_id,
            },
            Binding {
                id: protocol_binding(3_001),
                destination_id: client_1.as_str().parse()?,
                session_id,
            },
        ],
    )?;
    wait_for_resolve(&handle, client_0.clone()).await?;
    wait_for_resolve(&handle, client_1.clone()).await?;
    wait_for_counts(&handle, 2, 0).await?;
    assert_eq!(
        handle.current_counts().dependency_health,
        RouteDependencyHealth::Ready
    );

    let lease_client_0 = connect_lease_client(
        endpoint_0,
        gateway_name.clone(),
        gateway_id,
        gateway_key.clone(),
        client_config,
    )
    .await?;
    let lease_client_1 = connect_lease_client(
        endpoint_1,
        gateway_name.clone(),
        gateway_id,
        gateway_key.clone(),
        client_config,
    )
    .await?;
    let relay_session_id = project_session_id(session_id);
    let key_0 = RegistrationKey::new(gateway_id, relay_session_id, ShardId::new("rt-0")?);
    let key_1 = RegistrationKey::new(gateway_id, relay_session_id, ShardId::new("rt-1")?);
    let first_0 = lease_client_0.register(generation, &key_0).await?;
    let first_1 = lease_client_1.register(generation, &key_1).await?;

    assert_eq!(
        first_0.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );
    assert_eq!(
        first_1.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    let replacement_0 = client_for_shard(&directory, "rt-0", "replacement")?;
    handle.publish_session(
        session_id,
        vec![
            Binding {
                id: protocol_binding(3_002),
                destination_id: replacement_0.as_str().parse()?,
                session_id,
            },
            Binding {
                id: protocol_binding(3_001),
                destination_id: client_1.as_str().parse()?,
                session_id,
            },
        ],
    )?;
    wait_for_resolve(&handle, replacement_0.clone()).await?;
    wait_for_not_found(&handle, client_0.clone()).await?;
    wait_for_counts(&handle, 2, 0).await?;

    let changed_0 = lease_client_0.register(generation, &key_0).await?;
    let unchanged_1 = lease_client_1.register(generation, &key_1).await?;
    assert_eq!(changed_0.lease_id(), first_0.lease_id());
    assert_eq!(unchanged_1.lease_id(), first_1.lease_id());
    assert_eq!(
        changed_0.accepted_revision(),
        Some(RegistrationRevision::new(2)?)
    );
    assert_eq!(
        unchanged_1.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    shutdown_0.cancel();
    timeout(Duration::from_secs(5), task_0).await???;
    wait_for_counts(&handle, 1, 1).await?;
    assert_eq!(
        handle.current_counts().dependency_health,
        RouteDependencyHealth::Degraded
    );
    wait_for_resolve(&handle, client_1.clone()).await?;

    handle.publish_session(session_id, Vec::new())?;
    wait_for_not_found(&handle, client_1.clone()).await?;
    wait_for_counts(&handle, 0, 0).await?;

    routing_shutdown.cancel();
    timeout(Duration::from_secs(5), runtime.wait()).await??;
    shutdown_1.cancel();
    timeout(Duration::from_secs(5), task_1).await???;
    Ok(())
}

#[tokio::test]
async fn terminal_shard_does_not_block_unaffected_shard_resolve() -> TestResult {
    let listener_0 = TcpListener::bind("127.0.0.1:0").await?;
    let listener_1 = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint_0 = listener_0.local_addr()?;
    let endpoint_1 = listener_1.local_addr()?;
    let directory = two_live_shard_directory(endpoint_0, endpoint_1)?;
    let gateway_id = gateway(4_000);
    let gateway_name = GatewayName::new("gw-terminal-shard")?;
    let gateway_key = InternalGatewayKey::new("correct-key")?;
    let service_config =
        RouteTableServiceConfig::new(32, 32, 8, 256 * 1024, Duration::from_secs(1))?;

    let shutdown_0 = CancellationToken::new();
    let shutdown_1 = CancellationToken::new();
    let task_0 = spawn_service(
        listener_0,
        directory.clone(),
        "rt-0",
        Duration::from_secs(3),
        gateway_name.clone(),
        InternalGatewayKey::new("wrong-key")?,
        service_config,
        shutdown_0.clone(),
    )?;
    let task_1 = spawn_service(
        listener_1,
        directory.clone(),
        "rt-1",
        Duration::from_secs(3),
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
        Duration::from_millis(200),
    )?;
    let routing_shutdown = CancellationToken::new();
    let runtime = RoutingRuntime::start(
        GatewayRoutingConfig::new(
            directory.clone(),
            gateway_name,
            gateway_key,
            GatewayLocator::new("gw-terminal-shard.internal:27431")?,
            client_config,
        )
        .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_desired_scan_interval(Duration::from_millis(5))
        .with_shutdown_timeout(Duration::from_millis(200)),
        gateway_id,
        routing_shutdown.clone(),
    )?;
    let handle = runtime.handle();
    let healthy_client = client_for_shard(&directory, "rt-1", "healthy")?;
    let session_id = protocol_session(5_000);

    handle.publish_session(
        session_id,
        vec![Binding {
            id: protocol_binding(6_000),
            destination_id: healthy_client.as_str().parse()?,
            session_id,
        }],
    )?;
    wait_for_resolve(&handle, healthy_client.clone()).await?;
    wait_for_health(&handle, RouteDependencyHealth::Terminal).await?;

    let resolved = handle.resolve(healthy_client).await?;
    assert_eq!(resolved.len(), 1);

    routing_shutdown.cancel();
    timeout(Duration::from_secs(5), runtime.wait()).await??;
    shutdown_0.cancel();
    shutdown_1.cancel();
    timeout(Duration::from_secs(5), task_0).await???;
    timeout(Duration::from_secs(5), task_1).await???;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_service(
    listener: TcpListener,
    directory: ShardDirectory,
    shard_id: &str,
    lease_ttl: Duration,
    gateway_name: GatewayName,
    gateway_key: InternalGatewayKey,
    config: RouteTableServiceConfig,
    shutdown: CancellationToken,
) -> TestResult<tokio::task::JoinHandle<Result<(), relaygate_route_table_transport::TransportError>>>
{
    let service = RouteTableService::new(
        RouteTableShard::new(
            directory,
            ShardId::new(shard_id)?,
            RouteTableConfig::new(lease_ttl)?,
        )?,
        TrustedGatewayKeys::new([(gateway_name, gateway_key)])?,
        config,
    );
    Ok(tokio::spawn(service.serve(listener, shutdown)))
}

async fn connect_lease_client(
    endpoint: std::net::SocketAddr,
    gateway_name: GatewayName,
    gateway_id: relaygate_route_table::GatewayId,
    gateway_key: InternalGatewayKey,
    config: RouteTableClientConfig,
) -> Result<RouteTableClient, relaygate_route_table_transport::TransportError> {
    RouteTableClient::connect(endpoint, gateway_name, gateway_id, gateway_key, config).await
}

async fn wait_for_counts(handle: &RoutingHandle, synced: usize, unsynced: usize) -> TestResult {
    for _ in 0..800 {
        let counts = handle.current_counts();
        if counts.synced == synced && counts.unsynced == unsynced {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(format!("routing counts did not converge to {synced} synced/{unsynced} unsynced").into())
}

async fn wait_for_health(handle: &RoutingHandle, expected: RouteDependencyHealth) -> TestResult {
    for _ in 0..800 {
        if handle.current_counts().dependency_health == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(format!("route dependency health did not converge to {expected:?}").into())
}

fn two_live_shard_directory(
    endpoint_0: std::net::SocketAddr,
    endpoint_1: std::net::SocketAddr,
) -> TestResult<ShardDirectory> {
    let artifact = format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"rt-0","endpoint":"{endpoint_0}"}},{{"id":"rt-1","endpoint":"{endpoint_1}"}}]}}"#
    );
    Ok(ShardDirectory::from_json_bytes(artifact.as_bytes())?)
}

fn clients_by_shard(directory: &ShardDirectory) -> TestResult<BTreeMap<String, DestinationId>> {
    let mut clients = BTreeMap::new();
    for index in 0..10_000 {
        let destination_id = DestinationId::new(format!("00000000-0000-4000-8000-{index:012x}"))?;
        clients
            .entry(
                directory
                    .authority(&destination_id)
                    .id()
                    .as_str()
                    .to_owned(),
            )
            .or_insert(destination_id);
        if clients.len() == directory.shards().len() {
            return Ok(clients);
        }
    }
    Err("failed to find one DestinationId per shard".into())
}

fn client_for_shard(
    directory: &ShardDirectory,
    shard_id: &str,
    prefix: &str,
) -> TestResult<DestinationId> {
    for index in 0..10_000 {
        let prefix_byte = prefix.as_bytes().first().copied().unwrap_or_default();
        let suffix = (u128::from(prefix_byte) << 32) | index;
        let destination_id = DestinationId::new(format!("00000000-0000-4000-8000-{suffix:012x}"))?;
        if directory.authority(&destination_id).id().as_str() == shard_id {
            return Ok(destination_id);
        }
    }
    Err(format!("failed to find DestinationId for {shard_id}").into())
}
