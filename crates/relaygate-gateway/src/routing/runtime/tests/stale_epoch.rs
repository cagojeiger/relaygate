use std::{error::Error, time::Duration};

use relaygate_protocol::{BindingId as ProtocolBindingId, SessionId};
use relaygate_route_table::{
    BindingId, ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableError,
    RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use tokio::{net::TcpListener, sync::watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{TestResult, update_gate_proxy::UpdateGateProxy};
use crate::{
    registry::Binding,
    routing::{GatewayRoutingConfig, RoutingError},
};

use super::super::{ClientAvailability, ClientFailure, RoutingHandle, RoutingRuntime};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_reconnect_epoch_ignores_late_update_completion() -> TestResult {
    tokio::time::timeout(Duration::from_secs(8), stale_epoch_case()).await??;
    Ok(())
}

async fn stale_epoch_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = UpdateGateProxy::start(route_endpoint).await?;
    let directory = one_shard_directory(proxy.endpoint())?;
    let gateway_id = GatewayId::from_uuid(Uuid::from_u128(100));
    let gateway_name = GatewayName::new("gw-worker-epoch")?;
    let gateway_key = InternalGatewayKey::new("worker-epoch-test-key")?;
    let route_shutdown = CancellationToken::new();
    let route_task = tokio::spawn(
        RouteTableService::new(
            RouteTableShard::new(
                directory.clone(),
                ShardId::new("rt-0")?,
                RouteTableConfig::new(Duration::from_secs(30))?,
            )?,
            TrustedGatewayKeys::new([(gateway_name.clone(), gateway_key.clone())])?,
            RouteTableServiceConfig::new(16, 16, 4, 256 * 1024, Duration::from_secs(1))?,
        )
        .serve(route_listener, route_shutdown.clone()),
    );

    let routing_shutdown = CancellationToken::new();
    let runtime = RoutingRuntime::start(
        GatewayRoutingConfig::new(
            directory,
            gateway_name,
            gateway_key,
            GatewayLocator::new("gw-worker-epoch.internal:27431")?,
            RouteTableClientConfig::new(
                16,
                256 * 1024,
                Duration::from_millis(100),
                Duration::from_millis(100),
                Duration::from_secs(1),
            )?,
        )
        .with_command_queue_capacity(4)
        .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_desired_scan_interval(Duration::from_millis(5))
        .with_shutdown_timeout(Duration::from_millis(200)),
        gateway_id,
        routing_shutdown.clone(),
    )?;
    let handle = runtime.handle();
    let shard_id = ShardId::new("rt-0")?;
    let shard = handle.shards.get(&shard_id).ok_or("missing shard worker")?;
    let mut availability = shard.client.clone();
    let failure = shard.failure.clone();
    let session_id = SessionId::from_uuid(Uuid::from_u128(200));

    handle.publish_session(session_id, vec![binding(session_id, 301)])?;
    wait_for_counts(&handle, 1, 0).await?;
    let first_epoch = ready_epoch(&availability).ok_or("first worker epoch is not ready")?;
    assert_eq!(first_epoch, 1);

    let client_id = ClientId::new("client-a")?;
    let first_keep_alive_count = proxy.keep_alive_count();
    proxy.disconnect_next_resolve();
    let failed_resolve = handle.resolve(client_id.clone()).await;
    assert!(matches!(
        failed_resolve,
        Err(RoutingError::Transport(ref error)) if error.code() == ErrorCode::Unavailable
    ));
    wait_for_counts(&handle, 0, 1).await?;
    let second_epoch = wait_for_new_epoch(&mut availability, first_epoch).await?;
    assert_eq!(second_epoch, first_epoch + 1);
    proxy
        .wait_for_keep_alive_after(first_keep_alive_count)
        .await?;
    wait_for_counts(&handle, 1, 0).await?;

    proxy.arm();
    handle.publish_session(session_id, vec![binding(session_id, 302)])?;
    proxy.wait_until_update_blocked().await?;

    failure.send_replace(Some(ClientFailure {
        epoch: second_epoch,
        error: TransportError::from(RouteTableError::DeadlineOverflow),
    }));
    wait_for_counts(&handle, 0, 1).await?;
    let third_epoch = wait_for_new_epoch(&mut availability, second_epoch).await?;
    assert_eq!(third_epoch, second_epoch + 1);

    let second_keep_alive_count = proxy.keep_alive_count();
    proxy.release();
    proxy
        .wait_for_keep_alive_after(second_keep_alive_count)
        .await?;
    wait_for_counts(&handle, 1, 0).await?;
    let resolved = handle.resolve(client_id).await?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        resolved.entries()[0].identity().binding_id(),
        BindingId::from_uuid(Uuid::from_u128(302))
    );

    routing_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), runtime.wait()).await??;
    route_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), route_task).await???;
    proxy.stop().await?;
    Ok(())
}

fn binding(session_id: SessionId, binding_id: u128) -> Binding {
    Binding {
        id: ProtocolBindingId::from_uuid(Uuid::from_u128(binding_id)),
        client_id: "client-a".to_owned(),
        session_id,
    }
}

fn ready_epoch(availability: &watch::Receiver<ClientAvailability>) -> Option<u64> {
    match &*availability.borrow() {
        ClientAvailability::Ready(client) => Some(client.epoch),
        ClientAvailability::Unavailable | ClientAvailability::Terminal(_) => None,
    }
}

async fn wait_for_new_epoch(
    availability: &mut watch::Receiver<ClientAvailability>,
    previous: u64,
) -> TestResult<u64> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(epoch) = ready_epoch(availability)
                && epoch > previous
            {
                return Ok::<u64, Box<dyn Error + Send + Sync>>(epoch);
            }
            availability.changed().await.map_err(|_| {
                Box::<dyn Error + Send + Sync>::from("worker availability channel closed")
            })?;
        }
    })
    .await
    .map_err(|_| "worker did not advance its connection epoch")?
}

async fn wait_for_counts(handle: &RoutingHandle, synced: usize, unsynced: usize) -> TestResult {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let counts = handle.current_counts();
            if counts.synced == synced && counts.unsynced == unsynced {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| {
        format!("routing counts did not converge to {synced} synced/{unsynced} unsynced")
    })?;
    Ok(())
}

fn one_shard_directory(endpoint: std::net::SocketAddr) -> TestResult<ShardDirectory> {
    let artifact = format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"rt-0","endpoint":"{endpoint}"}}]}}"#
    );
    Ok(ShardDirectory::from_json_bytes(artifact.as_bytes())?)
}
