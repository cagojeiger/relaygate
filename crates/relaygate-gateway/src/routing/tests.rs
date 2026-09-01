use std::{error::Error, time::Duration};

use relaygate_protocol::{BindingId as ProtocolBindingId, SessionId};
use relaygate_route_table::{
    BindingId, ClientId, GatewayId, GatewayLocator, LeaseId, ListenerSessionId, MappingEntry,
    MappingSnapshot, RegistrationAck, RegistrationKey, RouteTableConfig, RouteTableShard,
    ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TrustedGatewayKeys,
};
use tokio::{net::TcpListener, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::registry::Binding;

use super::{
    GatewayRoutingConfig, RoutingError, RoutingRuntime,
    lifecycle::{RegistrationAction, RegistrationState, next_backoff},
    projection::{project_binding_id, project_session, project_session_id},
    runtime::{is_connection_error, is_terminal_control_error},
};

mod multi_shard;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn projection_preserves_exact_ids_and_gateway_location() -> TestResult {
    let directory = one_shard_directory("127.0.0.1:27430")?;
    let gateway_id = gateway(1);
    let session_id = protocol_session(2);
    let binding_id = protocol_binding(3);
    let locator = GatewayLocator::new("gw-a.internal:27431")?;

    let projected = project_session(
        &directory,
        gateway_id,
        &locator,
        session_id,
        vec![Binding {
            id: binding_id,
            client_id: "Client.Exact/한글".to_owned(),
            session_id,
        }],
    )?;

    let snapshot = projected[0]
        .snapshot
        .as_ref()
        .ok_or("missing projected snapshot")?;
    let entry = snapshot.entries().next().ok_or("missing mapping")?;
    assert_eq!(entry.client_id().as_str(), "Client.Exact/한글");
    assert_eq!(entry.identity().gateway_id(), gateway_id);
    assert_eq!(
        entry.identity().listener_session_id(),
        ListenerSessionId::from_uuid(session_id.as_uuid())
    );
    assert_eq!(
        entry.identity().binding_id(),
        BindingId::from_uuid(binding_id.as_uuid())
    );
    assert_eq!(entry.gateway_locator(), &locator);
    assert_eq!(
        project_session_id(session_id).as_uuid(),
        session_id.as_uuid()
    );
    assert_eq!(
        project_binding_id(binding_id).as_uuid(),
        binding_id.as_uuid()
    );
    Ok(())
}

#[test]
fn projection_rejects_a_binding_from_another_session() -> TestResult {
    let directory = one_shard_directory("127.0.0.1:27430")?;
    let result = project_session(
        &directory,
        gateway(1),
        &GatewayLocator::new("gw-a.internal:27431")?,
        protocol_session(2),
        vec![Binding {
            id: protocol_binding(3),
            client_id: "client-a".to_owned(),
            session_id: protocol_session(4),
        }],
    );
    assert!(matches!(result, Err(RoutingError::InvalidProjection(_))));
    Ok(())
}

#[test]
fn projection_splits_one_complete_session_across_directory_shards() -> TestResult {
    let directory = two_shard_directory()?;
    let (first_client, second_client) = clients_on_distinct_shards(&directory)?;
    let session_id = protocol_session(20);
    let projected = project_session(
        &directory,
        gateway(1),
        &GatewayLocator::new("gw-a.internal:27431")?,
        session_id,
        vec![
            Binding {
                id: protocol_binding(21),
                client_id: first_client,
                session_id,
            },
            Binding {
                id: protocol_binding(22),
                client_id: second_client,
                session_id,
            },
        ],
    )?;

    assert_eq!(projected.len(), 2);
    assert!(projected.iter().all(|shard| shard.snapshot.is_some()));
    assert!(projected.iter().all(|shard| {
        shard.snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .entries()
                .all(|entry| directory.authority(entry.client_id()).id() == &shard.shard_id)
        })
    }));
    Ok(())
}

#[test]
fn late_register_result_cannot_replace_newer_desired_state() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let old = state.begin_next(now)?.ok_or("missing REGISTER")?;
    assert!(matches!(old.action, RegistrationAction::Register { .. }));

    state.publish(2, Some(snapshot("client-b")?), now);
    state.register_succeeded(&old, registration_ack(10, None), now);

    assert_eq!(state.desired_version(), 2);
    assert!(!state.is_synced());
    let current = state.begin_next(now)?.ok_or("missing current REGISTER")?;
    assert_eq!(current.desired_version, 2);
    assert!(matches!(
        current.action,
        RegistrationAction::Register { .. }
    ));
    Ok(())
}

#[test]
fn one_registration_key_serializes_every_lease_operation() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;

    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    assert!(matches!(
        register.action,
        RegistrationAction::Register { .. }
    ));
    assert!(state.begin_next(now)?.is_none());
    state.register_succeeded(&register, registration_ack(10, None), now);

    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    assert!(matches!(update.action, RegistrationAction::Update { .. }));
    assert!(state.begin_next(now)?.is_none());
    state.update_succeeded(
        &update,
        registration_ack(10, Some(relaygate_route_table::RegistrationRevision::FIRST)),
        now,
    );

    let keep_alive_at = now + Duration::from_secs(3);
    let keep_alive = state
        .begin_next(keep_alive_at)?
        .ok_or("missing KEEP_ALIVE")?;
    assert!(matches!(
        keep_alive.action,
        RegistrationAction::KeepAlive { .. }
    ));
    assert!(state.begin_next(keep_alive_at)?.is_none());
    state.keep_alive_succeeded(
        &keep_alive,
        registration_ack(10, Some(relaygate_route_table::RegistrationRevision::FIRST)),
        keep_alive_at,
    );

    state.publish(2, None, keep_alive_at);
    let deregister = state
        .begin_next(keep_alive_at)?
        .ok_or("missing DEREGISTER")?;
    assert!(matches!(
        deregister.action,
        RegistrationAction::Deregister { .. }
    ));
    assert!(state.begin_next(keep_alive_at)?.is_none());
    state.finish_deregister(&deregister);
    assert!(state.is_removable());
    Ok(())
}

#[test]
fn terminal_failed_register_ignores_late_success() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;

    state.terminal_failure(&register);
    state.register_succeeded(&register, registration_ack(10, None), now);

    assert!(state.active_lease().is_none());
    assert!(!state.is_synced());
    assert!(state.begin_next(now + Duration::from_secs(60))?.is_none());
    Ok(())
}

#[test]
fn stale_lease_re_registers_while_transient_failure_retries_same_update() -> TestResult {
    let now = Instant::now();
    let retry = Duration::from_millis(10);
    let mut state = registration_state_with_retry(now, 1, "client-a", retry)?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);

    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    assert!(matches!(update.action, RegistrationAction::Update { .. }));
    state.transient_failure(&update, now);
    assert!(state.begin_next(now)?.is_none());
    let repeated = state
        .begin_next(now + retry)?
        .ok_or("missing repeated UPDATE")?;
    assert_eq!(repeated.action, update.action);

    state.precondition_failed(&repeated, now + retry);
    let renewed = state
        .begin_next(now + retry)?
        .ok_or("missing renewed REGISTER")?;
    assert!(matches!(
        renewed.action,
        RegistrationAction::Register { .. }
    ));
    Ok(())
}

#[test]
fn known_connection_loss_is_immediately_unsynced_and_validates_the_lease() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);
    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.update_succeeded(
        &update,
        registration_ack(10, Some(relaygate_route_table::RegistrationRevision::FIRST)),
        now,
    );
    assert!(state.is_synced());

    state.connection_lost(now);
    assert!(!state.is_synced());
    let validate = state
        .begin_next(now)?
        .ok_or("missing immediate KEEP_ALIVE validation")?;
    assert!(matches!(
        validate.action,
        RegistrationAction::KeepAlive { .. }
    ));
    state.precondition_failed(&validate, now);
    assert!(matches!(
        state.begin_next(now)?.map(|ticket| ticket.action),
        Some(RegistrationAction::Register { .. })
    ));
    Ok(())
}

#[test]
fn failed_precondition_allows_one_register_probe_then_stops() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);

    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.precondition_failed(&update, now);
    let probe = state
        .begin_next(now)?
        .ok_or("missing REGISTER precondition probe")?;
    assert!(matches!(probe.action, RegistrationAction::Register { .. }));

    state.precondition_failed(&probe, now);
    assert!(state.begin_next(now + Duration::from_secs(60))?.is_none());
    Ok(())
}

#[test]
fn repeated_precondition_after_successful_probe_is_terminal() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);

    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.precondition_failed(&update, now);
    let probe = state
        .begin_next(now)?
        .ok_or("missing REGISTER precondition probe")?;
    state.register_succeeded(&probe, registration_ack(10, None), now);

    let repeated_update = state
        .begin_next(now)?
        .ok_or("missing UPDATE after successful probe")?;
    state.precondition_failed(&repeated_update, now);
    assert!(state.begin_next(now + Duration::from_secs(60))?.is_none());
    Ok(())
}

#[test]
fn successful_lease_operation_resets_precondition_probe_budget() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);

    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.precondition_failed(&update, now);
    let first_probe = state
        .begin_next(now)?
        .ok_or("missing first REGISTER precondition probe")?;
    state.register_succeeded(&first_probe, registration_ack(10, None), now);

    let recovered_update = state.begin_next(now)?.ok_or("missing recovered UPDATE")?;
    state.update_succeeded(
        &recovered_update,
        registration_ack(10, Some(relaygate_route_table::RegistrationRevision::FIRST)),
        now,
    );
    state.connection_lost(now);

    let keep_alive = state
        .begin_next(now)?
        .ok_or("missing KEEP_ALIVE validation")?;
    state.precondition_failed(&keep_alive, now);
    assert!(matches!(
        state.begin_next(now)?.map(|ticket| ticket.action),
        Some(RegistrationAction::Register { .. })
    ));
    Ok(())
}

#[test]
fn terminal_failure_does_not_hot_retry_and_backoff_is_bounded() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.terminal_failure(&register);

    assert!(state.begin_next(now + Duration::from_secs(60))?.is_none());
    assert!(is_terminal_control_error(ErrorCode::Unauthenticated));
    assert!(is_terminal_control_error(ErrorCode::PermissionDenied));
    assert!(is_terminal_control_error(ErrorCode::FailedPrecondition));
    assert!(!is_terminal_control_error(ErrorCode::Unavailable));
    assert!(is_connection_error(ErrorCode::Unavailable));
    assert_eq!(
        next_backoff(Duration::from_millis(80), Duration::from_millis(100)),
        Duration::from_millis(100)
    );
    Ok(())
}

#[test]
fn removed_terminal_registration_drops_unusable_lease_state() -> TestResult {
    let now = Instant::now();
    let mut state = registration_state(now, 1, "client-a")?;
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(10, None), now);
    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.update_succeeded(
        &update,
        registration_ack(10, Some(relaygate_route_table::RegistrationRevision::FIRST)),
        now,
    );
    state.mark_terminal();

    state.publish(2, None, now);

    assert!(state.active_lease().is_none());
    assert!(state.is_removable());
    Ok(())
}

#[test]
fn routing_config_rejects_runtime_panics_before_start() -> TestResult {
    let config = GatewayRoutingConfig::new(
        one_shard_directory("127.0.0.1:27430")?,
        GatewayName::new("gw-a")?,
        InternalGatewayKey::new("key")?,
        GatewayLocator::new("gw-a.internal:27431")?,
        RouteTableClientConfig::new(
            1,
            1024,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )?,
    );
    assert!(
        config
            .clone()
            .with_command_queue_capacity(0)
            .validate()
            .is_err()
    );
    assert!(
        config
            .clone()
            .with_command_queue_capacity(usize::MAX)
            .validate()
            .is_err()
    );
    assert!(
        config
            .clone()
            .with_reconnect_backoff(Duration::from_secs(2), Duration::from_secs(1))
            .validate()
            .is_err()
    );
    assert!(
        config
            .with_desired_scan_interval(Duration::ZERO)
            .validate()
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn bounded_wake_coalesces_to_latest_snapshot_over_one_live_rt() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let directory = one_shard_directory(&endpoint.to_string())?;
    let generation = directory.generation();
    let gateway_id = gateway(100);
    let gateway_name = GatewayName::new("gw-live")?;
    let gateway_key = InternalGatewayKey::new("test-key")?;
    let shard = RouteTableShard::new(
        directory.clone(),
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_millis(300))?,
    )?;
    let service = RouteTableService::new(
        shard,
        TrustedGatewayKeys::new([(gateway_name.clone(), gateway_key.clone())])?,
        RouteTableServiceConfig::new(32, 32, 8, 256 * 1024, Duration::from_secs(1))?,
    );
    let client_config = RouteTableClientConfig::new(
        32,
        256 * 1024,
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_millis(200),
    )?;
    let routing_shutdown = CancellationToken::new();
    let runtime = RoutingRuntime::start(
        GatewayRoutingConfig::new(
            directory.clone(),
            gateway_name.clone(),
            gateway_key.clone(),
            GatewayLocator::new("gw-live.internal:27431")?,
            client_config,
        )
        .with_command_queue_capacity(1)
        .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_desired_scan_interval(Duration::from_millis(5))
        .with_shutdown_timeout(Duration::from_millis(200)),
        gateway_id,
        routing_shutdown.clone(),
    )?;
    let handle = runtime.handle();
    let session_id = protocol_session(200);

    for value in 1..=64_u128 {
        handle.publish_session(
            session_id,
            vec![Binding {
                id: protocol_binding(1_000 + value),
                client_id: format!("client-{value}"),
                session_id,
            }],
        )?;
    }

    // The RT listener exists but is not serving yet. At least one handshake
    // attempt expires, proving that the worker reconnects from current desired
    // state instead of requiring another publication.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let service_shutdown = CancellationToken::new();
    let service_task = tokio::spawn(service.serve(listener, service_shutdown.clone()));

    let final_client = ClientId::new("client-64")?;
    let resolved = wait_for_resolve(&handle, final_client.clone()).await?;
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        resolved.entries()[0].identity().binding_id().as_uuid(),
        protocol_binding(1_064).as_uuid()
    );
    assert_eq!(handle.current_counts().synced, 1);
    assert_eq!(handle.current_counts().unsynced, 0);

    let old = handle.resolve(ClientId::new("client-1")?).await;
    assert!(matches!(
        old,
        Err(RoutingError::Transport(ref error)) if error.code() == ErrorCode::NotFound
    ));

    // Restart the same logical shard as READY-empty. The old connection and
    // lease disappear; the worker must mark the registration UNSYNCED, obtain
    // a new lease, and publish the manager-owned current snapshot again.
    service_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), service_task).await???;
    wait_for_unsynced(&handle).await?;

    let restarted_listener = bind_endpoint(endpoint).await?;
    let restarted_shard = RouteTableShard::new(
        directory.clone(),
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_millis(300))?,
    )?;
    assert_eq!(restarted_shard.stats().mapping_count, 0);
    let restarted_service = RouteTableService::new(
        restarted_shard,
        TrustedGatewayKeys::new([(gateway_name, gateway_key)])?,
        RouteTableServiceConfig::new(32, 32, 8, 256 * 1024, Duration::from_secs(1))?,
    );
    let restarted_shutdown = CancellationToken::new();
    let restarted_task =
        tokio::spawn(restarted_service.serve(restarted_listener, restarted_shutdown.clone()));

    let recovered = wait_for_resolve(&handle, final_client.clone()).await?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(handle.current_counts().synced, 1);
    assert_eq!(handle.current_counts().unsynced, 0);

    handle.publish_session(session_id, Vec::new())?;
    wait_for_not_found(&handle, final_client).await?;

    routing_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), runtime.wait()).await??;
    restarted_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), restarted_task).await???;
    assert_eq!(
        generation,
        one_shard_directory(&endpoint.to_string())?.generation()
    );
    Ok(())
}

async fn wait_for_unsynced(handle: &super::RoutingHandle) -> TestResult {
    for _ in 0..400 {
        let counts = handle.current_counts();
        if counts.synced == 0 && counts.unsynced == 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err("routing did not observe the lost RouteTable connection".into())
}

async fn bind_endpoint(endpoint: std::net::SocketAddr) -> TestResult<TcpListener> {
    let mut last_error = None;
    for _ in 0..100 {
        match TcpListener::bind(endpoint).await {
            Ok(listener) => return Ok(listener),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(last_error
        .map_or_else(
            || "RouteTable endpoint could not be rebound".to_owned(),
            |error| format!("RouteTable endpoint could not be rebound: {error}"),
        )
        .into())
}

async fn wait_for_resolve(
    handle: &super::RoutingHandle,
    client_id: ClientId,
) -> TestResult<relaygate_route_table::BindingSet> {
    for _ in 0..200 {
        if let Ok(bindings) = handle.resolve(client_id.clone()).await {
            return Ok(bindings);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err("routing did not converge to the current snapshot".into())
}

async fn wait_for_not_found(handle: &super::RoutingHandle, client_id: ClientId) -> TestResult {
    for _ in 0..200 {
        match handle.resolve(client_id.clone()).await {
            Err(RoutingError::Transport(error)) if error.code() == ErrorCode::NotFound => {
                return Ok(());
            }
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    Err("routing did not remove the empty desired snapshot".into())
}

fn registration_state(
    now: Instant,
    version: u64,
    client_id: &str,
) -> TestResult<RegistrationState> {
    registration_state_with_retry(now, version, client_id, Duration::from_millis(10))
}

fn registration_state_with_retry(
    now: Instant,
    version: u64,
    client_id: &str,
    retry: Duration,
) -> TestResult<RegistrationState> {
    Ok(RegistrationState::new(
        RegistrationKey::new(gateway(1), listener_session(2), ShardId::new("rt-0")?),
        version,
        Some(snapshot(client_id)?),
        now,
        retry,
        Duration::from_secs(1),
    ))
}

fn snapshot(client_id: &str) -> TestResult<MappingSnapshot> {
    Ok(MappingSnapshot::new([MappingEntry::new(
        ClientId::new(client_id)?,
        gateway(1),
        listener_session(2),
        BindingId::from_uuid(Uuid::from_u128(3)),
        GatewayLocator::new("gw-a.internal:27431")?,
    )])?)
}

fn registration_ack(
    lease: u128,
    revision: Option<relaygate_route_table::RegistrationRevision>,
) -> RegistrationAck {
    RegistrationAck::from_parts(
        LeaseId::from_uuid(Uuid::from_u128(lease)),
        revision,
        Duration::from_secs(5),
    )
}

fn one_shard_directory(endpoint: &str) -> TestResult<ShardDirectory> {
    let artifact = format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"rt-0","endpoint":"{endpoint}"}}]}}"#
    );
    Ok(ShardDirectory::from_json_bytes(artifact.as_bytes())?)
}

fn two_shard_directory() -> TestResult<ShardDirectory> {
    Ok(ShardDirectory::from_json_bytes(
        br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"rt-0:27430"},{"id":"rt-1","endpoint":"rt-1:27430"}]}"#,
    )?)
}

fn clients_on_distinct_shards(directory: &ShardDirectory) -> TestResult<(String, String)> {
    let mut by_shard = std::collections::BTreeMap::new();
    for index in 0..1_000 {
        let candidate = format!("client-{index}");
        let client_id = ClientId::new(candidate.clone())?;
        by_shard
            .entry(directory.authority(&client_id).id().clone())
            .or_insert(candidate);
        if by_shard.len() == 2 {
            let mut clients = by_shard.into_values();
            return Ok((
                clients.next().ok_or("missing first shard client")?,
                clients.next().ok_or("missing second shard client")?,
            ));
        }
    }
    Err("failed to find clients on distinct shards".into())
}

const fn gateway(value: u128) -> GatewayId {
    GatewayId::from_uuid(Uuid::from_u128(value))
}

const fn listener_session(value: u128) -> ListenerSessionId {
    ListenerSessionId::from_uuid(Uuid::from_u128(value))
}

const fn protocol_session(value: u128) -> SessionId {
    SessionId::from_uuid(Uuid::from_u128(value))
}

const fn protocol_binding(value: u128) -> ProtocolBindingId {
    ProtocolBindingId::from_uuid(Uuid::from_u128(value))
}
