use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig, GatewaySnapshot,
    RouteDependencyHealth, check,
};
use relaygate_protocol::{
    BindingId as ProtocolBindingId, ClientKey, ErrorCode as ProtocolErrorCode, Frame, FrameCodec,
    PeerObservation, SessionId, SessionRole,
};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, ListenerSessionId, MappingEntry, RegistrationKey,
    RouteTableConfig, RouteTableShard, ShardDirectory, ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode as RouteErrorCode, GatewayName, InternalGatewayKey, RouteTableClient,
    RouteTableClientConfig, RouteTableService, RouteTableServiceConfig, TransportError,
    TrustedGatewayKeys,
};
use relaygate_sdk::{Config as SdkConfig, Connector, ListenerRuntime, ListenerStatus};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::Instant,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type SdkTransport = Framed<TcpStream, FrameCodec>;

const CLIENT_ID: &str = "echo.recover";
const ALPHA_ID: &str = "echo.alpha";
const BETA_ID: &str = "echo.beta";
const GAMMA_ID: &str = "echo.gamma";
const CLIENT_KEY: &str = "listener-secret";
const GATEWAY_NAME: &str = "gateway-under-test";
const GATEWAY_KEY: &str = "gateway-key";
const PROBE_NAME: &str = "probe";
const PROBE_KEY: &str = "probe-key";
const SHARD_ID: &str = "rt-0";
const ROUTE_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(200);
const ROUTING_RECONNECT_INITIAL: Duration = Duration::from_millis(20);
const ROUTING_RECONNECT_MAX: Duration = Duration::from_millis(40);

#[tokio::test]
async fn routed_gateway_republishes_current_listener_state_after_route_table_restart() -> TestResult
{
    tokio::time::timeout(Duration::from_secs(5), recovery_case()).await??;
    Ok(())
}

async fn recovery_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = ControlledRouteProxy::start(route_endpoint).await?;
    let directory_bytes = one_shard_directory(proxy.endpoint);
    let directory = ShardDirectory::from_json_bytes(&directory_bytes)?;
    let generation = directory.generation();
    let mut route_table = RunningRouteTable::serve(
        route_listener,
        directory.clone(),
        Duration::from_millis(250),
    )?;
    assert!(route_table.started_empty());

    let gateway = RunningGateway::start(directory.clone()).await?;
    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.route_dependency_health == RouteDependencyHealth::Ready
            && snapshot.route_registrations_synced == 0
            && snapshot.route_registrations_unsynced == 0
    })
    .await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    let connector = Connector::connect(sdk_config(gateway.endpoint)).await?;

    let probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(&probe, generation, CLIENT_ID, &gateway.peer_locator, 1).await?;

    let (mut existing_connector_pipe, mut existing_listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    route_table.stop().await?;
    proxy.pause();
    let restarted_listener = TcpListener::bind(route_endpoint).await?;
    route_table =
        RunningRouteTable::serve(restarted_listener, directory, Duration::from_millis(250))?;
    assert!(route_table.started_empty());
    let restarted_probe = connect_probe(route_endpoint, generation).await?;

    wait_for_not_found(&restarted_probe, generation, CLIENT_ID).await?;
    existing_connector_pipe.write_all(b"existing").await?;
    let mut existing_received = [0_u8; 8];
    existing_listener_pipe
        .read_exact(&mut existing_received)
        .await?;
    assert_eq!(&existing_received, b"existing");

    let (mut local_connector_pipe, mut local_listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    local_listener_pipe.write_all(b"local-open").await?;
    let mut local_received = [0_u8; 10];
    local_connector_pipe.read_exact(&mut local_received).await?;
    assert_eq!(&local_received, b"local-open");

    proxy.resume();
    wait_for_mapping(
        &restarted_probe,
        generation,
        CLIENT_ID,
        &gateway.peer_locator,
        1,
    )
    .await?;

    assert_eq!(listener.client_id(), CLIENT_ID);
    existing_connector_pipe.close().await?;
    existing_listener_pipe.close().await?;
    local_connector_pipe.close().await?;
    local_listener_pipe.close().await?;
    connector.close();
    listener_runtime.close();
    wait_for_not_found(&restarted_probe, generation, CLIENT_ID).await?;

    route_table.stop().await?;
    proxy.stop().await?;
    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn graceful_drain_withdraws_route_before_closing_existing_pipe() -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), graceful_drain_case()).await??;
    Ok(())
}

async fn graceful_drain_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::serve(
        route_listener,
        directory.clone(),
        Duration::from_millis(250),
    )?;
    let gateway =
        RunningGateway::start_with_drain_timeout(directory, Duration::from_secs(2)).await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    let connector = Connector::connect(sdk_config(gateway.endpoint)).await?;
    let probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(&probe, generation, CLIENT_ID, &gateway.peer_locator, 1).await?;

    let (mut connector_pipe, mut listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    gateway.shutdown.cancel();
    wait_for_not_found(&probe, generation, CLIENT_ID).await?;
    assert!(gateway.gateway.snapshot().draining);
    assert!(!gateway.task.is_finished());

    connector_pipe.write_all(b"existing").await?;
    let mut received = [0_u8; 8];
    listener_pipe.read_exact(&mut received).await?;
    assert_eq!(&received, b"existing");

    connector_pipe.close().await?;
    listener_pipe.close().await?;
    connector.close();
    listener_runtime.close();
    gateway.stop().await?;
    route_table.stop().await?;
    Ok(())
}

#[tokio::test]
async fn generation_mismatch_recovers_only_after_coordinated_restart() -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), generation_mismatch_recovery_case()).await??;
    Ok(())
}

async fn generation_mismatch_recovery_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = RegistrationCountingProxy::start(route_endpoint).await?;
    let route_directory_bytes = one_shard_directory(proxy.endpoint);
    let route_directory = ShardDirectory::from_json_bytes(&route_directory_bytes)?;
    let route_generation = route_directory.generation();
    let mut gateway_directory_bytes = route_directory_bytes;
    gateway_directory_bytes.push(b'\n');
    let gateway_directory = ShardDirectory::from_json_bytes(&gateway_directory_bytes)?;
    let gateway_generation = gateway_directory.generation();
    assert_ne!(gateway_generation, route_generation);

    let route_table = RunningRouteTable::serve(
        route_listener,
        route_directory.clone(),
        Duration::from_millis(250),
    )?;
    assert!(route_table.started_empty());
    let probe = connect_probe(route_endpoint, route_generation).await?;

    let gateway = RunningGateway::start(gateway_directory).await?;
    let gateway_endpoint = gateway.endpoint;
    let peer_endpoint = gateway.peer_locator.parse()?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;

    // The RT is reachable and authenticated, but rejects the Gateway's exact
    // directory generation. REGISTER itself is terminal, so a process with the
    // same configuration must not retry it after the response arrives.
    wait_for_registration_rejection(&proxy).await?;
    tokio::time::sleep(ROUTING_RECONNECT_MAX.saturating_mul(5)).await;
    assert_eq!(proxy.register_count(), 1);
    assert_eq!(proxy.failed_precondition_count(), 1);
    let unsynced = gateway.gateway.snapshot();
    assert_eq!(unsynced.listener_bindings, 1);
    assert_eq!(
        unsynced.route_dependency_health,
        RouteDependencyHealth::Terminal
    );
    assert_eq!(unsynced.route_registrations_synced, 0);
    assert_eq!(unsynced.route_registrations_unsynced, 1);
    wait_for_not_found(&probe, route_generation, CLIENT_ID).await?;
    let mismatched_resolve = probe
        .resolve(gateway_generation, &ClientId::new(CLIENT_ID)?)
        .await;
    match mismatched_resolve {
        Err(error) => assert_eq!(error.code(), RouteErrorCode::FailedPrecondition),
        Ok(_) => return Err("mismatched generation unexpectedly resolved route state".into()),
    }
    wait_for_not_found(&probe, route_generation, CLIENT_ID).await?;

    let connector = Connector::connect(sdk_config(gateway.endpoint)).await?;
    let (mut connector_pipe, mut listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    let local = gateway.gateway.snapshot();
    assert_eq!(local.listener_bindings, 1);
    assert_eq!(local.live_pipes, 1);
    assert_eq!(local.route_registrations_synced, 0);
    assert_eq!(local.route_registrations_unsynced, 1);
    assert_eq!(local.remote_open_attempts, 0);
    assert_eq!(local.peer_transports_connecting, 0);
    assert_eq!(local.peer_transports_ready, 0);
    assert_eq!(local.peer_streams, 0);

    connector_pipe.write_all(b"before rt").await?;
    let mut received = [0_u8; 9];
    listener_pipe.read_exact(&mut received).await?;
    assert_eq!(&received, b"before rt");

    connector_pipe.close().await?;
    listener_pipe.close().await?;
    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.listener_bindings == 1
            && snapshot.live_pipes == 0
            && snapshot.route_registrations_synced == 0
            && snapshot.route_registrations_unsynced == 1
    })
    .await?;

    connector.close();
    gateway.stop().await?;
    poll_until(Duration::from_secs(2), || async {
        (listener.status() == ListenerStatus::Suspended).then_some(())
    })
    .await?;
    route_table.stop().await?;
    assert_eq!(proxy.register_count(), 1);
    assert_eq!(proxy.failed_precondition_count(), 1);

    drop(probe);
    let restarted_route_listener = TcpListener::bind(route_endpoint).await?;
    let restarted_route_table = RunningRouteTable::serve(
        restarted_route_listener,
        route_directory.clone(),
        Duration::from_millis(250),
    )?;
    assert!(restarted_route_table.started_empty());
    let restarted_probe = connect_probe(route_endpoint, route_generation).await?;
    wait_for_not_found(&restarted_probe, route_generation, CLIENT_ID).await?;

    let restarted_gateway = RunningGateway::start_on(
        route_directory,
        Duration::from_millis(100),
        gateway_endpoint,
        peer_endpoint,
    )
    .await?;
    poll_until(Duration::from_secs(2), || async {
        (listener.status() == ListenerStatus::Active).then_some(())
    })
    .await?;
    wait_for_gateway_snapshot(&restarted_gateway, |snapshot| {
        snapshot.listener_bindings == 1
            && snapshot.route_dependency_health == RouteDependencyHealth::Ready
            && snapshot.route_registrations_synced == 1
            && snapshot.route_registrations_unsynced == 0
    })
    .await?;
    wait_for_mapping(
        &restarted_probe,
        route_generation,
        CLIENT_ID,
        &restarted_gateway.peer_locator,
        1,
    )
    .await?;
    assert_eq!(proxy.register_count(), 2);
    assert_eq!(proxy.failed_precondition_count(), 1);

    let recovered_connector = Connector::connect(sdk_config(restarted_gateway.endpoint)).await?;
    let (mut recovered_connector_pipe, mut recovered_listener_pipe) =
        tokio::try_join!(recovered_connector.open(CLIENT_ID), listener.accept())?;
    recovered_connector_pipe.write_all(b"after restart").await?;
    let mut recovered = [0_u8; 13];
    recovered_listener_pipe.read_exact(&mut recovered).await?;
    assert_eq!(&recovered, b"after restart");
    recovered_connector_pipe.close().await?;
    recovered_listener_pipe.close().await?;
    recovered_connector.close();

    listener_runtime.close();
    wait_for_not_found(&restarted_probe, route_generation, CLIENT_ID).await?;
    restarted_gateway.stop().await?;
    restarted_route_table.stop().await?;
    proxy.stop().await?;
    Ok(())
}

#[tokio::test]
async fn route_table_authentication_failure_keeps_local_binding_and_stops_retries() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(5),
        route_table_authentication_failure_case(),
    )
    .await??;
    Ok(())
}

async fn route_table_authentication_failure_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = RegistrationCountingProxy::start(route_endpoint).await?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(proxy.endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::serve_with_trusted_gateways(
        route_listener,
        directory.clone(),
        Duration::from_millis(250),
        probe_only_trusted_gateway_keys()?,
    )?;
    let probe = connect_probe(route_endpoint, generation).await?;

    let gateway = RunningGateway::start(directory).await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;

    poll_until(Duration::from_secs(2), || async {
        (proxy.authentication_rejection_count() >= 1).then_some(())
    })
    .await?;
    tokio::time::sleep(ROUTE_HANDSHAKE_TIMEOUT + ROUTING_RECONNECT_MAX.saturating_mul(5)).await;
    assert_eq!(proxy.connection_count(), 1);
    assert_eq!(proxy.authentication_rejection_count(), 1);
    let snapshot = gateway.gateway.snapshot();
    assert_eq!(snapshot.listener_bindings, 1);
    assert_eq!(
        snapshot.route_dependency_health,
        RouteDependencyHealth::Terminal
    );
    assert_eq!(snapshot.route_registrations_synced, 0);
    assert_eq!(snapshot.route_registrations_unsynced, 1);
    wait_for_not_found(&probe, generation, CLIENT_ID).await?;

    let connector = Connector::connect(sdk_config(gateway.endpoint)).await?;
    let (mut connector_pipe, mut listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    connector_pipe.write_all(b"local only").await?;
    let mut received = [0_u8; 10];
    listener_pipe.read_exact(&mut received).await?;
    assert_eq!(&received, b"local only");

    connector_pipe.close().await?;
    listener_pipe.close().await?;
    connector.close();
    listener_runtime.close();
    gateway.stop().await?;
    route_table.stop().await?;
    proxy.stop().await?;
    Ok(())
}

#[tokio::test]
async fn unanswered_deregister_falls_back_to_lease_expiry() -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), deregister_loss_expiry_case()).await??;
    Ok(())
}

async fn deregister_loss_expiry_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let proxy = DeregisterBlackholeProxy::start(route_endpoint).await?;
    let directory_bytes = one_shard_directory(proxy.endpoint);
    let directory = ShardDirectory::from_json_bytes(&directory_bytes)?;
    let generation = directory.generation();
    let route_table =
        RunningRouteTable::serve(route_listener, directory.clone(), Duration::from_secs(1))?;

    let gateway = RunningGateway::start(directory).await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    let probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(&probe, generation, CLIENT_ID, &gateway.peer_locator, 1).await?;

    listener_runtime.close();
    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.listener_sessions == 0
            && snapshot.listener_bindings == 0
            && snapshot.live_pipes == 0
    })
    .await?;
    poll_until(Duration::from_secs(2), || async {
        proxy.dropped_deregister().then_some(())
    })
    .await?;
    wait_for_mapping(&probe, generation, CLIENT_ID, &gateway.peer_locator, 1).await?;
    wait_for_not_found(&probe, generation, CLIENT_ID).await?;

    assert_eq!(listener.client_id(), CLIENT_ID);
    gateway.stop().await?;
    proxy.stop().await?;
    route_table.stop().await?;
    Ok(())
}

#[tokio::test]
async fn distributed_offer_timeout_removes_only_selected_registration() -> TestResult {
    tokio::time::timeout(Duration::from_secs(8), distributed_offer_timeout_case()).await??;
    Ok(())
}

async fn distributed_offer_timeout_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory_bytes = one_shard_directory(route_endpoint);
    let directory = ShardDirectory::from_json_bytes(&directory_bytes)?;
    let generation = directory.generation();
    let route_table =
        RunningRouteTable::serve(route_listener, directory.clone(), Duration::from_secs(2))?;
    let gateway =
        RunningGateway::start_with_offer_timeout(directory, Duration::from_secs(1)).await?;
    let probe = connect_probe(route_endpoint, generation).await?;

    let (mut selected, selected_session) =
        sdk_session(gateway.endpoint, SessionRole::Listener).await?;
    let selected_alpha = register_listener(&mut selected, 1, ALPHA_ID).await?;
    let selected_beta = register_listener(&mut selected, 2, BETA_ID).await?;
    let (mut sibling, sibling_session) =
        sdk_session(gateway.endpoint, SessionRole::Listener).await?;
    let sibling_gamma = register_listener(&mut sibling, 1, GAMMA_ID).await?;

    wait_for_mapping(&probe, generation, ALPHA_ID, &gateway.peer_locator, 1).await?;
    wait_for_mapping(&probe, generation, BETA_ID, &gateway.peer_locator, 1).await?;
    wait_for_mapping(&probe, generation, GAMMA_ID, &gateway.peer_locator, 1).await?;
    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.listener_sessions == 2
            && snapshot.listener_bindings == 3
            && snapshot.route_registrations_synced == 2
            && snapshot.route_registrations_unsynced == 0
    })
    .await?;

    let selected_alpha_entry = resolve_single(&probe, generation, ALPHA_ID).await?;
    let selected_beta_entry = resolve_single(&probe, generation, BETA_ID).await?;
    let sibling_gamma_entry = resolve_single(&probe, generation, GAMMA_ID).await?;
    let selected_key = registration_key(&selected_alpha_entry)?;
    let sibling_key = registration_key(&sibling_gamma_entry)?;
    assert_eq!(selected_key, registration_key(&selected_beta_entry)?);
    assert_ne!(selected_key, sibling_key);
    assert_eq!(
        selected_key.listener_session_id(),
        ListenerSessionId::from_uuid(selected_session.as_uuid())
    );
    assert_eq!(
        sibling_key.listener_session_id(),
        ListenerSessionId::from_uuid(sibling_session.as_uuid())
    );
    assert_eq!(selected_key.gateway_id(), sibling_key.gateway_id());
    assert_eq!(
        selected_alpha_entry.identity().binding_id().as_uuid(),
        selected_alpha.as_uuid()
    );
    assert_eq!(
        selected_beta_entry.identity().binding_id().as_uuid(),
        selected_beta.as_uuid()
    );
    assert_eq!(
        sibling_gamma_entry.identity().binding_id().as_uuid(),
        sibling_gamma.as_uuid()
    );

    let (mut connector, _) = sdk_session(gateway.endpoint, SessionRole::Connector).await?;
    connector
        .send(Frame::Open {
            connection_id: 1,
            client_id: BETA_ID.to_owned(),
        })
        .await?;
    let beta_pipe = match next_sdk_frame(&mut selected).await? {
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id,
        } if pipe_id.connection_id() == 1
            && binding_id == selected_beta
            && client_id == BETA_ID =>
        {
            pipe_id
        }
        other => return Err(format!("expected selected beta OFFER, got {other:?}").into()),
    };
    selected
        .send(Frame::OfferAccepted { pipe_id: beta_pipe })
        .await?;
    assert!(matches!(
        next_sdk_frame(&mut connector).await?,
        Frame::Opened { pipe_id } if pipe_id == beta_pipe
    ));

    connector
        .send(Frame::Open {
            connection_id: 2,
            client_id: ALPHA_ID.to_owned(),
        })
        .await?;
    let unanswered_pipe = match next_sdk_frame(&mut selected).await? {
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id,
        } if pipe_id.connection_id() == 2
            && binding_id == selected_alpha
            && client_id == ALPHA_ID =>
        {
            pipe_id
        }
        other => return Err(format!("expected selected alpha OFFER, got {other:?}").into()),
    };

    let sibling_alpha = register_listener(&mut sibling, 2, ALPHA_ID).await?;
    wait_for_mapping(&probe, generation, ALPHA_ID, &gateway.peer_locator, 2).await?;
    let alpha_entries = resolve_entries(&probe, generation, ALPHA_ID).await?;
    assert!(alpha_entries.iter().any(|entry| {
        registration_key(entry).is_ok_and(|key| key == selected_key)
            && entry.identity().binding_id().as_uuid() == selected_alpha.as_uuid()
    }));
    assert!(alpha_entries.iter().any(|entry| {
        registration_key(entry).is_ok_and(|key| key == sibling_key)
            && entry.identity().binding_id().as_uuid() == sibling_alpha.as_uuid()
    }));

    let mut saw_open_failure = false;
    let mut saw_beta_reset = false;
    for _ in 0..2 {
        match next_sdk_frame(&mut connector).await? {
            Frame::OpenFailed {
                connection_id: 2,
                code: ProtocolErrorCode::DeadlineExceeded,
                observation: PeerObservation::MaybeObserved,
                ..
            } => saw_open_failure = true,
            Frame::Reset {
                pipe_id,
                code: ProtocolErrorCode::Unavailable,
                ..
            } if pipe_id == beta_pipe => saw_beta_reset = true,
            other => return Err(format!("unexpected timeout cleanup frame: {other:?}").into()),
        }
    }
    assert!(saw_open_failure, "unanswered alpha OPEN did not fail");
    assert!(saw_beta_reset, "existing beta Pipe was not reset");
    assert_ne!(unanswered_pipe, beta_pipe);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), selected.next()).await,
        Ok(None) | Ok(Some(Err(_)))
    ));

    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.listener_sessions == 1
            && snapshot.listener_bindings == 2
            && snapshot.pending_offers == 0
            && snapshot.live_pipes == 0
            && snapshot.route_registrations_synced == 1
            && snapshot.route_registrations_unsynced == 0
    })
    .await?;
    wait_for_mapping(&probe, generation, ALPHA_ID, &gateway.peer_locator, 1).await?;
    wait_for_mapping(&probe, generation, GAMMA_ID, &gateway.peer_locator, 1).await?;
    wait_for_not_found(&probe, generation, BETA_ID).await?;
    let surviving_alpha = resolve_single(&probe, generation, ALPHA_ID).await?;
    let surviving_gamma = resolve_single(&probe, generation, GAMMA_ID).await?;
    assert_eq!(registration_key(&surviving_alpha)?, sibling_key);
    assert_eq!(registration_key(&surviving_gamma)?, sibling_key);
    assert_eq!(
        surviving_alpha.identity().binding_id().as_uuid(),
        sibling_alpha.as_uuid()
    );
    assert_eq!(
        surviving_gamma.identity().binding_id().as_uuid(),
        sibling_gamma.as_uuid()
    );

    connector
        .send(Frame::Open {
            connection_id: 3,
            client_id: ALPHA_ID.to_owned(),
        })
        .await?;
    let fresh_pipe = match next_sdk_frame(&mut sibling).await? {
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id,
        } if pipe_id.connection_id() == 3
            && binding_id == sibling_alpha
            && client_id == ALPHA_ID =>
        {
            pipe_id
        }
        other => return Err(format!("expected sibling alpha OFFER, got {other:?}").into()),
    };
    sibling
        .send(Frame::OfferAccepted {
            pipe_id: fresh_pipe,
        })
        .await?;
    assert!(matches!(
        next_sdk_frame(&mut connector).await?,
        Frame::Opened { pipe_id } if pipe_id == fresh_pipe
    ));

    drop(connector);
    drop(sibling);
    gateway.stop().await?;
    route_table.stop().await?;
    Ok(())
}

struct RunningGateway {
    endpoint: SocketAddr,
    peer_locator: String,
    gateway: Gateway,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
}

struct DeregisterBlackholeProxy {
    endpoint: SocketAddr,
    shutdown: CancellationToken,
    dropped_deregister: Arc<AtomicBool>,
    task: JoinHandle<io::Result<()>>,
}

struct ControlledRouteProxy {
    endpoint: SocketAddr,
    gate: watch::Sender<bool>,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

struct RegistrationCountingProxy {
    endpoint: SocketAddr,
    shutdown: CancellationToken,
    connection_count: Arc<AtomicUsize>,
    register_count: Arc<AtomicUsize>,
    failed_precondition_count: Arc<AtomicUsize>,
    authentication_rejection_count: Arc<AtomicUsize>,
    task: JoinHandle<io::Result<()>>,
}

impl RegistrationCountingProxy {
    async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let connection_count = Arc::new(AtomicUsize::new(0));
        let register_count = Arc::new(AtomicUsize::new(0));
        let failed_precondition_count = Arc::new(AtomicUsize::new(0));
        let authentication_rejection_count = Arc::new(AtomicUsize::new(0));
        let task_connection_count = Arc::clone(&connection_count);
        let task_register_count = Arc::clone(&register_count);
        let task_failed_precondition_count = Arc::clone(&failed_precondition_count);
        let task_authentication_rejection_count = Arc::clone(&authentication_rejection_count);
        let task = tokio::spawn(async move {
            loop {
                let (inbound, _) = tokio::select! {
                    accepted = listener.accept() => accepted?,
                    () = task_shutdown.cancelled() => return Ok(()),
                };
                task_connection_count.fetch_add(1, Ordering::AcqRel);
                let connection_shutdown = task_shutdown.clone();
                let connection_register_count = Arc::clone(&task_register_count);
                let connection_failed_precondition_count =
                    Arc::clone(&task_failed_precondition_count);
                let connection_authentication_rejection_count =
                    Arc::clone(&task_authentication_rejection_count);
                tokio::spawn(async move {
                    let Ok(outbound) = TcpStream::connect(target).await else {
                        return;
                    };
                    let _ = forward_and_count_route_events(
                        inbound,
                        outbound,
                        connection_shutdown,
                        connection_register_count,
                        connection_failed_precondition_count,
                        connection_authentication_rejection_count,
                    )
                    .await;
                });
            }
        });
        Ok(Self {
            endpoint,
            shutdown,
            connection_count,
            register_count,
            failed_precondition_count,
            authentication_rejection_count,
            task,
        })
    }

    fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Acquire)
    }

    fn register_count(&self) -> usize {
        self.register_count.load(Ordering::Acquire)
    }

    fn failed_precondition_count(&self) -> usize {
        self.failed_precondition_count.load(Ordering::Acquire)
    }

    fn authentication_rejection_count(&self) -> usize {
        self.authentication_rejection_count.load(Ordering::Acquire)
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "Registration counting proxy shutdown timed out")???;
        Ok(())
    }
}

async fn forward_and_count_route_events(
    client: TcpStream,
    server: TcpStream,
    shutdown: CancellationToken,
    register_count: Arc<AtomicUsize>,
    failed_precondition_count: Arc<AtomicUsize>,
    authentication_rejection_count: Arc<AtomicUsize>,
) -> io::Result<()> {
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut server_reader, mut server_writer) = server.into_split();

    loop {
        tokio::select! {
            frame = read_rt_frame(&mut client_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                let is_register = frame_contains(&frame, br#""operation":"REGISTER""#);
                server_writer.write_all(&frame).await?;
                if is_register {
                    register_count.fetch_add(1, Ordering::AcqRel);
                }
            }
            frame = read_rt_frame(&mut server_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                let is_failed_precondition =
                    frame_contains(&frame, br#""code":"FAILED_PRECONDITION""#);
                let is_authentication_rejection =
                    frame_contains(&frame, br#""frame":"HANDSHAKE_REJECTED""#)
                        && frame_contains(&frame, br#""code":"UNAUTHENTICATED""#);
                client_writer.write_all(&frame).await?;
                if is_failed_precondition {
                    failed_precondition_count.fetch_add(1, Ordering::AcqRel);
                }
                if is_authentication_rejection {
                    authentication_rejection_count.fetch_add(1, Ordering::AcqRel);
                }
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

impl ControlledRouteProxy {
    async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let (gate, gate_rx) = watch::channel(true);
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                let (inbound, _) = tokio::select! {
                    accepted = listener.accept() => accepted?,
                    () = task_shutdown.cancelled() => return Ok(()),
                };
                let connection_gate = gate_rx.clone();
                let connection_shutdown = task_shutdown.clone();
                tokio::spawn(async move {
                    let _ =
                        forward_when_open(inbound, target, connection_gate, connection_shutdown)
                            .await;
                });
            }
        });
        Ok(Self {
            endpoint,
            gate,
            shutdown,
            task,
        })
    }

    fn pause(&self) {
        let _ = self.gate.send(false);
    }

    fn resume(&self) {
        let _ = self.gate.send(true);
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "Controlled route proxy shutdown timed out")???;
        Ok(())
    }
}

async fn forward_when_open(
    mut inbound: TcpStream,
    target: SocketAddr,
    mut gate: watch::Receiver<bool>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    while !*gate.borrow_and_update() {
        tokio::select! {
            changed = gate.changed() => changed.map_err(|_| io::ErrorKind::BrokenPipe)?,
            () = shutdown.cancelled() => return Ok(()),
        }
    }
    let mut outbound = TcpStream::connect(target).await?;
    tokio::select! {
        result = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => result.map(|_| ()),
        () = shutdown.cancelled() => Ok(()),
    }
}

impl DeregisterBlackholeProxy {
    async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let dropped_deregister = Arc::new(AtomicBool::new(false));
        let task_dropped_deregister = Arc::clone(&dropped_deregister);
        let task = tokio::spawn(async move {
            let (client, _) = tokio::select! {
                accepted = listener.accept() => accepted?,
                () = task_shutdown.cancelled() => return Ok(()),
            };
            let server = TcpStream::connect(target).await?;
            forward_until_deregister(client, server, task_shutdown, task_dropped_deregister).await
        });
        Ok(Self {
            endpoint,
            shutdown,
            dropped_deregister,
            task,
        })
    }

    fn dropped_deregister(&self) -> bool {
        self.dropped_deregister.load(Ordering::Acquire)
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "Deregister blackhole proxy shutdown timed out")???;
        Ok(())
    }
}

async fn forward_until_deregister(
    client: TcpStream,
    server: TcpStream,
    shutdown: CancellationToken,
    dropped_deregister: Arc<AtomicBool>,
) -> io::Result<()> {
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut server_reader, mut server_writer) = server.into_split();

    loop {
        tokio::select! {
            frame = read_rt_frame(&mut client_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                if frame_contains(&frame, br#""operation":"DEREGISTER""#) {
                    dropped_deregister.store(true, Ordering::Release);
                    return Ok(());
                }
                server_writer.write_all(&frame).await?;
            }
            frame = read_rt_frame(&mut server_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                client_writer.write_all(&frame).await?;
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

async fn read_rt_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 7];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut frame = header.to_vec();
    frame.resize(7 + payload_len, 0);
    reader.read_exact(&mut frame[7..]).await?;
    Ok(Some(frame))
}

fn frame_contains(frame: &[u8], needle: &[u8]) -> bool {
    frame.windows(needle.len()).any(|window| window == needle)
}

impl RunningGateway {
    async fn start(directory: ShardDirectory) -> TestResult<Self> {
        Self::start_with_offer_timeout(directory, Duration::from_millis(100)).await
    }

    async fn start_with_offer_timeout(
        directory: ShardDirectory,
        offer_timeout: Duration,
    ) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::serve(directory, offer_timeout, listener, peer_listener).await
    }

    async fn start_with_drain_timeout(
        directory: ShardDirectory,
        drain_timeout: Duration,
    ) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::serve_with_timeouts(
            directory,
            Duration::from_millis(100),
            drain_timeout,
            listener,
            peer_listener,
        )
        .await
    }

    async fn start_on(
        directory: ShardDirectory,
        offer_timeout: Duration,
        endpoint: SocketAddr,
        peer_endpoint: SocketAddr,
    ) -> TestResult<Self> {
        let listener = bind_listener(endpoint).await?;
        let peer_listener = bind_listener(peer_endpoint).await?;
        Self::serve(directory, offer_timeout, listener, peer_listener).await
    }

    async fn serve(
        directory: ShardDirectory,
        offer_timeout: Duration,
        listener: TcpListener,
        peer_listener: TcpListener,
    ) -> TestResult<Self> {
        Self::serve_with_timeouts(
            directory,
            offer_timeout,
            Duration::from_millis(100),
            listener,
            peer_listener,
        )
        .await
    }

    async fn serve_with_timeouts(
        directory: ShardDirectory,
        offer_timeout: Duration,
        drain_timeout: Duration,
        listener: TcpListener,
        peer_listener: TcpListener,
    ) -> TestResult<Self> {
        let endpoint = listener.local_addr()?;
        let peer_endpoint = peer_listener.local_addr()?;
        let config = GatewayConfig::new(
            [CLIENT_ID, ALPHA_ID, BETA_ID, GAMMA_ID]
                .into_iter()
                .map(|client_id| (client_id.to_owned(), CLIENT_KEY.to_owned())),
        )
        .with_offer_timeout(offer_timeout)
        .with_drain_timeout(drain_timeout);
        let routing_config = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(GATEWAY_NAME)?,
            InternalGatewayKey::new(GATEWAY_KEY)?,
            GatewayLocator::new(peer_endpoint.to_string())?,
            route_client_config()?,
        )
        .with_command_queue_capacity(8)
        .with_reconnect_backoff(ROUTING_RECONNECT_INITIAL, ROUTING_RECONNECT_MAX)
        .with_desired_scan_interval(Duration::from_millis(20))
        .with_shutdown_timeout(Duration::from_millis(200));
        let shutdown = CancellationToken::new();
        let peer_config = GatewayPeerConfig::new(GATEWAY_NAME, GATEWAY_KEY, [])?;
        let gateway = Gateway::new_distributed(config, routing_config, peer_config)?;
        let snapshot_gateway = gateway.clone();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            gateway
                .serve_distributed(listener, peer_listener, serve_shutdown)
                .await
        });
        check(endpoint, Duration::from_secs(1)).await?;
        Ok(Self {
            endpoint,
            peer_locator: peer_endpoint.to_string(),
            gateway: snapshot_gateway,
            shutdown,
            task,
        })
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "Gateway shutdown timed out")???;
        Ok(())
    }
}

async fn bind_listener(endpoint: SocketAddr) -> io::Result<TcpListener> {
    let expires = Instant::now() + Duration::from_secs(1);
    loop {
        match TcpListener::bind(endpoint).await {
            Ok(listener) => return Ok(listener),
            Err(error) if error.kind() == io::ErrorKind::AddrInUse && Instant::now() < expires => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_gateway_snapshot(
    gateway: &RunningGateway,
    matches: impl Fn(GatewaySnapshot) -> bool,
) -> TestResult {
    poll_until(Duration::from_secs(2), || async {
        matches(gateway.gateway.snapshot()).then_some(())
    })
    .await
}

async fn sdk_session(
    endpoint: SocketAddr,
    role: SessionRole,
) -> TestResult<(SdkTransport, SessionId)> {
    let stream = TcpStream::connect(endpoint).await?;
    let mut transport = Framed::new(stream, FrameCodec::default());
    transport.send(Frame::Hello { role }).await?;
    match next_sdk_frame(&mut transport).await? {
        Frame::Welcome { session_id } => Ok((transport, session_id)),
        other => Err(format!("expected WELCOME, got {other:?}").into()),
    }
}

async fn next_sdk_frame(transport: &mut SdkTransport) -> TestResult<Frame> {
    match tokio::time::timeout(Duration::from_secs(2), transport.next()).await {
        Ok(Some(Ok(frame))) => Ok(frame),
        Ok(Some(Err(error))) => Err(error.into()),
        Ok(None) => Err("Gateway closed the SDK session".into()),
        Err(error) => Err(error.into()),
    }
}

async fn register_listener(
    listener: &mut SdkTransport,
    request_id: u64,
    client_id: &str,
) -> TestResult<ProtocolBindingId> {
    listener
        .send(Frame::Register {
            request_id,
            client_id: client_id.to_owned(),
            client_key: ClientKey::new(CLIENT_KEY),
        })
        .await?;
    match next_sdk_frame(listener).await? {
        Frame::Registered {
            request_id: registered_request_id,
            binding_id,
        } if registered_request_id == request_id => Ok(binding_id),
        other => Err(format!("expected REGISTERED for {client_id}, got {other:?}").into()),
    }
}

async fn resolve_entries(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
) -> TestResult<Vec<MappingEntry>> {
    let client_id = ClientId::new(client_id)?;
    Ok(client
        .resolve(generation, &client_id)
        .await?
        .entries()
        .to_vec())
}

async fn resolve_single(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
) -> TestResult<MappingEntry> {
    let entries = resolve_entries(client, generation, client_id).await?;
    match entries.as_slice() {
        [entry] => Ok(entry.clone()),
        _ => Err(format!("expected one {client_id} mapping, got {}", entries.len()).into()),
    }
}

fn registration_key(entry: &MappingEntry) -> TestResult<RegistrationKey> {
    let identity = entry.identity();
    Ok(RegistrationKey::new(
        identity.gateway_id(),
        identity.listener_session_id(),
        ShardId::new(SHARD_ID)?,
    ))
}

struct RunningRouteTable {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), TransportError>>,
    started_empty: bool,
}

impl RunningRouteTable {
    fn serve(
        listener: TcpListener,
        directory: ShardDirectory,
        ttl: Duration,
    ) -> Result<Self, TransportError> {
        Self::serve_with_trusted_gateways(listener, directory, ttl, trusted_gateway_keys()?)
    }

    fn serve_with_trusted_gateways(
        listener: TcpListener,
        directory: ShardDirectory,
        ttl: Duration,
        trusted_gateways: TrustedGatewayKeys,
    ) -> Result<Self, TransportError> {
        let shard = RouteTableShard::new(
            directory,
            ShardId::new(SHARD_ID).map_err(TransportError::from)?,
            RouteTableConfig::new(ttl).map_err(TransportError::from)?,
        )
        .map_err(TransportError::from)?;
        let started_empty = {
            let stats = shard.stats();
            stats.registration_count == 0
                && stats.mapping_count == 0
                && stats.route_count == 0
                && stats.expiry_record_count == 0
        };
        let service = RouteTableService::new(
            shard,
            trusted_gateways,
            RouteTableServiceConfig::new(16, 8, 4, 256 * 1024, Duration::from_secs(1))?,
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(service.serve(listener, shutdown.clone()));
        Ok(Self {
            shutdown,
            task,
            started_empty,
        })
    }

    const fn started_empty(&self) -> bool {
        self.started_empty
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "RouteTable shutdown timed out")???;
        Ok(())
    }
}

async fn connect_probe(
    endpoint: SocketAddr,
    generation: ShardDirectoryGeneration,
) -> Result<RouteTableClient, TransportError> {
    let client = RouteTableClient::connect(
        endpoint,
        GatewayName::new(PROBE_NAME)?,
        GatewayId::new(),
        InternalGatewayKey::new(PROBE_KEY)?,
        route_client_config()?,
    )
    .await?;
    let _ = client
        .resolve(
            generation,
            &ClientId::new(CLIENT_ID).map_err(TransportError::from)?,
        )
        .await;
    Ok(client)
}

async fn wait_for_mapping(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
    expected_locator: &str,
    expected_len: usize,
) -> TestResult {
    let client_id = ClientId::new(client_id)?;
    poll_until(Duration::from_secs(2), || async {
        let bindings = client.resolve(generation, &client_id).await.ok()?;
        let entries = bindings.entries();
        (entries.len() == expected_len
            && entries.iter().all(|entry| {
                entry.client_id() == &client_id
                    && entry.gateway_locator().as_str() == expected_locator
            }))
        .then_some(())
    })
    .await
}

async fn wait_for_not_found(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
) -> TestResult {
    let client_id = ClientId::new(client_id)?;
    poll_until(Duration::from_secs(2), || async {
        let error = client.resolve(generation, &client_id).await.err()?;
        (error.code() == RouteErrorCode::NotFound).then_some(())
    })
    .await
}

async fn wait_for_registration_rejection(proxy: &RegistrationCountingProxy) -> TestResult {
    poll_until(Duration::from_secs(2), || async {
        (proxy.register_count() >= 1 && proxy.failed_precondition_count() >= 1).then_some(())
    })
    .await
}

async fn poll_until<F, Fut>(deadline: Duration, mut probe: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<()>>,
{
    let expires = Instant::now() + deadline;
    loop {
        if probe().await.is_some() {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err("condition did not converge before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn route_client_config() -> Result<RouteTableClientConfig, TransportError> {
    RouteTableClientConfig::new(
        8,
        256 * 1024,
        Duration::from_millis(200),
        ROUTE_HANDSHAKE_TIMEOUT,
        Duration::from_millis(200),
    )
}

fn sdk_config(endpoint: SocketAddr) -> SdkConfig {
    SdkConfig::new(endpoint.to_string())
        .with_connect_timeout(Duration::from_millis(200))
        .with_operation_timeout(Duration::from_secs(1))
        .with_reconnect_backoff(Duration::from_millis(20), Duration::from_millis(40))
}

fn trusted_gateway_keys() -> Result<TrustedGatewayKeys, TransportError> {
    TrustedGatewayKeys::new([
        (
            GatewayName::new(GATEWAY_NAME)?,
            InternalGatewayKey::new(GATEWAY_KEY)?,
        ),
        (
            GatewayName::new(PROBE_NAME)?,
            InternalGatewayKey::new(PROBE_KEY)?,
        ),
    ])
}

fn probe_only_trusted_gateway_keys() -> Result<TrustedGatewayKeys, TransportError> {
    TrustedGatewayKeys::new([(
        GatewayName::new(PROBE_NAME)?,
        InternalGatewayKey::new(PROBE_KEY)?,
    )])
}

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}
