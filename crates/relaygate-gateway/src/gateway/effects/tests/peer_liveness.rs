use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{
    Config as SdkConfig, Connector, Error as SdkError, ErrorCode as SdkErrorCode, Listener,
    ListenerRuntime, PeerObservation as SdkPeerObservation, Pipe,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    TrustedPeerConfig, check,
    peer::{ConnectGate, DropHeartbeatPongGate},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_KEY: &str = "listener-key";
const CLIENT_A: &str = "echo.a";
const CLIENT_B: &str = "echo.b";
const CLIENT_C: &str = "echo.c";
const GATEWAY_A: &str = "gateway-a";
const GATEWAY_A_KEY: &str = "gateway-a-key";
const GATEWAY_B: &str = "gateway-b";
const GATEWAY_B_KEY: &str = "gateway-b-key";
const GATEWAY_C: &str = "gateway-c";
const GATEWAY_C_KEY: &str = "gateway-c-key";
const SHARD_ID: &str = "rt-0";

const ALL_GATEWAYS: [(&str, &str); 3] = [
    (GATEWAY_A, GATEWAY_A_KEY),
    (GATEWAY_B, GATEWAY_B_KEY),
    (GATEWAY_C, GATEWAY_C_KEY),
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum LivenessCase {
    ActiveHeartbeatTimeout,
    IdleRetirement,
}

#[derive(Clone, Copy)]
struct LivenessConfig {
    heartbeat_idle: Duration,
    heartbeat_response: Duration,
    idle_retirement: Duration,
}

struct GatewayFixture {
    name: &'static str,
    key: &'static str,
    liveness: LivenessConfig,
    connect_gate: Option<ConnectGate>,
    inbound_admission_gate: Option<ConnectGate>,
    drop_pong_gate: Option<DropHeartbeatPongGate>,
}

struct OppositePairFixture<'a> {
    connector_a: &'a Connector,
    connector_b: &'a Connector,
    listener_a: &'a Listener,
    listener_b: &'a Listener,
    gateway_a: &'a RunningGateway,
    gateway_b: &'a RunningGateway,
    connect_gate_a: &'a ConnectGate,
    connect_gate_b: &'a ConnectGate,
    inbound_gate_a: &'a ConnectGate,
    inbound_gate_b: &'a ConnectGate,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn active_heartbeat_timeout_preserves_unrelated_runtime_state() -> TestResult {
    timeout(
        Duration::from_secs(12),
        peer_liveness_scope_case(LivenessCase::ActiveHeartbeatTimeout),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn idle_retirement_preserves_unrelated_runtime_state() -> TestResult {
    timeout(
        Duration::from_secs(12),
        peer_liveness_scope_case(LivenessCase::IdleRetirement),
    )
    .await??;
    Ok(())
}

async fn peer_liveness_scope_case(case: LivenessCase) -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let connect_gate_a = ConnectGate::new();
    let connect_gate_b = ConnectGate::new();
    let inbound_gate_a = ConnectGate::new();
    let inbound_gate_b = ConnectGate::new();
    let drop_pong_gate = DropHeartbeatPongGate::new();
    let gateway_a_liveness = match case {
        LivenessCase::ActiveHeartbeatTimeout => LivenessConfig {
            heartbeat_idle: Duration::from_millis(40),
            heartbeat_response: Duration::from_millis(40),
            idle_retirement: Duration::from_secs(1),
        },
        LivenessCase::IdleRetirement => LivenessConfig {
            heartbeat_idle: Duration::from_secs(1),
            heartbeat_response: Duration::from_millis(100),
            idle_retirement: Duration::from_millis(60),
        },
    };
    let stable_liveness = LivenessConfig {
        heartbeat_idle: Duration::from_secs(1),
        heartbeat_response: Duration::from_millis(100),
        idle_retirement: Duration::from_secs(1),
    };

    let gateway_a = RunningGateway::start(
        directory.clone(),
        GatewayFixture {
            name: GATEWAY_A,
            key: GATEWAY_A_KEY,
            liveness: gateway_a_liveness,
            connect_gate: Some(connect_gate_a.clone()),
            inbound_admission_gate: Some(inbound_gate_a.clone()),
            drop_pong_gate: (case == LivenessCase::ActiveHeartbeatTimeout)
                .then(|| drop_pong_gate.clone()),
        },
    )
    .await?;
    let gateway_b = RunningGateway::start(
        directory.clone(),
        GatewayFixture {
            name: GATEWAY_B,
            key: GATEWAY_B_KEY,
            liveness: stable_liveness,
            connect_gate: Some(connect_gate_b.clone()),
            inbound_admission_gate: Some(inbound_gate_b.clone()),
            drop_pong_gate: None,
        },
    )
    .await?;
    let gateway_c = RunningGateway::start(
        directory,
        GatewayFixture {
            name: GATEWAY_C,
            key: GATEWAY_C_KEY,
            liveness: stable_liveness,
            connect_gate: None,
            inbound_admission_gate: None,
            drop_pong_gate: None,
        },
    )
    .await?;

    let listener_runtime_a = ListenerRuntime::connect(sdk_config(gateway_a.sdk_address)).await?;
    let listener_runtime_b = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener_runtime_c = ListenerRuntime::connect(sdk_config(gateway_c.sdk_address)).await?;
    let listener_a = listener_runtime_a.listen(CLIENT_A, CLIENT_KEY).await?;
    let listener_b = listener_runtime_b.listen(CLIENT_B, CLIENT_KEY).await?;
    let listener_c = listener_runtime_c.listen(CLIENT_C, CLIENT_KEY).await?;
    wait_until("all registrations synced", Duration::from_secs(2), || {
        [&gateway_a, &gateway_b, &gateway_c]
            .into_iter()
            .all(|gateway| gateway.gateway.snapshot().route_registrations_synced == 1)
    })
    .await?;

    let route_observer = RouteTableClient::connect(
        route_endpoint,
        GatewayName::new(GATEWAY_A)?,
        GatewayId::new(),
        InternalGatewayKey::new(GATEWAY_A_KEY)?,
        route_client_config()?,
    )
    .await?;
    assert_current_routes(&route_observer, generation).await?;

    let connector_a = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let connector_b = Connector::connect(sdk_config(gateway_b.sdk_address)).await?;
    let (mut target_connector, mut target_listener, mut reverse_connector, mut reverse_listener) =
        open_opposite_pair(OppositePairFixture {
            connector_a: &connector_a,
            connector_b: &connector_b,
            listener_a: &listener_a,
            listener_b: &listener_b,
            gateway_a: &gateway_a,
            gateway_b: &gateway_b,
            connect_gate_a: &connect_gate_a,
            connect_gate_b: &connect_gate_b,
            inbound_gate_a: &inbound_gate_a,
            inbound_gate_b: &inbound_gate_b,
        })
        .await?;

    connect_gate_b.release();
    let (mut other_connector, mut other_listener) =
        open_pipe(&connector_b, CLIENT_C, &listener_c).await?;
    wait_until(
        "three directional transports ready",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.peer_transports_connecting == 0
                && a.peer_transports_ready == 2
                && a.peer_streams == 2
                && a.live_pipes == 2
                && b.peer_transports_connecting == 0
                && b.peer_transports_ready == 3
                && b.peer_streams == 3
                && b.live_pipes == 3
                && c.peer_transports_connecting == 0
                && c.peer_transports_ready == 1
                && c.peer_streams == 1
                && c.live_pipes == 1
        },
    )
    .await?;
    assert_bidirectional(&mut target_connector, &mut target_listener, "target-before").await?;
    assert_bidirectional(
        &mut reverse_connector,
        &mut reverse_listener,
        "reverse-before",
    )
    .await?;
    assert_bidirectional(&mut other_connector, &mut other_listener, "other-before").await?;

    match case {
        LivenessCase::ActiveHeartbeatTimeout => {
            drop_pong_gate.arm();
            timeout(Duration::from_secs(2), drop_pong_gate.wait_until_tripped()).await?;
        }
        LivenessCase::IdleRetirement => {
            target_connector.close().await?;
            target_listener.close().await?;
        }
    }

    wait_until("fault scope cleanup", Duration::from_secs(3), || {
        let a = gateway_a.gateway.snapshot();
        let b = gateway_b.gateway.snapshot();
        let c = gateway_c.gateway.snapshot();
        a.peer_transports_connecting == 0
            && a.peer_transports_ready == 1
            && a.peer_streams == 1
            && a.live_pipes == 1
            && a.remote_open_attempts == 0
            && b.peer_transports_connecting == 0
            && b.peer_transports_ready == 2
            && b.peer_streams == 2
            && b.live_pipes == 2
            && b.remote_open_attempts == 0
            && c.peer_transports_connecting == 0
            && c.peer_transports_ready == 1
            && c.peer_streams == 1
            && c.live_pipes == 1
            && c.remote_open_attempts == 0
            && [a, b, c].into_iter().all(|snapshot| {
                snapshot.listener_bindings == 1 && snapshot.route_registrations_synced == 1
            })
    })
    .await?;

    if case == LivenessCase::ActiveHeartbeatTimeout {
        assert_connector_unavailable(&mut target_connector).await?;
        assert_listener_unavailable(&mut target_listener).await?;
    }
    assert_current_routes(&route_observer, generation).await?;
    assert_bidirectional(
        &mut reverse_connector,
        &mut reverse_listener,
        "reverse-after",
    )
    .await?;
    assert_bidirectional(&mut other_connector, &mut other_listener, "other-after").await?;

    let (mut fresh_connector, mut fresh_listener) =
        open_pipe(&connector_a, CLIENT_B, &listener_b).await?;
    wait_until(
        "surviving opposite transport reused",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.peer_transports_connecting == 0
                && a.peer_transports_ready == 1
                && a.peer_streams == 2
                && a.live_pipes == 2
                && b.peer_transports_connecting == 0
                && b.peer_transports_ready == 2
                && b.peer_streams == 3
                && b.live_pipes == 3
                && c.peer_transports_connecting == 0
                && c.peer_transports_ready == 1
                && c.peer_streams == 1
                && c.live_pipes == 1
        },
    )
    .await?;
    assert_bidirectional(&mut fresh_connector, &mut fresh_listener, "fresh").await?;

    reverse_connector.close().await?;
    reverse_listener.close().await?;
    fresh_connector.close().await?;
    fresh_listener.close().await?;
    wait_until("drained peer pair retires", Duration::from_secs(3), || {
        let a = gateway_a.gateway.snapshot();
        let b = gateway_b.gateway.snapshot();
        let c = gateway_c.gateway.snapshot();
        a.peer_transports_connecting == 0
            && a.peer_transports_ready == 0
            && a.peer_streams == 0
            && a.live_pipes == 0
            && b.peer_transports_connecting == 0
            && b.peer_transports_ready == 1
            && b.peer_streams == 1
            && b.live_pipes == 1
            && c.peer_transports_connecting == 0
            && c.peer_transports_ready == 1
            && c.peer_streams == 1
            && c.live_pipes == 1
    })
    .await?;

    let recovered_open = {
        let connector = connector_a.clone();
        tokio::spawn(async move { connector.open(CLIENT_B).await })
    };
    timeout(Duration::from_secs(2), connect_gate_a.wait_until_entered()).await?;
    wait_until(
        "retired peer transport reconnecting",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            a.remote_open_attempts == 1
                && a.peer_transports_connecting == 1
                && a.peer_transports_ready == 0
                && a.peer_streams == 0
        },
    )
    .await?;
    connect_gate_a.release();
    timeout(Duration::from_secs(2), inbound_gate_b.wait_until_entered()).await?;
    inbound_gate_b.release();
    let mut recovered_listener = timeout(Duration::from_secs(2), listener_b.accept()).await??;
    let mut recovered_connector = timeout(Duration::from_secs(2), recovered_open).await???;
    wait_until(
        "retired peer transport ready again",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.remote_open_attempts == 0
                && a.peer_transports_connecting == 0
                && a.peer_transports_ready == 1
                && a.peer_streams == 1
                && a.live_pipes == 1
                && b.remote_open_attempts == 0
                && b.peer_transports_connecting == 0
                && b.peer_transports_ready == 2
                && b.peer_streams == 2
                && b.live_pipes == 2
                && c.remote_open_attempts == 0
                && c.peer_transports_connecting == 0
                && c.peer_transports_ready == 1
                && c.peer_streams == 1
                && c.live_pipes == 1
        },
    )
    .await?;
    assert_bidirectional(
        &mut recovered_connector,
        &mut recovered_listener,
        "recovered",
    )
    .await?;

    let _ = target_connector.close().await;
    let _ = target_listener.close().await;
    let _ = other_connector.close().await;
    let _ = other_listener.close().await;
    let _ = recovered_connector.close().await;
    let _ = recovered_listener.close().await;
    connector_a.close();
    connector_b.close();
    listener_runtime_a.close();
    listener_runtime_b.close();
    listener_runtime_c.close();
    drop(route_observer);
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    gateway_c.stop().await?;
    route_table.stop().await?;
    Ok(())
}

async fn open_opposite_pair(
    fixture: OppositePairFixture<'_>,
) -> TestResult<(Pipe, Pipe, Pipe, Pipe)> {
    let open_a_to_b = {
        let connector = fixture.connector_a.clone();
        tokio::spawn(async move { connector.open(CLIENT_B).await })
    };
    timeout(
        Duration::from_secs(2),
        fixture.connect_gate_a.wait_until_entered(),
    )
    .await?;
    let open_b_to_a = {
        let connector = fixture.connector_b.clone();
        tokio::spawn(async move { connector.open(CLIENT_A).await })
    };
    timeout(
        Duration::from_secs(2),
        fixture.connect_gate_b.wait_until_entered(),
    )
    .await?;
    wait_until(
        "opposite outbound transports connecting",
        Duration::from_secs(2),
        || {
            let a = fixture.gateway_a.gateway.snapshot();
            let b = fixture.gateway_b.gateway.snapshot();
            a.remote_open_attempts == 1
                && a.peer_transports_connecting == 1
                && a.peer_transports_ready == 0
                && a.peer_streams == 0
                && b.remote_open_attempts == 1
                && b.peer_transports_connecting == 1
                && b.peer_transports_ready == 0
                && b.peer_streams == 0
        },
    )
    .await?;
    fixture.connect_gate_a.release();
    fixture.connect_gate_b.release();
    timeout(
        Duration::from_secs(2),
        fixture.inbound_gate_a.wait_until_entered(),
    )
    .await?;
    timeout(
        Duration::from_secs(2),
        fixture.inbound_gate_b.wait_until_entered(),
    )
    .await?;
    wait_until(
        "outbound directions admitted",
        Duration::from_secs(2),
        || {
            let a = fixture.gateway_a.gateway.snapshot();
            let b = fixture.gateway_b.gateway.snapshot();
            a.peer_transports_ready == 1
                && a.peer_streams == 1
                && b.peer_transports_ready == 1
                && b.peer_streams == 1
        },
    )
    .await?;
    fixture.inbound_gate_a.release();
    fixture.inbound_gate_b.release();

    let target_listener = timeout(Duration::from_secs(2), fixture.listener_b.accept()).await??;
    let reverse_listener = timeout(Duration::from_secs(2), fixture.listener_a.accept()).await??;
    let target_connector = timeout(Duration::from_secs(2), open_a_to_b).await???;
    let reverse_connector = timeout(Duration::from_secs(2), open_b_to_a).await???;
    wait_until("opposite directions ready", Duration::from_secs(2), || {
        let a = fixture.gateway_a.gateway.snapshot();
        let b = fixture.gateway_b.gateway.snapshot();
        a.peer_transports_connecting == 0
            && a.peer_transports_ready == 2
            && a.peer_streams == 2
            && b.peer_transports_connecting == 0
            && b.peer_transports_ready == 2
            && b.peer_streams == 2
    })
    .await?;
    Ok((
        target_connector,
        target_listener,
        reverse_connector,
        reverse_listener,
    ))
}

async fn open_pipe(
    connector: &Connector,
    client_id: &str,
    listener: &Listener,
) -> TestResult<(Pipe, Pipe)> {
    timeout(Duration::from_secs(2), async {
        let (connector_pipe, listener_pipe) =
            tokio::join!(connector.open(client_id), listener.accept());
        Ok::<_, relaygate_sdk::Error>((connector_pipe?, listener_pipe?))
    })
    .await
    .map_err(|_| "Pipe open timed out")?
    .map_err(Into::into)
}

async fn assert_bidirectional(
    connector: &mut Pipe,
    listener: &mut Pipe,
    marker: &str,
) -> TestResult {
    timeout(Duration::from_secs(1), async {
        let toward_listener = format!("connector:{marker}").into_bytes();
        connector.write_all(&toward_listener).await?;
        let mut received = vec![0; toward_listener.len()];
        listener.read_exact(&mut received).await?;
        if received != toward_listener {
            return Err(io::Error::other("Listener received different bytes"));
        }

        let toward_connector = format!("listener:{marker}").into_bytes();
        listener.write_all(&toward_connector).await?;
        let mut received = vec![0; toward_connector.len()];
        connector.read_exact(&mut received).await?;
        if received != toward_connector {
            return Err(io::Error::other("Connector received different bytes"));
        }
        Ok::<_, io::Error>(())
    })
    .await??;
    Ok(())
}

async fn assert_connector_unavailable(pipe: &mut Pipe) -> TestResult {
    let mut byte = [0_u8; 1];
    let error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or("target Connector Pipe unexpectedly remained readable")?;
    assert_observed_unavailable(&error, "target Connector Pipe")
}

async fn assert_listener_unavailable(pipe: &mut Pipe) -> TestResult {
    let mut byte = [0_u8; 1];
    let error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or("target Listener Pipe unexpectedly remained readable")?;
    assert_observed_unavailable(&error, "target Listener Pipe")
}

fn assert_observed_unavailable(error: &SdkError, label: &str) -> TestResult {
    if error.code() != SdkErrorCode::Unavailable
        || error.observation() != SdkPeerObservation::Observed
    {
        return Err(format!("{label} returned unexpected terminal error: {error}").into());
    }
    Ok(())
}

async fn assert_current_routes(
    client: &RouteTableClient,
    generation: relaygate_route_table::ShardDirectoryGeneration,
) -> TestResult {
    for client_id in [CLIENT_A, CLIENT_B, CLIENT_C] {
        wait_for_binding_count(client, generation, client_id, 1, Duration::from_secs(2)).await?;
    }
    Ok(())
}

async fn wait_for_binding_count(
    client: &RouteTableClient,
    generation: relaygate_route_table::ShardDirectoryGeneration,
    client_id: &str,
    expected: usize,
    deadline: Duration,
) -> TestResult {
    let client_id = ClientId::new(client_id)?;
    let expires = Instant::now() + deadline;
    loop {
        if client
            .resolve(generation, &client_id)
            .await
            .is_ok_and(|bindings| bindings.len() == expected)
        {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err(format!(
                "RouteTable did not converge to {expected} bindings for {client_id}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

struct RunningGateway {
    name: String,
    sdk_address: SocketAddr,
    gateway: Gateway,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl RunningGateway {
    async fn start(directory: ShardDirectory, fixture: GatewayFixture) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        let sdk_address = sdk_listener.local_addr()?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_address = peer_listener.local_addr()?;
        let routing = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(fixture.name)?,
            InternalGatewayKey::new(fixture.key)?,
            GatewayLocator::new(peer_address.to_string())?,
            route_client_config()?,
        )
        .with_command_queue_capacity(16)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(200));
        let trusted_peers = ALL_GATEWAYS
            .into_iter()
            .filter(|(name, _)| *name != fixture.name)
            .map(|(name, key)| TrustedPeerConfig::new(name, key))
            .collect::<Result<Vec<_>, _>>()?;
        let mut peer = GatewayPeerConfig::new(fixture.name, fixture.key, trusted_peers)?
            .with_timeouts(
                Duration::from_secs(2),
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
            .with_liveness(
                fixture.liveness.heartbeat_idle,
                fixture.liveness.heartbeat_response,
                fixture.liveness.idle_retirement,
            );
        if let Some(gate) = fixture.connect_gate {
            peer = peer.with_connect_gate(gate);
        }
        if let Some(gate) = fixture.inbound_admission_gate {
            peer = peer.with_inbound_admission_gate(gate);
        }
        if let Some(gate) = fixture.drop_pong_gate {
            peer = peer.with_drop_dialer_heartbeat_pong_gate(gate);
        }
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([
                (CLIENT_A.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_B.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_C.to_owned(), CLIENT_KEY.to_owned()),
            ])
            .with_max_pending_offers(8),
            routing,
            peer,
            shutdown.clone(),
        )?;
        let served = gateway.clone();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            served
                .serve_distributed(sdk_listener, peer_listener, serve_shutdown)
                .await
        });
        check(sdk_address, Duration::from_secs(1)).await?;
        Ok(Self {
            name: fixture.name.to_owned(),
            sdk_address,
            gateway,
            shutdown,
            task,
        })
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        let result = timeout(Duration::from_secs(3), self.task)
            .await
            .map_err(|_| io::Error::other(format!("{} shutdown timed out", self.name)))?
            .map_err(|error| {
                io::Error::other(format!("{} task could not be joined: {error}", self.name))
            })?;
        result.map_err(|error| {
            io::Error::other(format!("{} shutdown failed: {error}", self.name)).into()
        })
    }
}

struct RunningRouteTable {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), TransportError>>,
}

impl RunningRouteTable {
    fn start(listener: TcpListener, directory: ShardDirectory) -> Result<Self, TransportError> {
        let shard = RouteTableShard::new(
            directory,
            ShardId::new(SHARD_ID).map_err(TransportError::from)?,
            RouteTableConfig::new(Duration::from_secs(2)).map_err(TransportError::from)?,
        )
        .map_err(TransportError::from)?;
        let trusted = TrustedGatewayKeys::new(
            ALL_GATEWAYS
                .into_iter()
                .map(|(name, key)| Ok((GatewayName::new(name)?, InternalGatewayKey::new(key)?)))
                .collect::<Result<Vec<_>, TransportError>>()?,
        )?;
        let service = RouteTableService::new(
            shard,
            trusted,
            RouteTableServiceConfig::new(64, 32, 16, 256 * 1024, Duration::from_millis(200))?,
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(service.serve(listener, shutdown.clone()));
        Ok(Self { shutdown, task })
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "RouteTable shutdown timed out")???;
        Ok(())
    }
}

fn route_client_config() -> Result<RouteTableClientConfig, TransportError> {
    RouteTableClientConfig::new(
        32,
        256 * 1024,
        Duration::from_millis(200),
        Duration::from_millis(200),
        Duration::from_millis(200),
    )
}

fn sdk_config(endpoint: SocketAddr) -> SdkConfig {
    SdkConfig::new(endpoint.to_string())
        .with_connect_timeout(Duration::from_millis(200))
        .with_operation_timeout(Duration::from_secs(2))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
}

async fn wait_until(
    label: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> bool,
) -> TestResult {
    let expires = Instant::now() + deadline;
    while !condition() {
        if Instant::now() >= expires {
            return Err(format!("{label} did not converge before deadline").into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}
