use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_route_table::{
    GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{
    Config as SdkConfig, Connector, ErrorCode as SdkErrorCode, ListenerRuntime,
    PeerObservation as SdkPeerObservation,
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
    peer::{ConnectGate, ResetCommitGate},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_A: &str = "echo.a";
const CLIENT_B: &str = "echo.b";
const CLIENT_KEY: &str = "listener-key";
const GATEWAY_A: &str = "gateway-a";
const GATEWAY_A_KEY: &str = "gateway-a-key";
const GATEWAY_B: &str = "gateway-b";
const GATEWAY_B_KEY: &str = "gateway-b-key";
const SHARD_ID: &str = "rt-0";

struct GatewayFixture {
    name: &'static str,
    key: &'static str,
    peer_name: &'static str,
    peer_key: &'static str,
    connect_gate: ConnectGate,
    inbound_admission_gate: ConnectGate,
    reset_commit_gate: Option<ResetCommitGate>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_commit_failure_closes_only_current_peer_transport_scope() -> TestResult {
    timeout(
        Duration::from_secs(15),
        reset_commit_failure_closes_only_current_peer_transport_scope_case(),
    )
    .await??;
    Ok(())
}

async fn reset_commit_failure_closes_only_current_peer_transport_scope_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;
    let connect_gate_a = ConnectGate::new();
    let connect_gate_b = ConnectGate::new();
    let inbound_gate_a = ConnectGate::new();
    let inbound_gate_b = ConnectGate::new();
    let reset_gate_a = ResetCommitGate::new();

    let gateway_a = RunningGateway::start(
        directory.clone(),
        GatewayFixture {
            name: GATEWAY_A,
            key: GATEWAY_A_KEY,
            peer_name: GATEWAY_B,
            peer_key: GATEWAY_B_KEY,
            connect_gate: connect_gate_a.clone(),
            inbound_admission_gate: inbound_gate_a.clone(),
            reset_commit_gate: Some(reset_gate_a.clone()),
        },
    )
    .await?;
    let gateway_b = RunningGateway::start(
        directory,
        GatewayFixture {
            name: GATEWAY_B,
            key: GATEWAY_B_KEY,
            peer_name: GATEWAY_A,
            peer_key: GATEWAY_A_KEY,
            connect_gate: connect_gate_b.clone(),
            inbound_admission_gate: inbound_gate_b.clone(),
            reset_commit_gate: None,
        },
    )
    .await?;

    let listener_runtime_a = ListenerRuntime::connect(sdk_config(gateway_a.sdk_address)).await?;
    let listener_a = listener_runtime_a.listen(CLIENT_A, CLIENT_KEY).await?;
    let listener_runtime_b = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener_b = listener_runtime_b.listen(CLIENT_B, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(5), || {
        gateway_a.gateway.snapshot().route_registrations_synced == 1
            && gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;

    let connector_a0 = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let connector_a1 = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let connector_a2 = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let connector_b = Connector::connect(sdk_config(gateway_b.sdk_address)).await?;

    let open_a0_to_b = {
        let connector = connector_a0.clone();
        tokio::spawn(async move { connector.open(CLIENT_B).await })
    };
    timeout(Duration::from_secs(2), connect_gate_a.wait_until_entered()).await?;
    let open_a1_to_b = {
        let connector = connector_a1.clone();
        tokio::spawn(async move { connector.open(CLIENT_B).await })
    };
    let open_a2_to_b = {
        let connector = connector_a2.clone();
        tokio::spawn(async move { connector.open(CLIENT_B).await })
    };
    let open_b_to_a = {
        let connector = connector_b.clone();
        tokio::spawn(async move { connector.open(CLIENT_A).await })
    };
    timeout(Duration::from_secs(2), connect_gate_b.wait_until_entered()).await?;
    connect_gate_a.release();
    connect_gate_b.release();
    timeout(Duration::from_secs(2), inbound_gate_a.wait_until_entered()).await?;
    timeout(Duration::from_secs(2), inbound_gate_b.wait_until_entered()).await?;
    wait_until(Duration::from_secs(2), || {
        let gateway_a = gateway_a.gateway.snapshot();
        let gateway_b = gateway_b.gateway.snapshot();
        gateway_a.peer_transports_ready == 1
            && gateway_a.peer_streams == 3
            && gateway_b.peer_transports_ready == 1
            && gateway_b.peer_streams == 1
    })
    .await?;
    inbound_gate_a.release();
    inbound_gate_b.release();

    let mut listener_b_pipes = [
        listener_b.accept().await?,
        listener_b.accept().await?,
        listener_b.accept().await?,
    ];
    let mut listener_a_pipe = listener_a.accept().await?;
    let mut connector_a_pipe0 = open_a0_to_b.await??;
    let mut connector_a_pipe1 = open_a1_to_b.await??;
    let mut connector_a_pipe2 = open_a2_to_b.await??;
    let mut connector_b_pipe = open_b_to_a.await??;

    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.peer_transports_ready == 2
            && owner.peer_transports_ready == 2
            && entry.peer_streams == 4
            && owner.peer_streams == 4
    })
    .await?;

    timeout(
        Duration::from_secs(1),
        connector_a_pipe0.write_all_bytes(&[0]),
    )
    .await??;
    timeout(
        Duration::from_secs(1),
        connector_a_pipe1.write_all_bytes(&[1]),
    )
    .await??;
    timeout(
        Duration::from_secs(1),
        connector_a_pipe2.write_all_bytes(&[2]),
    )
    .await??;
    let mut listener_for_connector = [usize::MAX; 3];
    for (listener_index, listener_pipe) in listener_b_pipes.iter_mut().enumerate() {
        let mut connector_tag = [0_u8; 1];
        timeout(
            Duration::from_secs(1),
            listener_pipe.read_exact(&mut connector_tag),
        )
        .await??;
        let connector_index = usize::from(connector_tag[0]);
        if connector_index >= listener_for_connector.len()
            || listener_for_connector[connector_index] != usize::MAX
        {
            return Err(format!("invalid connector tag: {connector_tag:?}").into());
        }
        listener_for_connector[connector_index] = listener_index;
    }

    connector_a0.close();
    wait_until(Duration::from_secs(5), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.peer_transports_ready == 2
            && owner.peer_transports_ready == 2
            && entry.peer_streams == 3
            && owner.peer_streams == 3
            && entry.live_pipes == 3
            && owner.live_pipes == 3
    })
    .await?;
    let mut normally_closed_payload = [0_u8; 1];
    let normal_cleanup = timeout(
        Duration::from_secs(1),
        listener_b_pipes[listener_for_connector[0]].read_into(&mut normally_closed_payload),
    )
    .await?
    .err()
    .ok_or("normal ConnectorSession cleanup did not reset its Listener Pipe")?;
    assert_eq!(normal_cleanup.code(), SdkErrorCode::Cancelled);
    assert_eq!(normal_cleanup.observation(), SdkPeerObservation::Observed);
    timeout(
        Duration::from_secs(1),
        connector_a_pipe1.write_all_bytes(b"sibling-before-failure"),
    )
    .await??;
    let mut sibling_payload = [0_u8; 22];
    timeout(
        Duration::from_secs(1),
        listener_b_pipes[listener_for_connector[1]].read_exact(&mut sibling_payload),
    )
    .await??;
    assert_eq!(&sibling_payload, b"sibling-before-failure");

    reset_gate_a.arm();
    connector_a1.close();
    timeout(Duration::from_secs(2), reset_gate_a.wait_until_tripped()).await?;

    let converged = wait_until(Duration::from_secs(5), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.peer_transports_ready == 1
            && owner.peer_transports_ready == 1
            && entry.peer_streams == 1
            && owner.peer_streams == 1
            && entry.remote_open_attempts == 0
            && owner.remote_open_attempts == 0
            && entry.live_pipes == 1
            && owner.live_pipes == 1
            && entry.listener_bindings == 1
            && owner.listener_bindings == 1
            && entry.route_registrations_synced == 1
            && owner.route_registrations_synced == 1
    })
    .await;
    if converged.is_err() {
        return Err(format!(
            "cleanup scope did not converge: entry={:?}, owner={:?}",
            gateway_a.gateway.snapshot(),
            gateway_b.gateway.snapshot()
        )
        .into());
    }
    let sibling_error = timeout(
        Duration::from_secs(1),
        connector_a_pipe2.write_all_bytes(b"sibling after transport close"),
    )
    .await?
    .err()
    .ok_or("sibling Pipe on the closed transport unexpectedly survived")?;
    assert_eq!(sibling_error.code(), SdkErrorCode::Unavailable);
    assert_eq!(sibling_error.observation(), SdkPeerObservation::Observed);
    for connector_index in [1, 2] {
        let mut lost_payload = [0_u8; 1];
        let owner_error = timeout(
            Duration::from_secs(1),
            listener_b_pipes[listener_for_connector[connector_index]].read_into(&mut lost_payload),
        )
        .await?
        .err()
        .ok_or("Listener Pipe on the closed transport unexpectedly survived")?;
        assert_eq!(owner_error.code(), SdkErrorCode::Unavailable);
        assert_eq!(owner_error.observation(), SdkPeerObservation::Observed);
    }

    timeout(
        Duration::from_secs(1),
        connector_b_pipe.write_all(b"reverse-survives"),
    )
    .await??;
    let mut reverse_payload = [0_u8; 16];
    timeout(
        Duration::from_secs(1),
        listener_a_pipe.read_exact(&mut reverse_payload),
    )
    .await??;
    assert_eq!(&reverse_payload, b"reverse-survives");
    timeout(
        Duration::from_secs(1),
        listener_a_pipe.write_all(b"reverse-returns"),
    )
    .await??;
    let mut reverse_reply = [0_u8; 15];
    timeout(
        Duration::from_secs(1),
        connector_b_pipe.read_exact(&mut reverse_reply),
    )
    .await??;
    assert_eq!(&reverse_reply, b"reverse-returns");

    let fresh_open = connector_a2.open(CLIENT_B);
    let fresh_accept = listener_b.accept();
    let (fresh_connector, fresh_listener) = tokio::join!(fresh_open, fresh_accept);
    let mut fresh_connector = fresh_connector?;
    let mut fresh_listener = fresh_listener?;
    timeout(Duration::from_secs(1), fresh_connector.write_all(b"fresh")).await??;
    let mut fresh_payload = [0_u8; 5];
    timeout(
        Duration::from_secs(1),
        fresh_listener.read_exact(&mut fresh_payload),
    )
    .await??;
    assert_eq!(&fresh_payload, b"fresh");

    for listener_pipe in &mut listener_b_pipes {
        let _ = listener_pipe.close().await;
    }
    let _ = listener_a_pipe.close().await;
    let _ = connector_a_pipe0.close().await;
    let _ = connector_a_pipe1.close().await;
    let _ = connector_a_pipe2.close().await;
    let _ = connector_b_pipe.close().await;
    let _ = fresh_connector.close().await;
    let _ = fresh_listener.close().await;
    connector_a0.close();
    connector_a1.close();
    connector_a2.close();
    connector_b.close();
    listener_runtime_a.close();
    listener_runtime_b.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    route_table.stop().await?;
    Ok(())
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
        let mut peer = GatewayPeerConfig::new(
            fixture.name,
            fixture.key,
            [TrustedPeerConfig::new(fixture.peer_name, fixture.peer_key)?],
        )?
        .with_timeouts(
            Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(2),
        )
        .with_connect_gate(fixture.connect_gate)
        .with_inbound_admission_gate(fixture.inbound_admission_gate);
        if let Some(gate) = fixture.reset_commit_gate {
            peer = peer.with_reset_commit_gate(gate);
        }
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([
                (CLIENT_A.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_B.to_owned(), CLIENT_KEY.to_owned()),
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
        let result = timeout(Duration::from_secs(2), self.task)
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
        let trusted = TrustedGatewayKeys::new([
            (
                GatewayName::new(GATEWAY_A)?,
                InternalGatewayKey::new(GATEWAY_A_KEY)?,
            ),
            (
                GatewayName::new(GATEWAY_B)?,
                InternalGatewayKey::new(GATEWAY_B_KEY)?,
            ),
        ])?;
        let service = RouteTableService::new(
            shard,
            trusted,
            RouteTableServiceConfig::new(32, 16, 8, 256 * 1024, Duration::from_millis(200))?,
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
        16,
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

async fn wait_until(deadline: Duration, mut condition: impl FnMut() -> bool) -> TestResult {
    let expires = Instant::now() + deadline;
    while !condition() {
        if Instant::now() >= expires {
            return Err("condition did not converge before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}
