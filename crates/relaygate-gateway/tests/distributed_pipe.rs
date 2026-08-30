use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    TrustedPeerConfig, check,
};
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

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_ID: &str = "echo.remote";
const CLIENT_KEY: &str = "listener-key";
const GATEWAY_A: &str = "gateway-a";
const GATEWAY_A_KEY: &str = "gateway-a-key";
const GATEWAY_B: &str = "gateway-b";
const GATEWAY_B_KEY: &str = "gateway-b-key";
const SHARD_ID: &str = "rt-0";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pipes_share_one_peer_transport_and_survive_route_table_loss() -> TestResult {
    timeout(Duration::from_secs(10), remote_pipe_case()).await??;
    Ok(())
}

async fn remote_pipe_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
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
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(2), || {
        gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    let connector = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;

    let mut first_connector = connector.open(CLIENT_ID).await?;
    let mut first_listener = listener.accept().await?;
    let mut second_connector = connector.open(CLIENT_ID).await?;
    let mut second_listener = listener.accept().await?;

    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.peer_transports_ready == 1
            && owner.peer_transports_ready == 1
            && entry.peer_streams == 2
            && owner.peer_streams == 2
    })
    .await?;
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;

    route_table.stop().await?;

    let failed_open = connector
        .open(CLIENT_ID)
        .await
        .err()
        .ok_or("remote open unexpectedly succeeded while RouteTable was unavailable")?;
    assert_eq!(failed_open.code(), SdkErrorCode::Unavailable);
    assert_eq!(failed_open.observation(), SdkPeerObservation::NotObserved);
    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.remote_open_attempts == 0 && entry.peer_streams == 2 && owner.peer_streams == 2
    })
    .await?;

    first_connector.write_all(b"from connector").await?;
    let mut from_connector = [0_u8; 14];
    first_listener.read_exact(&mut from_connector).await?;
    assert_eq!(&from_connector, b"from connector");

    first_listener.write_all(b"from listener").await?;
    let mut from_listener = [0_u8; 13];
    first_connector.read_exact(&mut from_listener).await?;
    assert_eq!(&from_listener, b"from listener");

    first_connector.close().await?;
    first_listener.close().await?;
    second_connector.close().await?;
    second_listener.close().await?;
    wait_until(Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 0
            && gateway_b.gateway.snapshot().peer_streams == 0
            && gateway_a.gateway.snapshot().peer_transports_ready == 1
            && gateway_b.gateway.snapshot().peer_transports_ready == 1
    })
    .await?;

    connector.close();
    listener_runtime.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
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
    async fn start(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
    ) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        let sdk_address = sdk_listener.local_addr()?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_address = peer_listener.local_addr()?;
        let routing = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(name)?,
            InternalGatewayKey::new(key)?,
            GatewayLocator::new(peer_address.to_string())?,
            route_client_config()?,
        )
        .with_command_queue_capacity(16)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(200));
        let peer =
            GatewayPeerConfig::new(name, key, [TrustedPeerConfig::new(peer_name, peer_key)?])?
                .with_timeouts(
                    Duration::from_millis(200),
                    Duration::from_millis(200),
                    Duration::from_secs(1),
                );
        let shutdown = CancellationToken::new();
        // One slot proves that Resolve -> OpenPeer hands the same attempt to
        // its next phase instead of transiently requiring two control slots.
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([(CLIENT_ID.to_owned(), CLIENT_KEY.to_owned())])
                .with_max_pending_offers(1),
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
            name: name.to_owned(),
            sdk_address,
            gateway,
            shutdown,
            task,
        })
    }

    async fn assert_running(&mut self) -> TestResult {
        if !self.task.is_finished() {
            return Ok(());
        }
        let result = (&mut self.task).await.map_err(|error| {
            io::Error::other(format!("{} task could not be joined: {error}", self.name))
        })?;
        match result {
            Ok(()) => Err(io::Error::other(format!(
                "{} distributed runtime stopped unexpectedly",
                self.name
            ))
            .into()),
            Err(error) => Err(io::Error::other(format!(
                "{} distributed runtime failed: {error}",
                self.name
            ))
            .into()),
        }
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
