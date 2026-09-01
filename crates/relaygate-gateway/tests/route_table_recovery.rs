use std::{error::Error, net::SocketAddr, time::Duration};

use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig, GatewaySnapshot, check,
};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory,
    ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig,
    RouteTableService, RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{Config as SdkConfig, Connector, ListenerRuntime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net::TcpListener, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_ID: &str = "echo.recover";
const CLIENT_KEY: &str = "listener-secret";
const GATEWAY_NAME: &str = "gateway-under-test";
const GATEWAY_KEY: &str = "gateway-key";
const PROBE_NAME: &str = "probe";
const PROBE_KEY: &str = "probe-key";
const SHARD_ID: &str = "rt-0";

#[tokio::test]
async fn routed_gateway_republishes_current_listener_state_after_route_table_restart() -> TestResult
{
    tokio::time::timeout(Duration::from_secs(5), recovery_case()).await??;
    Ok(())
}

async fn recovery_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory_bytes = one_shard_directory(route_endpoint);
    let directory = ShardDirectory::from_json_bytes(&directory_bytes)?;
    let generation = directory.generation();
    let mut route_table = RunningRouteTable::serve(
        route_listener,
        directory.clone(),
        Duration::from_millis(250),
    )?;
    assert!(route_table.started_empty());

    let gateway = RunningGateway::start(directory.clone()).await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;

    let probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(&probe, generation, CLIENT_ID, &gateway.peer_locator, 1).await?;

    route_table.stop().await?;
    let restarted_listener = TcpListener::bind(route_endpoint).await?;
    route_table =
        RunningRouteTable::serve(restarted_listener, directory, Duration::from_millis(250))?;
    assert!(route_table.started_empty());
    let restarted_probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(
        &restarted_probe,
        generation,
        CLIENT_ID,
        &gateway.peer_locator,
        1,
    )
    .await?;

    assert_eq!(listener.client_id(), CLIENT_ID);
    listener_runtime.close();
    wait_for_not_found(&restarted_probe, generation, CLIENT_ID).await?;

    route_table.stop().await?;
    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn listener_registers_and_opens_local_pipe_before_route_table_is_available() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(5),
        local_pipe_before_initial_sync_case(),
    )
    .await??;
    Ok(())
}

async fn local_pipe_before_initial_sync_case() -> TestResult {
    let stalled_route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = stalled_route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;

    let gateway = RunningGateway::start(directory).await?;
    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway.endpoint)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_for_gateway_snapshot(&gateway, |snapshot| {
        snapshot.listener_bindings == 1
            && snapshot.route_registrations_synced == 0
            && snapshot.route_registrations_unsynced == 1
    })
    .await?;

    // The endpoint is bound but no RouteTable service consumes the handshake.
    // Waiting past the client deadline proves that at least one initial
    // publication attempt failed without removing the local binding.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let unsynced = gateway.gateway.snapshot();
    assert_eq!(unsynced.listener_bindings, 1);
    assert_eq!(unsynced.route_registrations_synced, 0);
    assert_eq!(unsynced.route_registrations_unsynced, 1);

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
    listener_runtime.close();
    gateway.stop().await?;
    drop(stalled_route_listener);
    Ok(())
}

struct RunningGateway {
    endpoint: SocketAddr,
    peer_locator: String,
    gateway: Gateway,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
}

impl RunningGateway {
    async fn start(directory: ShardDirectory) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_endpoint = peer_listener.local_addr()?;
        let config = GatewayConfig::new([(CLIENT_ID.to_owned(), CLIENT_KEY.to_owned())])
            .with_offer_timeout(Duration::from_millis(100));
        let routing_config = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(GATEWAY_NAME)?,
            InternalGatewayKey::new(GATEWAY_KEY)?,
            GatewayLocator::new(peer_endpoint.to_string())?,
            route_client_config()?,
        )
        .with_command_queue_capacity(8)
        .with_reconnect_backoff(Duration::from_millis(20), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(20))
        .with_shutdown_timeout(Duration::from_millis(200));
        let shutdown = CancellationToken::new();
        let peer_config = GatewayPeerConfig::new(GATEWAY_NAME, GATEWAY_KEY, [])?;
        let gateway =
            Gateway::new_distributed(config, routing_config, peer_config, shutdown.clone())?;
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

async fn wait_for_gateway_snapshot(
    gateway: &RunningGateway,
    matches: impl Fn(GatewaySnapshot) -> bool,
) -> TestResult {
    poll_until(Duration::from_secs(2), || async {
        matches(gateway.gateway.snapshot()).then_some(())
    })
    .await
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
            trusted_gateway_keys()?,
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
        (error.code() == ErrorCode::NotFound).then_some(())
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
        Duration::from_millis(200),
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

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}
