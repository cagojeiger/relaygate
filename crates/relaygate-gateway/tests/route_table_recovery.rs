use std::{error::Error, net::SocketAddr, time::Duration};

use relaygate_gateway::{Gateway, GatewayConfig, GatewayRoutingConfig, check};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory,
    ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig,
    RouteTableService, RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{Config as SdkConfig, ListenerRuntime};
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
const GATEWAY_LOCATOR: &str = "gateway-under-test:27431";

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
    wait_for_mapping(&probe, generation, CLIENT_ID, 1).await?;

    route_table.stop().await?;
    let restarted_listener = TcpListener::bind(route_endpoint).await?;
    route_table =
        RunningRouteTable::serve(restarted_listener, directory, Duration::from_millis(250))?;
    assert!(route_table.started_empty());
    let restarted_probe = connect_probe(route_endpoint, generation).await?;
    wait_for_mapping(&restarted_probe, generation, CLIENT_ID, 1).await?;

    assert_eq!(listener.client_id(), CLIENT_ID);
    listener_runtime.close();
    wait_for_not_found(&restarted_probe, generation, CLIENT_ID).await?;

    route_table.stop().await?;
    gateway.stop().await?;
    Ok(())
}

struct RunningGateway {
    endpoint: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
}

impl RunningGateway {
    async fn start(directory: ShardDirectory) -> TestResult<Self> {
        let config = GatewayConfig::new([(CLIENT_ID.to_owned(), CLIENT_KEY.to_owned())])
            .with_offer_timeout(Duration::from_millis(100));
        let routing_config = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(GATEWAY_NAME)?,
            InternalGatewayKey::new(GATEWAY_KEY)?,
            GatewayLocator::new(GATEWAY_LOCATOR)?,
            route_client_config()?,
        )
        .with_command_queue_capacity(8)
        .with_reconnect_backoff(Duration::from_millis(20), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(20))
        .with_shutdown_timeout(Duration::from_millis(200));
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_routed(config, routing_config, shutdown.clone())?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { gateway.serve(listener, serve_shutdown).await });
        check(endpoint, Duration::from_secs(1)).await?;
        Ok(Self {
            endpoint,
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
    expected_len: usize,
) -> TestResult {
    let client_id = ClientId::new(client_id)?;
    poll_until(Duration::from_secs(2), || async {
        let bindings = client.resolve(generation, &client_id).await.ok()?;
        let entries = bindings.entries();
        (entries.len() == expected_len
            && entries.iter().all(|entry| {
                entry.client_id() == &client_id
                    && entry.gateway_locator().as_str() == GATEWAY_LOCATOR
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
