use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    TrustedPeerConfig, check,
};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, MappingEntry, RouteTableConfig, RouteTableShard,
    ShardDirectory, ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{Config as SdkConfig, Connector, ErrorCode as SdkErrorCode, ListenerRuntime};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_ID: &str = "echo.restart";
const CLIENT_KEY: &str = "listener-secret";
const OWNER_NAME: &str = "owner-gateway";
const OWNER_KEY: &str = "owner-key";
const ENTRY_NAME: &str = "entry-gateway";
const ENTRY_KEY: &str = "entry-key";
const PROBE_NAME: &str = "probe";
const PROBE_KEY: &str = "probe-key";
const SHARD_ID: &str = "rt-0";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_gateway_with_reused_locator_publishes_a_new_identity() -> TestResult {
    timeout(Duration::from_secs(12), restart_identity_case()).await??;
    Ok(())
}

async fn restart_identity_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let owner_v1 = RunningGateway::start_owner(directory.clone(), None).await?;
    let reused_peer_locator = owner_v1.peer_address;
    let listener_v1_runtime = ListenerRuntime::connect(sdk_config(owner_v1.sdk_address)).await?;
    let listener_v1 = listener_v1_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;

    let probe = connect_probe(route_endpoint, generation).await?;
    let old_entry = wait_for_single_mapping(&probe, generation, CLIENT_ID).await?;
    let old_identity = old_entry.identity();
    assert_eq!(
        old_entry.gateway_locator().as_str(),
        reused_peer_locator.to_string()
    );
    assert_eq!(listener_v1.client_id(), CLIENT_ID);

    owner_v1.crash().await;
    drop(listener_v1);
    drop(listener_v1_runtime);

    let owner_v2 =
        RunningGateway::start_owner(directory.clone(), Some(reused_peer_locator)).await?;
    assert_eq!(owner_v2.peer_address, reused_peer_locator);
    wait_for_tcp(reused_peer_locator, Duration::from_secs(1)).await?;

    let entry_gateway = RunningGateway::start_entry(directory).await?;
    let connector = Connector::connect(sdk_config(entry_gateway.sdk_address)).await?;
    wait_for_permission_denied_open(&connector).await?;
    assert_eq!(owner_v2.gateway.snapshot().listener_bindings, 0);
    assert_eq!(owner_v2.gateway.snapshot().pending_offers, 0);
    assert_eq!(owner_v2.gateway.snapshot().live_pipes, 0);

    let listener_v2_runtime = ListenerRuntime::connect(sdk_config(owner_v2.sdk_address)).await?;
    let listener_v2 = listener_v2_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    let new_entry =
        wait_for_new_mapping(&probe, generation, CLIENT_ID, old_identity.gateway_id()).await?;
    assert_eq!(
        new_entry.gateway_locator().as_str(),
        reused_peer_locator.to_string()
    );
    assert_ne!(new_entry.identity().gateway_id(), old_identity.gateway_id());
    assert_ne!(new_entry.identity().binding_id(), old_identity.binding_id());
    assert_eq!(listener_v2.client_id(), CLIENT_ID);

    connector.close();
    listener_v2_runtime.close();
    entry_gateway.stop().await?;
    owner_v2.stop().await?;
    route_table.stop().await?;
    Ok(())
}

struct RunningGateway {
    sdk_address: SocketAddr,
    peer_address: SocketAddr,
    gateway: Gateway,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl RunningGateway {
    async fn start_owner(
        directory: ShardDirectory,
        peer_address: Option<SocketAddr>,
    ) -> TestResult<Self> {
        Self::start(
            OWNER_NAME,
            OWNER_KEY,
            [TrustedPeerConfig::new(ENTRY_NAME, ENTRY_KEY)?],
            directory,
            peer_address,
        )
        .await
    }

    async fn start_entry(directory: ShardDirectory) -> TestResult<Self> {
        Self::start(
            ENTRY_NAME,
            ENTRY_KEY,
            [TrustedPeerConfig::new(OWNER_NAME, OWNER_KEY)?],
            directory,
            None,
        )
        .await
    }

    async fn start<const N: usize>(
        name: &str,
        key: &str,
        peers: [TrustedPeerConfig; N],
        directory: ShardDirectory,
        peer_address: Option<SocketAddr>,
    ) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        let sdk_address = sdk_listener.local_addr()?;
        let peer_listener = bind_peer_listener(peer_address).await?;
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
        let peer = GatewayPeerConfig::new(name, key, peers)?.with_timeouts(
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_secs(1),
        );
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([(CLIENT_ID.to_owned(), CLIENT_KEY.to_owned())])
                .with_offer_timeout(Duration::from_millis(100)),
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
            sdk_address,
            peer_address,
            gateway,
            shutdown,
            task,
        })
    }

    async fn crash(self) {
        self.task.abort();
        let _ = timeout(Duration::from_secs(1), self.task).await;
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(2), self.task)
            .await
            .map_err(|_| io::Error::other("Gateway shutdown timed out"))?
            .map_err(|error| io::Error::other(format!("Gateway task join failed: {error}")))?
            .map_err(|error| io::Error::other(format!("Gateway shutdown failed: {error}")))?;
        Ok(())
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
            RouteTableConfig::new(Duration::from_secs(30)).map_err(TransportError::from)?,
        )
        .map_err(TransportError::from)?;
        let service = RouteTableService::new(
            shard,
            trusted_gateway_keys()?,
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
            .map_err(|_| io::Error::other("RouteTable shutdown timed out"))?
            .map_err(|error| io::Error::other(format!("RouteTable task join failed: {error}")))?
            .map_err(|error| io::Error::other(format!("RouteTable shutdown failed: {error}")))?;
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

async fn wait_for_single_mapping(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
) -> TestResult<MappingEntry> {
    wait_for_resolve(client, generation, client_id, |entries| {
        (entries.len() == 1).then(|| entries[0].clone())
    })
    .await
}

async fn wait_for_new_mapping(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
    old_gateway_id: GatewayId,
) -> TestResult<MappingEntry> {
    wait_for_resolve(client, generation, client_id, |entries| {
        entries
            .iter()
            .find(|entry| entry.identity().gateway_id() != old_gateway_id)
            .cloned()
    })
    .await
}

async fn wait_for_resolve<T>(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    client_id: &str,
    mut accept: impl FnMut(&[MappingEntry]) -> Option<T>,
) -> TestResult<T> {
    let client_id = ClientId::new(client_id)?;
    let expires = Instant::now() + Duration::from_secs(4);
    loop {
        if let Ok(bindings) = client.resolve(generation, &client_id).await
            && let Some(value) = accept(bindings.entries())
        {
            return Ok(value);
        }
        if Instant::now() >= expires {
            return Err("RouteTable mapping did not converge before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_permission_denied_open(connector: &Connector) -> TestResult {
    let expires = Instant::now() + Duration::from_secs(2);
    loop {
        if let Err(error) = connector.open(CLIENT_ID).await
            && error.code() == SdkErrorCode::PermissionDenied
        {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err("stale peer OPEN did not fail with PermissionDenied".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_tcp(address: SocketAddr, deadline: Duration) -> TestResult {
    let expires = Instant::now() + deadline;
    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err("TCP listener did not become reachable before deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn bind_peer_listener(address: Option<SocketAddr>) -> io::Result<TcpListener> {
    let Some(address) = address else {
        return TcpListener::bind("127.0.0.1:0").await;
    };
    let expires = Instant::now() + Duration::from_secs(1);
    loop {
        match TcpListener::bind(address).await {
            Ok(listener) => return Ok(listener),
            Err(error) if Instant::now() < expires => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if error.kind() != io::ErrorKind::AddrInUse {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
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
        .with_operation_timeout(Duration::from_secs(1))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
}

fn trusted_gateway_keys() -> Result<TrustedGatewayKeys, TransportError> {
    TrustedGatewayKeys::new([
        (
            GatewayName::new(OWNER_NAME)?,
            InternalGatewayKey::new(OWNER_KEY)?,
        ),
        (
            GatewayName::new(ENTRY_NAME)?,
            InternalGatewayKey::new(ENTRY_KEY)?,
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
