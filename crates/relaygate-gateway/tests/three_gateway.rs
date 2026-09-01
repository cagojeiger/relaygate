use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    TrustedPeerConfig, check,
};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{
    Config as SdkConfig, Connector, ErrorCode as SdkErrorCode, Listener, ListenerRuntime,
    PeerObservation as SdkPeerObservation, Pipe,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLIENT_KEY: &str = "listener-key";
const CLIENT_A: &str = "echo.a";
const CLIENT_B: &str = "echo.b";
const CLIENT_C: &str = "echo.c";
const CLIENT_MISSING: &str = "echo.missing";
const CLIENT_SHARED: &str = "echo.shared";
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

#[path = "three_gateway/open_identity.rs"]
mod open_identity;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rt_one_gateway_three_forms_a_closed_current_state_relay() -> TestResult {
    timeout(Duration::from_secs(12), three_gateway_case()).await??;
    Ok(())
}

async fn three_gateway_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let mut gateway_a = RunningGateway::start(GATEWAY_A, GATEWAY_A_KEY, directory.clone()).await?;
    let mut gateway_b = RunningGateway::start(GATEWAY_B, GATEWAY_B_KEY, directory.clone()).await?;
    let mut gateway_c = RunningGateway::start(GATEWAY_C, GATEWAY_C_KEY, directory).await?;

    let listener_runtime_a = ListenerRuntime::connect(sdk_config(gateway_a.sdk_address)).await?;
    let listener_runtime_b = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener_runtime_c = ListenerRuntime::connect(sdk_config(gateway_c.sdk_address)).await?;
    let listener_a = listener_runtime_a.listen(CLIENT_A, CLIENT_KEY).await?;
    let listener_b = listener_runtime_b.listen(CLIENT_B, CLIENT_KEY).await?;
    let listener_c = listener_runtime_c.listen(CLIENT_C, CLIENT_KEY).await?;
    let shared_b = listener_runtime_b.listen(CLIENT_SHARED, CLIENT_KEY).await?;
    let shared_c = listener_runtime_c.listen(CLIENT_SHARED, CLIENT_KEY).await?;

    wait_until("all registrations synced", Duration::from_secs(2), || {
        [&gateway_a, &gateway_b, &gateway_c]
            .into_iter()
            .all(|gateway| gateway.gateway.snapshot().route_registrations_synced == 1)
    })
    .await?;
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    gateway_c.assert_running().await?;

    let route_observer = RouteTableClient::connect(
        route_endpoint,
        GatewayName::new(GATEWAY_A)?,
        GatewayId::new(),
        InternalGatewayKey::new(GATEWAY_A_KEY)?,
        route_client_config()?,
    )
    .await?;
    wait_for_binding_count(
        &route_observer,
        generation,
        CLIENT_SHARED,
        2,
        Duration::from_secs(2),
    )
    .await?;

    let connector_a = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let connector_b = Connector::connect(sdk_config(gateway_b.sdk_address)).await?;
    let connector_c = Connector::connect(sdk_config(gateway_c.sdk_address)).await?;

    exercise_pipe(&connector_a, CLIENT_A, &listener_a, "local-a").await?;
    exercise_pipe(&connector_b, CLIENT_B, &listener_b, "local-b").await?;
    exercise_pipe(&connector_c, CLIENT_C, &listener_c, "local-c").await?;

    exercise_pipe(&connector_a, CLIENT_B, &listener_b, "a-to-b").await?;
    exercise_pipe(&connector_a, CLIENT_C, &listener_c, "a-to-c").await?;
    exercise_pipe(&connector_b, CLIENT_A, &listener_a, "b-to-a").await?;
    exercise_pipe(&connector_b, CLIENT_C, &listener_c, "b-to-c").await?;
    exercise_pipe(&connector_c, CLIENT_A, &listener_a, "c-to-a").await?;
    exercise_pipe(&connector_c, CLIENT_B, &listener_b, "c-to-b").await?;

    wait_until(
        "three peer pairs idle on shared transports",
        Duration::from_secs(2),
        || {
            [&gateway_a, &gateway_b, &gateway_c]
                .into_iter()
                .all(|gateway| {
                    let snapshot = gateway.gateway.snapshot();
                    snapshot.peer_transports_ready == 2
                        && snapshot.peer_streams == 0
                        && snapshot.live_pipes == 0
                })
        },
    )
    .await?;

    let (mut shared_connector, mut shared_listener, shared_owner) =
        open_shared_pipe(&connector_a, &shared_b, &shared_c).await?;
    let (owner, non_owner) = match shared_owner {
        SharedOwner::B => (&gateway_b, &gateway_c),
        SharedOwner::C => (&gateway_c, &gateway_b),
    };
    wait_until(
        "one shared binding is selected without fan-out",
        Duration::from_secs(2),
        || {
            let entry = gateway_a.gateway.snapshot();
            let owner = owner.gateway.snapshot();
            let non_owner = non_owner.gateway.snapshot();
            entry.live_pipes == 1
                && entry.peer_streams == 1
                && owner.live_pipes == 1
                && owner.peer_streams == 1
                && non_owner.live_pipes == 0
                && non_owner.peer_streams == 0
        },
    )
    .await?;
    assert_bidirectional(
        &mut shared_connector,
        &mut shared_listener,
        "a-to-one-of-shared-b-c",
    )
    .await?;
    shared_connector.close().await?;
    shared_listener.close().await?;
    wait_until(
        "shared selected Pipe cleanup",
        Duration::from_secs(2),
        || {
            [&gateway_a, &gateway_b, &gateway_c]
                .into_iter()
                .all(|gateway| {
                    let snapshot = gateway.gateway.snapshot();
                    snapshot.peer_streams == 0 && snapshot.live_pipes == 0
                })
        },
    )
    .await?;

    exercise_repeated_failure_recovery(
        &connector_a,
        &listener_c,
        [&gateway_a, &gateway_b, &gateway_c],
    )
    .await?;

    let (mut durable_connector, mut durable_listener) =
        open_pipe(&connector_a, CLIENT_C, &listener_c).await?;
    let (mut reused_connector, mut reused_listener) =
        open_pipe(&connector_a, CLIENT_C, &listener_c).await?;
    wait_until(
        "a-c pair reuses its ready transport",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.peer_transports_ready == 2
                && c.peer_transports_ready == 2
                && a.peer_streams == 2
                && c.peer_streams == 2
                && b.peer_streams == 0
        },
    )
    .await?;
    assert_bidirectional(&mut reused_connector, &mut reused_listener, "a-c-reused").await?;
    reused_connector.close().await?;
    reused_listener.close().await?;
    wait_until(
        "only the durable a-c stream remains",
        Duration::from_secs(2),
        || {
            gateway_a.gateway.snapshot().peer_streams == 1
                && gateway_c.gateway.snapshot().peer_streams == 1
        },
    )
    .await?;

    connector_b.close();
    listener_runtime_b.close();
    gateway_b.stop().await?;
    gateway_a.assert_running().await?;
    gateway_c.assert_running().await?;
    wait_for_binding_count(
        &route_observer,
        generation,
        CLIENT_SHARED,
        1,
        Duration::from_secs(2),
    )
    .await?;
    exercise_pipe(
        &connector_a,
        CLIENT_SHARED,
        &shared_c,
        "shared-survives-b-stop",
    )
    .await?;
    assert_bidirectional(
        &mut durable_connector,
        &mut durable_listener,
        "a-c-after-b-stop",
    )
    .await?;

    drop(route_observer);
    route_table.stop().await?;
    let failed_open = connector_a
        .open(CLIENT_C)
        .await
        .err()
        .ok_or("remote open unexpectedly succeeded while RouteTable was unavailable")?;
    assert_eq!(failed_open.code(), SdkErrorCode::Unavailable);
    assert_eq!(failed_open.observation(), SdkPeerObservation::NotObserved);
    wait_until(
        "failed open leaves no pending attempt",
        Duration::from_secs(2),
        || {
            gateway_a.gateway.snapshot().remote_open_attempts == 0
                && gateway_a.gateway.snapshot().peer_streams == 1
                && gateway_c.gateway.snapshot().peer_streams == 1
        },
    )
    .await?;
    assert_bidirectional(
        &mut durable_connector,
        &mut durable_listener,
        "a-c-after-rt-stop",
    )
    .await?;

    durable_connector.close().await?;
    durable_listener.close().await?;
    wait_until("durable stream cleanup", Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 0
            && gateway_c.gateway.snapshot().peer_streams == 0
    })
    .await?;

    connector_a.close();
    connector_c.close();
    listener_runtime_a.close();
    listener_runtime_c.close();
    gateway_a.stop().await?;
    gateway_c.stop().await?;
    Ok(())
}

async fn exercise_repeated_failure_recovery(
    connector: &Connector,
    listener: &Listener,
    gateways: [&RunningGateway; 3],
) -> TestResult {
    for cycle in 0..100 {
        let failure = connector
            .open(CLIENT_MISSING)
            .await
            .err()
            .ok_or("missing ClientId unexpectedly opened a Pipe")?;
        assert_eq!(failure.code(), SdkErrorCode::NotFound);
        assert_eq!(failure.observation(), SdkPeerObservation::NotObserved);

        exercise_pipe(
            connector,
            CLIENT_C,
            listener,
            &format!("failure-recovery-{cycle}"),
        )
        .await?;
    }

    wait_until(
        "100 failure/recovery cycles return to current-state baseline",
        Duration::from_secs(2),
        || {
            gateways.iter().all(|gateway| {
                let snapshot = gateway.gateway.snapshot();
                snapshot.pending_offers == 0
                    && snapshot.live_pipes == 0
                    && snapshot.remote_open_attempts == 0
                    && snapshot.peer_transports_connecting == 0
                    && snapshot.peer_transports_ready == 2
                    && snapshot.peer_streams == 0
            })
        },
    )
    .await
}

async fn exercise_pipe(
    connector: &Connector,
    client_id: &str,
    listener: &Listener,
    marker: &str,
) -> TestResult {
    let (mut connector_pipe, mut listener_pipe) = open_pipe(connector, client_id, listener).await?;
    assert_bidirectional(&mut connector_pipe, &mut listener_pipe, marker).await?;
    connector_pipe.close().await?;
    listener_pipe.close().await?;
    Ok(())
}

async fn open_pipe(
    connector: &Connector,
    client_id: &str,
    listener: &Listener,
) -> TestResult<(Pipe, Pipe)> {
    let connector_pipe = connector.open(client_id).await?;
    let listener_pipe = listener.accept().await?;
    Ok((connector_pipe, listener_pipe))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedOwner {
    B,
    C,
}

async fn open_shared_pipe(
    connector: &Connector,
    listener_b: &Listener,
    listener_c: &Listener,
) -> TestResult<(Pipe, Pipe, SharedOwner)> {
    timeout(Duration::from_secs(2), async {
        let accepted = async {
            tokio::select! {
                result = listener_b.accept() => result.map(|pipe| (pipe, SharedOwner::B)),
                result = listener_c.accept() => result.map(|pipe| (pipe, SharedOwner::C)),
            }
        };
        let (opened, accepted) = tokio::join!(connector.open(CLIENT_SHARED), accepted);
        let connector_pipe = opened?;
        let (listener_pipe, owner) = accepted?;
        Ok::<_, relaygate_sdk::Error>((connector_pipe, listener_pipe, owner))
    })
    .await
    .map_err(|_| "shared OPEN or concurrent accept timed out")?
    .map_err(Into::into)
}

async fn assert_bidirectional(
    connector: &mut Pipe,
    listener: &mut Pipe,
    marker: &str,
) -> TestResult {
    let toward_listener = format!("connector:{marker}").into_bytes();
    connector.write_all(&toward_listener).await?;
    let mut received = vec![0; toward_listener.len()];
    listener.read_exact(&mut received).await?;
    assert_eq!(received, toward_listener);

    let toward_connector = format!("listener:{marker}").into_bytes();
    listener.write_all(&toward_connector).await?;
    let mut received = vec![0; toward_connector.len()];
    connector.read_exact(&mut received).await?;
    assert_eq!(received, toward_connector);
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
    async fn start(name: &str, key: &str, directory: ShardDirectory) -> TestResult<Self> {
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
        .with_command_queue_capacity(32)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(200));
        let trusted_peers = ALL_GATEWAYS
            .into_iter()
            .filter(|(peer_name, _)| *peer_name != name)
            .map(|(peer_name, peer_key)| TrustedPeerConfig::new(peer_name, peer_key))
            .collect::<Result<Vec<_>, _>>()?;
        let peer = GatewayPeerConfig::new(name, key, trusted_peers)?.with_timeouts(
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_secs(1),
        );
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([
                (CLIENT_A.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_B.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_C.to_owned(), CLIENT_KEY.to_owned()),
                (CLIENT_SHARED.to_owned(), CLIENT_KEY.to_owned()),
            ])
            .with_max_pending_offers(16),
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
