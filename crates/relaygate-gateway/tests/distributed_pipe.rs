use std::{error::Error, io, net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    TrustedPeerConfig, check,
};
use relaygate_protocol::{Frame, FrameCodec};
use relaygate_route_table::{
    ClientId, GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory,
    ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use relaygate_sdk::{
    Config as SdkConfig, Connector, ErrorCode as SdkErrorCode, Listener, ListenerRuntime,
    PeerObservation as SdkPeerObservation,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
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
const PEER_OPEN_KIND: u8 = 4;
const CONCURRENT_REMOTE_PIPES: usize = 32;
const CONCURRENT_CONTROL_CAPACITY: usize = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pipes_share_one_peer_transport_and_survive_route_table_loss() -> TestResult {
    timeout(Duration::from_secs(10), remote_pipe_case()).await??;
    Ok(())
}

async fn remote_pipe_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let restart_directory = directory.clone();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let mut gateway_a = RunningGateway::start_with_control_capacity(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory.clone(),
        CONCURRENT_CONTROL_CAPACITY,
    )
    .await?;
    let mut gateway_b = RunningGateway::start_with_control_capacity(
        GATEWAY_B,
        GATEWAY_B_KEY,
        GATEWAY_A,
        GATEWAY_A_KEY,
        directory,
        CONCURRENT_CONTROL_CAPACITY,
    )
    .await?;

    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener = Arc::new(listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?);
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

    wait_until(Duration::from_secs(2), || {
        let owner = gateway_b.gateway.snapshot();
        owner.route_registrations_synced == 0 && owner.route_registrations_unsynced == 1
    })
    .await?;
    let restarted_listener = TcpListener::bind(route_endpoint).await?;
    let restarted_route_table = RunningRouteTable::start(restarted_listener, restart_directory)?;
    wait_until(Duration::from_secs(2), || {
        let owner = gateway_b.gateway.snapshot();
        owner.route_registrations_synced == 1 && owner.route_registrations_unsynced == 0
    })
    .await?;
    wait_for_remote_pipe(&connector, &listener).await?;
    assert_concurrent_remote_echo(&connector, Arc::clone(&listener), CONCURRENT_REMOTE_PIPES)
        .await?;
    wait_until(Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 2
            && gateway_b.gateway.snapshot().peer_streams == 2
    })
    .await?;

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
    restarted_route_table.stop().await?;
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    Ok(())
}

async fn wait_for_remote_pipe(connector: &Connector, listener: &Listener) -> TestResult {
    let expires = Instant::now() + Duration::from_secs(2);
    loop {
        match connector.open(CLIENT_ID).await {
            Ok(mut connector_pipe) => {
                let mut listener_pipe = listener.accept().await?;
                connector_pipe.close().await?;
                listener_pipe.close().await?;
                return Ok(());
            }
            Err(error)
                if matches!(
                    error.code(),
                    SdkErrorCode::NotFound | SdkErrorCode::Unavailable
                ) && error.observation() == SdkPeerObservation::NotObserved =>
            {
                if Instant::now() >= expires {
                    return Err(error.into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn assert_concurrent_remote_echo(
    connector: &Connector,
    listener: Arc<Listener>,
    count: usize,
) -> TestResult {
    let mut tasks: JoinSet<TestResult> = JoinSet::new();
    for sequence in 0..count {
        let connector = connector.clone();
        let listener = Arc::clone(&listener);
        tasks.spawn(async move {
            let (mut connector_pipe, listener_pipe) =
                tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
            let echo = tokio::spawn(async move {
                let (mut reader, mut writer) = listener_pipe.into_split();
                tokio::io::copy(&mut reader, &mut writer).await?;
                writer.shutdown().await
            });
            let payload = vec![(sequence % 251) as u8; 4_096 + sequence * 257];
            connector_pipe.write_all(&payload).await?;
            connector_pipe.shutdown().await?;
            let mut received = Vec::new();
            connector_pipe.read_to_end(&mut received).await?;
            echo.await??;
            if received != payload {
                return Err(io::Error::other(format!(
                    "concurrent remote echo {sequence} returned {} of {} bytes",
                    received.len(),
                    payload.len()
                ))
                .into());
            }
            Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_remote_listener_selection_fails_without_falling_back() -> TestResult {
    timeout(Duration::from_secs(12), stale_remote_listener_case()).await??;
    Ok(())
}

async fn stale_remote_listener_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;
    let route_observer = RouteTableClient::connect(
        route_endpoint,
        GatewayName::new(GATEWAY_A)?,
        GatewayId::new(),
        InternalGatewayKey::new(GATEWAY_A_KEY)?,
        route_client_config()?,
    )
    .await?;

    let owner_peer_listener = TcpListener::bind("127.0.0.1:0").await?;
    let owner_peer_address = owner_peer_listener.local_addr()?;
    let mut proxy = OpenBlockingPeerProxy::start(owner_peer_address).await?;
    let mut gateway_b = RunningGateway::start_with_peer_listener(
        GATEWAY_B,
        GATEWAY_B_KEY,
        GATEWAY_A,
        GATEWAY_A_KEY,
        directory.clone(),
        owner_peer_listener,
        proxy.address,
        Duration::from_secs(5),
    )
    .await?;
    let mut gateway_a = RunningGateway::start_with_open_response_timeout(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory,
        Duration::from_secs(5),
    )
    .await?;

    let old_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let old_listener = old_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(2), || {
        gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;
    wait_for_binding_count(&route_observer, generation, 1).await?;

    let connector = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let opening = {
        let connector = connector.clone();
        tokio::spawn(async move { connector.open(CLIENT_ID).await })
    };
    proxy.wait_until_open_blocked().await?;
    assert_eq!(gateway_a.gateway.snapshot().remote_open_attempts, 1);
    assert_eq!(gateway_a.gateway.snapshot().peer_streams, 1);
    assert_eq!(gateway_b.gateway.snapshot().peer_streams, 0);

    old_runtime.close();
    drop(old_listener);
    wait_until(Duration::from_secs(2), || {
        let owner = gateway_b.gateway.snapshot();
        owner.listener_sessions == 0
            && owner.listener_bindings == 0
            && owner.route_registrations_synced == 0
    })
    .await?;
    wait_for_binding_count(&route_observer, generation, 0).await?;

    let replacement_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let replacement = replacement_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(2), || {
        let owner = gateway_b.gateway.snapshot();
        owner.listener_sessions == 1
            && owner.listener_bindings == 1
            && owner.route_registrations_synced == 1
    })
    .await?;
    wait_for_binding_count(&route_observer, generation, 1).await?;

    proxy.release_open()?;
    let error = opening
        .await?
        .err()
        .ok_or("stale remote OPEN unexpectedly used the replacement Listener")?;
    assert_eq!(error.code(), SdkErrorCode::Unavailable);
    assert_eq!(error.observation(), SdkPeerObservation::NotObserved);
    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.remote_open_attempts == 0
            && entry.live_pipes == 0
            && entry.peer_streams == 0
            && owner.pending_offers == 0
            && owner.live_pipes == 0
            && owner.peer_streams == 0
            && owner.listener_sessions == 1
            && owner.listener_bindings == 1
    })
    .await?;

    let mut connector_pipe = connector.open(CLIENT_ID).await?;
    let mut listener_pipe = replacement.accept().await?;
    connector_pipe.write_all(b"fresh selection").await?;
    let mut payload = [0_u8; 15];
    listener_pipe.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"fresh selection");
    connector_pipe.close().await?;
    listener_pipe.close().await?;
    wait_until(Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 0
            && gateway_b.gateway.snapshot().peer_streams == 0
    })
    .await?;

    connector.close();
    replacement_runtime.close();
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    proxy.stop().await;
    drop(route_observer);
    route_table.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_entry_opened_closes_connector_session_and_remote_pipe() -> TestResult {
    timeout(Duration::from_secs(10), lost_entry_opened_case()).await??;
    Ok(())
}

async fn lost_entry_opened_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let gateway_a = RunningGateway::start(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory.clone(),
    )
    .await?;
    let gateway_b = RunningGateway::start(
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

    let proxy = OpenedDroppingProxy::start(gateway_a.sdk_address).await?;
    let connector = Connector::connect(
        SdkConfig::new(proxy.address.to_string())
            .with_connect_timeout(Duration::from_millis(200))
            .with_operation_timeout(Duration::from_millis(200))
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let opening = {
        let connector = connector.clone();
        tokio::spawn(async move { connector.open(CLIENT_ID).await })
    };
    let mut listener_pipe = listener.accept().await?;
    timeout(Duration::from_secs(2), proxy.opened_dropped).await??;

    let error = opening
        .await?
        .err()
        .ok_or("OPEN unexpectedly succeeded after Entry OPENED was lost")?;
    assert_eq!(error.code(), SdkErrorCode::DeadlineExceeded);
    assert_eq!(error.observation(), SdkPeerObservation::MaybeObserved);
    timeout(Duration::from_secs(2), proxy.task).await???;

    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.connector_sessions == 0
            && entry.remote_open_attempts == 0
            && entry.live_pipes == 0
            && entry.peer_streams == 0
            && owner.live_pipes == 0
            && owner.peer_streams == 0
    })
    .await?;
    let mut byte = [0_u8; 1];
    let listener_error = timeout(Duration::from_secs(1), listener_pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or("Owner Listener Pipe survived the lost Entry OPENED cleanup")?;
    assert_eq!(listener_error.code(), SdkErrorCode::Cancelled);

    connector.close();
    listener_runtime.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    route_table.stop().await?;
    Ok(())
}

struct OpenedDroppingProxy {
    address: SocketAddr,
    opened_dropped: oneshot::Receiver<()>,
    task: JoinHandle<TestResult>,
}

impl OpenedDroppingProxy {
    async fn start(upstream: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (opened_dropped_tx, opened_dropped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (sdk_stream, _) = listener.accept().await?;
            let gateway_stream = TcpStream::connect(upstream).await?;
            let (mut sdk_sink, mut sdk_source) =
                tokio_util::codec::Framed::new(sdk_stream, FrameCodec::default()).split();
            let (mut gateway_sink, mut gateway_source) =
                tokio_util::codec::Framed::new(gateway_stream, FrameCodec::default()).split();

            let sdk_to_gateway = async {
                while let Some(frame) = sdk_source.next().await {
                    gateway_sink.send(frame?).await?;
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            let gateway_to_sdk = async {
                let mut opened_dropped_tx = Some(opened_dropped_tx);
                while let Some(frame) = gateway_source.next().await {
                    let frame = frame?;
                    if matches!(&frame, Frame::Opened { .. })
                        && let Some(sender) = opened_dropped_tx.take()
                    {
                        let _ = sender.send(());
                        continue;
                    }
                    sdk_sink.send(frame).await?;
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            tokio::select! {
                result = sdk_to_gateway => result?,
                result = gateway_to_sdk => result?,
            }
            Ok(())
        });
        Ok(Self {
            address,
            opened_dropped,
            task,
        })
    }
}

struct OpenBlockingPeerProxy {
    address: SocketAddr,
    open_blocked: oneshot::Receiver<()>,
    release_open: Option<oneshot::Sender<()>>,
    task: JoinHandle<TestResult>,
}

impl OpenBlockingPeerProxy {
    async fn start(upstream: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (open_blocked_tx, open_blocked) = oneshot::channel();
        let (release_open, release_open_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (entry_stream, _) = listener.accept().await?;
            let owner_stream = TcpStream::connect(upstream).await?;
            let (mut entry_source, mut entry_sink) = entry_stream.into_split();
            let (mut owner_source, mut owner_sink) = owner_stream.into_split();

            let entry_to_owner = async {
                let mut open_blocked_tx = Some(open_blocked_tx);
                let mut release_open_rx = Some(release_open_rx);
                loop {
                    let frame = read_peer_frame(&mut entry_source).await?;
                    if frame[3] == PEER_OPEN_KIND
                        && let Some(sender) = open_blocked_tx.take()
                    {
                        let _ = sender.send(());
                        release_open_rx
                            .take()
                            .ok_or("peer OPEN release gate was already consumed")?
                            .await
                            .map_err(|_| "peer OPEN release sender was dropped")?;
                    }
                    owner_sink.write_all(&frame).await?;
                }
                #[allow(unreachable_code)]
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            let owner_to_entry = async {
                loop {
                    let frame = read_peer_frame(&mut owner_source).await?;
                    entry_sink.write_all(&frame).await?;
                }
                #[allow(unreachable_code)]
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            tokio::select! {
                result = entry_to_owner => result?,
                result = owner_to_entry => result?,
            }
            Ok(())
        });
        Ok(Self {
            address,
            open_blocked,
            release_open: Some(release_open),
            task,
        })
    }

    async fn wait_until_open_blocked(&mut self) -> TestResult {
        timeout(Duration::from_secs(2), &mut self.open_blocked)
            .await
            .map_err(|_| "peer OPEN was not intercepted before the deadline")??;
        Ok(())
    }

    fn release_open(&mut self) -> TestResult {
        self.release_open
            .take()
            .ok_or("peer OPEN was already released")?
            .send(())
            .map_err(|_| "peer OPEN proxy stopped before release")?;
        Ok(())
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn read_peer_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    const HEADER_LEN: usize = 8;
    const MAX_FRAME_LEN: usize = 1024 * 1024;

    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header).await?;
    if &header[..2] != b"GP" || header[2] != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer frame header",
        ));
    }
    let payload_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
    if payload_len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer frame exceeds test proxy limit",
        ));
    }
    let mut frame = Vec::with_capacity(HEADER_LEN + payload_len);
    frame.extend_from_slice(&header);
    frame.resize(HEADER_LEN + payload_len, 0);
    reader.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(frame)
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
        Self::start_with_open_response_timeout(
            name,
            key,
            peer_name,
            peer_key,
            directory,
            Duration::from_secs(1),
        )
        .await
    }

    async fn start_with_control_capacity(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
        control_capacity: usize,
    ) -> TestResult<Self> {
        Self::start_with_limits(
            name,
            key,
            peer_name,
            peer_key,
            directory,
            Duration::from_secs(1),
            control_capacity,
        )
        .await
    }

    async fn start_with_open_response_timeout(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
        open_response_timeout: Duration,
    ) -> TestResult<Self> {
        Self::start_with_limits(
            name,
            key,
            peer_name,
            peer_key,
            directory,
            open_response_timeout,
            1,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_limits(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
        open_response_timeout: Duration,
        control_capacity: usize,
    ) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_address = peer_listener.local_addr()?;
        Self::start_with_listeners(
            name,
            key,
            peer_name,
            peer_key,
            directory,
            sdk_listener,
            peer_listener,
            peer_address,
            open_response_timeout,
            control_capacity,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_peer_listener(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
        peer_listener: TcpListener,
        advertised_peer_address: SocketAddr,
        open_response_timeout: Duration,
    ) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listeners(
            name,
            key,
            peer_name,
            peer_key,
            directory,
            sdk_listener,
            peer_listener,
            advertised_peer_address,
            open_response_timeout,
            1,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_listeners(
        name: &str,
        key: &str,
        peer_name: &str,
        peer_key: &str,
        directory: ShardDirectory,
        sdk_listener: TcpListener,
        peer_listener: TcpListener,
        advertised_peer_address: SocketAddr,
        open_response_timeout: Duration,
        control_capacity: usize,
    ) -> TestResult<Self> {
        let sdk_address = sdk_listener.local_addr()?;
        let routing_command_capacity = control_capacity.max(16);
        let routing = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(name)?,
            InternalGatewayKey::new(key)?,
            GatewayLocator::new(advertised_peer_address.to_string())?,
            route_client_config_with_capacity(routing_command_capacity)?,
        )
        .with_command_queue_capacity(routing_command_capacity)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(200));
        let peer =
            GatewayPeerConfig::new(name, key, [TrustedPeerConfig::new(peer_name, peer_key)?])?
                .with_timeouts(
                    Duration::from_millis(200),
                    Duration::from_millis(200),
                    open_response_timeout,
                );
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new([(CLIENT_ID.to_owned(), CLIENT_KEY.to_owned())])
                .with_max_pending_offers(control_capacity),
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
    route_client_config_with_capacity(16)
}

fn route_client_config_with_capacity(
    command_queue_capacity: usize,
) -> Result<RouteTableClientConfig, TransportError> {
    RouteTableClientConfig::new(
        command_queue_capacity,
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

async fn wait_for_binding_count(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    expected: usize,
) -> TestResult {
    let client_id = ClientId::new(CLIENT_ID)?;
    let expires = Instant::now() + Duration::from_secs(2);
    loop {
        let converged = match client.resolve(generation, &client_id).await {
            Ok(bindings) => bindings.len() == expected,
            Err(error) => {
                expected == 0
                    && error.code() == relaygate_route_table_transport::ErrorCode::NotFound
            }
        };
        if converged {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err(format!("RouteTable did not converge to {expected} bindings").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}
