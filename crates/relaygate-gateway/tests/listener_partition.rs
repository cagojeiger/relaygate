#[allow(dead_code)]
mod support;

use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relaygate_gateway::GatewayConfig;
use relaygate_protocol::{Frame, FrameCodec};
use relaygate_sdk::{
    Config, Connector, Error, ErrorCode, Listener, ListenerRuntime, ListenerStatus,
    PeerObservation, Pipe,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, timeout},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use support::{TestGateway, TestResult};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_gateway_register_response_loss_cleans_session_and_recovers_returned_listener()
-> TestResult {
    timeout(Duration::from_secs(6), register_response_loss_case()).await??;
    Ok(())
}

async fn register_response_loss_case() -> TestResult {
    let gateway = TestGateway::start(&[("echo.alpha", "secret"), ("echo.beta", "secret")]).await?;
    let proxy = FrameProxy::start(
        gateway.address,
        [ProxyMode::DropNthRegistered(2), ProxyMode::Pass],
    )
    .await?;
    let runtime = ListenerRuntime::connect(sdk_config(proxy.address)).await?;
    let alpha = runtime.listen("echo.alpha", "secret").await?;
    let connector = connector(gateway.address).await?;

    let mut old_connector_pipe = connector.open("echo.alpha").await?;
    let mut old_listener_pipe = timeout(Duration::from_secs(1), alpha.accept()).await??;
    old_connector_pipe.write_all_bytes(b"old-pipe").await?;
    let mut payload = [0_u8; 8];
    old_listener_pipe.read_into(&mut payload).await?;
    assert_eq!(&payload, b"old-pipe");
    wait_until("old ListenerSession owns one live Pipe", || {
        gateway.snapshot().live_pipes == 1
    })
    .await?;

    let beta_error = runtime
        .listen("echo.beta", "secret")
        .await
        .err()
        .ok_or_else(|| io::Error::other("dropped REGISTERED unexpectedly succeeded"))?;
    assert_eq!(beta_error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(beta_error.observation(), PeerObservation::MaybeObserved);
    assert_eq!(proxy.dropped_registered(), 1);

    assert_pipe_failed(&mut old_connector_pipe, ErrorCode::Unavailable).await?;
    assert_pipe_failed(&mut old_listener_pipe, ErrorCode::Unavailable).await?;
    wait_until(
        "only the returned Listener is active on a replacement session",
        || {
            let snapshot = gateway.snapshot();
            proxy.connections() >= 2
                && alpha.status() == ListenerStatus::Active
                && snapshot.listener_sessions == 1
                && snapshot.listener_bindings == 1
                && snapshot.live_pipes == 0
        },
    )
    .await?;

    let beta_open = connector
        .open("echo.beta")
        .await
        .err()
        .ok_or_else(|| io::Error::other("failed initial Listener was replayed on recovery"))?;
    assert_eq!(beta_open.code(), ErrorCode::NotFound);
    assert_round_trip(&connector, &alpha, "echo.alpha").await?;

    let beta = runtime.listen("echo.beta", "secret").await?;
    wait_until("application retry installs beta", || {
        gateway.snapshot().listener_bindings == 2
    })
    .await?;
    assert_round_trip(&connector, &beta, "echo.beta").await?;

    connector.close();
    runtime.close();
    proxy.stop().await?;
    gateway.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_recovery_partition_is_cleaned_by_offer_timeout_without_same_open_reroute()
-> TestResult {
    timeout(Duration::from_secs(8), silent_recovery_partition_case()).await??;
    Ok(())
}

async fn silent_recovery_partition_case() -> TestResult {
    let gateway = TestGateway::start_with_config(
        GatewayConfig::new([("echo.alpha".to_owned(), "secret".to_owned())])
            .with_offer_timeout(Duration::from_millis(80))
            .with_heartbeat(Duration::from_secs(5), Duration::from_secs(5)),
    )
    .await?;
    let (disconnect_tx, disconnect_rx) = tokio::sync::watch::channel(false);
    let proxy = FrameProxy::start(
        gateway.address,
        [
            ProxyMode::DisconnectOnSignal(disconnect_rx),
            ProxyMode::DropRegisteredAndHoldGateway(1),
            ProxyMode::Pass,
        ],
    )
    .await?;
    let runtime = ListenerRuntime::connect(sdk_config(proxy.address)).await?;
    let alpha = runtime.listen("echo.alpha", "secret").await?;
    wait_until("initial Listener is active", || {
        let snapshot = gateway.snapshot();
        snapshot.listener_sessions == 1 && snapshot.listener_bindings == 1
    })
    .await?;

    disconnect_tx.send_replace(true);
    wait_until(
        "stale recovery and current Listener bindings coexist",
        || {
            let snapshot = gateway.snapshot();
            proxy.connections() >= 3
                && proxy.dropped_registered() == 1
                && alpha.status() == ListenerStatus::Active
                && snapshot.listener_sessions == 2
                && snapshot.listener_bindings == 2
        },
    )
    .await?;

    let connector = connector(gateway.address).await?;
    let stale_error = open_until_stale_timeout(&connector, &alpha, "echo.alpha").await?;
    assert_eq!(stale_error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(stale_error.observation(), PeerObservation::MaybeObserved);
    assert_no_queued_pipe(&alpha).await?;
    wait_until(
        "OFFER timeout removes only the stale recovery session",
        || {
            let snapshot = gateway.snapshot();
            snapshot.listener_sessions == 1
                && snapshot.listener_bindings == 1
                && snapshot.pending_offers == 0
                && snapshot.live_pipes == 0
        },
    )
    .await?;

    assert_round_trip(&connector, &alpha, "echo.alpha").await?;

    connector.close();
    runtime.close();
    proxy.stop().await?;
    gateway.stop().await?;
    Ok(())
}

fn sdk_config(address: SocketAddr) -> Config {
    Config::new(address.to_string())
        .with_connect_timeout(Duration::from_millis(200))
        .with_operation_timeout(Duration::from_millis(80))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
        .with_heartbeat(Duration::from_secs(5), Duration::from_secs(5))
}

async fn connector(address: SocketAddr) -> TestResult<Connector> {
    Ok(Connector::connect(
        Config::new(address.to_string())
            .with_connect_timeout(Duration::from_millis(200))
            .with_operation_timeout(Duration::from_millis(500)),
    )
    .await?)
}

async fn assert_pipe_failed(pipe: &mut Pipe, code: ErrorCode) -> TestResult {
    let mut byte = [0_u8; 1];
    let error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("old Pipe unexpectedly remained readable"))?;
    assert_eq!(error.code(), code);
    Ok(())
}

async fn assert_round_trip(
    connector: &Connector,
    listener: &Listener,
    client_id: &str,
) -> TestResult {
    let mut opened = connector.open(client_id).await?;
    let mut accepted = timeout(Duration::from_secs(1), listener.accept()).await??;
    opened.write_all_bytes(b"round-trip").await?;
    let mut payload = [0_u8; 10];
    accepted.read_into(&mut payload).await?;
    assert_eq!(&payload, b"round-trip");
    opened.close().await?;
    accepted.close().await?;
    Ok(())
}

async fn assert_no_queued_pipe(listener: &Listener) -> TestResult {
    match timeout(Duration::from_millis(30), listener.accept()).await {
        Err(_) => Ok(()),
        Ok(Ok(_)) => Err("same OPEN was rerouted to the live Listener".into()),
        Ok(Err(error)) => Err(error.into()),
    }
}

async fn open_until_stale_timeout(
    connector: &Connector,
    listener: &Listener,
    client_id: &str,
) -> TestResult<Error> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match connector.open(client_id).await {
            Ok(mut opened) => {
                let mut accepted = timeout(Duration::from_secs(1), listener.accept()).await??;
                opened.close().await?;
                accepted.close().await?;
            }
            Err(error)
                if error.code() == ErrorCode::DeadlineExceeded
                    && error.observation() == PeerObservation::MaybeObserved =>
            {
                return Ok(error);
            }
            Err(error)
                if matches!(error.code(), ErrorCode::Unavailable | ErrorCode::NotFound)
                    && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Err("stale recovery binding was not selected before the deadline".into());
        }
    }
}

async fn wait_until(label: &'static str, mut condition: impl FnMut() -> bool) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {label}").into());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

struct FrameProxy {
    address: SocketAddr,
    connections: Arc<AtomicUsize>,
    dropped_registered: Arc<AtomicUsize>,
    cancel: CancellationToken,
    task: JoinHandle<TestResult>,
}

#[derive(Clone)]
enum ProxyMode {
    Pass,
    DropNthRegistered(usize),
    DropRegisteredAndHoldGateway(usize),
    DisconnectOnSignal(tokio::sync::watch::Receiver<bool>),
}

impl FrameProxy {
    async fn start(
        target: SocketAddr,
        modes: impl IntoIterator<Item = ProxyMode>,
    ) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let modes = Arc::new(Mutex::new(modes.into_iter().collect::<VecDeque<_>>()));
        let connections = Arc::new(AtomicUsize::new(0));
        let dropped_registered = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task_connections = Arc::clone(&connections);
        let task_dropped = Arc::clone(&dropped_registered);
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    accepted = listener.accept() => {
                        let (sdk, _) = accepted?;
                        task_connections.fetch_add(1, Ordering::AcqRel);
                        let mode = {
                            let mut modes = modes.lock().map_err(|_| "proxy mode lock poisoned")?;
                            modes.pop_front().unwrap_or(ProxyMode::Pass)
                        };
                        sessions.spawn(proxy_connection(
                            sdk,
                            target,
                            mode,
                            Arc::clone(&task_dropped),
                            task_cancel.clone(),
                        ));
                    }
                    completed = sessions.join_next(), if !sessions.is_empty() => {
                        if let Some(completed) = completed {
                            completed??;
                        }
                    }
                }
            }
            sessions.abort_all();
            while sessions.join_next().await.is_some() {}
            Ok(())
        });
        Ok(Self {
            address,
            connections,
            dropped_registered,
            cancel,
            task,
        })
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::Acquire)
    }

    fn dropped_registered(&self) -> usize {
        self.dropped_registered.load(Ordering::Acquire)
    }

    async fn stop(self) -> TestResult {
        self.cancel.cancel();
        self.task.await??;
        Ok(())
    }
}

async fn proxy_connection(
    sdk_stream: TcpStream,
    target: SocketAddr,
    mode: ProxyMode,
    dropped_registered: Arc<AtomicUsize>,
    cancel: CancellationToken,
) -> TestResult {
    let gateway_stream = TcpStream::connect(target).await?;
    let sdk = Framed::new(sdk_stream, FrameCodec::default());
    let gateway = Framed::new(gateway_stream, FrameCodec::default());
    match mode {
        ProxyMode::Pass => {
            forward_frames(sdk, gateway, None, false, dropped_registered, cancel).await
        }
        ProxyMode::DropNthRegistered(nth) => {
            forward_frames(sdk, gateway, Some(nth), false, dropped_registered, cancel).await
        }
        ProxyMode::DropRegisteredAndHoldGateway(nth) => {
            forward_frames(sdk, gateway, Some(nth), true, dropped_registered, cancel).await
        }
        ProxyMode::DisconnectOnSignal(signal) => {
            forward_until_signal(sdk, gateway, signal, cancel).await
        }
    }
}

async fn forward_frames(
    mut sdk: Framed<TcpStream, FrameCodec>,
    mut gateway: Framed<TcpStream, FrameCodec>,
    drop_registered: Option<usize>,
    hold_gateway_after_sdk_close: bool,
    dropped_registered: Arc<AtomicUsize>,
    cancel: CancellationToken,
) -> TestResult {
    let mut registered_seen = 0;
    let mut sdk_closed = false;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            frame = sdk.next(), if !sdk_closed => {
                let Some(frame) = frame else {
                    if hold_gateway_after_sdk_close {
                        sdk_closed = true;
                        continue;
                    }
                    return Ok(());
                };
                gateway.send(frame?).await?;
            }
            frame = gateway.next() => {
                let Some(frame) = frame else { return Ok(()); };
                let frame = frame?;
                if matches!(frame, Frame::Registered { .. }) {
                    registered_seen += 1;
                    if drop_registered == Some(registered_seen) {
                        dropped_registered.fetch_add(1, Ordering::AcqRel);
                        continue;
                    }
                }
                if sdk_closed {
                    continue;
                }
                if let Err(error) = sdk.send(frame).await {
                    if hold_gateway_after_sdk_close {
                        sdk_closed = true;
                    } else {
                        return Err(error.into());
                    }
                }
            }
        }
    }
}

async fn forward_until_signal(
    mut sdk: Framed<TcpStream, FrameCodec>,
    mut gateway: Framed<TcpStream, FrameCodec>,
    mut signal: tokio::sync::watch::Receiver<bool>,
    cancel: CancellationToken,
) -> TestResult {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            changed = signal.changed() => {
                if changed.is_err() || *signal.borrow() {
                    return Ok(());
                }
            }
            frame = sdk.next() => {
                let Some(frame) = frame else { return Ok(()); };
                gateway.send(frame?).await?;
            }
            frame = gateway.next() => {
                let Some(frame) = frame else { return Ok(()); };
                sdk.send(frame?).await?;
            }
        }
    }
}
