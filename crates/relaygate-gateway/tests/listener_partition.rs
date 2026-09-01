#[allow(dead_code)]
mod support;

use std::{
    collections::{HashMap, VecDeque},
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
use relaygate_protocol::{BindingId, Frame, FrameCodec, SessionId};
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
    let connector = connector(gateway.address, Duration::from_millis(500)).await?;

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

    let connector = connector(gateway.address, Duration::from_millis(500)).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_gateway_offer_timeout_cleans_selected_session_and_recovers_returned_listeners()
-> TestResult {
    timeout(Duration::from_secs(10), offer_timeout_recovery_case()).await??;
    Ok(())
}

async fn offer_timeout_recovery_case() -> TestResult {
    let gateway = TestGateway::start_with_config(
        GatewayConfig::new([
            ("echo.alpha".to_owned(), "secret".to_owned()),
            ("echo.beta".to_owned(), "secret".to_owned()),
            ("echo.gamma".to_owned(), "secret".to_owned()),
        ])
        .with_offer_timeout(Duration::from_secs(1))
        .with_heartbeat(Duration::from_secs(5), Duration::from_secs(5)),
    )
    .await?;
    let proxy = FrameProxy::start(
        gateway.address,
        [
            ProxyMode::DropOfferAcceptedForClient("echo.alpha"),
            ProxyMode::Pass,
        ],
    )
    .await?;

    let selected_runtime = ListenerRuntime::connect(
        sdk_config(proxy.address)
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let selected_alpha = selected_runtime.listen("echo.alpha", "secret").await?;
    let selected_beta = selected_runtime.listen("echo.beta", "secret").await?;
    let (initial_alpha_registration, initial_beta_registration) = proxy
        .registration_pair("echo.alpha", "echo.beta", None)
        .ok_or_else(|| io::Error::other("initial listener registrations were not observed"))?;
    assert_ne!(
        initial_alpha_registration.binding_id,
        initial_beta_registration.binding_id
    );

    let sibling_runtime = ListenerRuntime::connect(sdk_config(gateway.address)).await?;
    let sibling_gamma = sibling_runtime.listen("echo.gamma", "secret").await?;
    wait_until(
        "both ListenerSessions own their pre-fault binding sets",
        || {
            let snapshot = gateway.snapshot();
            snapshot.listener_sessions == 2 && snapshot.listener_bindings == 3
        },
    )
    .await?;

    let connector = connector(gateway.address, Duration::from_secs(3)).await?;
    let mut old_connector_pipe = connector.open("echo.beta").await?;
    let mut old_listener_pipe = timeout(Duration::from_secs(1), selected_beta.accept()).await??;
    old_connector_pipe.write_all_bytes(b"old-pipe").await?;
    let mut payload = [0_u8; 8];
    old_listener_pipe.read_into(&mut payload).await?;
    assert_eq!(&payload, b"old-pipe");
    wait_until(
        "selected ListenerSession owns the existing beta Pipe",
        || gateway.snapshot().live_pipes == 1,
    )
    .await?;

    let opening = {
        let connector = connector.clone();
        tokio::spawn(async move { connector.open("echo.alpha").await })
    };
    wait_until("selected OFFER_ACCEPTED is dropped", || {
        proxy.dropped_offer_accepted() == 1
    })
    .await?;
    let sibling_alpha = sibling_runtime.listen("echo.alpha", "secret").await?;
    wait_until(
        "same-ClientId sibling is registered before OFFER expiry",
        || gateway.snapshot().listener_bindings == 4,
    )
    .await?;

    let alpha_error = opening
        .await?
        .err()
        .ok_or_else(|| io::Error::other("dropped OFFER_ACCEPTED unexpectedly opened a Pipe"))?;
    assert_eq!(alpha_error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(alpha_error.observation(), PeerObservation::MaybeObserved);
    assert_eq!(proxy.dropped_offer_accepted(), 1);
    assert_no_queued_pipe(&sibling_alpha).await?;

    assert_pipe_failed(&mut old_connector_pipe, ErrorCode::Unavailable).await?;
    assert_pipe_failed(&mut old_listener_pipe, ErrorCode::Unavailable).await?;
    wait_until(
        "offer timeout removes only the selected ListenerSession",
        || {
            let snapshot = gateway.snapshot();
            proxy.connections() == 1
                && selected_alpha.status() == ListenerStatus::Suspended
                && selected_beta.status() == ListenerStatus::Suspended
                && sibling_alpha.status() == ListenerStatus::Active
                && sibling_gamma.status() == ListenerStatus::Active
                && snapshot.listener_sessions == 1
                && snapshot.listener_bindings == 2
                && snapshot.pending_offers == 0
                && snapshot.live_pipes == 0
        },
    )
    .await?;

    assert_round_trip(&connector, &sibling_alpha, "echo.alpha").await?;
    assert_round_trip(&connector, &sibling_gamma, "echo.gamma").await?;

    wait_until(
        "returned alpha and beta recover on a replacement ListenerSession",
        || {
            let snapshot = gateway.snapshot();
            proxy.connections() >= 2
                && selected_alpha.status() == ListenerStatus::Active
                && selected_beta.status() == ListenerStatus::Active
                && snapshot.listener_sessions == 2
                && snapshot.listener_bindings == 4
                && snapshot.pending_offers == 0
                && snapshot.live_pipes == 0
                && proxy
                    .registration_pair(
                        "echo.alpha",
                        "echo.beta",
                        Some(initial_alpha_registration.session_id),
                    )
                    .is_some()
        },
    )
    .await?;
    let (recovered_alpha_registration, recovered_beta_registration) = proxy
        .registration_pair(
            "echo.alpha",
            "echo.beta",
            Some(initial_alpha_registration.session_id),
        )
        .ok_or_else(|| io::Error::other("recovered listener registrations were not observed"))?;
    assert_ne!(
        recovered_alpha_registration.session_id,
        initial_alpha_registration.session_id
    );
    assert_ne!(
        recovered_alpha_registration.binding_id,
        initial_alpha_registration.binding_id
    );
    assert_ne!(
        recovered_beta_registration.binding_id,
        initial_beta_registration.binding_id
    );
    assert_ne!(
        recovered_alpha_registration.binding_id,
        recovered_beta_registration.binding_id
    );
    assert_no_queued_pipe(&selected_alpha).await?;
    assert_round_trip(&connector, &selected_beta, "echo.beta").await?;

    connector.close();
    selected_runtime.close();
    sibling_runtime.close();
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

async fn connector(address: SocketAddr, operation_timeout: Duration) -> TestResult<Connector> {
    Ok(Connector::connect(
        Config::new(address.to_string())
            .with_connect_timeout(Duration::from_millis(200))
            .with_operation_timeout(operation_timeout),
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
    drops: DropCounters,
    registrations: Arc<Mutex<Vec<RegistrationObservation>>>,
    cancel: CancellationToken,
    task: JoinHandle<TestResult>,
}

#[derive(Clone)]
struct RegistrationObservation {
    client_id: String,
    session_id: SessionId,
    binding_id: BindingId,
}

#[derive(Clone)]
struct DropCounters {
    registered: Arc<AtomicUsize>,
    offer_accepted: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum ProxyMode {
    Pass,
    DropNthRegistered(usize),
    DropOfferAcceptedForClient(&'static str),
    DropRegisteredAndHoldGateway(usize),
    DisconnectOnSignal(tokio::sync::watch::Receiver<bool>),
}

struct ForwardBehavior {
    drop_registered: Option<usize>,
    drop_offer_accepted_for: Option<&'static str>,
    hold_gateway_after_sdk_close: bool,
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
        let drops = DropCounters {
            registered: Arc::new(AtomicUsize::new(0)),
            offer_accepted: Arc::new(AtomicUsize::new(0)),
        };
        let registrations = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task_connections = Arc::clone(&connections);
        let task_drops = drops.clone();
        let task_registrations = Arc::clone(&registrations);
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
                            task_drops.clone(),
                            Arc::clone(&task_registrations),
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
            drops,
            registrations,
            cancel,
            task,
        })
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::Acquire)
    }

    fn dropped_registered(&self) -> usize {
        self.drops.registered.load(Ordering::Acquire)
    }

    fn dropped_offer_accepted(&self) -> usize {
        self.drops.offer_accepted.load(Ordering::Acquire)
    }

    fn registration_pair(
        &self,
        first_client_id: &str,
        second_client_id: &str,
        excluded_session_id: Option<SessionId>,
    ) -> Option<(RegistrationObservation, RegistrationObservation)> {
        let registrations = match self.registrations.lock() {
            Ok(registrations) => registrations,
            Err(poisoned) => poisoned.into_inner(),
        };
        registrations
            .iter()
            .filter(|registration| {
                registration.client_id == first_client_id
                    && excluded_session_id != Some(registration.session_id)
            })
            .find_map(|first| {
                registrations
                    .iter()
                    .find(|second| {
                        second.client_id == second_client_id
                            && second.session_id == first.session_id
                    })
                    .map(|second| (first.clone(), second.clone()))
            })
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
    drops: DropCounters,
    registrations: Arc<Mutex<Vec<RegistrationObservation>>>,
    cancel: CancellationToken,
) -> TestResult {
    let gateway_stream = TcpStream::connect(target).await?;
    let sdk = Framed::new(sdk_stream, FrameCodec::default());
    let gateway = Framed::new(gateway_stream, FrameCodec::default());
    let behavior = match mode {
        ProxyMode::Pass => ForwardBehavior {
            drop_registered: None,
            drop_offer_accepted_for: None,
            hold_gateway_after_sdk_close: false,
        },
        ProxyMode::DropNthRegistered(nth) => ForwardBehavior {
            drop_registered: Some(nth),
            drop_offer_accepted_for: None,
            hold_gateway_after_sdk_close: false,
        },
        ProxyMode::DropOfferAcceptedForClient(client_id) => ForwardBehavior {
            drop_registered: None,
            drop_offer_accepted_for: Some(client_id),
            hold_gateway_after_sdk_close: false,
        },
        ProxyMode::DropRegisteredAndHoldGateway(nth) => ForwardBehavior {
            drop_registered: Some(nth),
            drop_offer_accepted_for: None,
            hold_gateway_after_sdk_close: true,
        },
        ProxyMode::DisconnectOnSignal(signal) => {
            return forward_until_signal(sdk, gateway, signal, cancel).await;
        }
    };
    forward_frames(sdk, gateway, behavior, drops, registrations, cancel).await
}

async fn forward_frames(
    mut sdk: Framed<TcpStream, FrameCodec>,
    mut gateway: Framed<TcpStream, FrameCodec>,
    behavior: ForwardBehavior,
    drops: DropCounters,
    registrations: Arc<Mutex<Vec<RegistrationObservation>>>,
    cancel: CancellationToken,
) -> TestResult {
    let mut registered_seen = 0;
    let mut offer_to_drop = None;
    let mut sdk_closed = false;
    let mut session_id = None;
    let mut pending_registrations = HashMap::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            frame = sdk.next(), if !sdk_closed => {
                let Some(frame) = frame else {
                    if behavior.hold_gateway_after_sdk_close {
                        sdk_closed = true;
                        continue;
                    }
                    return Ok(());
                };
                let frame = frame?;
                if let Frame::Register {
                    request_id,
                    client_id,
                    ..
                } = &frame
                {
                    pending_registrations.insert(*request_id, client_id.clone());
                }
                if let Frame::OfferAccepted { pipe_id } = &frame
                    && offer_to_drop == Some(*pipe_id)
                {
                    drops.offer_accepted.fetch_add(1, Ordering::AcqRel);
                    offer_to_drop = None;
                    continue;
                }
                gateway.send(frame).await?;
            }
            frame = gateway.next() => {
                let Some(frame) = frame else { return Ok(()); };
                let frame = frame?;
                if let Frame::Welcome {
                    session_id: welcomed_session_id,
                } = &frame
                {
                    session_id = Some(*welcomed_session_id);
                }
                if let Frame::Registered {
                    request_id,
                    binding_id,
                } = &frame
                    && let (Some(client_id), Some(session_id)) =
                        (pending_registrations.remove(request_id), session_id)
                {
                    let observation = RegistrationObservation {
                        client_id,
                        session_id,
                        binding_id: *binding_id,
                    };
                    match registrations.lock() {
                        Ok(mut registrations) => registrations.push(observation),
                        Err(poisoned) => poisoned.into_inner().push(observation),
                    }
                }
                if let Frame::RegisterFailed { request_id, .. } = &frame {
                    pending_registrations.remove(request_id);
                }
                if matches!(frame, Frame::Registered { .. }) {
                    registered_seen += 1;
                    if behavior.drop_registered == Some(registered_seen) {
                        drops.registered.fetch_add(1, Ordering::AcqRel);
                        continue;
                    }
                }
                if let Frame::Offer {
                    pipe_id,
                    client_id,
                    ..
                } = &frame
                    && behavior.drop_offer_accepted_for == Some(client_id.as_str())
                {
                    offer_to_drop = Some(*pipe_id);
                }
                if sdk_closed {
                    continue;
                }
                if let Err(error) = sdk.send(frame).await {
                    if behavior.hold_gateway_after_sdk_close {
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
