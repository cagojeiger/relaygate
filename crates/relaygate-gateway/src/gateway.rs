use std::{
    collections::{HashSet, VecDeque},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionRole};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{Semaphore, mpsc},
    task::JoinSet,
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::{
    GatewayConfig, GatewayError, GatewayRoutingConfig, GatewaySnapshot,
    auth::ClientKeyStore,
    routing::{RoutingHandle, RoutingRuntime},
    state::GatewayAction,
    state::GatewayLimits,
    state::GatewayState,
    state::ProtocolViolation,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const MIN_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(10);
const MAX_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<GatewayState>,
    writer_queue_capacity: usize,
    max_frame_len: usize,
    offer_timeout: Duration,
    session_slots: Arc<Semaphore>,
    routing: Option<RoutingHandle>,
    routing_runtime: Mutex<Option<RoutingRuntime>>,
}

impl fmt::Debug for Gateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Gateway")
            .field("routing_enabled", &self.inner.routing.is_some())
            .finish_non_exhaustive()
    }
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Result<Self, GatewayError> {
        Self::build(config, None)
    }

    /// Creates a Gateway whose local ListenerBinding state is projected to
    /// the configured memory-only RouteTable shards.
    ///
    /// RouteTable availability is not a startup prerequisite. The manager
    /// reconnects in the background while local SDK sessions remain usable.
    /// Construction must occur inside a running Tokio runtime.
    pub fn new_routed(
        config: GatewayConfig,
        routing_config: GatewayRoutingConfig,
        shutdown: CancellationToken,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            GatewayError::Routing(format!(
                "routed Gateway must be constructed inside a Tokio runtime: {error}"
            ))
        })?;
        let runtime = RoutingRuntime::start(
            routing_config,
            relaygate_route_table::GatewayId::new(),
            shutdown,
        )?;
        let handle = runtime.handle();
        Self::build(config, Some((handle, runtime)))
    }

    fn build(
        config: GatewayConfig,
        routing: Option<(RoutingHandle, RoutingRuntime)>,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let limits = GatewayLimits {
            max_sessions: config.max_sessions,
            max_bindings: config.max_bindings,
            max_pending_offers: config.max_pending_offers,
            max_live_pipes: config.max_live_pipes,
            offer_timeout: config.offer_timeout,
        };
        let (routing, routing_runtime) = routing
            .map(|(handle, runtime)| (Some(handle), Some(runtime)))
            .unwrap_or((None, None));
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(GatewayState::new(
                    ClientKeyStore::new(config.client_keys),
                    limits,
                )),
                writer_queue_capacity: config.writer_queue_capacity,
                max_frame_len: config.max_frame_len,
                offer_timeout: config.offer_timeout,
                session_slots: Arc::new(Semaphore::new(config.max_sessions)),
                routing,
                routing_runtime: Mutex::new(routing_runtime),
            }),
        })
    }

    /// Serves SDK sessions until `shutdown` is cancelled.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let routing_runtime = self.inner.take_routing_runtime();
        if self.inner.routing.is_some() && routing_runtime.is_none() {
            return Err(GatewayError::Routing(
                "a routed Gateway runtime can only be served once".to_owned(),
            ));
        }
        let sdk = self.serve_sdk(listener, shutdown.clone());
        let Some(routing_runtime) = routing_runtime else {
            return sdk.await;
        };

        tokio::pin!(sdk);
        let routing = routing_runtime.wait();
        tokio::pin!(routing);
        tokio::select! {
            result = &mut sdk => {
                shutdown.cancel();
                let routing_result = routing.await;
                result?;
                routing_result.map_err(GatewayError::from)
            }
            result = &mut routing => {
                shutdown.cancel();
                let sdk_result = sdk.await;
                result.map_err(GatewayError::from)?;
                sdk_result
            }
        }
    }

    async fn serve_sdk(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut sessions = JoinSet::new();
        let mut offer_sweep = tokio::time::interval(offer_sweep_interval(self.inner.offer_timeout));
        offer_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = offer_sweep.tick() => self.inner.expire_offers(),
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    let Ok(session_slot) = Arc::clone(&self.inner.session_slots).try_acquire_owned()
                    else {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.session.rejected",
                            %peer_addr,
                            reason = "session_limit",
                            "rejecting SDK connection because the session limit is reached"
                        );
                        drop(stream);
                        continue;
                    };
                    if let Err(error) = stream.set_nodelay(true) {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.socket.configure_failed",
                            %peer_addr,
                            %error,
                            "failed to enable TCP_NODELAY"
                        );
                    }
                    let inner = Arc::clone(&self.inner);
                    let session_shutdown = shutdown.child_token();
                    sessions.spawn(async move {
                        let _session_slot = session_slot;
                        if let Err(error) = inner.run_session(stream, session_shutdown).await {
                            tracing::debug!(
                                component = "gateway",
                                event = "gateway.session.task_ended",
                                %peer_addr,
                                %error,
                                "SDK session ended"
                            );
                        }
                    });
                }
                completed = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.session.task_failed",
                            %error,
                            "SDK session task failed"
                        );
                    }
                }
            }
        }

        shutdown.cancel();
        while let Some(result) = sessions.join_next().await {
            if let Err(error) = result {
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.session.shutdown_task_failed",
                    %error,
                    "SDK session task failed during shutdown"
                );
            }
        }
        Ok(())
    }

    /// Returns the current counts for local SDK sessions, bindings, and Pipes.
    ///
    /// This is an instantaneous observation of this Gateway instance. Callers
    /// must not interpret it as a cluster-wide or durable view.
    #[must_use]
    pub fn snapshot(&self) -> GatewaySnapshot {
        let mut snapshot = self.inner.lock_state().snapshot();
        if let Some(routing) = &self.inner.routing {
            let counts = routing.current_counts();
            snapshot.route_registrations_synced = counts.synced;
            snapshot.route_registrations_unsynced = counts.unsynced;
        }
        snapshot
    }
}

impl Inner {
    async fn run_session(
        self: Arc<Self>,
        stream: TcpStream,
        cancellation: CancellationToken,
    ) -> Result<(), SessionError> {
        let mut framed = Framed::new(stream, FrameCodec::new(self.max_frame_len));
        let first = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = timeout(HANDSHAKE_TIMEOUT, framed.next()) => {
                result
                    .map_err(|_| SessionError::HandshakeTimeout)?
                    .ok_or(SessionError::HandshakeClosed)??
            }
        };
        let Frame::Hello { role } = first else {
            return Err(SessionError::ExpectedHello);
        };

        let (sender, receiver) = mpsc::channel(self.writer_queue_capacity);
        let Some(session_id) = self
            .lock_state()
            .add_session(role, sender, cancellation.clone())
        else {
            return Err(SessionError::ResourceExhausted);
        };
        if let Err(error) = framed.send(Frame::Welcome { session_id }).await {
            self.cleanup(session_id);
            return Err(SessionError::Protocol(error));
        }
        let (sink, source) = framed.split();
        let read = Arc::clone(&self).read_frames(session_id, source);
        let write = write_frames(receiver, sink);
        let result = tokio::select! {
            _ = cancellation.cancelled() => Ok(()),
            result = read => result,
            result = write => result,
        };
        cancellation.cancel();
        self.cleanup(session_id);
        result
    }

    async fn read_frames(
        self: Arc<Self>,
        session_id: relaygate_protocol::SessionId,
        mut source: futures_util::stream::SplitStream<Framed<TcpStream, FrameCodec>>,
    ) -> Result<(), SessionError> {
        while let Some(frame) = source.next().await {
            let actions = {
                let mut state = self.lock_state();
                let actions = state.handle(session_id, frame?)?;
                self.commit_registration_actions(&actions);
                actions
            };
            self.execute_all(actions);
        }
        Ok(())
    }

    fn cleanup(&self, session_id: relaygate_protocol::SessionId) {
        let actions = {
            let mut state = self.lock_state();
            let actions = state.remove_session(session_id);
            self.commit_registration_actions(&actions);
            actions
        };
        self.execute_all(actions);
    }

    fn expire_offers(&self) {
        let actions = {
            let mut state = self.lock_state();
            let actions = state.expire_offers(std::time::Instant::now());
            self.commit_registration_actions(&actions);
            actions
        };
        self.execute_all(actions);
    }

    fn execute_all(&self, actions: Vec<GatewayAction>) {
        let mut pending = VecDeque::from(actions);
        let mut cleaned = HashSet::new();
        while let Some(action) = pending.pop_front() {
            match action {
                GatewayAction::SendSdkFrame(delivery) => {
                    let Some(failed_session) = delivery.deliver() else {
                        continue;
                    };
                    if cleaned.insert(failed_session) {
                        let cleanup_actions = {
                            let mut state = self.lock_state();
                            let actions = state.remove_session(failed_session);
                            self.commit_registration_actions(&actions);
                            actions
                        };
                        pending.extend(cleanup_actions);
                    }
                }
                GatewayAction::PublishRegistration { .. } => {}
            }
        }
    }

    /// Commits the latest complete snapshot while the Gateway state lock still
    /// orders the corresponding local mutation. The manager wake is bounded
    /// and synchronous; no network I/O occurs under this lock. This prevents a
    /// delayed action from publishing an older snapshot after session cleanup.
    fn commit_registration_actions(&self, actions: &[GatewayAction]) {
        let Some(routing) = &self.routing else {
            return;
        };
        for action in actions {
            let GatewayAction::PublishRegistration {
                session_id,
                bindings,
            } = action
            else {
                continue;
            };
            if let Err(error) = routing.publish_session(*session_id, bindings.clone()) {
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.registration.publish_failed",
                    listener_session_id = %session_id.as_uuid(),
                    %error,
                    "local registration remains active while RouteTable publication is unavailable"
                );
            }
        }
    }

    fn take_routing_runtime(&self) -> Option<RoutingRuntime> {
        match self.routing_runtime.lock() {
            Ok(mut runtime) => runtime.take(),
            Err(poisoned) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.routing_runtime.lock_poisoned",
                    "recovering poisoned routing runtime lock"
                );
                poisoned.into_inner().take()
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, GatewayState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.state.lock_poisoned",
                    "recovering poisoned Gateway state lock"
                );
                poisoned.into_inner()
            }
        }
    }
}

fn offer_sweep_interval(offer_timeout: Duration) -> Duration {
    offer_timeout.clamp(MIN_OFFER_SWEEP_INTERVAL, MAX_OFFER_SWEEP_INTERVAL)
}

async fn write_frames(
    mut receiver: mpsc::Receiver<Frame>,
    mut sink: futures_util::stream::SplitSink<Framed<TcpStream, FrameCodec>, Frame>,
) -> Result<(), SessionError> {
    while let Some(frame) = receiver.recv().await {
        sink.send(frame).await?;
    }
    Ok(())
}

/// Performs a protocol-level TCP health check without creating application state.
pub async fn check(address: impl ToSocketAddrs, deadline: Duration) -> Result<(), GatewayError> {
    timeout(deadline, async {
        let stream = TcpStream::connect(address).await?;
        let mut framed = Framed::new(stream, FrameCodec::default());
        framed
            .send(Frame::Hello {
                role: SessionRole::Connector,
            })
            .await?;
        match framed.next().await {
            Some(Ok(Frame::Welcome { .. })) => Ok(()),
            Some(Ok(_)) | None => Err(GatewayError::UnexpectedHealthResponse),
            Some(Err(error)) => Err(GatewayError::Protocol(error)),
        }
    })
    .await
    .map_err(|_| GatewayError::HealthCheckTimeout)?
}

#[derive(Debug, thiserror::Error)]
enum SessionError {
    #[error("SDK session closed before HELLO")]
    HandshakeClosed,
    #[error("SDK session did not send HELLO before the handshake deadline")]
    HandshakeTimeout,
    #[error("first SDK frame was not HELLO")]
    ExpectedHello,
    #[error("Gateway SDK session limit reached")]
    ResourceExhausted,
    #[error(transparent)]
    Protocol(#[from] relaygate_protocol::ProtocolError),
    #[error(transparent)]
    ProtocolViolation(#[from] ProtocolViolation),
}

#[cfg(test)]
mod tests {
    use crate::state::GatewayAction;
    use relaygate_protocol::{ClientKey, ErrorCode, Frame, SessionRole};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{Gateway, GatewayConfig};

    #[test]
    fn full_delivery_queue_immediately_removes_the_failed_session_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(GatewayConfig::new([(
            "echo.shared".to_owned(),
            "secret".to_owned(),
        )]))?;
        let (listener_sender, _listener_receiver) = mpsc::channel(1);
        listener_sender.try_send(Frame::Ping { nonce: 1 })?;
        let (connector_sender, _connector_receiver) = mpsc::channel(8);
        let (listener, connector, offer) = {
            let mut state = gateway.inner.lock_state();
            let listener = state
                .add_session(
                    SessionRole::Listener,
                    listener_sender,
                    CancellationToken::new(),
                )
                .ok_or("missing listener session")?;
            let connector = state
                .add_session(
                    SessionRole::Connector,
                    connector_sender,
                    CancellationToken::new(),
                )
                .ok_or("missing connector session")?;
            let _registration = state.handle(
                listener,
                Frame::Register {
                    request_id: 1,
                    client_id: "echo.shared".to_owned(),
                    client_key: ClientKey::new("secret"),
                },
            )?;
            let offer = state.handle(
                connector,
                Frame::Open {
                    connection_id: 1,
                    client_id: "echo.shared".to_owned(),
                },
            )?;
            (listener, connector, offer)
        };

        gateway.inner.execute_all(offer);

        let after_cleanup = gateway.inner.lock_state().handle(
            connector,
            Frame::Open {
                connection_id: 2,
                client_id: "echo.shared".to_owned(),
            },
        )?;
        assert!(matches!(
            after_cleanup.first().and_then(|action| match action {
                GatewayAction::SendSdkFrame(delivery) => Some(&delivery.frame),
                GatewayAction::PublishRegistration { .. } => None,
            }),
            Some(Frame::OpenFailed {
                code: ErrorCode::NotFound,
                ..
            })
        ));
        assert!(
            gateway
                .inner
                .lock_state()
                .handle(listener, Frame::Ping { nonce: 2 })?
                .is_empty()
        );
        Ok(())
    }
}
