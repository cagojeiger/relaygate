mod runtime;
mod state;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak, atomic::AtomicU64},
};

use relaygate_protocol::{BearerToken, PipeId, SessionId};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot, watch},
    time::{sleep_until, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    AccessAction, AccessTokenRequest, AccessTokenSource, Config, Destination, Error, ErrorCode,
    PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime,
    resource::{LivePipeReservation, RelayResources},
    session::{ReconnectBackoff, establish},
};

use self::{
    runtime::relay_supervisor,
    state::{ListenerLifecycle, ListenerState, RelayInner, is_current_desired},
};

/// Current state of one desired Listener handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListenerStatus {
    /// The Listener is publishing or republishing its binding.
    Registering,
    /// The Listener has a current Gateway binding and can receive Pipes.
    Active,
    /// The returned Listener is waiting for a transient republish recovery.
    Suspended,
    /// Republish failed permanently; recreate the Listener with new inputs.
    Blocked,
    /// The Listener is terminal.
    Closed,
}

/// Current state of the shared Relay session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelayStatus {
    /// A current HELLO/WELCOME transport session is installed.
    Active,
    /// No current session is installed and managed reconnect is running.
    Reconnecting,
    /// The Relay runtime is terminal.
    Closed,
}

/// Subscription to the latest Relay status.
///
/// This is a coalescing subscription: slow consumers observe the latest status
/// after each change, not every intermediate transition. Calling [`current`]
/// consumes the current version, so a later [`changed`] call waits for a newer
/// status.
///
/// [`current`]: Self::current
/// [`changed`]: Self::changed
pub struct RelayStatusSubscription {
    status: watch::Receiver<RelayStatus>,
}

/// Subscription to the latest Listener status.
///
/// This is a coalescing subscription: slow consumers observe the latest status
/// after each change, not every intermediate transition. Calling [`current`]
/// consumes the current version, so a later [`changed`] call waits for a newer
/// status.
///
/// [`current`]: Self::current
/// [`changed`]: Self::changed
pub struct ListenerStatusSubscription {
    status: watch::Receiver<ListenerStatus>,
}

/// A shared application session to one RelayGate Gateway.
///
/// A Relay manages reconnect and republishes each desired [`Listener`]. Pipes
/// remain session-scoped and are never replayed across reconnects.
#[derive(Clone)]
pub struct Relay {
    inner: Arc<RelayInner>,
    _lifetime: Arc<RuntimeLifetime>,
}

/// A desired publication for one [`Destination`].
///
/// The SDK keeps the publication desired across managed Relay reconnects until
/// the Listener is closed or dropped.
pub struct Listener {
    inner: Arc<RelayInner>,
    _lifetime: Arc<RuntimeLifetime>,
    state: Arc<ListenerState>,
}

pub(super) struct RelaySession {
    pub(super) id: SessionId,
    pub(super) next_connection_id: Mutex<u64>,
    pub(super) commands: mpsc::Sender<RelayCommand>,
    pub(super) cancellations: mpsc::UnboundedSender<PipeId>,
    pub(super) cancel: CancellationToken,
}

pub(super) enum RelayCommand {
    Dial {
        connection_id: u64,
        destination: Destination,
        access_token: relaygate_protocol::BearerToken,
        response: oneshot::Sender<Result<Pipe>>,
        resources: LivePipeReservation,
    },
}

struct DialGuard {
    cancellations: mpsc::UnboundedSender<PipeId>,
    pipe_id: PipeId,
    armed: bool,
}

impl Drop for DialGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cancellations.send(self.pipe_id);
        }
    }
}

struct ListenGuard {
    inner: Weak<RelayInner>,
    state: Arc<ListenerState>,
    armed: bool,
}

impl Drop for ListenGuard {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            inner.terminate_initial_listener(
                &self.state,
                ErrorCode::Cancelled,
                "listen operation was cancelled",
            );
        }
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Listener")
            .field("destination", &self.state.destination)
            .field("status", &self.status())
            .finish()
    }
}

impl Relay {
    /// Connects the initial shared Relay session and starts managed
    /// reconnection for every desired Listener handle.
    pub async fn connect(config: Config) -> Result<Self> {
        config.validate()?;
        let established = establish(&config).await?;
        let (current, _) = watch::channel(None);
        let (status, _) = watch::channel(RelayStatus::Active);
        let cancel = CancellationToken::new();
        let lifetime = Arc::new(RuntimeLifetime::new(cancel.clone()));
        let inner = Arc::new(RelayInner {
            resources: RelayResources::new(config.resource_limits),
            reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
            republish_retry_epoch: Arc::new(AtomicU64::new(0)),
            republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
                config.reconnect_initial,
                config.reconnect_maximum,
            ))),
            config,
            desired: StdMutex::new(HashMap::new()),
            current,
            status,
            reconcile: Arc::new(Notify::new()),
            cancel,
            lifetime: Arc::downgrade(&lifetime),
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(relay_supervisor(
            Arc::clone(&inner),
            established,
            Some(ready_tx),
        ));
        ready_rx.await.map_err(|_| Error::closed())?;
        Ok(Self {
            inner,
            _lifetime: lifetime,
        })
    }

    /// Creates one desired Listener for a destination and waits until its initial
    /// Gateway-local binding is active.
    pub async fn listen(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Listener> {
        let deadline = self.inner.config.operation_deadline()?;
        let limits = self.inner.config.resource_limits;
        let (incoming_tx, incoming_rx) = mpsc::channel(limits.max_pending_pipes_per_listener);
        let (status, _) = watch::channel(ListenerStatus::Registering);
        let state = Arc::new(ListenerState {
            destination: destination.clone(),
            access_token_source,
            status,
            last_error: StdMutex::new(None),
            incoming_tx,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
            initial_deadline: deadline,
            lifecycle: StdMutex::new(ListenerLifecycle::Pending),
            registration_committed: StdMutex::new(false),
            live_pipe_slots: self
                .inner
                .resources
                .listener_slots(limits.max_live_pipes_per_listener),
        });
        {
            let mut desired = self.inner.desired.lock().map_err(|_| {
                self.inner.cancel.cancel();
                Error::new(
                    ErrorCode::Internal,
                    PeerObservation::NotObserved,
                    "Listener registry lock is poisoned",
                )
            })?;
            if desired.contains_key(&destination) {
                return Err(Error::new(
                    ErrorCode::AlreadyExists,
                    PeerObservation::NotObserved,
                    "a non-closed Listener already owns this destination in the Relay",
                ));
            }
            desired.insert(destination, Arc::clone(&state));
        }

        let mut guard = ListenGuard {
            inner: Arc::downgrade(&self.inner),
            state: Arc::clone(&state),
            armed: true,
        };
        self.inner.reconcile.notify_one();

        let mut status = state.status.subscribe();
        loop {
            match *status.borrow() {
                ListenerStatus::Active => {
                    if !state.promote_returned() {
                        continue;
                    }
                    guard.armed = false;
                    return Ok(Listener {
                        inner: Arc::clone(&self.inner),
                        _lifetime: Arc::clone(&self._lifetime),
                        state,
                    });
                }
                ListenerStatus::Blocked => {
                    return Err(state.last_error().unwrap_or_else(|| {
                        Error::new(
                            ErrorCode::PermissionDenied,
                            PeerObservation::NotObserved,
                            "Listener registration is blocked",
                        )
                    }));
                }
                ListenerStatus::Closed => {
                    return Err(state.last_error().unwrap_or_else(Error::closed));
                }
                ListenerStatus::Registering | ListenerStatus::Suspended => {}
            }
            tokio::select! {
                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                _ = sleep_until(deadline) => {
                    let error = self.inner.terminate_initial_listener(
                        &state,
                        ErrorCode::DeadlineExceeded,
                        "operation deadline exceeded",
                    );
                    return Err(error);
                }
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
            }
        }
    }

    /// Opens one Pipe to a destination. A committed dial is never replayed.
    pub async fn dial(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Pipe> {
        crate::observability::observe("dial", self.dial_inner(destination, access_token_source))
            .await
    }

    async fn dial_inner(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Pipe> {
        let deadline = self.inner.config.operation_deadline()?;
        let mut current = self.inner.current.subscribe();
        let mut supplied_access_token: Option<BearerToken> = None;
        loop {
            if self.inner.cancel.is_cancelled() {
                return Err(Error::closed());
            }
            let session = current.borrow().clone();
            if let Some(session) = session {
                let access_token = match supplied_access_token.as_ref() {
                    Some(access_token) => access_token.clone(),
                    None => {
                        let access_token = timeout_at(
                            deadline,
                            access_token_source.supply(AccessTokenRequest {
                                action: AccessAction::Dial,
                                destination: destination.clone(),
                            }),
                        )
                        .await
                        .map_err(|_| Error::deadline(PeerObservation::NotObserved))??;
                        supplied_access_token = Some(access_token.clone());
                        access_token
                    }
                };
                let resources = self.inner.resources.try_reserve_outgoing()?;
                let mut next_connection_id =
                    timeout_at(deadline, session.next_connection_id.lock())
                        .await
                        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?;
                let connection_id = *next_connection_id;
                *next_connection_id = connection_id.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::ResourceExhausted,
                        PeerObservation::NotObserved,
                        "RelaySession exhausted ConnectionId space",
                    )
                })?;
                let pipe_id = PipeId::new(session.id, connection_id);
                let (response_tx, response_rx) = oneshot::channel();
                let committed = timeout_at(
                    deadline,
                    session.commands.send(RelayCommand::Dial {
                        connection_id,
                        destination: destination.clone(),
                        access_token: access_token.clone(),
                        response: response_tx,
                        resources,
                    }),
                )
                .await;
                drop(next_connection_id);
                match committed {
                    Ok(Ok(())) => {
                        let mut guard = DialGuard {
                            cancellations: session.cancellations.clone(),
                            pipe_id,
                            armed: true,
                        };
                        return match timeout_at(deadline, response_rx).await {
                            Ok(Ok(result)) => {
                                guard.armed = false;
                                result
                            }
                            Ok(Err(_)) => {
                                guard.armed = false;
                                Err(Error::maybe_observed(
                                    "RelaySession ended after DIAL commit",
                                ))
                            }
                            Err(_) => {
                                session.cancel.cancel();
                                Err(Error::deadline(PeerObservation::MaybeObserved))
                            }
                        };
                    }
                    Ok(Err(_)) => {
                        if current
                            .borrow()
                            .as_ref()
                            .is_some_and(|active| Arc::ptr_eq(active, &session))
                        {
                            tokio::select! {
                                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                                _ = sleep_until(deadline) => {
                                    return Err(Error::deadline(PeerObservation::NotObserved));
                                }
                                changed = current.changed() => {
                                    if changed.is_err() { return Err(Error::closed()); }
                                }
                            }
                        }
                    }
                    Err(_) => return Err(Error::deadline(PeerObservation::NotObserved)),
                }
                continue;
            }

            tokio::select! {
                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                _ = sleep_until(deadline) => {
                    return Err(Error::deadline(PeerObservation::NotObserved));
                }
                changed = current.changed() => {
                    if changed.is_err() { return Err(Error::closed()); }
                }
            }
        }
    }

    /// Stops managed reconnection and closes all desired Listener handles.
    pub fn close(&self) {
        self.inner.cancel.cancel();
        self.inner.close_all();
    }

    /// Returns the latest shared Relay session status.
    #[must_use]
    pub fn status(&self) -> RelayStatus {
        *self.inner.status.borrow()
    }

    /// Subscribes to coalesced Relay status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> RelayStatusSubscription {
        RelayStatusSubscription {
            status: self.inner.status.subscribe(),
        }
    }

    /// Waits until a current Relay session is active.
    ///
    /// Returns an error with [`ErrorCode::Cancelled`] when the Relay has
    /// already closed.
    ///
    /// [`ErrorCode::Cancelled`]: crate::ErrorCode::Cancelled
    pub async fn wait_ready(&self) -> Result<()> {
        let mut status = self.inner.status.subscribe();
        let mut current = self.inner.current.subscribe();
        loop {
            match *status.borrow() {
                RelayStatus::Active if current.borrow().is_some() => return Ok(()),
                RelayStatus::Active => {}
                RelayStatus::Closed => return Err(Error::closed()),
                RelayStatus::Reconnecting => {}
            }
            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
                changed = current.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
            }
        }
    }
}

impl Listener {
    /// Returns the destination owned by this Listener.
    #[must_use]
    pub fn destination(&self) -> &Destination {
        &self.state.destination
    }

    /// Returns this Listener's latest status.
    #[must_use]
    pub fn status(&self) -> ListenerStatus {
        *self.state.status.borrow()
    }

    /// Subscribes to coalesced Listener status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> ListenerStatusSubscription {
        ListenerStatusSubscription {
            status: self.state.status.subscribe(),
        }
    }

    /// Returns one incoming Pipe exactly once.
    ///
    /// While registration is suspended or being recovered, this waits for a
    /// Pipe from the next active Relay session. Unaccepted Pipes owned by an
    /// ended session are discarded. A blocked or closed Listener returns its
    /// terminal error without yielding an older queued Pipe.
    pub async fn accept(&self) -> Result<Pipe> {
        let mut status = self.state.status.subscribe();
        loop {
            let current_status = *status.borrow();
            match current_status {
                ListenerStatus::Blocked => {
                    self.state.drain_unaccepted(true).await;
                    return Err(self.state.blocked_error());
                }
                ListenerStatus::Closed => {
                    self.state.drain_unaccepted(true).await;
                    return Err(Error::closed());
                }
                ListenerStatus::Registering | ListenerStatus::Suspended => {
                    if status.changed().await.is_err() {
                        return Err(Error::closed());
                    }
                    continue;
                }
                ListenerStatus::Active => {}
            }

            // Hold the single-consumer lane only while ACTIVE. A session-end
            // status change wins the biased select, releases this lock, and
            // lets the session actor drain the old queue before reconnecting.
            let mut incoming = self.state.incoming_rx.lock().await;
            if *status.borrow() != ListenerStatus::Active {
                drop(incoming);
                continue;
            }
            match incoming.try_recv() {
                Ok(pipe) => {
                    drop(incoming);
                    if let Some(result) = self.classify_received_pipe(pipe, &status) {
                        return result;
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(Error::closed()),
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            tokio::select! {
                biased;
                changed = status.changed() => {
                    drop(incoming);
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
                pipe = incoming.recv() => {
                    let pipe = pipe.ok_or_else(Error::closed)?;
                    drop(incoming);
                    if let Some(result) = self.classify_received_pipe(pipe, &status) {
                        return result;
                    }
                }
            }
        }
    }

    /// Removes this desired Listener without closing sibling handles or Pipes
    /// that the application already accepted.
    pub async fn close(&self) -> Result<()> {
        self.inner.detach_listener(&self.state);
        self.drain_unaccepted().await
    }
}

impl RelayStatusSubscription {
    /// Returns the latest status and marks it as observed.
    #[must_use]
    pub fn current(&mut self) -> RelayStatus {
        *self.status.borrow_and_update()
    }

    /// Waits for a newer status and returns the latest value.
    ///
    /// Returns `None` when the Relay runtime has dropped the sender.
    pub async fn changed(&mut self) -> Option<RelayStatus> {
        self.status.changed().await.ok()?;
        Some(*self.status.borrow_and_update())
    }
}

impl ListenerStatusSubscription {
    /// Returns the latest status and marks it as observed.
    #[must_use]
    pub fn current(&mut self) -> ListenerStatus {
        *self.status.borrow_and_update()
    }

    /// Waits for a newer status and returns the latest value.
    ///
    /// Returns `None` when the Listener state has dropped the sender.
    pub async fn changed(&mut self) -> Option<ListenerStatus> {
        self.status.changed().await.ok()?;
        Some(*self.status.borrow_and_update())
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.inner.drop_listener(&self.state);
    }
}

impl Listener {
    fn classify_received_pipe(
        &self,
        pipe: Pipe,
        status: &watch::Receiver<ListenerStatus>,
    ) -> Option<Result<Pipe>> {
        // The final ACTIVE + non-terminal observation is accept's success
        // linearization point. A later session/peer failure is observed by
        // Pipe I/O, just like a socket may close immediately after accept.
        match *status.borrow() {
            ListenerStatus::Active if !pipe.is_terminal() => Some(Ok(pipe)),
            ListenerStatus::Active => {
                drop(pipe);
                None
            }
            ListenerStatus::Blocked => {
                drop(pipe);
                Some(Err(self.state.blocked_error()))
            }
            ListenerStatus::Closed => {
                drop(pipe);
                Some(Err(Error::closed()))
            }
            ListenerStatus::Registering | ListenerStatus::Suspended => {
                drop(pipe);
                None
            }
        }
    }

    async fn drain_unaccepted(&self) -> Result<()> {
        let operation = async {
            self.state.drain_unaccepted(true).await;
        };
        timeout(self.inner.config.operation_timeout, operation)
            .await
            .map_err(|_| Error::deadline(PeerObservation::MaybeObserved))
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, time::Duration};

    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, SessionId};
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};
    use tokio_util::codec::Framed;

    use super::{Relay, RelayStatus};
    use crate::Config;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

    #[tokio::test]
    async fn connect_returns_after_initial_current_session_is_installed() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            if !matches!(
                transport.next().await.ok_or("SDK closed before HELLO")??,
                Frame::Hello
            ) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            transport
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            let _ = shutdown_rx.await;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let relay = Relay::connect(Config::new_insecure_for_tests(address.to_string())).await?;
        assert_eq!(relay.status(), RelayStatus::Active);
        assert!(relay.inner.current.borrow().is_some());
        timeout(Duration::from_secs(1), relay.wait_ready()).await??;

        relay.close();
        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }
}
