mod runtime;
mod state;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use relaygate_protocol::{PipeId, SessionId};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot, watch},
    time::{sleep_until, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Config, DestinationId, Error, ErrorCode, PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime, session::establish,
};

use self::{
    runtime::relay_supervisor,
    state::{ListenerLifecycle, ListenerState, RelayInner, is_current_desired},
};

/// Current state of one desired Listener handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListenerStatus {
    Registering,
    Active,
    Suspended,
    Blocked,
    Closed,
}

#[derive(Clone)]
pub struct Relay {
    inner: Arc<RelayInner>,
    _lifetime: Arc<RuntimeLifetime>,
}

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
        destination_id: DestinationId,
        response: oneshot::Sender<Result<Pipe>>,
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
            .field("destination_id", &self.state.destination_id)
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
        let cancel = CancellationToken::new();
        let lifetime = Arc::new(RuntimeLifetime::new(cancel.clone()));
        let inner = Arc::new(RelayInner {
            config,
            desired: StdMutex::new(HashMap::new()),
            current,
            reconcile: Arc::new(Notify::new()),
            cancel,
            lifetime: Arc::downgrade(&lifetime),
        });
        tokio::spawn(relay_supervisor(Arc::clone(&inner), established));
        Ok(Self {
            inner,
            _lifetime: lifetime,
        })
    }

    /// Creates one desired Listener for a Destination and waits until its initial
    /// Gateway-local binding is active.
    pub async fn listen(&self, destination_id: DestinationId) -> Result<Listener> {
        let deadline = self.inner.config.operation_deadline()?;
        let (incoming_tx, incoming_rx) = mpsc::channel(self.inner.config.listener_queue_capacity);
        let (status, _) = watch::channel(ListenerStatus::Registering);
        let state = Arc::new(ListenerState {
            destination_id,
            status,
            last_error: StdMutex::new(None),
            incoming_tx,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
            initial_deadline: deadline,
            lifecycle: StdMutex::new(ListenerLifecycle::Pending),
            registration_committed: StdMutex::new(false),
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
            if desired.contains_key(&destination_id) {
                return Err(Error::new(
                    ErrorCode::AlreadyExists,
                    PeerObservation::NotObserved,
                    "a non-closed Listener already owns this Destination in the Relay",
                ));
            }
            desired.insert(destination_id, Arc::clone(&state));
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

    /// Opens one Pipe to a Destination. A committed dial is never replayed.
    pub async fn dial(&self, destination_id: DestinationId) -> Result<Pipe> {
        let deadline = self.inner.config.operation_deadline()?;
        let mut current = self.inner.current.subscribe();
        loop {
            if self.inner.cancel.is_cancelled() {
                return Err(Error::closed());
            }
            let session = current.borrow().clone();
            if let Some(session) = session {
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
                        destination_id,
                        response: response_tx,
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
}

impl Listener {
    #[must_use]
    pub fn destination_id(&self) -> DestinationId {
        self.state.destination_id
    }

    #[must_use]
    pub fn status(&self) -> ListenerStatus {
        *self.state.status.borrow()
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
