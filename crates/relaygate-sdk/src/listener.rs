mod runtime;
mod state;
#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use relaygate_protocol::SessionRole;
use tokio::{
    sync::{Notify, mpsc, watch},
    time::{sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Config, Error, ErrorCode, PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime,
    session::{establish, valid_identity},
};

use self::{
    runtime::listener_supervisor,
    state::{ListenerLifecycle, ListenerRuntimeInner, ListenerState, is_current_desired},
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
pub struct ListenerRuntime {
    inner: Arc<ListenerRuntimeInner>,
    _lifetime: Arc<RuntimeLifetime>,
}

pub struct Listener {
    inner: Arc<ListenerRuntimeInner>,
    _lifetime: Arc<RuntimeLifetime>,
    state: Arc<ListenerState>,
}

struct ListenGuard {
    inner: Weak<ListenerRuntimeInner>,
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
            .field("client_id", &self.state.client_id)
            .field("status", &self.status())
            .finish()
    }
}

impl ListenerRuntime {
    /// Connects the initial shared Listener session and starts managed
    /// reconnection for every desired Listener handle.
    pub async fn connect(config: Config) -> Result<Self> {
        config.validate()?;
        let established = establish(&config, SessionRole::Listener).await?;
        let cancel = CancellationToken::new();
        let lifetime = Arc::new(RuntimeLifetime::new(cancel.clone()));
        let inner = Arc::new(ListenerRuntimeInner {
            config,
            desired: StdMutex::new(HashMap::new()),
            reconcile: Arc::new(Notify::new()),
            cancel,
            lifetime: Arc::downgrade(&lifetime),
        });
        tokio::spawn(listener_supervisor(Arc::clone(&inner), established));
        Ok(Self {
            inner,
            _lifetime: lifetime,
        })
    }

    /// Creates one desired Listener for `ClientId` and waits until its initial
    /// Gateway-local binding is active.
    pub async fn listen(
        &self,
        client_id: impl Into<String>,
        client_key: impl Into<String>,
    ) -> Result<Listener> {
        let client_id = client_id.into();
        let client_key = client_key.into();
        if !valid_identity(&client_id) || !valid_identity(&client_key) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "ClientId and ClientKey must be non-empty and fit the wire limit",
            ));
        }
        let deadline = self.inner.config.operation_deadline()?;
        let (incoming_tx, incoming_rx) = mpsc::channel(self.inner.config.listener_queue_capacity);
        let (status, _) = watch::channel(ListenerStatus::Registering);
        let state = Arc::new(ListenerState {
            client_id: client_id.clone(),
            client_key,
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
            if desired.contains_key(&client_id) {
                return Err(Error::new(
                    ErrorCode::AlreadyExists,
                    PeerObservation::NotObserved,
                    "a non-closed Listener already owns this ClientId in the runtime",
                ));
            }
            desired.insert(client_id, Arc::clone(&state));
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

    /// Stops managed reconnection and closes all desired Listener handles.
    pub fn close(&self) {
        self.inner.cancel.cancel();
        self.inner.close_all();
    }
}

impl Listener {
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.state.client_id
    }

    #[must_use]
    pub fn status(&self) -> ListenerStatus {
        *self.state.status.borrow()
    }

    /// Returns one incoming Pipe exactly once.
    ///
    /// While registration is suspended or being recovered, this waits for a
    /// Pipe from the next active Listener session. Unaccepted Pipes owned by an
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
