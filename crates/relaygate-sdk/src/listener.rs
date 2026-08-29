mod runtime;
#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use relaygate_protocol::SessionRole;
use tokio::{
    sync::{Notify, mpsc, watch},
    time::{Instant, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Config, Error, ErrorCode, PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime,
    session::{establish, valid_identity},
};

use self::runtime::listener_supervisor;

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

pub(super) struct ListenerRuntimeInner {
    pub(super) config: Config,
    pub(super) desired: StdMutex<HashMap<String, Arc<ListenerState>>>,
    pub(super) reconcile: Arc<Notify>,
    pub(super) cancel: CancellationToken,
    pub(super) lifetime: Weak<RuntimeLifetime>,
}

pub(super) struct ListenerState {
    pub(super) client_id: String,
    pub(super) client_key: String,
    pub(super) status: watch::Sender<ListenerStatus>,
    pub(super) last_error: StdMutex<Option<Error>>,
    pub(super) incoming_tx: mpsc::Sender<Pipe>,
    pub(super) incoming_rx: tokio::sync::Mutex<mpsc::Receiver<Pipe>>,
    pub(super) initial_deadline: Instant,
    lifecycle: StdMutex<ListenerLifecycle>,
    registration_committed: StdMutex<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenerLifecycle {
    Pending,
    Returned,
    Terminal,
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
        let deadline = Instant::now() + self.inner.config.operation_timeout;
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
                    let observation = self.inner.terminate_initial_listener(
                        &state,
                        ErrorCode::DeadlineExceeded,
                        "operation deadline exceeded",
                    );
                    return Err(Error::deadline(observation));
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
    pub async fn accept(&self) -> Result<Pipe> {
        let mut incoming = self.state.incoming_rx.lock().await;
        let mut status = self.state.status.subscribe();
        loop {
            if *status.borrow() == ListenerStatus::Closed {
                return Err(Error::closed());
            }
            match incoming.try_recv() {
                Ok(pipe) => {
                    if let Some(error) = self.accepted_pipe_error(&status) {
                        drop(pipe);
                        return Err(error);
                    }
                    return Ok(pipe);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(Error::closed()),
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            match *status.borrow() {
                ListenerStatus::Blocked => return Err(self.state.blocked_error()),
                ListenerStatus::Closed => return Err(Error::closed()),
                ListenerStatus::Registering
                | ListenerStatus::Active
                | ListenerStatus::Suspended => {}
            }
            tokio::select! {
                biased;
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
                pipe = incoming.recv() => {
                    let pipe = pipe.ok_or_else(Error::closed)?;
                    if let Some(error) = self.accepted_pipe_error(&status) {
                        drop(pipe);
                        return Err(error);
                    }
                    return Ok(pipe);
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

impl ListenerRuntimeInner {
    fn detach_listener(&self, state: &Arc<ListenerState>) {
        state.close(None);
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.client_id)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.client_id);
            self.reconcile.notify_one();
        }
    }

    fn drop_listener(&self, state: &Arc<ListenerState>) {
        self.detach_listener(state);
    }

    pub(super) fn close_all(&self) {
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => poisoned.into_inner(),
        };
        let states = desired.drain().map(|(_, state)| state).collect::<Vec<_>>();
        for state in states {
            state.close(None);
        }
        self.reconcile.notify_one();
    }

    pub(super) fn fail_initial_listener(&self, state: &Arc<ListenerState>, error: Error) {
        if !state.fail_initial(error) {
            return;
        }
        self.remove_terminal_listener(state);
    }

    fn terminate_initial_listener(
        &self,
        state: &Arc<ListenerState>,
        code: ErrorCode,
        message: &str,
    ) -> PeerObservation {
        let observation = state
            .terminate_initial_operation(code, message)
            .unwrap_or(PeerObservation::NotObserved);
        self.remove_terminal_listener(state);
        observation
    }

    pub(super) fn remove_terminal_listener(&self, state: &Arc<ListenerState>) {
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.client_id)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.client_id);
        }
        self.reconcile.notify_one();
    }
}

impl ListenerState {
    pub(super) fn set_status(&self, status: ListenerStatus, error: Option<Error>) {
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = error;
        }
        self.status.send_replace(status);
    }

    fn last_error(&self) -> Option<Error> {
        self.last_error.lock().ok().and_then(|error| error.clone())
    }

    fn blocked_error(&self) -> Error {
        self.last_error().unwrap_or_else(|| {
            Error::new(
                ErrorCode::PermissionDenied,
                PeerObservation::NotObserved,
                "Listener registration is blocked",
            )
        })
    }

    pub(super) fn was_returned(&self) -> bool {
        self.lifecycle
            .lock()
            .is_ok_and(|lifecycle| *lifecycle == ListenerLifecycle::Returned)
    }

    fn promote_returned(&self) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        match *lifecycle {
            ListenerLifecycle::Pending => {
                *lifecycle = ListenerLifecycle::Returned;
                true
            }
            ListenerLifecycle::Returned => true,
            ListenerLifecycle::Terminal => false,
        }
    }

    fn fail_initial(&self, error: Error) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle != ListenerLifecycle::Pending {
            return false;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Closed, Some(error));
        true
    }

    pub(super) fn suspend_or_fail_initial(
        &self,
        recovery_error: Error,
        initial_error: Error,
    ) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        match *lifecycle {
            ListenerLifecycle::Pending => {
                *lifecycle = ListenerLifecycle::Terminal;
                self.finish_registration_attempt();
                self.set_status(ListenerStatus::Closed, Some(initial_error));
                true
            }
            ListenerLifecycle::Returned => {
                self.finish_registration_attempt();
                self.set_status(ListenerStatus::Suspended, Some(recovery_error));
                false
            }
            ListenerLifecycle::Terminal => false,
        }
    }

    pub(super) fn handle_precommit_session_end(&self, recovery_error: Error) {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return;
        };
        self.finish_registration_attempt();
        match *lifecycle {
            ListenerLifecycle::Pending => {
                self.set_status(ListenerStatus::Registering, None);
            }
            ListenerLifecycle::Returned => {
                self.set_status(ListenerStatus::Suspended, Some(recovery_error));
            }
            ListenerLifecycle::Terminal => {}
        }
    }

    pub(super) fn block(&self, error: Error) {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Blocked, Some(error));
    }

    pub(super) fn activate(&self) -> bool {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Active, None);
        true
    }

    fn close(&self, error: Option<Error>) {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return;
        };
        if *lifecycle == ListenerLifecycle::Terminal
            && *self.status.borrow() == ListenerStatus::Closed
        {
            return;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = false;
        }
        self.set_status(ListenerStatus::Closed, error);
    }

    pub(super) fn begin_registration_commit(&self) -> bool {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = true;
            self.set_status(ListenerStatus::Registering, None);
            true
        } else {
            false
        }
    }

    pub(super) fn finish_registration_attempt(&self) {
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = false;
        }
    }

    fn terminate_initial_operation(
        &self,
        code: ErrorCode,
        message: &str,
    ) -> Option<PeerObservation> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        if *lifecycle != ListenerLifecycle::Pending {
            return None;
        }
        let observation = self.registration_committed.lock().map_or(
            PeerObservation::NotObserved,
            |mut committed| {
                if std::mem::take(&mut *committed) {
                    PeerObservation::MaybeObserved
                } else {
                    PeerObservation::NotObserved
                }
            },
        );
        *lifecycle = ListenerLifecycle::Terminal;
        self.set_status(
            ListenerStatus::Closed,
            Some(Error::new(code, observation, message)),
        );
        Some(observation)
    }
}

impl Listener {
    fn accepted_pipe_error(&self, status: &watch::Receiver<ListenerStatus>) -> Option<Error> {
        match *status.borrow() {
            ListenerStatus::Closed => Some(Error::closed()),
            ListenerStatus::Registering
            | ListenerStatus::Active
            | ListenerStatus::Suspended
            | ListenerStatus::Blocked => None,
        }
    }

    async fn drain_unaccepted(&self) -> Result<()> {
        let operation = async {
            let mut incoming = self.state.incoming_rx.lock().await;
            incoming.close();
            while let Ok(pipe) = incoming.try_recv() {
                drop(pipe);
            }
        };
        timeout(self.inner.config.operation_timeout, operation)
            .await
            .map_err(|_| Error::deadline(PeerObservation::MaybeObserved))
    }
}
