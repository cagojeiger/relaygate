mod core;

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{Instant, sleep},
};
use tonic::Code;
use uuid::Uuid;

use crate::{
    BindError, Client, Config, ConnectError, Listener, ManagedError, Offer, OpenError, Pipe,
    runtime::{MAX_ENDPOINT_BYTES, MAX_IDENTITY_BYTES, MAX_LISTENERS, valid_text},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAXIMUM_BACKOFF: Duration = Duration::from_secs(10);
const STABLE_WINDOW: Duration = Duration::from_secs(30);
const JITTER_PERCENT: u128 = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ManagedState {
    Connecting,
    Rebinding,
    Ready,
    Backoff,
    Failed,
    Closed,
}

type BindingKey = (String, String);

struct ManagedBinding {
    endpoint: String,
    target_id: String,
    active: AtomicBool,
    current: Mutex<Option<(u64, Listener)>>,
}

struct ManagedData {
    state: ManagedState,
    current: Option<Client>,
    generation: u64,
    bindings: HashMap<BindingKey, Arc<ManagedBinding>>,
    failure: Option<String>,
}

trait Connector: Send + Sync {
    fn connect(
        &self,
        config: Config,
    ) -> Pin<Box<dyn Future<Output = Result<Client, ConnectError>> + Send + '_>>;
}

struct DefaultConnector;

impl Connector for DefaultConnector {
    fn connect(
        &self,
        config: Config,
    ) -> Pin<Box<dyn Future<Output = Result<Client, ConnectError>> + Send + '_>> {
        Box::pin(Client::connect(config))
    }
}

struct ManagedCore {
    config: Config,
    data: StdMutex<ManagedData>,
    state_tx: watch::Sender<ManagedState>,
    cancel_tx: watch::Sender<bool>,
    task: StdMutex<Option<JoinHandle<()>>>,
    connector: Arc<dyn Connector>,
}

/// An in-process supervisor for fresh authenticated RelayGate sessions.
///
/// It rebinds only current Listener declarations. Open, Offer, Pipe, and
/// payload state never cross a session boundary.
pub struct ManagedClient {
    core: Arc<ManagedCore>,
}

/// A logical current-state Listener declaration whose underlying Listener is
/// replaced after a successful reconnect.
pub struct ManagedListener {
    core: Arc<ManagedCore>,
    binding: Arc<ManagedBinding>,
}

struct ManagedConnectGuard {
    core: Weak<ManagedCore>,
    armed: bool,
}

struct ManagedBindingGuard {
    core: Weak<ManagedCore>,
    binding: Arc<ManagedBinding>,
    armed: bool,
}

impl Drop for ManagedConnectGuard {
    fn drop(&mut self) {
        if self.armed
            && let Some(core) = self.core.upgrade()
        {
            core.cancel();
        }
    }
}

impl Drop for ManagedBindingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.binding.active.store(false, Ordering::Release);
        if let Some(core) = self.core.upgrade() {
            core.remove_binding(&self.binding);
        }
    }
}

impl ManagedClient {
    /// Recommended application entry point. Starts one Tokio supervision task
    /// and waits until the first session is authenticated and all desired
    /// Listener declarations are bound.
    pub async fn connect(config: Config) -> Result<Self, ManagedError> {
        config
            .validate()
            .map_err(|error| ManagedError::Failed(error.to_string()))?;
        let (state_tx, _) = watch::channel(ManagedState::Connecting);
        let (cancel_tx, _) = watch::channel(false);
        let core = Arc::new(ManagedCore {
            config,
            data: StdMutex::new(ManagedData {
                state: ManagedState::Connecting,
                current: None,
                generation: 0,
                bindings: HashMap::new(),
                failure: None,
            }),
            state_tx,
            cancel_tx,
            task: StdMutex::new(None),
            connector: Arc::new(DefaultConnector),
        });
        let task_core = Arc::clone(&core);
        let task = tokio::spawn(async move { task_core.run().await });
        *core.task.lock().expect("managed task lock poisoned") = Some(task);
        let mut guard = ManagedConnectGuard {
            core: Arc::downgrade(&core),
            armed: true,
        };
        core.wait_ready().await?;
        guard.armed = false;
        Ok(Self { core })
    }

    pub fn state(&self) -> ManagedState {
        self.core
            .data
            .lock()
            .expect("managed data lock poisoned")
            .state
    }

    pub async fn wait_ready(&self) -> Result<(), ManagedError> {
        self.core.wait_ready().await
    }

    /// Declares a logical Listener and waits for its current-session binding.
    pub async fn bind(
        &self,
        endpoint: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Result<ManagedListener, ManagedError> {
        let endpoint = endpoint.into();
        let target_id = target_id.into();
        if !valid_text(&endpoint, MAX_ENDPOINT_BYTES) || !valid_text(&target_id, MAX_IDENTITY_BYTES)
        {
            return Err(ManagedError::Bind(BindError::InvalidRequest));
        }
        let key = (endpoint.clone(), target_id.clone());
        let binding = Arc::new(ManagedBinding {
            endpoint,
            target_id,
            active: AtomicBool::new(true),
            current: Mutex::new(None),
        });
        {
            let mut data = self.core.data.lock().expect("managed data lock poisoned");
            if data.state == ManagedState::Failed {
                return Err(ManagedError::Failed(
                    data.failure
                        .clone()
                        .unwrap_or_else(|| "managed connection failed".into()),
                ));
            }
            if data.state == ManagedState::Closed {
                return Err(ManagedError::Closed);
            }
            if data.bindings.contains_key(&key) {
                return Err(ManagedError::BindingExists);
            }
            if data.bindings.len() >= MAX_LISTENERS {
                return Err(ManagedError::CapacityReached);
            }
            data.bindings.insert(key, Arc::clone(&binding));
        }
        self.core.publish_state();
        let mut guard = ManagedBindingGuard {
            core: Arc::downgrade(&self.core),
            binding: Arc::clone(&binding),
            armed: true,
        };
        if let Err(error) = self.core.bind_declaration(&binding).await {
            self.core.remove_binding(&binding);
            return Err(error);
        }
        guard.armed = false;
        Ok(ManagedListener {
            core: Arc::clone(&self.core),
            binding,
        })
    }

    /// Performs exactly one Open on the current Ready session. It never waits
    /// in a reconnect queue and never retries on a later session.
    pub async fn open(
        &self,
        endpoint: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Result<Pipe, ManagedError> {
        let client = {
            let data = self.core.data.lock().expect("managed data lock poisoned");
            if data.state != ManagedState::Ready {
                return Err(ManagedError::NotReady);
            }
            data.current.as_ref().map(Client::managed_handle)
        }
        .ok_or(ManagedError::NotReady)?;
        client
            .open(endpoint, target_id)
            .await
            .map_err(OpenError::into)
    }

    pub async fn done(&self) -> ManagedError {
        let mut state = self.core.state_tx.subscribe();
        loop {
            match *state.borrow() {
                ManagedState::Failed => return self.core.failure(),
                ManagedState::Closed => return ManagedError::Closed,
                _ => {}
            }
            if state.changed().await.is_err() {
                return ManagedError::Closed;
            }
        }
    }

    /// Cancels backoff/connect work and joins the single supervision task.
    pub async fn close(self) {
        self.core.cancel();
        let task = self
            .core
            .task
            .lock()
            .expect("managed task lock poisoned")
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for ManagedClient {
    fn drop(&mut self) {
        self.core.cancel();
    }
}

impl ManagedListener {
    pub fn endpoint(&self) -> &str {
        &self.binding.endpoint
    }

    pub fn target_id(&self) -> &str {
        &self.binding.target_id
    }

    /// Waits across reconnects for an Offer on the current underlying
    /// Listener. Returned Offers remain bound to the session that created them.
    pub async fn next(&mut self) -> Result<Option<Offer>, ManagedError> {
        let mut observed = 0;
        loop {
            let generation = self.core.wait_binding(&self.binding, observed).await?;
            let mut current = self.binding.current.lock().await;
            let Some((current_generation, listener)) = current.as_mut() else {
                observed = generation;
                continue;
            };
            if *current_generation != generation {
                observed = generation;
                continue;
            }
            match listener.next().await {
                Ok(offer) => return Ok(offer),
                Err(error) => {
                    let session_ended = listener.shared.terminal().is_some();
                    observed = generation;
                    if !session_ended {
                        return Err(ManagedError::Session(error));
                    }
                }
            }
        }
    }

    /// Removes the desired declaration before current-session cleanup.
    pub async fn unbind(&self) -> Result<(), ManagedError> {
        if self.binding.active.swap(false, Ordering::AcqRel) {
            self.core.remove_binding(&self.binding);
        }
        let listener = self.binding.current.lock().await.take();
        if let Some((_, listener)) = listener {
            listener.unbind().await?;
        }
        Ok(())
    }
}

impl Drop for ManagedListener {
    fn drop(&mut self) {
        if self.binding.active.swap(false, Ordering::AcqRel) {
            self.core.remove_binding(&self.binding);
            if let Ok(mut current) = self.binding.current.try_lock() {
                current.take();
            }
        }
    }
}

async fn wait_cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() {
            return;
        }
        if cancel.changed().await.is_err() {
            return;
        }
    }
}

fn permanent_connect_error(error: &ConnectError) -> bool {
    match error {
        ConnectError::InvalidConfig(_) | ConnectError::Protocol(_) => true,
        ConnectError::Rpc(status) => matches!(
            status.code(),
            Code::InvalidArgument
                | Code::Unauthenticated
                | Code::PermissionDenied
                | Code::FailedPrecondition
        ),
        ConnectError::Transport(_) => false,
    }
}

fn retryable_managed_error(error: &ManagedError) -> bool {
    match error {
        ManagedError::Session(error) | ManagedError::Bind(BindError::Session(error)) => {
            !permanent_session_error(error)
        }
        ManagedError::Bind(BindError::Unavailable) => true,
        _ => false,
    }
}

fn permanent_session_error(error: &crate::SessionError) -> bool {
    match error {
        crate::SessionError::Protocol(_) => true,
        crate::SessionError::Rpc { code, .. } => matches!(
            code,
            Code::InvalidArgument
                | Code::Unauthenticated
                | Code::PermissionDenied
                | Code::FailedPrecondition
        ),
        crate::SessionError::Closed | crate::SessionError::Transport(_) => false,
    }
}

fn next_backoff(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(MAXIMUM_BACKOFF)
}

fn jitter(delay: Duration) -> Duration {
    let width = JITTER_PERCENT * 2 + 1;
    let sample = Uuid::new_v4().as_u128() % width;
    let percent = 100 - JITTER_PERCENT + sample;
    delay.mul_f64(percent as f64 / 100.0)
}

#[cfg(test)]
mod tests;
