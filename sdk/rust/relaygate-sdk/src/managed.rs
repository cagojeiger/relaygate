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
    /// Starts one Tokio supervision task and waits until the first session is
    /// authenticated and all desired Listener declarations are bound.
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

impl ManagedCore {
    async fn run(self: Arc<Self>) {
        let mut delay = INITIAL_BACKOFF;
        loop {
            if self.cancelled() {
                self.finish(ManagedState::Closed, None).await;
                return;
            }
            self.set_state(ManagedState::Connecting, None);
            let result = tokio::time::timeout(
                CONNECT_TIMEOUT,
                self.connector.connect(self.config.reconnect_copy()),
            )
            .await;
            let client = match result {
                Ok(Ok(client)) => client,
                Ok(Err(error)) if permanent_connect_error(&error) => {
                    self.finish(ManagedState::Failed, Some(error.to_string()))
                        .await;
                    return;
                }
                Ok(Err(error)) => {
                    self.set_state(ManagedState::Backoff, Some(error.to_string()));
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
                Err(_) => {
                    self.set_state(
                        ManagedState::Backoff,
                        Some("connection attempt timed out".into()),
                    );
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
            };

            let ready_at = match self.install_and_rebind(&client).await {
                Ok(ready_at) => ready_at,
                Err(error) => {
                    client.shared.terminate(crate::SessionError::Closed);
                    if !retryable_managed_error(&error) {
                        self.finish(ManagedState::Failed, Some(error.to_string()))
                            .await;
                        return;
                    }
                    self.set_state(ManagedState::Backoff, Some(error.to_string()));
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
            };

            let mut cancel = self.cancel_tx.subscribe();
            tokio::select! {
                error = client.done() => {
                    if matches!(error, crate::SessionError::Protocol(_)) {
                        self.finish(ManagedState::Failed, Some(error.to_string())).await;
                        return;
                    }
                    if ready_at.elapsed() >= STABLE_WINDOW {
                        delay = INITIAL_BACKOFF;
                    }
                    self.detach().await;
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                }
                _ = wait_cancelled(&mut cancel) => {
                    client.shared.terminate(crate::SessionError::Closed);
                    self.finish(ManagedState::Closed, None).await;
                    return;
                }
            }
        }
    }

    async fn install_and_rebind(&self, client: &Client) -> Result<Instant, ManagedError> {
        let generation = {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.generation = data.generation.wrapping_add(1);
            data.current = Some(client.managed_handle());
            data.state = ManagedState::Rebinding;
            data.failure = None;
            data.generation
        };
        self.clear_current_listeners().await;
        self.publish_state();

        loop {
            let pending = {
                let data = self.data.lock().expect("managed data lock poisoned");
                data.bindings.values().cloned().collect::<Vec<_>>()
            };
            let mut rebound = 0usize;
            for binding in &pending {
                if !binding.active.load(Ordering::Acquire) {
                    continue;
                }
                if binding
                    .current
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|(bound_generation, _)| *bound_generation == generation)
                {
                    rebound += 1;
                    continue;
                }
                let listener = client
                    .bind(binding.endpoint.clone(), binding.target_id.clone())
                    .await
                    .map_err(ManagedError::Bind)?;
                if binding.active.load(Ordering::Acquire) {
                    *binding.current.lock().await = Some((generation, listener));
                    rebound += 1;
                }
            }
            let data = self.data.lock().expect("managed data lock poisoned");
            if data.generation != generation {
                return Err(ManagedError::NotReady);
            }
            let active = data
                .bindings
                .values()
                .filter(|binding| binding.active.load(Ordering::Acquire))
                .count();
            drop(data);
            if rebound == active {
                self.set_state(ManagedState::Ready, None);
                return Ok(Instant::now());
            }
        }
    }

    async fn bind_declaration(&self, binding: &Arc<ManagedBinding>) -> Result<(), ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            if !binding.active.load(Ordering::Acquire) {
                return Err(ManagedError::Closed);
            }
            let (managed_state, generation, client, failure) = {
                let data = self.data.lock().expect("managed data lock poisoned");
                (
                    data.state,
                    data.generation,
                    data.current.as_ref().map(Client::managed_handle),
                    data.failure.clone(),
                )
            };
            match managed_state {
                ManagedState::Failed => {
                    return Err(ManagedError::Failed(
                        failure.unwrap_or_else(|| "managed connection failed".into()),
                    ));
                }
                ManagedState::Closed => return Err(ManagedError::Closed),
                ManagedState::Ready => {
                    let client = client.ok_or(ManagedError::NotReady)?;
                    match client
                        .bind(binding.endpoint.clone(), binding.target_id.clone())
                        .await
                    {
                        Ok(listener) => {
                            let current_generation = self
                                .data
                                .lock()
                                .expect("managed data lock poisoned")
                                .generation;
                            if current_generation == generation
                                && binding.active.load(Ordering::Acquire)
                            {
                                *binding.current.lock().await = Some((generation, listener));
                                self.publish_state();
                                return Ok(());
                            }
                        }
                        Err(BindError::Session(_)) => {}
                        Err(error) => return Err(ManagedError::Bind(error)),
                    }
                }
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    async fn wait_binding(
        &self,
        binding: &Arc<ManagedBinding>,
        observed: u64,
    ) -> Result<u64, ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            if !binding.active.load(Ordering::Acquire) {
                return Err(ManagedError::Closed);
            }
            if let Some((generation, _)) = binding.current.lock().await.as_ref()
                && *generation > observed
            {
                return Ok(*generation);
            }
            match *state.borrow() {
                ManagedState::Failed => return Err(self.failure()),
                ManagedState::Closed => return Err(ManagedError::Closed),
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    async fn wait_ready(&self) -> Result<(), ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            match *state.borrow() {
                ManagedState::Ready => return Ok(()),
                ManagedState::Failed => return Err(self.failure()),
                ManagedState::Closed => return Err(ManagedError::Closed),
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    async fn detach(&self) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.current = None;
            data.state = ManagedState::Backoff;
        }
        self.clear_current_listeners().await;
        self.publish_state();
    }

    async fn clear_current_listeners(&self) {
        let bindings = self
            .data
            .lock()
            .expect("managed data lock poisoned")
            .bindings
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for binding in bindings {
            binding.current.lock().await.take();
        }
    }

    async fn finish(&self, state: ManagedState, failure: Option<String>) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            if let Some(client) = data.current.take() {
                client.shared.terminate(crate::SessionError::Closed);
            }
            data.state = state;
            data.failure = failure;
        }
        self.clear_current_listeners().await;
        self.publish_state();
    }

    fn set_state(&self, state: ManagedState, failure: Option<String>) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.state = state;
            data.failure = failure;
        }
        self.publish_state();
    }

    fn publish_state(&self) {
        let state = self.data.lock().expect("managed data lock poisoned").state;
        self.state_tx.send_replace(state);
    }

    fn remove_binding(&self, binding: &Arc<ManagedBinding>) {
        let key = (binding.endpoint.clone(), binding.target_id.clone());
        let mut data = self.data.lock().expect("managed data lock poisoned");
        if data
            .bindings
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, binding))
        {
            data.bindings.remove(&key);
        }
        drop(data);
        self.publish_state();
    }

    async fn wait_backoff(&self, delay: Duration) -> bool {
        let mut cancel = self.cancel_tx.subscribe();
        tokio::select! {
            _ = sleep(jitter(delay)) => true,
            _ = wait_cancelled(&mut cancel) => false,
        }
    }

    fn failure(&self) -> ManagedError {
        ManagedError::Failed(
            self.data
                .lock()
                .expect("managed data lock poisoned")
                .failure
                .clone()
                .unwrap_or_else(|| "managed connection failed".into()),
        )
    }

    fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    fn cancelled(&self) -> bool {
        *self.cancel_tx.borrow()
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
    matches!(
        error,
        ManagedError::Session(_) | ManagedError::Bind(BindError::Session(_))
    )
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
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        Session, SessionError,
        runtime::{OUTBOUND_CAPACITY, Shared, dispatch_response},
        wire::{self, connect_request, connect_response},
    };

    struct FakeSession {
        number: usize,
        shared: Arc<Shared>,
        outbound: mpsc::Receiver<wire::ConnectRequest>,
    }

    struct FakeConnector {
        attempts: AtomicUsize,
        sessions: mpsc::UnboundedSender<FakeSession>,
    }

    impl Connector for FakeConnector {
        fn connect(
            &self,
            _config: Config,
        ) -> Pin<Box<dyn Future<Output = Result<Client, ConnectError>> + Send + '_>> {
            let number = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
            let sessions = self.sessions.clone();
            Box::pin(async move {
                let (outbound, receiver) = mpsc::channel(OUTBOUND_CAPACITY);
                let shared = Arc::new(Shared::new(
                    outbound,
                    Session {
                        client_session_id: format!("managed-session-{number}"),
                        client_id: "client-1".into(),
                        api_key_id: "key-1".into(),
                        auth_revision: "revision-1".into(),
                    },
                ));
                sessions
                    .send(FakeSession {
                        number,
                        shared: Arc::clone(&shared),
                        outbound: receiver,
                    })
                    .map_err(|_| ConnectError::Protocol("managed test session receiver closed"))?;
                Ok(Client { shared })
            })
        }
    }

    fn harness() -> (ManagedClient, mpsc::UnboundedReceiver<FakeSession>) {
        let (sessions_tx, sessions_rx) = mpsc::unbounded_channel();
        let (state_tx, _) = watch::channel(ManagedState::Connecting);
        let (cancel_tx, _) = watch::channel(false);
        let core = Arc::new(ManagedCore {
            config: Config::new("https://relay.test", "client-1", "key-1", "secret"),
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
            connector: Arc::new(FakeConnector {
                attempts: AtomicUsize::new(0),
                sessions: sessions_tx,
            }),
        });
        let task_core = Arc::clone(&core);
        let task = tokio::spawn(async move { task_core.run().await });
        *core.task.lock().expect("managed task lock poisoned") = Some(task);
        (ManagedClient { core }, sessions_rx)
    }

    async fn next_message(
        outbound: &mut mpsc::Receiver<wire::ConnectRequest>,
    ) -> connect_request::Message {
        outbound
            .recv()
            .await
            .expect("managed outbound request")
            .message
            .expect("managed request message")
    }

    async fn acknowledge_bind(session: &FakeSession, endpoint: &str, target_id: &str) {
        dispatch_response(
            &session.shared,
            wire::ConnectResponse {
                message: Some(connect_response::Message::ListenerBound(
                    wire::ListenerBound {
                        binding: Some(wire::ListenerBinding {
                            listener_binding_id: format!("managed-binding-{}", session.number),
                            endpoint_pattern: endpoint.into(),
                            target_id: target_id.into(),
                        }),
                    },
                )),
            },
        )
        .await
        .expect("dispatch ListenerBound");
    }

    #[tokio::test]
    async fn reconnect_redeclares_current_listener_and_does_not_queue_open() {
        let (client, mut sessions) = harness();
        let mut first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");

        let bind = client.bind("/echo", "server");
        let acknowledge = async {
            assert!(matches!(
                next_message(&mut first.outbound).await,
                connect_request::Message::BindListener(_)
            ));
            acknowledge_bind(&first, "/echo", "server").await;
        };
        let (listener, ()) = tokio::join!(bind, acknowledge);
        let mut listener = listener.expect("managed bind");

        first
            .shared
            .terminate(SessionError::Transport("injected loss".into()));
        let mut state = client.core.state_tx.subscribe();
        while *state.borrow() != ManagedState::Backoff {
            state.changed().await.expect("state transition");
        }
        assert!(matches!(
            client.open("/echo", "server").await,
            Err(ManagedError::NotReady)
        ));

        let mut second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
            .await
            .expect("reconnect timeout")
            .expect("second session");
        assert!(matches!(
            next_message(&mut second.outbound).await,
            connect_request::Message::BindListener(_)
        ));
        acknowledge_bind(&second, "/echo", "server").await;
        client.wait_ready().await.expect("ready after reconnect");
        dispatch_response(
            &second.shared,
            wire::ConnectResponse {
                message: Some(connect_response::Message::ListenerOffer(
                    wire::ListenerOffer {
                        attempt_id: "managed-attempt".into(),
                        listener_binding_id: "managed-binding-2".into(),
                        endpoint: "/echo".into(),
                        target_id: "server".into(),
                        caller_session_id: "caller-session".into(),
                    },
                )),
            },
        )
        .await
        .expect("dispatch ListenerOffer");
        let offer = listener.next().await.expect("managed next").expect("offer");
        assert_eq!(offer.metadata().attempt_id(), "managed-attempt");
        client.close().await;
    }

    #[tokio::test]
    async fn close_cancels_backoff_and_joins_supervisor() {
        let (client, mut sessions) = harness();
        let first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");
        first
            .shared
            .terminate(SessionError::Transport("injected loss".into()));
        let mut state = client.core.state_tx.subscribe();
        while *state.borrow() != ManagedState::Backoff {
            state.changed().await.expect("state transition");
        }
        tokio::time::timeout(Duration::from_millis(200), client.close())
            .await
            .expect("Close must cancel backoff promptly");
    }

    #[tokio::test]
    async fn cancelled_bind_does_not_leave_a_desired_listener() {
        let (client, mut sessions) = harness();
        let mut first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");
        let mut bind = Box::pin(client.bind("/cancelled", "server"));
        let request = tokio::select! {
            request = next_message(&mut first.outbound) => request,
            _ = &mut bind => panic!("Bind completed before response"),
        };
        assert!(matches!(request, connect_request::Message::BindListener(_)));
        drop(bind);
        assert!(
            client
                .core
                .data
                .lock()
                .expect("managed data lock poisoned")
                .bindings
                .is_empty()
        );
        client.close().await;
    }

    #[tokio::test]
    async fn unbind_during_backoff_is_not_redeclared() {
        let (client, mut sessions) = harness();
        let mut first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");
        let bind = client.bind("/temporary", "server");
        let acknowledge = async {
            assert!(matches!(
                next_message(&mut first.outbound).await,
                connect_request::Message::BindListener(_)
            ));
            acknowledge_bind(&first, "/temporary", "server").await;
        };
        let (listener, ()) = tokio::join!(bind, acknowledge);
        let listener = listener.expect("managed bind");
        first
            .shared
            .terminate(SessionError::Transport("injected loss".into()));
        let mut state = client.core.state_tx.subscribe();
        while *state.borrow() != ManagedState::Backoff {
            state.changed().await.expect("state transition");
        }
        listener.unbind().await.expect("unbind during backoff");
        let mut second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
            .await
            .expect("reconnect timeout")
            .expect("second session");
        client
            .wait_ready()
            .await
            .expect("ready without removed bind");
        assert!(matches!(
            second.outbound.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        client.close().await;
    }

    #[tokio::test]
    async fn protocol_failure_stops_without_reconnect() {
        let (client, mut sessions) = harness();
        let first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");
        first
            .shared
            .terminate(SessionError::Protocol("injected protocol failure"));
        assert!(matches!(client.done().await, ManagedError::Failed(_)));
        assert_eq!(client.state(), ManagedState::Failed);
        assert!(sessions.try_recv().is_err());
        client.close().await;
    }
}
