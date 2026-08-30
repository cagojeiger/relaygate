use std::{
    collections::{BTreeMap, HashMap},
    future::{Future, pending},
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use relaygate_protocol::SessionId;
use relaygate_route_table::{
    BindingSet, ClientId, GatewayId, ListenerSessionId, MappingSnapshot, RegistrationKey,
    ShardDirectory, ShardDirectoryGeneration, ShardEndpoint, ShardId,
};
use relaygate_route_table_transport::{
    ErrorCode, GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig,
    TransportError,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::registry::Binding;

use super::{
    GatewayRoutingConfig, RoutingError,
    lifecycle::{OperationTicket, RegistrationAction, RegistrationState, next_backoff},
    projection::{ProjectedShardSnapshot, project_session, project_session_id},
};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone)]
struct DesiredShardEntry {
    version: u64,
    snapshot: MappingSnapshot,
}

#[derive(Debug, Default)]
struct DesiredState {
    version: u64,
    by_shard: BTreeMap<ShardId, HashMap<ListenerSessionId, DesiredShardEntry>>,
}

#[derive(Debug, Default)]
struct DesiredStore(RwLock<DesiredState>);

impl DesiredStore {
    fn commit(
        &self,
        session_id: ListenerSessionId,
        projected: Vec<ProjectedShardSnapshot>,
    ) -> Result<u64, RoutingError> {
        let mut state = self.0.write().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        let version = state.version.checked_add(1).ok_or_else(|| {
            RoutingError::WorkerFailed("routing desired version exhausted".to_owned())
        })?;
        state.version = version;
        for projected in projected {
            let shard = state.by_shard.entry(projected.shard_id).or_default();
            if let Some(snapshot) = projected.snapshot {
                shard.insert(session_id, DesiredShardEntry { version, snapshot });
            } else {
                shard.remove(&session_id);
            }
        }
        Ok(version)
    }

    fn shard_view_after(
        &self,
        shard_id: &ShardId,
        observed_version: u64,
    ) -> Result<Option<ShardDesiredView>, RoutingError> {
        let state = self.0.read().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        if state.version <= observed_version {
            return Ok(None);
        }
        let sessions = state
            .by_shard
            .get(shard_id)
            .into_iter()
            .flatten()
            .map(|(session_id, desired)| {
                (
                    *session_id,
                    (desired.version, Some(desired.snapshot.clone())),
                )
            })
            .collect();
        Ok(Some(ShardDesiredView {
            store_version: state.version,
            sessions,
        }))
    }
}

struct ShardDesiredView {
    store_version: u64,
    sessions: HashMap<ListenerSessionId, (u64, Option<MappingSnapshot>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingCounts {
    pub(crate) synced: usize,
    pub(crate) unsynced: usize,
}

#[derive(Debug, Default)]
struct WorkerCounts {
    synced: AtomicUsize,
    unsynced: AtomicUsize,
}

impl WorkerCounts {
    fn update(&self, registrations: &BTreeMap<ListenerSessionId, RegistrationState>) {
        let (synced, unsynced) = registrations
            .values()
            .filter(|state| state.is_desired())
            .fold((0, 0), |(synced, unsynced), state| {
                if state.is_synced() {
                    (synced + 1, unsynced)
                } else {
                    (synced, unsynced + 1)
                }
            });
        self.synced.store(synced, Ordering::Relaxed);
        self.unsynced.store(unsynced, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct ConnectedClient {
    epoch: u64,
    client: RouteTableClient,
}

#[derive(Clone)]
enum ClientAvailability {
    Unavailable,
    Ready(ConnectedClient),
    Terminal(TransportError),
}

#[derive(Clone)]
struct ClientFailure {
    epoch: u64,
    error: TransportError,
}

struct ShardHandle {
    shard_id: ShardId,
    wake: mpsc::Sender<()>,
    client: watch::Receiver<ClientAvailability>,
    failure: watch::Sender<Option<ClientFailure>>,
    counts: Arc<WorkerCounts>,
}

/// Cloneable Gateway-local handle. Publication commits manager-owned desired
/// state before sending a coalescible bounded wake signal.
#[derive(Clone)]
pub(crate) struct RoutingHandle {
    directory: ShardDirectory,
    gateway_id: GatewayId,
    gateway_locator: relaygate_route_table::GatewayLocator,
    desired: Arc<DesiredStore>,
    shards: Arc<BTreeMap<ShardId, ShardHandle>>,
}

impl RoutingHandle {
    pub(crate) fn publish_session(
        &self,
        session_id: SessionId,
        bindings: Vec<Binding>,
    ) -> Result<(), RoutingError> {
        let projected = project_session(
            &self.directory,
            self.gateway_id,
            &self.gateway_locator,
            session_id,
            bindings,
        )?;
        self.desired
            .commit(project_session_id(session_id), projected)?;

        let mut stopped = None;
        for worker in self.shards.values() {
            match worker.wake.try_send(()) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    stopped.get_or_insert_with(|| worker.shard_id.to_string());
                }
            }
        }
        if let Some(shard_id) = stopped {
            Err(RoutingError::WorkerStopped { shard_id })
        } else {
            Ok(())
        }
    }

    pub(crate) async fn resolve(&self, client_id: ClientId) -> Result<BindingSet, RoutingError> {
        let record = self.directory.authority(&client_id);
        let worker = self.shards.get(record.id()).ok_or_else(|| {
            RoutingError::InvalidConfig("authority shard worker is missing".to_owned())
        })?;
        let availability = worker.client.borrow().clone();
        let connected = match availability {
            ClientAvailability::Ready(connected) => connected,
            ClientAvailability::Terminal(error) => return Err(RoutingError::Transport(error)),
            ClientAvailability::Unavailable => {
                return Err(RoutingError::ShardUnavailable {
                    shard_id: record.id().to_string(),
                });
            }
        };

        let result = connected
            .client
            .resolve(self.directory.generation(), &client_id)
            .await;
        if let Err(error) = &result
            && should_report_resolve_failure(error.code())
        {
            worker.failure.send_replace(Some(ClientFailure {
                epoch: connected.epoch,
                error: error.clone(),
            }));
        }
        result.map_err(RoutingError::from)
    }

    #[must_use]
    pub(crate) fn current_counts(&self) -> RoutingCounts {
        self.shards.values().fold(
            RoutingCounts {
                synced: 0,
                unsynced: 0,
            },
            |counts, worker| RoutingCounts {
                synced: counts.synced + worker.counts.synced.load(Ordering::Relaxed),
                unsynced: counts.unsynced + worker.counts.unsynced.load(Ordering::Relaxed),
            },
        )
    }
}

/// Owns one worker per immutable ShardDirectory record.
pub(crate) struct RoutingRuntime {
    handle: RoutingHandle,
    workers: JoinSet<(ShardId, Result<(), RoutingError>)>,
    shutdown: CancellationToken,
}

impl RoutingRuntime {
    pub(crate) fn start(
        config: GatewayRoutingConfig,
        gateway_id: GatewayId,
        shutdown: CancellationToken,
    ) -> Result<Self, RoutingError> {
        config.validate()?;
        let desired = Arc::new(DesiredStore::default());
        let mut handles = BTreeMap::new();
        let mut workers = JoinSet::new();

        for record in config.directory.shards() {
            let (wake, wake_receiver) = mpsc::channel(config.command_queue_capacity);
            let (client_sender, client) = watch::channel(ClientAvailability::Unavailable);
            let (failure, failure_receiver) = watch::channel(None);
            let counts = Arc::new(WorkerCounts::default());
            let worker_config = ShardWorkerConfig {
                shard_id: record.id().clone(),
                endpoint: record.endpoint().clone(),
                generation: config.directory.generation(),
                gateway_id,
                gateway_name: config.gateway_name.clone(),
                internal_gateway_key: config.internal_gateway_key.clone(),
                client_config: config.client,
                reconnect_initial: config.reconnect_initial_backoff,
                reconnect_max: config.reconnect_max_backoff,
                scan_interval: config.desired_scan_interval,
                shutdown_timeout: config.shutdown_timeout,
            };
            let shard_id = record.id().clone();
            let worker_desired = Arc::clone(&desired);
            let worker_counts = Arc::clone(&counts);
            let worker_shutdown = shutdown.child_token();
            let task_shard_id = shard_id.clone();
            workers.spawn(async move {
                let result = run_shard_worker(
                    worker_config,
                    worker_desired,
                    wake_receiver,
                    client_sender,
                    failure_receiver,
                    worker_counts,
                    worker_shutdown,
                )
                .await;
                (task_shard_id, result)
            });
            handles.insert(
                shard_id.clone(),
                ShardHandle {
                    shard_id: shard_id.clone(),
                    wake,
                    client,
                    failure,
                    counts,
                },
            );
        }

        Ok(Self {
            handle: RoutingHandle {
                directory: config.directory,
                gateway_id,
                gateway_locator: config.gateway_locator,
                desired,
                shards: Arc::new(handles),
            },
            workers,
            shutdown,
        })
    }

    #[must_use]
    pub(crate) fn handle(&self) -> RoutingHandle {
        self.handle.clone()
    }

    pub(crate) async fn wait(mut self) -> Result<(), RoutingError> {
        while let Some(completed) = self.workers.join_next().await {
            match completed {
                Ok((_, Ok(()))) => {}
                Ok((_, Err(error))) => {
                    self.shutdown.cancel();
                    self.workers.abort_all();
                    return Err(error);
                }
                Err(error) => {
                    self.shutdown.cancel();
                    self.workers.abort_all();
                    return Err(RoutingError::WorkerFailed(format!(
                        "RouteTable shard worker task failed: {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

struct ShardWorkerConfig {
    shard_id: ShardId,
    endpoint: ShardEndpoint,
    generation: ShardDirectoryGeneration,
    gateway_id: GatewayId,
    gateway_name: GatewayName,
    internal_gateway_key: InternalGatewayKey,
    client_config: RouteTableClientConfig,
    reconnect_initial: Duration,
    reconnect_max: Duration,
    scan_interval: Duration,
    shutdown_timeout: Duration,
}

struct OperationCompletion {
    epoch: u64,
    ticket: OperationTicket,
    result: OperationResult,
}

enum OperationResult {
    Registration(Result<relaygate_route_table::RegistrationAck, TransportError>),
    Deregister(Result<(), TransportError>),
}

async fn run_shard_worker(
    config: ShardWorkerConfig,
    desired: Arc<DesiredStore>,
    mut wake: mpsc::Receiver<()>,
    client_sender: watch::Sender<ClientAvailability>,
    mut failure: watch::Receiver<Option<ClientFailure>>,
    counts: Arc<WorkerCounts>,
    shutdown: CancellationToken,
) -> Result<(), RoutingError> {
    let mut registrations = BTreeMap::new();
    let mut connected: Option<ConnectedClient> = None;
    let mut connection_epoch = 0_u64;
    let mut reconnect_at = Instant::now();
    let mut reconnect_backoff = config.reconnect_initial;
    let mut connect: Option<BoxFuture<Result<RouteTableClient, TransportError>>> = None;
    let mut operation: Option<BoxFuture<OperationCompletion>> = None;
    let mut terminal = false;
    let mut dirty = true;
    let mut observed_desired_version = 0_u64;
    let mut scan = tokio::time::interval(config.scan_interval);
    scan.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let now = Instant::now();
        if dirty {
            reconcile_desired(
                &config,
                &desired,
                &mut registrations,
                &mut observed_desired_version,
                now,
            )?;
            dirty = false;
            counts.update(&registrations);
        }
        registrations.retain(|_, state| !state.is_removable());

        if !terminal && connected.is_none() && connect.is_none() && now >= reconnect_at {
            connect = Some(connect_once(&config));
        }
        if !terminal
            && operation.is_none()
            && let Some(current) = connected.clone()
        {
            match begin_registration_operation(&mut registrations, now) {
                Ok(Some(ticket)) => {
                    operation = Some(execute_operation(
                        current.epoch,
                        current.client,
                        config.generation,
                        ticket,
                    ));
                    counts.update(&registrations);
                }
                Ok(None) => {}
                Err(message) => {
                    for state in registrations.values_mut() {
                        state.mark_terminal();
                    }
                    counts.update(&registrations);
                    return Err(RoutingError::WorkerFailed(format!(
                        "RouteTable shard {} lifecycle failed: {message}",
                        config.shard_id
                    )));
                }
            }
        }

        let registration_deadline = if connected.is_some() && operation.is_none() {
            registrations
                .values()
                .filter_map(RegistrationState::next_deadline)
                .min()
        } else {
            None
        };
        let reconnect_deadline =
            (!terminal && connected.is_none() && connect.is_none()).then_some(reconnect_at);

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            signal = wake.recv() => {
                if signal.is_none() {
                    break;
                }
                dirty = true;
            }
            changed = failure.changed() => {
                if changed.is_ok()
                    && let Some(observed) = failure.borrow_and_update().clone()
                    && connected.as_ref().is_some_and(|current| current.epoch == observed.epoch)
                {
                    if is_terminal_control_error(observed.error.code()) {
                        terminal = true;
                        connected = None;
                        mark_all_terminal(&mut registrations);
                        counts.update(&registrations);
                        client_sender.send_replace(ClientAvailability::Terminal(observed.error));
                    } else if is_connection_error(observed.error.code()) {
                        connected = None;
                        client_sender.send_replace(ClientAvailability::Unavailable);
                        mark_connection_lost(&mut registrations, Instant::now());
                        counts.update(&registrations);
                        reconnect_at = Instant::now() + reconnect_backoff;
                        reconnect_backoff = next_backoff(reconnect_backoff, config.reconnect_max);
                    }
                }
            }
            result = poll_optional(&mut connect) => {
                connect = None;
                match result {
                    Ok(client) => {
                        connection_epoch = connection_epoch.checked_add(1).ok_or_else(|| {
                            RoutingError::WorkerFailed("RouteTable connection epoch exhausted".to_owned())
                        })?;
                        let current = ConnectedClient { epoch: connection_epoch, client };
                        connected = Some(current.clone());
                        client_sender.send_replace(ClientAvailability::Ready(current));
                        reconnect_backoff = config.reconnect_initial;
                    }
                    Err(error) if is_terminal_control_error(error.code()) => {
                        terminal = true;
                        mark_all_terminal(&mut registrations);
                        counts.update(&registrations);
                        client_sender.send_replace(ClientAvailability::Terminal(error));
                    }
                    Err(_) => {
                        client_sender.send_replace(ClientAvailability::Unavailable);
                        reconnect_at = Instant::now() + reconnect_backoff;
                        reconnect_backoff = next_backoff(reconnect_backoff, config.reconnect_max);
                    }
                }
            }
            completion = poll_optional(&mut operation) => {
                operation = None;
                let current_epoch = connected.as_ref().map(|current| current.epoch);
                let result = if completion.epoch != current_epoch.unwrap_or_default() {
                    if let Some(state) = registration_for_ticket(&mut registrations, &completion.ticket) {
                        state.transient_failure(&completion.ticket, Instant::now());
                    }
                    None
                } else {
                    apply_operation_completion(
                        &mut registrations,
                        &completion,
                        Instant::now(),
                    )
                };
                if let Some(error) = result {
                    if is_terminal_control_error(error.code()) {
                        if matches!(completion.ticket.action, RegistrationAction::Register { .. }) {
                            terminal = true;
                            connected = None;
                            mark_all_terminal(&mut registrations);
                            counts.update(&registrations);
                            client_sender.send_replace(ClientAvailability::Terminal(error));
                        }
                    } else if is_connection_error(error.code()) {
                        connected = None;
                        client_sender.send_replace(ClientAvailability::Unavailable);
                        mark_connection_lost(&mut registrations, Instant::now());
                        counts.update(&registrations);
                        reconnect_at = Instant::now() + reconnect_backoff;
                        reconnect_backoff = next_backoff(reconnect_backoff, config.reconnect_max);
                    }
                }
                registrations.retain(|_, state| !state.is_removable());
                counts.update(&registrations);
            }
            _ = scan.tick() => dirty = true,
            _ = wait_until(registration_deadline) => {}
            _ = wait_until(reconnect_deadline) => {}
        }
    }

    client_sender.send_replace(ClientAvailability::Unavailable);
    drop(operation);
    drop(connect);
    if let Some(current) = connected {
        best_effort_deregister(
            &current.client,
            config.generation,
            &registrations,
            config.shutdown_timeout,
        )
        .await;
    }
    counts.synced.store(0, Ordering::Relaxed);
    counts.unsynced.store(0, Ordering::Relaxed);
    Ok(())
}

fn reconcile_desired(
    config: &ShardWorkerConfig,
    desired: &DesiredStore,
    registrations: &mut BTreeMap<ListenerSessionId, RegistrationState>,
    observed_version: &mut u64,
    now: Instant,
) -> Result<(), RoutingError> {
    let Some(view) = desired.shard_view_after(&config.shard_id, *observed_version)? else {
        return Ok(());
    };
    *observed_version = view.store_version;
    for (session_id, (version, snapshot)) in &view.sessions {
        let key = RegistrationKey::new(config.gateway_id, *session_id, config.shard_id.clone());
        registrations
            .entry(*session_id)
            .and_modify(|state| state.publish(*version, snapshot.clone(), now))
            .or_insert_with(|| {
                RegistrationState::new(
                    key,
                    *version,
                    snapshot.clone(),
                    now,
                    config.reconnect_initial,
                    config.reconnect_max,
                )
            });
    }
    for (session_id, state) in registrations.iter_mut() {
        if !view.sessions.contains_key(session_id) {
            state.publish(view.store_version, None, now);
        }
    }
    Ok(())
}

fn begin_registration_operation(
    registrations: &mut BTreeMap<ListenerSessionId, RegistrationState>,
    now: Instant,
) -> Result<Option<OperationTicket>, &'static str> {
    for state in registrations.values_mut() {
        if let Some(ticket) = state.begin_next(now)? {
            return Ok(Some(ticket));
        }
    }
    Ok(None)
}

fn mark_connection_lost(
    registrations: &mut BTreeMap<ListenerSessionId, RegistrationState>,
    now: Instant,
) {
    for state in registrations.values_mut() {
        state.connection_lost(now);
    }
}

fn mark_all_terminal(registrations: &mut BTreeMap<ListenerSessionId, RegistrationState>) {
    for state in registrations.values_mut() {
        state.mark_terminal();
    }
}

fn connect_once(config: &ShardWorkerConfig) -> BoxFuture<Result<RouteTableClient, TransportError>> {
    let endpoint = config.endpoint.as_str().to_owned();
    let gateway_name = config.gateway_name.clone();
    let gateway_id = config.gateway_id;
    let key = config.internal_gateway_key.clone();
    let client = config.client_config;
    Box::pin(async move {
        RouteTableClient::connect(endpoint, gateway_name, gateway_id, key, client).await
    })
}

fn execute_operation(
    epoch: u64,
    client: RouteTableClient,
    generation: ShardDirectoryGeneration,
    ticket: OperationTicket,
) -> BoxFuture<OperationCompletion> {
    Box::pin(async move {
        let result = match &ticket.action {
            RegistrationAction::Register { key } => {
                OperationResult::Registration(client.register(generation, key).await)
            }
            RegistrationAction::Update {
                key,
                lease_id,
                revision,
                snapshot,
            } => OperationResult::Registration(
                client
                    .update(generation, key, *lease_id, *revision, snapshot)
                    .await,
            ),
            RegistrationAction::KeepAlive { key, lease_id } => {
                OperationResult::Registration(client.keep_alive(generation, key, *lease_id).await)
            }
            RegistrationAction::Deregister { key, lease_id } => {
                OperationResult::Deregister(client.deregister(generation, key, *lease_id).await)
            }
        };
        OperationCompletion {
            epoch,
            ticket,
            result,
        }
    })
}

/// Applies a completion only to the RegistrationKey captured in its ticket.
/// Returns the transport error so the worker can update shared connection state.
fn apply_operation_completion(
    registrations: &mut BTreeMap<ListenerSessionId, RegistrationState>,
    completion: &OperationCompletion,
    now: Instant,
) -> Option<TransportError> {
    let state = registration_for_ticket(registrations, &completion.ticket)?;
    match (&completion.ticket.action, &completion.result) {
        (RegistrationAction::Deregister { .. }, OperationResult::Deregister(result)) => {
            state.finish_deregister(&completion.ticket);
            result.clone().err()
        }
        (RegistrationAction::Register { .. }, OperationResult::Registration(Ok(ack))) => {
            state.register_succeeded(&completion.ticket, *ack, now);
            None
        }
        (RegistrationAction::Update { .. }, OperationResult::Registration(Ok(ack))) => {
            state.update_succeeded(&completion.ticket, *ack, now);
            None
        }
        (RegistrationAction::KeepAlive { .. }, OperationResult::Registration(Ok(ack))) => {
            state.keep_alive_succeeded(&completion.ticket, *ack, now);
            None
        }
        (_, OperationResult::Registration(Err(error))) => {
            match error.code() {
                ErrorCode::FailedPrecondition
                    if !matches!(
                        completion.ticket.action,
                        RegistrationAction::Register { .. }
                    ) =>
                {
                    state.stale_lease(&completion.ticket, now);
                }
                ErrorCode::Unauthenticated
                | ErrorCode::PermissionDenied
                | ErrorCode::InvalidArgument
                | ErrorCode::NotFound
                | ErrorCode::FailedPrecondition => state.terminal_failure(&completion.ticket),
                ErrorCode::Unavailable
                | ErrorCode::DeadlineExceeded
                | ErrorCode::ResourceExhausted
                | ErrorCode::ProtocolError
                | ErrorCode::Internal => state.transient_failure(&completion.ticket, now),
            }
            Some(error.clone())
        }
        _ => {
            state.mark_terminal();
            None
        }
    }
}

fn registration_for_ticket<'a>(
    registrations: &'a mut BTreeMap<ListenerSessionId, RegistrationState>,
    ticket: &OperationTicket,
) -> Option<&'a mut RegistrationState> {
    let key = match &ticket.action {
        RegistrationAction::Register { key }
        | RegistrationAction::Update { key, .. }
        | RegistrationAction::KeepAlive { key, .. }
        | RegistrationAction::Deregister { key, .. } => key,
    };
    registrations.get_mut(&key.listener_session_id())
}

async fn best_effort_deregister(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    registrations: &BTreeMap<ListenerSessionId, RegistrationState>,
    timeout: Duration,
) {
    let deregister = async {
        for state in registrations.values() {
            if let Some((key, lease_id)) = state.active_lease() {
                let _ = client.deregister(generation, key, lease_id).await;
            }
        }
    };
    let _ = tokio::time::timeout(timeout, deregister).await;
}

async fn poll_optional<T>(future: &mut Option<BoxFuture<T>>) -> T {
    match future {
        Some(future) => future.await,
        None => pending().await,
    }
}

async fn wait_until(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}

pub(super) const fn is_connection_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unavailable
            | ErrorCode::DeadlineExceeded
            | ErrorCode::ProtocolError
            | ErrorCode::Internal
    )
}

pub(super) const fn is_terminal_control_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied | ErrorCode::FailedPrecondition
    )
}

const fn should_report_resolve_failure(code: ErrorCode) -> bool {
    is_connection_error(code) || is_terminal_control_error(code)
}
