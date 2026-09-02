use std::{
    collections::BTreeMap,
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use relaygate_route_table::{
    GatewayId, ListenerSessionId, RegistrationKey, ShardDirectoryGeneration, ShardEndpoint, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, TransportError,
};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use super::{
    super::{
        RoutingError,
        lifecycle::{OperationTicket, RegistrationAction, RegistrationState, next_backoff},
    },
    BoxFuture,
    desired::DesiredStore,
    operation::{
        OperationCompletion, apply_epoch_scoped_operation_completion, execute_operation,
        is_connection_error, is_terminal_control_error,
    },
};

#[derive(Debug, Default)]
pub(super) struct WorkerCounts {
    pub(super) synced: AtomicUsize,
    pub(super) unsynced: AtomicUsize,
    pub(super) terminal: AtomicUsize,
}

impl WorkerCounts {
    pub(super) fn update(&self, registrations: &BTreeMap<ListenerSessionId, RegistrationState>) {
        let (synced, unsynced, terminal) = registrations
            .values()
            .filter(|state| state.is_desired())
            .fold((0, 0, 0), |(synced, unsynced, terminal), state| {
                if state.is_synced() {
                    (synced + 1, unsynced, terminal)
                } else {
                    (
                        synced,
                        unsynced + 1,
                        terminal + usize::from(state.is_terminal()),
                    )
                }
            });
        self.synced.store(synced, Ordering::Relaxed);
        self.unsynced.store(unsynced, Ordering::Relaxed);
        self.terminal.store(terminal, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(super) struct ConnectedClient {
    pub(super) epoch: u64,
    pub(super) client: RouteTableClient,
}

#[derive(Clone)]
pub(super) enum ClientAvailability {
    Unavailable,
    Ready(ConnectedClient),
    Terminal(TransportError),
}

#[derive(Clone)]
pub(super) struct ClientFailure {
    pub(super) epoch: u64,
    pub(super) error: TransportError,
}

pub(super) struct ShardHandle {
    pub(super) shard_id: ShardId,
    pub(super) wake: mpsc::Sender<()>,
    pub(super) client: watch::Receiver<ClientAvailability>,
    pub(super) failure: watch::Sender<Option<ClientFailure>>,
    pub(super) counts: Arc<WorkerCounts>,
}

pub(super) struct ShardWorkerConfig {
    pub(super) shard_id: ShardId,
    pub(super) endpoint: ShardEndpoint,
    pub(super) generation: ShardDirectoryGeneration,
    pub(super) gateway_id: GatewayId,
    pub(super) gateway_name: GatewayName,
    pub(super) internal_gateway_key: InternalGatewayKey,
    pub(super) client_config: RouteTableClientConfig,
    pub(super) reconnect_initial: Duration,
    pub(super) reconnect_max: Duration,
    pub(super) scan_interval: Duration,
    pub(super) shutdown_timeout: Duration,
}

pub(super) async fn run_shard_worker(
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
                let result = apply_epoch_scoped_operation_completion(
                    &mut registrations,
                    &completion,
                    current_epoch,
                    Instant::now(),
                );
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
    counts.terminal.store(0, Ordering::Relaxed);
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
