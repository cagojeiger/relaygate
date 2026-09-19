//! Per-shard asynchronous orchestration. The worker orders desired-state
//! reconciliation, RouteTable connections, one in-flight registration
//! operation, dependency observation, and bounded shutdown.

use std::{collections::BTreeMap, future::pending, sync::Arc, time::Duration};

use relaygate_route_table::{
    GatewayId, RelaySessionId, ShardDirectoryGeneration, ShardEndpoint, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, RouteTableClient, RouteTableClientConfig, TransportError,
};
use relaygate_transport::ClientTlsConfig;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use super::{
    super::{
        RoutingError,
        lifecycle::{RegistrationAction, RegistrationState},
    },
    BoxFuture,
    desired::DesiredStore,
    observation::RouteDependencyObservation,
    operation::{
        OperationCompletion, apply_epoch_scoped_operation_completion, execute_operation,
        is_connection_error, is_terminal_control_error,
    },
    reconnect::ReconnectBackoff,
    registration::{
        WorkerCounts, best_effort_deregister, mark_all_terminal, mark_connection_lost,
        prepare_registration_pass, reconcile_desired,
    },
};

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
    pub(super) client_config: RouteTableClientConfig,
    pub(super) tls: Option<ClientTlsConfig>,
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
    let mut worker = ShardWorker::new(config, client_sender, counts);
    let mut connect: Option<BoxFuture<Result<RouteTableClient, TransportError>>> = None;
    let mut operation: Option<BoxFuture<OperationCompletion>> = None;
    let mut dirty = true;
    let mut observed_desired_version = 0_u64;
    let mut scan = tokio::time::interval(worker.config.scan_interval);
    scan.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let now = Instant::now();
        if dirty {
            worker.reconcile(&desired, &mut observed_desired_version, now)?;
            dirty = false;
        }
        if worker.wants_connect(connect.is_none(), now) {
            connect = Some(connect_once(&worker.config));
        }
        let mut registration_deadline = None;
        if operation.is_none() {
            let prepared = worker.prepare_registrations(now)?;
            operation = prepared.operation;
            registration_deadline = prepared.deadline;
        } else {
            worker.prune();
        }
        let reconnect_deadline = worker.reconnect_deadline(connect.is_none());

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
                {
                    worker.on_client_failure(observed);
                }
            }
            result = poll_optional(&mut connect) => {
                connect = None;
                worker.on_connect_result(result)?;
            }
            completion = poll_optional(&mut operation) => {
                operation = None;
                worker.on_operation_completion(&completion);
            }
            _ = scan.tick() => dirty = true,
            _ = wait_until(registration_deadline) => {}
            _ = wait_until(reconnect_deadline) => {}
        }
    }

    drop(operation);
    drop(connect);
    worker.shutdown().await;
    Ok(())
}

/// Per-shard worker state: desired registrations, the current RouteTable
/// client, reconnect pacing, and the dependency observation.
struct ShardWorker {
    config: ShardWorkerConfig,
    registrations: BTreeMap<RelaySessionId, RegistrationState>,
    connected: Option<ConnectedClient>,
    connection_epoch: u64,
    reconnect_at: Instant,
    reconnect_backoff: ReconnectBackoff,
    terminal: bool,
    observation: RouteDependencyObservation,
    client_sender: watch::Sender<ClientAvailability>,
    counts: Arc<WorkerCounts>,
}

impl ShardWorker {
    fn new(
        config: ShardWorkerConfig,
        client_sender: watch::Sender<ClientAvailability>,
        counts: Arc<WorkerCounts>,
    ) -> Self {
        let reconnect_backoff = ReconnectBackoff::new(
            config.reconnect_initial,
            config.reconnect_max,
            config.gateway_id,
            &config.shard_id,
        );
        Self {
            config,
            registrations: BTreeMap::new(),
            connected: None,
            connection_epoch: 0,
            reconnect_at: Instant::now(),
            reconnect_backoff,
            terminal: false,
            observation: RouteDependencyObservation::new(),
            client_sender,
            counts,
        }
    }

    fn reconcile(
        &mut self,
        desired: &DesiredStore,
        observed_version: &mut u64,
        now: Instant,
    ) -> Result<(), RoutingError> {
        reconcile_desired(
            desired,
            &self.config.shard_id,
            self.config.gateway_id,
            (self.config.reconnect_initial, self.config.reconnect_max),
            &mut self.registrations,
            observed_version,
            now,
        )?;
        self.counts.update(&self.registrations);
        Ok(())
    }

    fn prune(&mut self) {
        self.registrations.retain(|_, state| !state.is_removable());
    }

    fn wants_connect(&self, no_connect_in_flight: bool, now: Instant) -> bool {
        !self.terminal
            && self.connected.is_none()
            && no_connect_in_flight
            && now >= self.reconnect_at
    }

    fn prepare_registrations(
        &mut self,
        now: Instant,
    ) -> Result<PreparedRegistrations, RoutingError> {
        if self.terminal {
            self.prune();
            return Ok(PreparedRegistrations::default());
        }
        let Some(current) = self.connected.clone() else {
            self.prune();
            return Ok(PreparedRegistrations::default());
        };
        match prepare_registration_pass(&mut self.registrations, now) {
            Ok(prepared) => {
                prepared.update_counts(&self.counts);
                Ok(PreparedRegistrations {
                    operation: prepared.ticket.map(|ticket| {
                        execute_operation(
                            current.epoch,
                            current.client,
                            self.config.generation,
                            ticket,
                        )
                    }),
                    deadline: prepared.deadline,
                })
            }
            Err(message) => {
                mark_all_terminal(&mut self.registrations);
                self.counts.update(&self.registrations);
                Err(RoutingError::WorkerFailed(format!(
                    "RouteTable shard {} lifecycle failed: {message}",
                    self.config.shard_id
                )))
            }
        }
    }

    fn reconnect_deadline(&self, no_connect_in_flight: bool) -> Option<Instant> {
        (!self.terminal && self.connected.is_none() && no_connect_in_flight)
            .then_some(self.reconnect_at)
    }

    fn on_client_failure(&mut self, observed: ClientFailure) {
        if self
            .connected
            .as_ref()
            .is_none_or(|current| current.epoch != observed.epoch)
        {
            return;
        }
        if is_terminal_control_error(observed.error.code()) {
            self.observation
                .terminal(&self.config.shard_id, &observed.error);
            self.enter_terminal(observed.error);
        } else if is_connection_error(observed.error.code()) {
            self.connection_lost(&observed.error);
        }
    }

    fn on_connect_result(
        &mut self,
        result: Result<RouteTableClient, TransportError>,
    ) -> Result<(), RoutingError> {
        match result {
            Ok(client) => {
                self.connection_epoch = self.connection_epoch.checked_add(1).ok_or_else(|| {
                    RoutingError::WorkerFailed("RouteTable connection epoch exhausted".to_owned())
                })?;
                let current = ConnectedClient {
                    epoch: self.connection_epoch,
                    client,
                };
                self.connected = Some(current.clone());
                self.client_sender
                    .send_replace(ClientAvailability::Ready(current));
                self.observation.ready(&self.config.shard_id);
                self.reconnect_backoff.reset();
            }
            Err(error) if is_terminal_control_error(error.code()) => {
                self.observation
                    .connect_terminal(&self.config.shard_id, &error);
                self.enter_terminal(error);
            }
            Err(error) => {
                self.observation
                    .connect_failed(&self.config.shard_id, &error);
                self.client_sender
                    .send_replace(ClientAvailability::Unavailable);
                self.reconnect_at = Instant::now() + self.reconnect_backoff.next_delay();
            }
        }
        Ok(())
    }

    fn on_operation_completion(&mut self, completion: &OperationCompletion) {
        let current_epoch = self.connected.as_ref().map(|current| current.epoch);
        let result = apply_epoch_scoped_operation_completion(
            &mut self.registrations,
            completion,
            current_epoch,
            Instant::now(),
        );
        if let Some(error) = result {
            if is_terminal_control_error(error.code()) {
                // Only a rejected REGISTER proves the shard is unusable; other
                // terminal codes stay scoped to their registration.
                if matches!(
                    completion.ticket.action,
                    RegistrationAction::Register { .. }
                ) {
                    self.observation.terminal(&self.config.shard_id, &error);
                    self.enter_terminal(error);
                }
            } else if is_connection_error(error.code()) {
                self.connection_lost(&error);
            }
        }
        self.prune();
        self.counts.update(&self.registrations);
    }

    fn enter_terminal(&mut self, error: TransportError) {
        self.terminal = true;
        self.connected = None;
        mark_all_terminal(&mut self.registrations);
        self.counts.update(&self.registrations);
        self.client_sender
            .send_replace(ClientAvailability::Terminal(error));
    }

    fn connection_lost(&mut self, error: &TransportError) {
        self.observation
            .connection_lost(&self.config.shard_id, error);
        self.connected = None;
        self.client_sender
            .send_replace(ClientAvailability::Unavailable);
        mark_connection_lost(&mut self.registrations, Instant::now());
        self.counts.update(&self.registrations);
        self.reconnect_at = Instant::now() + self.reconnect_backoff.next_delay();
    }

    async fn shutdown(self) {
        self.client_sender
            .send_replace(ClientAvailability::Unavailable);
        if let Some(current) = self.connected {
            best_effort_deregister(
                &current.client,
                self.config.generation,
                &self.registrations,
                self.config.shutdown_timeout,
            )
            .await;
        }
        self.counts.clear();
    }
}

#[derive(Default)]
struct PreparedRegistrations {
    operation: Option<BoxFuture<OperationCompletion>>,
    deadline: Option<Instant>,
}

fn connect_once(config: &ShardWorkerConfig) -> BoxFuture<Result<RouteTableClient, TransportError>> {
    let endpoint = config.endpoint.as_str().to_owned();
    let gateway_name = config.gateway_name.clone();
    let gateway_id = config.gateway_id;
    let client = config.client_config;
    let tls = config.tls.clone();
    Box::pin(async move {
        match tls {
            Some(tls) => {
                RouteTableClient::connect_secure(endpoint, gateway_name, gateway_id, client, tls)
                    .await
            }
            None => RouteTableClient::connect(endpoint, gateway_name, gateway_id, client).await,
        }
    })
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
