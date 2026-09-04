mod desired;
mod observation;
mod operation;
mod worker;

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
};

use relaygate_protocol::SessionId;
use relaygate_route_table::{BindingSet, ClientId, GatewayId, ShardDirectory, ShardId};
use relaygate_route_table_transport::ErrorCode;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{RouteDependencyHealth, registry::Binding};

use super::{
    GatewayRoutingConfig, RoutingError,
    projection::{project_session, project_session_id},
};

use desired::DesiredStore;
pub(super) use operation::{is_connection_error, is_terminal_control_error};
use worker::{
    ClientAvailability, ClientFailure, ShardHandle, ShardWorkerConfig, WorkerCounts,
    run_shard_worker,
};

#[cfg(test)]
use operation::{OperationCompletion, OperationResult, apply_epoch_scoped_operation_completion};

#[cfg(test)]
mod tests;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingCounts {
    pub(crate) synced: usize,
    pub(crate) unsynced: usize,
    pub(crate) dependency_health: RouteDependencyHealth,
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
                dependency_health: RouteDependencyHealth::Ready,
            },
            |counts, worker| {
                let synced = worker.counts.synced.load(Ordering::Relaxed);
                let unsynced = worker.counts.unsynced.load(Ordering::Relaxed);
                let availability = worker.client.borrow();
                let terminal = worker.counts.terminal.load(Ordering::Relaxed) > 0
                    || matches!(&*availability, ClientAvailability::Terminal(_));
                let unavailable = matches!(&*availability, ClientAvailability::Unavailable);
                let shard_health = if terminal {
                    RouteDependencyHealth::Terminal
                } else if unavailable || unsynced > 0 {
                    RouteDependencyHealth::Degraded
                } else {
                    RouteDependencyHealth::Ready
                };
                RoutingCounts {
                    synced: counts.synced + synced,
                    unsynced: counts.unsynced + unsynced,
                    dependency_health: merge_dependency_health(
                        counts.dependency_health,
                        shard_health,
                    ),
                }
            },
        )
    }
}

const fn merge_dependency_health(
    current: RouteDependencyHealth,
    shard: RouteDependencyHealth,
) -> RouteDependencyHealth {
    match (current, shard) {
        (RouteDependencyHealth::Terminal, _) | (_, RouteDependencyHealth::Terminal) => {
            RouteDependencyHealth::Terminal
        }
        (RouteDependencyHealth::Degraded, _) | (_, RouteDependencyHealth::Degraded) => {
            RouteDependencyHealth::Degraded
        }
        _ => RouteDependencyHealth::Ready,
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

const fn should_report_resolve_failure(code: ErrorCode) -> bool {
    is_connection_error(code) || is_terminal_control_error(code)
}
