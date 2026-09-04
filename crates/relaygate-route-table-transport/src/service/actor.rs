use std::future::pending;

use relaygate_route_table::{RequestContext, RouteTableShard};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    TransportError,
    dto::{DomainRequest, WireRequest, WireResponse},
};

pub(super) struct ServiceCommand {
    pub(super) context: RequestContext,
    pub(super) request: WireRequest,
    pub(super) reply: oneshot::Sender<Result<WireResponse, TransportError>>,
}

pub(super) fn spawn_shard_actor(
    shard: RouteTableShard,
    requests: mpsc::Receiver<ServiceCommand>,
    shutdown: CancellationToken,
) -> JoinHandle<RouteTableShard> {
    tokio::spawn(run_shard_actor(shard, requests, shutdown))
}

pub(super) async fn run_shard_actor(
    mut shard: RouteTableShard,
    mut requests: mpsc::Receiver<ServiceCommand>,
    shutdown: CancellationToken,
) -> RouteTableShard {
    observe_shard(&shard);
    loop {
        let next_expiry = shard.next_expiry_deadline();
        tokio::select! {
            _ = shutdown.cancelled() => break,
            request = requests.recv() => {
                let Some(request) = request else {
                    break;
                };
                let now = tokio::time::Instant::now().into_std();
                let started_at = std::time::Instant::now();
                let operation = request.request.operation_name();
                observe_expired(shard.expire_due(now));
                let response = request.request
                    .validate_preconditions(request.context, shard.generation())
                    .and_then(|()| request.request.into_domain())
                    .and_then(|operation| execute(&mut shard, request.context, operation, now));
                let outcome = if response.is_ok() { "success" } else { "error" };
                metrics::histogram!(
                    "relaygate_route_table_request_duration_seconds",
                    "operation" => operation,
                    "outcome" => outcome
                )
                .record(started_at.elapsed().as_secs_f64());
                observe_shard(&shard);
                let _ = request.reply.send(response);
            }
            () = wait_until(next_expiry) => {
                observe_expired(shard.expire_due(tokio::time::Instant::now().into_std()));
                observe_shard(&shard);
            }
        }
    }
    shard
}

fn observe_expired(expired: usize) {
    if expired > 0 {
        metrics::counter!("relaygate_route_table_expired_registrations_total")
            .increment(expired as u64);
    }
}

fn observe_shard(shard: &RouteTableShard) {
    let stats = shard.stats();
    metrics::gauge!("relaygate_route_table_registrations").set(stats.registration_count as f64);
    metrics::gauge!("relaygate_route_table_mappings").set(stats.mapping_count as f64);
    metrics::gauge!("relaygate_route_table_routes").set(stats.route_count as f64);
    metrics::gauge!("relaygate_route_table_expiry_records").set(stats.expiry_record_count as f64);
}

fn execute(
    shard: &mut RouteTableShard,
    context: RequestContext,
    request: DomainRequest,
    now: std::time::Instant,
) -> Result<WireResponse, TransportError> {
    match request {
        DomainRequest::Register { generation, key } => shard
            .register(context, generation, key, now)
            .map(WireResponse::registered)
            .map_err(TransportError::from),
        DomainRequest::Update {
            generation,
            key,
            lease_id,
            revision,
            snapshot,
        } => shard
            .update(context, generation, &key, lease_id, revision, snapshot, now)
            .map(WireResponse::updated)
            .map_err(TransportError::from),
        DomainRequest::KeepAlive {
            generation,
            key,
            lease_id,
        } => shard
            .keep_alive(context, generation, &key, lease_id, now)
            .map(WireResponse::kept_alive)
            .map_err(TransportError::from),
        DomainRequest::Deregister {
            generation,
            key,
            lease_id,
        } => shard
            .deregister(context, generation, &key, lease_id, now)
            .map(|()| WireResponse::Deregistered)
            .map_err(TransportError::from),
        DomainRequest::Resolve {
            generation,
            client_id,
        } => shard
            .resolve(context, generation, &client_id, now)
            .map(|bindings| WireResponse::resolved(&bindings))
            .map_err(TransportError::from),
    }
}

async fn wait_until(deadline: Option<std::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    } else {
        pending::<()>().await;
    }
}
