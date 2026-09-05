use std::time::Duration;

use anyhow::{Context, Result};
use relaygate_gateway::Gateway;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::GatewayRuntimeConfig;
use crate::metrics::{self, MetricsRuntime};

pub(crate) async fn serve(
    config: GatewayRuntimeConfig,
    shutdown: CancellationToken,
    metrics: Option<MetricsRuntime>,
) -> Result<()> {
    let listener = TcpListener::bind(&config.bind_address)
        .await
        .with_context(|| {
            format!(
                "failed to bind Gateway SDK listener at {}",
                config.bind_address
            )
        })?;
    let local_address = listener.local_addr()?;
    let (gateway, peer_listener, peer_address) = match config.distributed {
        Some(distributed) => {
            let peer_listener = TcpListener::bind(&distributed.peer_bind_address)
                .await
                .with_context(|| {
                    format!(
                        "failed to bind Gateway peer listener at {}",
                        distributed.peer_bind_address
                    )
                })?;
            let peer_address = peer_listener.local_addr()?;
            if distributed.insecure_transport {
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.route_table.trusted_local_enabled",
                    transport = "plain_tcp",
                    "local/CI RouteTable and peer adapter is running without TLS"
                );
            }
            (
                Gateway::new_distributed(config.gateway, distributed.routing, distributed.peer)?,
                Some(peer_listener),
                Some(peer_address),
            )
        }
        None => (Gateway::new(config.gateway)?, None, None),
    };
    let distributed_enabled = peer_listener.is_some();
    tracing::info!(
        component = "server",
        event = "server.started",
        role = "gateway",
        address = %local_address,
        distributed_enabled,
        peer_address = ?peer_address,
        "RelayGate Gateway started"
    );

    let observation_shutdown = CancellationToken::new();
    let mut observation_tasks = Vec::new();
    if let Some(interval) = config.stats_interval {
        log_gateway_snapshot(&gateway);
        let stats_gateway = gateway.clone();
        let stats_shutdown = observation_shutdown.clone();
        observation_tasks.push(tokio::spawn(async move {
            log_gateway_stats(stats_gateway, stats_shutdown, interval).await;
        }));
    }
    if let Some(metrics) = metrics {
        metrics::observe_gateway(gateway.snapshot());
        let metrics_gateway = gateway.clone();
        let metrics_shutdown = observation_shutdown.clone();
        observation_tasks.push(tokio::spawn(async move {
            publish_gateway_metrics(metrics_gateway, metrics_shutdown, metrics.interval()).await;
        }));
    }

    let serve_result = match peer_listener {
        Some(peer_listener) => {
            gateway
                .serve_distributed(listener, peer_listener, shutdown)
                .await
        }
        None => gateway.serve(listener, shutdown).await,
    };
    metrics::observe_gateway(gateway.snapshot());
    observation_shutdown.cancel();
    for task in observation_tasks {
        let _ = task.await;
    }
    serve_result?;
    tracing::info!(
        component = "server",
        event = "server.stopped",
        role = "gateway",
        "RelayGate Gateway stopped"
    );
    Ok(())
}

async fn publish_gateway_metrics(
    gateway: Gateway,
    shutdown: CancellationToken,
    interval_duration: Duration,
) {
    let start = tokio::time::Instant::now() + interval_duration;
    let mut interval = tokio::time::interval_at(start, interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => metrics::observe_gateway(gateway.snapshot()),
        }
    }
}

async fn log_gateway_stats(
    gateway: Gateway,
    shutdown: CancellationToken,
    interval_duration: Duration,
) {
    let start = tokio::time::Instant::now() + interval_duration;
    let mut interval = tokio::time::interval_at(start, interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => log_gateway_snapshot(&gateway),
        }
    }
}

fn log_gateway_snapshot(gateway: &Gateway) {
    let snapshot = gateway.snapshot();
    tracing::info!(
        component = "gateway",
        event = "gateway.snapshot",
        draining = snapshot.draining,
        sessions = snapshot.sessions,
        bindings = snapshot.bindings,
        pending_offers = snapshot.pending_offers,
        live_pipes = snapshot.live_pipes,
        route_dependency_health = snapshot.route_dependency_health.as_str(),
        route_registrations_synced = snapshot.route_registrations_synced,
        route_registrations_unsynced = snapshot.route_registrations_unsynced,
        remote_open_attempts = snapshot.remote_open_attempts,
        peer_transports_connecting = snapshot.peer_transports_connecting,
        peer_transports_ready = snapshot.peer_transports_ready,
        peer_streams = snapshot.peer_streams,
        "Gateway current-state snapshot"
    );
}
