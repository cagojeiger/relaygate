use std::time::Duration;

use anyhow::{Context, Result};
use relaygate_gateway::Gateway;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::GatewayRuntimeConfig;

pub(crate) async fn serve(config: GatewayRuntimeConfig, shutdown: CancellationToken) -> Result<()> {
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
            tracing::warn!(
                component = "gateway",
                event = "gateway.route_table.trusted_local_enabled",
                transport = "plain_tcp",
                "local/CI RouteTable and peer key adapter is enabled; channel security must be supplied by the deployment environment"
            );
            (
                Gateway::new_distributed(
                    config.gateway,
                    distributed.routing,
                    distributed.peer,
                    shutdown.child_token(),
                )?,
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
        configured_clients = config.configured_clients,
        distributed_enabled,
        peer_address = ?peer_address,
        "RelayGate Gateway started"
    );

    if let Some(interval) = config.stats_interval {
        log_gateway_snapshot(&gateway);
        let stats_gateway = gateway.clone();
        let stats_shutdown = shutdown.clone();
        tokio::spawn(async move {
            log_gateway_stats(stats_gateway, stats_shutdown, interval).await;
        });
    }

    match peer_listener {
        Some(peer_listener) => {
            gateway
                .serve_distributed(listener, peer_listener, shutdown)
                .await?
        }
        None => gateway.serve(listener, shutdown).await?,
    }
    tracing::info!(
        component = "server",
        event = "server.stopped",
        role = "gateway",
        "RelayGate Gateway stopped"
    );
    Ok(())
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
        sessions = snapshot.sessions,
        listener_sessions = snapshot.listener_sessions,
        connector_sessions = snapshot.connector_sessions,
        listener_bindings = snapshot.listener_bindings,
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
