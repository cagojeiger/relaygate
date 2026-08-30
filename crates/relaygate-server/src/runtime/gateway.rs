use std::time::Duration;

use anyhow::{Context, Result};
use relaygate_gateway::Gateway;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::GatewayRuntimeConfig;

pub(crate) async fn serve(config: GatewayRuntimeConfig, shutdown: CancellationToken) -> Result<()> {
    let routing_enabled = config.routing.is_some();
    let gateway = match config.routing {
        Some(routing) => {
            tracing::warn!(
                component = "gateway",
                event = "gateway.route_table.trusted_local_enabled",
                transport = "plain_tcp",
                "local/CI RouteTable key adapter is enabled; channel security must be supplied by the deployment environment"
            );
            Gateway::new_routed(config.gateway, routing, shutdown.child_token())?
        }
        None => Gateway::new(config.gateway)?,
    };
    let listener = TcpListener::bind(&config.bind_address)
        .await
        .with_context(|| format!("failed to bind Gateway at {}", config.bind_address))?;
    let local_address = listener.local_addr()?;
    tracing::info!(
        component = "server",
        event = "server.started",
        role = "gateway",
        address = %local_address,
        configured_clients = config.configured_clients,
        routing_enabled,
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

    gateway.serve(listener, shutdown).await?;
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
        route_registrations_synced = snapshot.route_registrations_synced,
        route_registrations_unsynced = snapshot.route_registrations_unsynced,
        "Gateway current-state snapshot"
    );
}
