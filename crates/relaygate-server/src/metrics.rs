use std::{env, net::SocketAddr, time::Duration};

use anyhow::{Context, Result, bail};
use metrics::{describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use relaygate_gateway::{GatewaySnapshot, RouteDependencyHealth};

use crate::config::optional_duration_millis;

const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(5);
const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetricsRuntime {
    interval: Duration,
}

impl MetricsRuntime {
    pub(crate) fn install(role: &'static str) -> Result<Option<Self>> {
        let bind_address = env::var("RELAYGATE_METRICS_BIND_ADDR").ok();
        if bind_address.is_none() && env::var_os("RELAYGATE_METRICS_INTERVAL_MS").is_some() {
            bail!(
                "RELAYGATE_METRICS_BIND_ADDR is required when RELAYGATE_METRICS_INTERVAL_MS is set"
            );
        }
        let Some(bind_address) = bind_address else {
            return Ok(None);
        };
        let bind_address = bind_address
            .parse::<SocketAddr>()
            .with_context(|| "RELAYGATE_METRICS_BIND_ADDR must be a socket address")?;
        let interval = optional_duration_millis("RELAYGATE_METRICS_INTERVAL_MS")?
            .unwrap_or(DEFAULT_METRICS_INTERVAL);

        PrometheusBuilder::new()
            .with_http_listener(bind_address)
            .add_global_label("role", role)
            .set_buckets(LATENCY_BUCKETS_SECONDS)
            .context("failed to configure Prometheus latency buckets")?
            .install()
            .context("failed to start Prometheus metrics exporter")?;
        describe_metrics();

        Ok(Some(Self { interval }))
    }

    #[must_use]
    pub(crate) const fn interval(self) -> Duration {
        self.interval
    }
}

pub(crate) fn observe_gateway(snapshot: GatewaySnapshot) {
    gauge!("relaygate_gateway_draining").set(if snapshot.draining { 1.0 } else { 0.0 });
    gauge!("relaygate_gateway_sessions").set(snapshot.sessions as f64);
    gauge!("relaygate_gateway_listener_sessions").set(snapshot.listener_sessions as f64);
    gauge!("relaygate_gateway_connector_sessions").set(snapshot.connector_sessions as f64);
    gauge!("relaygate_gateway_listener_bindings").set(snapshot.listener_bindings as f64);
    gauge!("relaygate_gateway_pending_offers").set(snapshot.pending_offers as f64);
    gauge!("relaygate_gateway_live_pipes").set(snapshot.live_pipes as f64);
    gauge!("relaygate_gateway_route_registrations_synced")
        .set(snapshot.route_registrations_synced as f64);
    gauge!("relaygate_gateway_route_registrations_unsynced")
        .set(snapshot.route_registrations_unsynced as f64);
    gauge!("relaygate_gateway_remote_open_attempts").set(snapshot.remote_open_attempts as f64);
    gauge!("relaygate_gateway_peer_transports_connecting")
        .set(snapshot.peer_transports_connecting as f64);
    gauge!("relaygate_gateway_peer_transports_ready").set(snapshot.peer_transports_ready as f64);
    gauge!("relaygate_gateway_peer_streams").set(snapshot.peer_streams as f64);

    for state in [
        RouteDependencyHealth::Disabled,
        RouteDependencyHealth::Ready,
        RouteDependencyHealth::Degraded,
        RouteDependencyHealth::Terminal,
    ] {
        gauge!(
            "relaygate_gateway_route_dependency",
            "state" => state.as_str()
        )
        .set(if state == snapshot.route_dependency_health {
            1.0
        } else {
            0.0
        });
    }
}

fn describe_metrics() {
    describe_counter!(
        "relaygate_gateway_open_requests_total",
        "Accepted Connector OPEN requests on this Gateway."
    );
    describe_counter!(
        "relaygate_gateway_open_results_total",
        "Terminal results for accepted Connector OPEN requests."
    );
    describe_histogram!(
        "relaygate_gateway_open_duration_seconds",
        "Time from accepted Connector OPEN to its terminal result."
    );
    describe_counter!(
        "relaygate_gateway_writer_queue_rejections_total",
        "Frames rejected by a full or closed bounded SDK writer queue."
    );
    describe_counter!(
        "relaygate_gateway_listener_registration_results_total",
        "Terminal Listener registration results by bounded outcome and code."
    );
    describe_gauge!(
        "relaygate_gateway_draining",
        "Whether this Gateway has stopped admitting new work and is draining existing work."
    );
    describe_gauge!(
        "relaygate_gateway_sessions",
        "Current SDK sessions on this Gateway."
    );
    describe_gauge!(
        "relaygate_gateway_listener_sessions",
        "Current Listener SDK sessions on this Gateway."
    );
    describe_gauge!(
        "relaygate_gateway_connector_sessions",
        "Current Connector SDK sessions on this Gateway."
    );
    describe_gauge!(
        "relaygate_gateway_listener_bindings",
        "Current Listener bindings owned by this Gateway."
    );
    describe_gauge!(
        "relaygate_gateway_pending_offers",
        "Current Pipe offers awaiting Listener admission."
    );
    describe_gauge!(
        "relaygate_gateway_live_pipes",
        "Current admitted Pipes on this Gateway."
    );
    describe_gauge!(
        "relaygate_gateway_route_registrations_synced",
        "Current session-shard registrations observed as synchronized."
    );
    describe_gauge!(
        "relaygate_gateway_route_registrations_unsynced",
        "Current session-shard registrations awaiting RouteTable convergence."
    );
    describe_gauge!(
        "relaygate_gateway_remote_open_attempts",
        "Current unresolved remote OPEN attempts."
    );
    describe_gauge!(
        "relaygate_gateway_peer_transports_connecting",
        "Current direction-scoped PeerTransport connection candidates."
    );
    describe_gauge!(
        "relaygate_gateway_peer_transports_ready",
        "Current reusable ready PeerTransports."
    );
    describe_gauge!(
        "relaygate_gateway_peer_streams",
        "Current RelayStreams across ready PeerTransports."
    );
    describe_gauge!(
        "relaygate_gateway_route_dependency",
        "One-hot current RouteTable dependency state."
    );
    describe_counter!(
        "relaygate_gateway_route_dependency_transitions_total",
        "RouteTable dependency transitions using bounded lifecycle states."
    );
    describe_counter!(
        "relaygate_gateway_route_connection_attempts_total",
        "Gateway RouteTable connection attempts by bounded outcome and stable result code."
    );
    describe_histogram!(
        "relaygate_gateway_route_recovery_duration_seconds",
        "Time from a RouteTable dependency degradation episode to recovery."
    );
    describe_counter!(
        "relaygate_gateway_peer_handshakes_total",
        "Terminal Gateway peer handshake results by direction, outcome, and bounded code."
    );
    describe_counter!(
        "relaygate_gateway_peer_transport_closed_total",
        "Gateway peer transport closures by bounded terminal reason."
    );
    describe_gauge!(
        "relaygate_route_table_registrations",
        "Current live registrations on this RouteTable shard."
    );
    describe_gauge!(
        "relaygate_route_table_mappings",
        "Current binding mappings on this RouteTable shard."
    );
    describe_gauge!(
        "relaygate_route_table_routes",
        "Current ClientIds with at least one mapping on this RouteTable shard."
    );
    describe_gauge!(
        "relaygate_route_table_expiry_records",
        "Current lease expiry index records on this RouteTable shard."
    );
    describe_counter!(
        "relaygate_route_table_requests_total",
        "RouteTable terminal wire responses by bounded operation, outcome, and stable result code."
    );
    describe_histogram!(
        "relaygate_route_table_request_duration_seconds",
        "RouteTable actor service time by bounded operation and outcome."
    );
    describe_counter!(
        "relaygate_route_table_handshakes_total",
        "Terminal Gateway-to-RouteTable handshake results by bounded outcome and code."
    );
    describe_counter!(
        "relaygate_route_table_expired_registrations_total",
        "Registrations removed after their soft-state lease expired."
    );
}
