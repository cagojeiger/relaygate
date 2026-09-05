/// Gateway-local summary of the RouteTable dependency's last observed state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RouteDependencyHealth {
    /// This Gateway runs without RouteTable orchestration.
    #[default]
    Disabled,
    /// Every configured shard is available and current desired registrations are synchronized.
    Ready,
    /// At least one shard is unavailable or a desired registration is not synchronized.
    Degraded,
    /// At least one shard or desired registration observed a non-retryable control failure.
    Terminal,
}

impl RouteDependencyHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Ready => "READY",
            Self::Degraded => "DEGRADED",
            Self::Terminal => "TERMINAL",
        }
    }
}

/// A point-in-time view of the Gateway's local, live runtime state.
///
/// Local counts come from the same in-memory indexes used for routing. Optional
/// routing fields describe the workers' latest observed dependency and convergence
/// state; they can briefly lag a local mutation and are not sampled atomically
/// with the local counts. They are not the RouteTable's mapping contents. Payload
/// and application-level delivery state are never included.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewaySnapshot {
    /// Whether this Gateway has stopped admitting new work and is draining existing work.
    pub draining: bool,
    /// Total number of live SDK sessions.
    pub sessions: usize,
    /// Number of live Destination bindings published on this Gateway.
    pub bindings: usize,
    /// Number of Pipe offers awaiting Listener admission.
    pub pending_offers: usize,
    /// Number of admitted Pipes currently relaying bytes.
    pub live_pipes: usize,
    /// Last observed RouteTable dependency summary for this Gateway.
    pub route_dependency_health: RouteDependencyHealth,
    /// Number of session-shard registrations last confirmed by routing workers.
    pub route_registrations_synced: usize,
    /// Number of worker-observed registrations awaiting RouteTable convergence.
    pub route_registrations_unsynced: usize,
    /// Number of request-local remote OPEN attempts that have not terminated.
    pub remote_open_attempts: usize,
    /// Number of direction-scoped peer transport candidates connecting now.
    pub peer_transports_connecting: usize,
    /// Number of reusable peer transports ready now.
    pub peer_transports_ready: usize,
    /// Number of current RelayStreams across ready peer transports.
    pub peer_streams: usize,
}

impl GatewaySnapshot {
    pub(crate) fn from_parts(
        sessions: usize,
        bindings: usize,
        pending_offers: usize,
        live_pipes: usize,
        draining: bool,
    ) -> Self {
        Self {
            draining,
            sessions,
            bindings,
            pending_offers,
            live_pipes,
            ..Self::default()
        }
    }
}
