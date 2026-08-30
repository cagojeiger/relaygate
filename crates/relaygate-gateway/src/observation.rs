use relaygate_protocol::SessionRole;

/// A point-in-time view of the Gateway's local, live runtime state.
///
/// Local counts come from the same in-memory indexes used for routing. Optional
/// registration counts describe the routing workers' latest observed convergence
/// state; they can briefly lag a local mutation and are not sampled atomically
/// with the local counts. They are not the RouteTable's mapping contents. Payload
/// and application-level delivery state are never included.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewaySnapshot {
    /// Total number of live SDK sessions.
    pub sessions: usize,
    /// Number of live Listener SDK sessions.
    pub listener_sessions: usize,
    /// Number of live Connector SDK sessions.
    pub connector_sessions: usize,
    /// Number of live Listener bindings registered on this Gateway.
    pub listener_bindings: usize,
    /// Number of Pipe offers awaiting Listener admission.
    pub pending_offers: usize,
    /// Number of admitted Pipes currently relaying bytes.
    pub live_pipes: usize,
    /// Number of session-shard registrations last confirmed by routing workers.
    pub route_registrations_synced: usize,
    /// Number of worker-observed registrations awaiting RouteTable convergence.
    pub route_registrations_unsynced: usize,
}

impl GatewaySnapshot {
    pub(crate) fn from_parts(
        session_roles: impl IntoIterator<Item = SessionRole>,
        listener_bindings: usize,
        pending_offers: usize,
        live_pipes: usize,
    ) -> Self {
        let mut snapshot = Self {
            listener_bindings,
            pending_offers,
            live_pipes,
            ..Self::default()
        };
        for role in session_roles {
            snapshot.sessions += 1;
            match role {
                SessionRole::Connector => snapshot.connector_sessions += 1,
                SessionRole::Listener => snapshot.listener_sessions += 1,
            }
        }
        snapshot
    }
}
