use relaygate_protocol::SessionRole;

/// A point-in-time view of the Gateway's local, live runtime state.
///
/// The snapshot is derived from the same in-memory indexes used for routing.
/// It does not include payload data, RouteTable mappings, or application-level
/// delivery state.
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
