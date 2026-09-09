use crate::GatewaySnapshot;

use super::Gateway;

impl Gateway {
    /// Returns the current counts for local SDK sessions, bindings, and Pipes.
    ///
    /// This is an instantaneous observation of this Gateway instance. Callers
    /// must not interpret it as a cluster-wide or durable view.
    #[must_use]
    pub fn snapshot(&self) -> GatewaySnapshot {
        let mut snapshot = self.inner.lock_state().snapshot();
        snapshot.session_slots_used = snapshot
            .max_sessions
            .saturating_sub(self.inner.session_slots.available_permits());
        snapshot.max_pending_handshakes = self.inner.max_pending_handshakes;
        snapshot.pending_handshakes = snapshot
            .max_pending_handshakes
            .saturating_sub(self.inner.handshake_slots.available_permits());
        snapshot.sdk_admission_ready = !snapshot.draining
            && self.inner.session_slots.available_permits() > 0
            && self.inner.handshake_slots.available_permits() > 0
            && self.inner.connection_rate.has_capacity();
        if let Some(routing) = &self.inner.routing {
            let counts = routing.current_counts();
            snapshot.route_dependency_health = counts.dependency_health;
            snapshot.route_registrations_synced = counts.synced;
            snapshot.route_registrations_unsynced = counts.unsynced;
        }
        if let Some(peer) = &self.inner.peer {
            let counts = peer.counts();
            snapshot.peer_transports_connecting = counts.connecting;
            snapshot.peer_transports_ready = counts.ready;
            snapshot.peer_streams = counts.streams;
        }
        snapshot
    }
}
