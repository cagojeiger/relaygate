use std::time::Instant;

use relaygate_protocol::SessionId;

use super::GatewayState;

impl GatewayState {
    pub(super) fn admit_control(
        &mut self,
        session_id: SessionId,
        operation: &'static str,
        now: Instant,
    ) -> bool {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return false;
        };
        let scope = if !session.control_rate.try_take(now) {
            "session"
        } else if !self.control_rate.try_take(now) {
            "gateway"
        } else {
            return true;
        };
        metrics::counter!("relaygate_gateway_control_rejections_total", "operation" => operation, "scope" => scope).increment(1);
        false
    }
}
