use crate::state::{GatewayAction, GatewayState};

use super::Inner;

/// Result of one Gateway state mutation whose registration actions must be
/// committed before the state lock is released.
pub(super) trait TransitionOutput {
    fn actions(&self) -> &[GatewayAction];
}

impl TransitionOutput for Vec<GatewayAction> {
    fn actions(&self) -> &[GatewayAction] {
        self
    }
}

impl TransitionOutput for Option<Vec<GatewayAction>> {
    fn actions(&self) -> &[GatewayAction] {
        self.as_deref().unwrap_or_default()
    }
}

impl<E> TransitionOutput for Result<Vec<GatewayAction>, E> {
    fn actions(&self) -> &[GatewayAction] {
        self.as_deref().unwrap_or_default()
    }
}

impl Inner {
    /// Applies one mutation under the Gateway state lock and commits its
    /// registration snapshot before the lock is released.
    pub(super) fn transition<T: TransitionOutput>(
        &self,
        apply: impl FnOnce(&mut GatewayState) -> T,
    ) -> T {
        let mut state = self.lock_state();
        let output = apply(&mut state);
        self.commit_registration_actions(output.actions());
        output
    }

    /// Commits the latest complete snapshot while the Gateway state lock still
    /// orders the corresponding local mutation. The manager wake is bounded
    /// and synchronous; no network I/O occurs under this lock. This prevents a
    /// delayed action from publishing an older snapshot after session cleanup.
    fn commit_registration_actions(&self, actions: &[GatewayAction]) {
        let Some(routing) = &self.routing else {
            return;
        };
        for action in actions {
            let GatewayAction::PublishRegistration {
                session_id,
                bindings,
            } = action
            else {
                continue;
            };
            if let Err(error) = routing.publish_session(*session_id, bindings.clone()) {
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.registration.publish_failed",
                    relay_session_id = %session_id.as_uuid(),
                    %error,
                    "local registration remains active while RouteTable publication is unavailable"
                );
            }
        }
    }
}
