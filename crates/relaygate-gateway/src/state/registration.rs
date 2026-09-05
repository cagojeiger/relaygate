use relaygate_protocol::{BindingId, DestinationId, ErrorCode, Frame, SessionId};

use crate::registry::Registration;

use super::{GatewayAction, GatewayState, error_code_name};

impl GatewayState {
    pub(super) fn publish(
        &mut self,
        session_id: SessionId,
        request_id: u64,
        destination_id: DestinationId,
    ) -> Vec<GatewayAction> {
        let (response, publish) = if self.draining {
            (
                Frame::PublishFailed {
                    request_id,
                    code: ErrorCode::Unavailable,
                    message: "Gateway is draining".to_owned(),
                },
                false,
            )
        } else if self.registry.binding_count() >= self.limits.max_bindings
            && !self
                .registry
                .contains_session_destination(session_id, destination_id)
        {
            tracing::warn!(
                component = "gateway",
                event = "gateway.publication.rejected",
                session_id = %session_id.as_uuid(),
                request_id,
                destination_id = %destination_id,
                error_code = ?ErrorCode::ResourceExhausted,
                bindings = self.registry.binding_count(),
                "Destination publication rejected"
            );
            (
                Frame::PublishFailed {
                    request_id,
                    code: ErrorCode::ResourceExhausted,
                    message: "Gateway ListenerBinding limit reached".to_owned(),
                },
                false,
            )
        } else {
            let registration = self.registry.register(session_id, destination_id);
            let (binding_id, created) = match registration {
                Registration::Created(binding) => (binding.id, true),
                Registration::Existing(binding) => (binding.id, false),
            };
            tracing::debug!(
                component = "gateway",
                event = "gateway.publication.active",
                session_id = %session_id.as_uuid(),
                request_id,
                destination_id = %destination_id,
                binding_id = %binding_id.as_uuid(),
                created,
                bindings = self.registry.binding_count(),
                "Destination publication accepted"
            );
            (
                Frame::Published {
                    request_id,
                    binding_id,
                },
                created,
            )
        };
        if let Some((outcome, code)) = registration_result(&response) {
            metrics::counter!(
                "relaygate_gateway_publish_results_total",
                "outcome" => outcome,
                "code" => code
            )
            .increment(1);
        }
        let mut actions = self
            .to(session_id, response)
            .map(GatewayAction::SendSdkFrame)
            .into_iter()
            .collect::<Vec<_>>();
        if publish {
            actions.push(self.registration_publication(session_id));
        }
        actions
    }

    pub(super) fn unpublish(
        &mut self,
        session_id: SessionId,
        request_id: u64,
        binding_id: BindingId,
    ) -> Vec<GatewayAction> {
        let removed = self.registry.remove_owned(session_id, binding_id).is_some();
        tracing::debug!(
            component = "gateway",
            event = "gateway.publication.removed",
            session_id = %session_id.as_uuid(),
            request_id,
            binding_id = %binding_id.as_uuid(),
            removed,
            bindings = self.registry.binding_count(),
            "Destination unpublication processed"
        );
        let mut actions = if removed {
            Self::send_actions(self.cancel_pending_binding(binding_id))
        } else {
            Vec::new()
        };
        if let Some(response) = self.to(session_id, Frame::Unpublished { request_id }) {
            actions.push(GatewayAction::SendSdkFrame(response));
        }
        if removed {
            actions.push(self.registration_publication(session_id));
        }
        actions
    }
}

fn registration_result(response: &Frame) -> Option<(&'static str, &'static str)> {
    match response {
        Frame::Published { .. } => Some(("success", "ok")),
        Frame::PublishFailed { code, .. } => Some(("error", error_code_name(*code))),
        _ => None,
    }
}
