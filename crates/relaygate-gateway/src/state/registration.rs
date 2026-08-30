use relaygate_protocol::{BindingId, ClientKey, ErrorCode, Frame, SessionId};

use crate::registry::Registration;

use super::{Delivery, GatewayState};

impl GatewayState {
    pub(super) fn register(
        &mut self,
        session_id: SessionId,
        request_id: u64,
        client_id: String,
        client_key: ClientKey,
    ) -> Vec<Delivery> {
        let response = if client_id.is_empty() {
            tracing::debug!(
                component = "gateway",
                event = "gateway.listener.registration_rejected",
                session_id = %session_id.as_uuid(),
                request_id,
                error_code = ?ErrorCode::InvalidArgument,
                "Listener registration rejected"
            );
            Frame::RegisterFailed {
                request_id,
                code: ErrorCode::InvalidArgument,
                message: "ClientId must not be empty".to_owned(),
            }
        } else if !self.auth.authorizes(&client_id, &client_key) {
            tracing::debug!(
                component = "gateway",
                event = "gateway.listener.registration_rejected",
                session_id = %session_id.as_uuid(),
                request_id,
                client_id = %client_id,
                error_code = ?ErrorCode::Unauthenticated,
                "Listener registration rejected"
            );
            Frame::RegisterFailed {
                request_id,
                code: ErrorCode::Unauthenticated,
                message: "ClientKey was not accepted".to_owned(),
            }
        } else if self.registry.binding_count() >= self.limits.max_bindings
            && !self
                .registry
                .contains_session_client(session_id, &client_id)
        {
            tracing::warn!(
                component = "gateway",
                event = "gateway.listener.registration_rejected",
                session_id = %session_id.as_uuid(),
                request_id,
                client_id = %client_id,
                error_code = ?ErrorCode::ResourceExhausted,
                listener_bindings = self.registry.binding_count(),
                "Listener registration rejected"
            );
            Frame::RegisterFailed {
                request_id,
                code: ErrorCode::ResourceExhausted,
                message: "Gateway ListenerBinding limit reached".to_owned(),
            }
        } else {
            let registration = self.registry.register(session_id, &client_id);
            let (binding_id, created) = match registration {
                Registration::Created(binding) => (binding.id, true),
                Registration::Existing(binding) => (binding.id, false),
            };
            tracing::debug!(
                component = "gateway",
                event = "gateway.listener.registered",
                session_id = %session_id.as_uuid(),
                request_id,
                client_id = %client_id,
                binding_id = %binding_id.as_uuid(),
                created,
                listener_bindings = self.registry.binding_count(),
                "Listener registration accepted"
            );
            Frame::Registered {
                request_id,
                binding_id,
            }
        };
        self.to(session_id, response).into_iter().collect()
    }

    pub(super) fn unregister(
        &mut self,
        session_id: SessionId,
        request_id: u64,
        binding_id: BindingId,
    ) -> Vec<Delivery> {
        let removed = self.registry.remove_owned(session_id, binding_id).is_some();
        tracing::debug!(
            component = "gateway",
            event = "gateway.listener.unregistered",
            session_id = %session_id.as_uuid(),
            request_id,
            binding_id = %binding_id.as_uuid(),
            removed,
            listener_bindings = self.registry.binding_count(),
            "Listener unregistration processed"
        );
        let mut deliveries = if removed {
            self.cancel_pending_binding(binding_id)
        } else {
            Vec::new()
        };
        if let Some(response) = self.to(session_id, Frame::Unregistered { request_id }) {
            deliveries.push(response);
        }
        deliveries
    }
}
