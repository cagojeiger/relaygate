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
            Frame::RegisterFailed {
                request_id,
                code: ErrorCode::InvalidArgument,
                message: "ClientId must not be empty".to_owned(),
            }
        } else if !self.auth.authorizes(&client_id, &client_key) {
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
            Frame::RegisterFailed {
                request_id,
                code: ErrorCode::ResourceExhausted,
                message: "Gateway ListenerBinding limit reached".to_owned(),
            }
        } else {
            let binding_id = match self.registry.register(session_id, &client_id) {
                Registration::Created(binding) | Registration::Existing(binding) => binding.id,
            };
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
        let mut deliveries = if self.registry.remove_owned(session_id, binding_id).is_some() {
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
