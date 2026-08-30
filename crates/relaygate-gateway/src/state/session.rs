use relaygate_protocol::{ErrorCode, Frame, SessionId, SessionRole};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Delivery, GatewayAction, GatewayState, SessionEntry};

impl GatewayState {
    pub(crate) fn add_session(
        &mut self,
        role: SessionRole,
        sender: mpsc::Sender<Frame>,
        cancellation: CancellationToken,
    ) -> Option<SessionId> {
        if self.sessions.len() >= self.limits.max_sessions {
            return None;
        }
        loop {
            let session_id = SessionId::new();
            if let std::collections::hash_map::Entry::Vacant(entry) =
                self.sessions.entry(session_id)
            {
                entry.insert(SessionEntry {
                    role,
                    sender,
                    cancellation,
                    highest_connection_id: None,
                });
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.session.added",
                    session_id = %session_id.as_uuid(),
                    role = role_name(role),
                    active_sessions = self.sessions.len(),
                    "SDK session added"
                );
                return Some(session_id);
            }
        }
    }

    pub(crate) fn remove_session(&mut self, session_id: SessionId) -> Vec<GatewayAction> {
        let Some(session) = self.sessions.remove(&session_id) else {
            return Vec::new();
        };
        session.cancellation.cancel();
        let removed_bindings = self.registry.remove_session(session_id).len();

        let owned: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.connector == session_id || pipe.listener == session_id).then_some(*pipe_id)
            })
            .collect();
        let removed_pipes = owned.len();
        let mut actions =
            Vec::with_capacity(owned.len() + usize::from(session.role == SessionRole::Listener));
        for pipe_id in owned {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            tracing::debug!(
                component = "gateway",
                event = "gateway.pipe.removed",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                phase = ?pipe.phase,
                reason = "session_disconnected",
                "Pipe removed during session cleanup"
            );
            let (counterpart, frame) = if pipe.listener == session_id
                && pipe.phase == super::PipePhase::Offered
            {
                (
                    pipe.connector,
                    Frame::OpenFailed {
                        connection_id: pipe_id.connection_id(),
                        code: ErrorCode::Unavailable,
                        observation: relaygate_protocol::PeerObservation::MaybeObserved,
                        message: "selected ListenerSession disconnected during OFFER".to_owned(),
                    },
                )
            } else {
                let counterpart = if pipe.connector == session_id {
                    pipe.listener
                } else {
                    pipe.connector
                };
                (
                    counterpart,
                    Frame::Reset {
                        pipe_id,
                        code: ErrorCode::Unavailable,
                        message: "counterpart session disconnected".to_owned(),
                    },
                )
            };
            if let Some(delivery) = self.to(counterpart, frame) {
                actions.push(GatewayAction::SendSdkFrame(delivery));
            }
        }
        tracing::debug!(
            component = "gateway",
            event = "gateway.session.removed",
            session_id = %session_id.as_uuid(),
            role = role_name(session.role),
            removed_bindings,
            removed_pipes,
            active_sessions = self.sessions.len(),
            listener_bindings = self.registry.binding_count(),
            pending_offers = self.pending_offer_count,
            live_pipes = self.live_pipe_count,
            "SDK session removed"
        );
        if session.role == SessionRole::Listener {
            actions.push(self.registration_publication(session_id));
        }
        actions
    }

    pub(super) fn to(&self, target: SessionId, frame: Frame) -> Option<Delivery> {
        let session = self.sessions.get(&target)?;
        Some(Delivery {
            target,
            frame,
            sender: session.sender.clone(),
            cancellation: session.cancellation.clone(),
        })
    }
}

fn role_name(role: SessionRole) -> &'static str {
    match role {
        SessionRole::Connector => "connector",
        SessionRole::Listener => "listener",
    }
}
