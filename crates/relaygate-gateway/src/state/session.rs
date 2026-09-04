use relaygate_protocol::{ErrorCode, Frame, PeerObservation, SessionId, SessionRole};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    Delivery, GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipePhase, RemoteOpenPhase,
    SessionEntry, observe_open_result,
};

impl GatewayState {
    pub(crate) fn add_session(
        &mut self,
        role: SessionRole,
        sender: mpsc::Sender<Frame>,
        cancellation: CancellationToken,
    ) -> Option<SessionId> {
        if self.draining || self.sessions.len() >= self.limits.max_sessions {
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

        let pending: Vec<_> = self
            .remote_open_attempts
            .iter()
            .filter_map(|(open_identity, attempt)| {
                (attempt.pipe_id.connector_session_id() == session_id).then_some(*open_identity)
            })
            .collect();
        let mut actions = Vec::new();
        for open_identity in pending {
            let Some(attempt) = self.remote_open_attempts.remove(&open_identity) else {
                continue;
            };
            observe_open_result(Some(attempt.started_at), Some(ErrorCode::Cancelled));
            self.active_peer_opens.remove(&open_identity);
            match attempt.phase {
                RemoteOpenPhase::Resolving => {}
                RemoteOpenPhase::StartingPeer { .. } => {
                    actions.push(GatewayAction::CancelPeerOpen { open_identity });
                }
                RemoteOpenPhase::AwaitingPeer { key, .. } => {
                    actions.push(
                        PeerDelivery::Reset {
                            key,
                            code: ErrorCode::Cancelled,
                            message: "ConnectorSession disconnected".to_owned(),
                        }
                        .into(),
                    );
                }
            }
        }

        let owned: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.connector == PipeEndpoint::Sdk(session_id)
                    || pipe.listener == PipeEndpoint::Sdk(session_id))
                .then_some(*pipe_id)
            })
            .collect();
        let removed_pipes = owned.len();
        for pipe_id in owned {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            if pipe.connector == PipeEndpoint::Sdk(session_id) && pipe.phase == PipePhase::Offered {
                observe_open_result(pipe.open_started_at, Some(ErrorCode::Cancelled));
            }
            if pipe.listener == PipeEndpoint::Sdk(session_id) && pipe.phase == PipePhase::Offered {
                actions.extend(self.connector_failure(
                    &pipe,
                    pipe_id,
                    ErrorCode::Unavailable,
                    PeerObservation::MaybeObserved,
                    "selected ListenerSession disconnected during OFFER",
                ));
                continue;
            }
            let counterpart = if pipe.connector == PipeEndpoint::Sdk(session_id) {
                pipe.listener
            } else {
                pipe.connector
            };
            let code = if pipe.connector == PipeEndpoint::Sdk(session_id)
                && counterpart.peer_key().is_some()
            {
                ErrorCode::Cancelled
            } else {
                ErrorCode::Unavailable
            };
            actions.extend(self.endpoint_reset(
                counterpart,
                pipe_id,
                code,
                "counterpart session disconnected",
            ));
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
            remote_open_attempts = self.remote_open_attempts.len(),
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
