use relaygate_protocol::{ErrorCode, Frame, SessionId, SessionRole};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Delivery, GatewayState, SessionEntry};

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
                return Some(session_id);
            }
        }
    }

    pub(crate) fn remove_session(&mut self, session_id: SessionId) -> Vec<Delivery> {
        if self.sessions.remove(&session_id).is_none() {
            return Vec::new();
        }
        self.registry.remove_session(session_id);

        let owned: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.connector == session_id || pipe.listener == session_id).then_some(*pipe_id)
            })
            .collect();
        let mut deliveries = Vec::with_capacity(owned.len());
        for pipe_id in owned {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            let counterpart = if pipe.connector == session_id {
                pipe.listener
            } else {
                pipe.connector
            };
            if let Some(delivery) = self.to(
                counterpart,
                Frame::Reset {
                    pipe_id,
                    code: ErrorCode::Unavailable,
                    message: "counterpart session disconnected".to_owned(),
                },
            ) {
                deliveries.push(delivery);
            }
        }
        deliveries
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
