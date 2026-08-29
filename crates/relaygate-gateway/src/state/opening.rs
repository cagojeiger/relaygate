use std::time::Instant;

use relaygate_protocol::{
    BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};

use super::{Delivery, GatewayState, PipeEntry, PipePhase};

impl GatewayState {
    pub(super) fn open(
        &mut self,
        connector: SessionId,
        connection_id: u64,
        client_id: String,
    ) -> Vec<Delivery> {
        let Some(session) = self.sessions.get_mut(&connector) else {
            return Vec::new();
        };
        if session
            .highest_connection_id
            .is_some_and(|highest| connection_id <= highest)
        {
            return Vec::new();
        }
        session.highest_connection_id = Some(connection_id);
        if client_id.is_empty() {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "ClientId must not be empty",
            );
        }
        if self.live_pipe_count() >= self.limits.max_live_pipes {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway live Pipe limit reached",
            );
        }
        if self.pending_offer_count() >= self.limits.max_pending_offers {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway pending OFFER limit reached",
            );
        }

        let Some(binding) = self.registry.select(&client_id) else {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::NotFound,
                PeerObservation::NotObserved,
                "no live ListenerBinding exists",
            );
        };
        let listener_is_live = self
            .sessions
            .get(&binding.session_id)
            .is_some_and(|session| session.role == SessionRole::Listener);
        if !listener_is_live {
            self.registry.remove_owned(binding.session_id, binding.id);
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::Unavailable,
                PeerObservation::NotObserved,
                "selected ListenerSession is no longer live",
            );
        }

        let pipe_id = PipeId::new(connector, connection_id);
        self.insert_offer(
            pipe_id,
            PipeEntry {
                connector,
                listener: binding.session_id,
                binding_id: binding.id,
                phase: PipePhase::Offered,
                offered_at: Instant::now(),
                connector_finished: false,
                listener_finished: false,
            },
        );
        self.to(
            binding.session_id,
            Frame::Offer {
                pipe_id,
                binding_id: binding.id,
                client_id,
            },
        )
        .into_iter()
        .collect()
    }

    fn open_failed(
        &self,
        connector: SessionId,
        connection_id: u64,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<Delivery> {
        self.to(
            connector,
            Frame::OpenFailed {
                connection_id,
                code,
                observation,
                message: message.to_owned(),
            },
        )
        .into_iter()
        .collect()
    }

    pub(super) fn offer_accepted(&mut self, listener: SessionId, pipe_id: PipeId) -> Vec<Delivery> {
        let connector = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Vec::new();
            };
            if pipe.listener != listener || pipe.phase != PipePhase::Offered {
                return Vec::new();
            }
            pipe.connector
        };
        if self.live_pipe_count() >= self.limits.max_live_pipes {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                return Vec::new();
            };
            let message = "Gateway live Pipe limit reached during admission";
            return [
                self.to(
                    pipe.connector,
                    Frame::OpenFailed {
                        connection_id: pipe_id.connection_id(),
                        code: ErrorCode::ResourceExhausted,
                        observation: PeerObservation::Observed,
                        message: message.to_owned(),
                    },
                ),
                self.to(
                    pipe.listener,
                    Frame::Reset {
                        pipe_id,
                        code: ErrorCode::ResourceExhausted,
                        message: message.to_owned(),
                    },
                ),
            ]
            .into_iter()
            .flatten()
            .collect();
        }
        if self.promote_offer(pipe_id).is_none() {
            return Vec::new();
        }
        self.to(connector, Frame::Opened { pipe_id })
            .into_iter()
            .collect()
    }

    pub(super) fn offer_rejected(
        &mut self,
        listener: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Vec<Delivery> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        if pipe.listener != listener || pipe.phase != PipePhase::Offered {
            return Vec::new();
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        self.to(
            pipe.connector,
            Frame::OpenFailed {
                connection_id: pipe_id.connection_id(),
                code,
                observation: PeerObservation::NotObserved,
                message,
            },
        )
        .into_iter()
        .collect()
    }

    pub(super) fn cancel(&mut self, connector: SessionId, pipe_id: PipeId) -> Vec<Delivery> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        if pipe.connector != connector || pipe_id.connector_session_id() != connector {
            return Vec::new();
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        self.to(
            pipe.listener,
            Frame::Reset {
                pipe_id,
                code: ErrorCode::Cancelled,
                message: "Connector cancelled the Pipe".to_owned(),
            },
        )
        .into_iter()
        .collect()
    }

    pub(super) fn cancel_pending_binding(&mut self, binding_id: BindingId) -> Vec<Delivery> {
        let pending: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.binding_id == binding_id && pipe.phase == PipePhase::Offered)
                    .then_some(*pipe_id)
            })
            .collect();
        pending
            .into_iter()
            .filter_map(|pipe_id| {
                let pipe = self.remove_pipe(pipe_id)?;
                self.to(
                    pipe.connector,
                    Frame::OpenFailed {
                        connection_id: pipe_id.connection_id(),
                        code: ErrorCode::Unavailable,
                        observation: PeerObservation::NotObserved,
                        message: "selected ListenerBinding was removed before admission".to_owned(),
                    },
                )
            })
            .collect()
    }

    pub(crate) fn expire_offers(&mut self, now: Instant) -> Vec<Delivery> {
        let expired: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.phase == PipePhase::Offered
                    && now.saturating_duration_since(pipe.offered_at) >= self.limits.offer_timeout)
                    .then_some(*pipe_id)
            })
            .collect();
        let mut deliveries = Vec::with_capacity(expired.len() * 2);
        for pipe_id in expired {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            if let Some(delivery) = self.to(
                pipe.connector,
                Frame::OpenFailed {
                    connection_id: pipe_id.connection_id(),
                    code: ErrorCode::DeadlineExceeded,
                    observation: PeerObservation::MaybeObserved,
                    message: "Listener did not answer OFFER before the Gateway deadline".to_owned(),
                },
            ) {
                deliveries.push(delivery);
            }
            if let Some(delivery) = self.to(
                pipe.listener,
                Frame::Reset {
                    pipe_id,
                    code: ErrorCode::DeadlineExceeded,
                    message: "Gateway OFFER deadline exceeded".to_owned(),
                },
            ) {
                deliveries.push(delivery);
            }
        }
        deliveries
    }
}
