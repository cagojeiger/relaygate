use std::time::Instant;

use relaygate_protocol::{
    BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};

use super::{Delivery, GatewayState, PipeEntry, PipePhase, ProtocolViolation};

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
        tracing::debug!(
            component = "gateway",
            event = "gateway.offer.created",
            connector_session_id = %connector.as_uuid(),
            listener_session_id = %binding.session_id.as_uuid(),
            connection_id,
            binding_id = %binding.id.as_uuid(),
            client_id = %client_id,
            pending_offers = self.pending_offer_count() + 1,
            live_pipes = self.live_pipe_count(),
            "Pipe offer created"
        );
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
        tracing::debug!(
            component = "gateway",
            event = "gateway.open.failed",
            connector_session_id = %connector.as_uuid(),
            connection_id,
            error_code = ?code,
            observation = ?observation,
            "Open attempt failed"
        );
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

    pub(super) fn offer_accepted(
        &mut self,
        listener: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let (connector, phase) = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Ok(Vec::new());
            };
            pipe.ensure_owner(listener, pipe_id, "OFFER_ACCEPTED")?;
            (pipe.connector, pipe.phase)
        };
        if phase != PipePhase::Offered {
            return Ok(
                self.protocol_reset(pipe_id, "OFFER_ACCEPTED is not valid after the Pipe opened")
            );
        }
        if self.live_pipe_count() >= self.limits.max_live_pipes {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                return Ok(Vec::new());
            };
            let message = "Gateway live Pipe limit reached during admission";
            tracing::warn!(
                component = "gateway",
                event = "gateway.offer.admission_failed",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                error_code = ?ErrorCode::ResourceExhausted,
                observation = ?PeerObservation::MaybeObserved,
                pending_offers = self.pending_offer_count(),
                live_pipes = self.live_pipe_count(),
                "Pipe offer could not be admitted"
            );
            return Ok([
                self.to(
                    pipe.connector,
                    Frame::OpenFailed {
                        connection_id: pipe_id.connection_id(),
                        code: ErrorCode::ResourceExhausted,
                        observation: PeerObservation::MaybeObserved,
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
            .collect());
        }
        if self.promote_offer(pipe_id).is_none() {
            return Ok(Vec::new());
        }
        tracing::debug!(
            component = "gateway",
            event = "gateway.pipe.opened",
            listener_session_id = %listener.as_uuid(),
            connector_session_id = %connector.as_uuid(),
            connection_id = pipe_id.connection_id(),
            pending_offers = self.pending_offer_count(),
            live_pipes = self.live_pipe_count(),
            "Pipe opened"
        );
        Ok(self
            .to(connector, Frame::Opened { pipe_id })
            .into_iter()
            .collect())
    }

    pub(super) fn offer_rejected(
        &mut self,
        listener: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_owner(listener, pipe_id, "OFFER_REJECTED")?;
        if pipe.phase != PipePhase::Offered {
            return Ok(
                self.protocol_reset(pipe_id, "OFFER_REJECTED is not valid after the Pipe opened")
            );
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.offer.rejected",
            listener_session_id = %listener.as_uuid(),
            connector_session_id = %pipe.connector.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %pipe.binding_id.as_uuid(),
            error_code = ?code,
            pending_offers = self.pending_offer_count(),
            live_pipes = self.live_pipe_count(),
            "Pipe offer rejected"
        );
        Ok(self
            .to(
                pipe.connector,
                Frame::OpenFailed {
                    connection_id: pipe_id.connection_id(),
                    code,
                    observation: PeerObservation::NotObserved,
                    message,
                },
            )
            .into_iter()
            .collect())
    }

    pub(super) fn cancel(
        &mut self,
        connector: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_owner(connector, pipe_id, "CANCEL")?;
        debug_assert_eq!(pipe_id.connector_session_id(), connector);
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.offer.cancelled",
            connector_session_id = %connector.as_uuid(),
            listener_session_id = %pipe.listener.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %pipe.binding_id.as_uuid(),
            reason = "connector_cancelled",
            pending_offers = self.pending_offer_count(),
            live_pipes = self.live_pipe_count(),
            "Pipe offer cancelled"
        );
        Ok(self
            .to(
                pipe.listener,
                Frame::Reset {
                    pipe_id,
                    code: ErrorCode::Cancelled,
                    message: "Connector cancelled the Pipe".to_owned(),
                },
            )
            .into_iter()
            .collect())
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
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.offer.cancelled",
                    connector_session_id = %pipe.connector.as_uuid(),
                    listener_session_id = %pipe.listener.as_uuid(),
                    connection_id = pipe_id.connection_id(),
                    binding_id = %pipe.binding_id.as_uuid(),
                    reason = "binding_removed",
                    pending_offers = self.pending_offer_count(),
                    live_pipes = self.live_pipe_count(),
                    "Pipe offer cancelled"
                );
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
        let mut deliveries = Vec::with_capacity(expired.len());
        let mut expired_listeners = std::collections::HashSet::new();
        for pipe_id in expired {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            tracing::warn!(
                component = "gateway",
                event = "gateway.offer.expired",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                error_code = ?ErrorCode::DeadlineExceeded,
                observation = ?PeerObservation::MaybeObserved,
                "Pipe offer expired"
            );
            expired_listeners.insert(pipe.listener);
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
        }
        for listener in expired_listeners {
            deliveries.extend(self.remove_session(listener));
        }
        deliveries
    }
}
