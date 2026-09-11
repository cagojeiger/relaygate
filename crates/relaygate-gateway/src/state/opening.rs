use std::time::Instant;

use relaygate_protocol::{
    BindingId, Destination, ErrorCode, Frame, PeerObservation, PipeId, SessionId,
};

use crate::{peer::OpenIdentity, registry::Binding};

use super::{
    GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipeEntry, PipePhase,
    ProtocolViolation, RemoteOpenAttempt, RemoteOpenPhase, observe_dial_result,
};

impl GatewayState {
    pub(super) fn dial(
        &mut self,
        dialer: SessionId,
        connection_id: u64,
        destination: Destination,
        now: Instant,
        started_at: Instant,
    ) -> Vec<GatewayAction> {
        if !self.sessions.contains_key(&dialer) {
            return Vec::new();
        }

        if self.draining {
            return self.new_open_failed(
                dialer,
                connection_id,
                ErrorCode::Unavailable,
                PeerObservation::NotObserved,
                "Gateway is draining",
                started_at,
            );
        }

        if self.live_pipe_count() >= self.limits.max_live_pipes {
            return self.new_open_failed(
                dialer,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway live Pipe limit reached",
                started_at,
            );
        }
        if self.pending_capacity_reached() {
            return self.new_open_failed(
                dialer,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway pending open limit reached",
                started_at,
            );
        }

        let pipe_id = PipeId::new(dialer, connection_id);
        let self_publishes_destination = self
            .registry
            .contains_session_destination(dialer, &destination);
        if let Some(binding) = self.registry.select_excluding(&destination, dialer) {
            return self.offer_local_at(pipe_id, binding, now, Some(started_at));
        }

        let Some(gateway_id) = self.gateway_id else {
            return self.new_open_failed(
                dialer,
                connection_id,
                if self_publishes_destination {
                    ErrorCode::FailedPrecondition
                } else {
                    ErrorCode::NotFound
                },
                PeerObservation::NotObserved,
                if self_publishes_destination {
                    "only the dialing Relay publishes this Destination"
                } else {
                    "no live Binding exists"
                },
                started_at,
            );
        };
        if self.remote_open_attempts.len() >= self.limits.max_remote_dial_attempts {
            return self.new_open_failed(
                dialer,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway remote DIAL admission limit reached",
                started_at,
            );
        }
        let open_identity = OpenIdentity::new(gateway_id, dialer, connection_id);
        let previous = self.remote_open_attempts.insert(
            open_identity,
            RemoteOpenAttempt {
                pipe_id,
                destination: destination.clone(),
                started_at,
                phase: RemoteOpenPhase::Resolving,
            },
        );
        debug_assert!(previous.is_none());
        vec![GatewayAction::ResolveRoute {
            open_identity,
            destination,
        }]
    }

    pub(super) fn offer_local_at(
        &mut self,
        pipe_id: PipeId,
        binding: Binding,
        now: Instant,
        open_started_at: Option<Instant>,
    ) -> Vec<GatewayAction> {
        let dialer = pipe_id.origin_session_id();
        let destination = binding.destination.clone();
        let listener_is_live = self.sessions.contains_key(&binding.session_id);
        if !listener_is_live {
            self.registry.remove_owned(binding.session_id, binding.id);
            observe_dial_result(open_started_at, Some(ErrorCode::Unavailable));
            let mut actions = self.open_failed(
                dialer,
                pipe_id.connection_id(),
                ErrorCode::Unavailable,
                PeerObservation::NotObserved,
                "selected RelaySession is no longer live",
            );
            actions.push(self.registration_publication(binding.session_id));
            return actions;
        }

        tracing::debug!(
            component = "gateway",
            event = "gateway.offer.created",
            dialer_session_id = %dialer.as_uuid(),
            relay_session_id = %binding.session_id.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %binding.id.as_uuid(),
            destination = %destination,
            pending_offers = self.pending_offer_count() + 1,
            live_pipes = self.live_pipe_count(),
            "Pipe offer created"
        );
        self.insert_offer(
            pipe_id,
            PipeEntry {
                dialer: PipeEndpoint::Sdk(dialer),
                acceptor: PipeEndpoint::Sdk(binding.session_id),
                binding_id: binding.id,
                open_identity: None,
                phase: PipePhase::Offered,
                offered_at: now,
                open_started_at,
                dialer_finished: false,
                acceptor_finished: false,
            },
        );
        self.to(
            binding.session_id,
            Frame::Offer {
                pipe_id,
                binding_id: binding.id,
                destination,
            },
        )
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    pub(super) fn open_failed(
        &self,
        dialer: SessionId,
        connection_id: u64,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        tracing::debug!(
            component = "gateway",
            event = "gateway.dial.failed",
            relay_session_id = %dialer.as_uuid(),
            connection_id,
            error_code = ?code,
            observation = ?observation,
            "Dial attempt failed"
        );
        self.to(
            dialer,
            Frame::DialFailed {
                connection_id,
                code,
                observation,
                message: message.to_owned(),
            },
        )
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    fn new_open_failed(
        &self,
        dialer: SessionId,
        connection_id: u64,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
        started_at: Instant,
    ) -> Vec<GatewayAction> {
        observe_dial_result(Some(started_at), Some(code));
        self.open_failed(dialer, connection_id, code, observation, message)
    }

    pub(super) fn offer_accepted(
        &mut self,
        acceptor: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let phase = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Ok(Vec::new());
            };
            pipe.ensure_sdk_owner(acceptor, pipe_id, "OFFER_ACCEPTED")?;
            pipe.phase
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
            let mut actions = self.dialer_failure(
                &pipe,
                pipe_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::MaybeObserved,
                message,
            );
            actions.extend(self.endpoint_reset(
                pipe.acceptor,
                pipe_id,
                ErrorCode::ResourceExhausted,
                message,
            ));
            return Ok(actions);
        }
        let Some(pipe) = self.promote_offer(pipe_id).cloned() else {
            return Ok(Vec::new());
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.pipe.opened",
            dialer_session_id = %pipe_id.origin_session_id().as_uuid(),
            relay_session_id = %acceptor.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %pipe.binding_id.as_uuid(),
            pending_offers = self.pending_offer_count(),
            live_pipes = self.live_pipe_count(),
            "Pipe opened"
        );
        Ok(self.dialer_opened(&pipe, pipe_id))
    }

    pub(super) fn offer_rejected(
        &mut self,
        acceptor: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_sdk_owner(acceptor, pipe_id, "OFFER_REJECTED")?;
        if pipe.phase != PipePhase::Offered {
            return Ok(
                self.protocol_reset(pipe_id, "OFFER_REJECTED is not valid after the Pipe opened")
            );
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        Ok(self.dialer_failure(&pipe, pipe_id, code, PeerObservation::NotObserved, &message))
    }

    pub(crate) fn offer_delivery_rejected(
        &mut self,
        acceptor: SessionId,
        pipe_id: PipeId,
    ) -> Vec<GatewayAction> {
        let matches_pending_offer = self.pipes.get(&pipe_id).is_some_and(|pipe| {
            pipe.phase == PipePhase::Offered && pipe.acceptor == PipeEndpoint::Sdk(acceptor)
        });
        if !matches_pending_offer {
            return Vec::new();
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        tracing::warn!(
            component = "gateway",
            event = "gateway.offer.admission_rejected",
            relay_session_id = %acceptor.as_uuid(),
            connection_id = pipe_id.connection_id(),
            error_code = ?ErrorCode::ResourceExhausted,
            observation = ?PeerObservation::NotObserved,
            "Acceptor writer queue was full before OFFER admission"
        );
        self.dialer_failure(
            &pipe,
            pipe_id,
            ErrorCode::ResourceExhausted,
            PeerObservation::NotObserved,
            "selected RelaySession writer queue is full",
        )
    }

    pub(super) fn cancel(
        &mut self,
        dialer: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        if !self.pipes.contains_key(&pipe_id) {
            return Ok(self.cancel_remote_attempt(dialer, pipe_id));
        }
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_sdk_owner(dialer, pipe_id, "CANCEL")?;
        if pipe.dialer != PipeEndpoint::Sdk(dialer) {
            return Err(ProtocolViolation::PipeOwnership {
                sender: dialer,
                pipe_id,
                frame_name: "CANCEL",
            });
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        if pipe.phase == PipePhase::Offered {
            observe_dial_result(pipe.open_started_at, Some(ErrorCode::Cancelled));
        }
        Ok(self.endpoint_reset(
            pipe.acceptor,
            pipe_id,
            ErrorCode::Cancelled,
            "Dialer cancelled the Pipe",
        ))
    }

    pub(super) fn cancel_pending_binding(&mut self, binding_id: BindingId) -> Vec<GatewayAction> {
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
            .flat_map(|pipe_id| {
                let Some(pipe) = self.remove_pipe(pipe_id) else {
                    return Vec::new();
                };
                self.dialer_failure(
                    &pipe,
                    pipe_id,
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    "selected Binding was removed before admission",
                )
            })
            .collect()
    }

    pub(crate) fn expire_offers(&mut self, now: Instant) -> Vec<GatewayAction> {
        let expired: Vec<_> = self
            .pipes
            .iter()
            .filter_map(|(pipe_id, pipe)| {
                (pipe.phase == PipePhase::Offered
                    && now.saturating_duration_since(pipe.offered_at) >= self.limits.offer_timeout)
                    .then_some(*pipe_id)
            })
            .collect();
        let mut actions = Vec::with_capacity(expired.len());
        let mut expired_listeners = std::collections::HashSet::new();
        for pipe_id in expired {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                continue;
            };
            if let Some(acceptor) = pipe.acceptor.sdk_session() {
                expired_listeners.insert(acceptor);
            }
            actions.extend(self.dialer_failure(
                &pipe,
                pipe_id,
                ErrorCode::DeadlineExceeded,
                PeerObservation::MaybeObserved,
                "Acceptor did not answer OFFER before the Gateway deadline",
            ));
        }
        for acceptor in expired_listeners {
            actions.extend(self.remove_session(acceptor));
        }
        actions
    }

    pub(super) fn pending_capacity_reached(&self) -> bool {
        self.pending_offer_count
            .saturating_add(self.remote_open_attempts.len())
            >= self.limits.max_pending_offers
    }

    pub(super) fn dialer_opened(&self, pipe: &PipeEntry, pipe_id: PipeId) -> Vec<GatewayAction> {
        observe_dial_result(pipe.open_started_at, None);
        match pipe.dialer {
            PipeEndpoint::Sdk(dialer) => self
                .to(dialer, Frame::Opened { pipe_id })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![PeerDelivery::Opened { key }.into()],
        }
    }

    pub(super) fn dialer_failure(
        &self,
        pipe: &PipeEntry,
        pipe_id: PipeId,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        if matches!(pipe.dialer, PipeEndpoint::Sdk(_)) {
            observe_dial_result(pipe.open_started_at, Some(code));
        }
        match pipe.dialer {
            PipeEndpoint::Sdk(dialer) => {
                self.open_failed(dialer, pipe_id.connection_id(), code, observation, message)
            }
            PipeEndpoint::Peer(key) => vec![
                PeerDelivery::Failed {
                    key,
                    code,
                    observation,
                    message: message.to_owned(),
                }
                .into(),
            ],
        }
    }
}
