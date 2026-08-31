use std::time::Instant;

use relaygate_protocol::{
    BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};
use relaygate_route_table::ClientId as RouteClientId;

use crate::{peer::OpenIdentity, registry::Binding};

use super::{
    GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipeEntry, PipePhase,
    ProtocolViolation, RemoteOpenAttempt, RemoteOpenPhase,
};

impl GatewayState {
    pub(super) fn open(
        &mut self,
        connector: SessionId,
        connection_id: u64,
        client_id: String,
        now: Instant,
    ) -> Vec<GatewayAction> {
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
        if self.pending_capacity_reached() {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "Gateway pending open limit reached",
            );
        }

        let pipe_id = PipeId::new(connector, connection_id);
        if let Some(binding) = self.registry.select(&client_id) {
            return self.offer_local_at(connector, pipe_id, binding, client_id, now);
        }

        let Some(gateway_id) = self.gateway_id else {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::NotFound,
                PeerObservation::NotObserved,
                "no live ListenerBinding exists",
            );
        };
        let Ok(route_client_id) = RouteClientId::new(client_id.clone()) else {
            return self.open_failed(
                connector,
                connection_id,
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "ClientId is invalid",
            );
        };
        let open_identity = OpenIdentity::new(gateway_id, connector, connection_id);
        let previous = self.remote_open_attempts.insert(
            open_identity,
            RemoteOpenAttempt {
                pipe_id,
                client_id,
                phase: RemoteOpenPhase::Resolving,
            },
        );
        debug_assert!(previous.is_none());
        vec![GatewayAction::ResolveRoute {
            open_identity,
            client_id: route_client_id,
        }]
    }

    pub(super) fn offer_local(
        &mut self,
        connector: SessionId,
        pipe_id: PipeId,
        binding: Binding,
        client_id: String,
    ) -> Vec<GatewayAction> {
        self.offer_local_at(connector, pipe_id, binding, client_id, Instant::now())
    }

    pub(super) fn offer_local_at(
        &mut self,
        connector: SessionId,
        pipe_id: PipeId,
        binding: Binding,
        client_id: String,
        now: Instant,
    ) -> Vec<GatewayAction> {
        let listener_is_live = self
            .sessions
            .get(&binding.session_id)
            .is_some_and(|session| session.role == SessionRole::Listener);
        if !listener_is_live {
            self.registry.remove_owned(binding.session_id, binding.id);
            let mut actions = self.open_failed(
                connector,
                pipe_id.connection_id(),
                ErrorCode::Unavailable,
                PeerObservation::NotObserved,
                "selected ListenerSession is no longer live",
            );
            actions.push(self.registration_publication(binding.session_id));
            return actions;
        }

        tracing::debug!(
            component = "gateway",
            event = "gateway.offer.created",
            connector_session_id = %connector.as_uuid(),
            listener_session_id = %binding.session_id.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %binding.id.as_uuid(),
            client_id = %client_id,
            pending_offers = self.pending_offer_count() + 1,
            live_pipes = self.live_pipe_count(),
            "Pipe offer created"
        );
        self.insert_offer(
            pipe_id,
            PipeEntry {
                connector: PipeEndpoint::Sdk(connector),
                listener: PipeEndpoint::Sdk(binding.session_id),
                binding_id: binding.id,
                open_identity: None,
                phase: PipePhase::Offered,
                offered_at: now,
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
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    pub(super) fn open_failed(
        &self,
        connector: SessionId,
        connection_id: u64,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
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
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    pub(super) fn offer_accepted(
        &mut self,
        listener: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let phase = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Ok(Vec::new());
            };
            pipe.ensure_sdk_owner(listener, pipe_id, "OFFER_ACCEPTED")?;
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
            let mut actions = self.connector_failure(
                &pipe,
                pipe_id,
                ErrorCode::ResourceExhausted,
                PeerObservation::MaybeObserved,
                message,
            );
            actions.extend(self.endpoint_reset(
                pipe.listener,
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
            listener_session_id = %listener.as_uuid(),
            connection_id = pipe_id.connection_id(),
            pending_offers = self.pending_offer_count(),
            live_pipes = self.live_pipe_count(),
            "Pipe opened"
        );
        Ok(self.connector_opened(&pipe, pipe_id))
    }

    pub(super) fn offer_rejected(
        &mut self,
        listener: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_sdk_owner(listener, pipe_id, "OFFER_REJECTED")?;
        if pipe.phase != PipePhase::Offered {
            return Ok(
                self.protocol_reset(pipe_id, "OFFER_REJECTED is not valid after the Pipe opened")
            );
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        Ok(self.connector_failure(&pipe, pipe_id, code, PeerObservation::NotObserved, &message))
    }

    pub(super) fn cancel(
        &mut self,
        connector: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        if !self.pipes.contains_key(&pipe_id) {
            return Ok(self.cancel_remote_attempt(connector, pipe_id));
        }
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        pipe.ensure_sdk_owner(connector, pipe_id, "CANCEL")?;
        if pipe.connector != PipeEndpoint::Sdk(connector) {
            return Err(ProtocolViolation::PipeOwnership {
                sender: connector,
                pipe_id,
                frame_name: "CANCEL",
            });
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Ok(Vec::new());
        };
        Ok(self.endpoint_reset(
            pipe.listener,
            pipe_id,
            ErrorCode::Cancelled,
            "Connector cancelled the Pipe",
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
                self.connector_failure(
                    &pipe,
                    pipe_id,
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    "selected ListenerBinding was removed before admission",
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
            if let Some(listener) = pipe.listener.sdk_session() {
                expired_listeners.insert(listener);
            }
            actions.extend(self.connector_failure(
                &pipe,
                pipe_id,
                ErrorCode::DeadlineExceeded,
                PeerObservation::MaybeObserved,
                "Listener did not answer OFFER before the Gateway deadline",
            ));
        }
        for listener in expired_listeners {
            actions.extend(self.remove_session(listener));
        }
        actions
    }

    pub(super) fn pending_capacity_reached(&self) -> bool {
        self.pending_offer_count
            .saturating_add(self.remote_open_attempts.len())
            >= self.limits.max_pending_offers
    }

    pub(super) fn connector_opened(&self, pipe: &PipeEntry, pipe_id: PipeId) -> Vec<GatewayAction> {
        match pipe.connector {
            PipeEndpoint::Sdk(connector) => self
                .to(connector, Frame::Opened { pipe_id })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![PeerDelivery::Opened { key }.into()],
        }
    }

    pub(super) fn connector_failure(
        &self,
        pipe: &PipeEntry,
        pipe_id: PipeId,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        match pipe.connector {
            PipeEndpoint::Sdk(connector) => self.open_failed(
                connector,
                pipe_id.connection_id(),
                code,
                observation,
                message,
            ),
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
