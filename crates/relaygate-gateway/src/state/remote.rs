use std::time::Instant;

use relaygate_protocol::{
    BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};
use relaygate_route_table::BindingSet;

use crate::peer::{OpenIdentity, PeerStreamKey};

use super::{
    GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipeEntry, PipePhase,
    RemoteOpenAttempt, RemoteOpenPhase, observe_open_result,
};

impl GatewayState {
    pub(crate) fn route_resolved(
        &mut self,
        open_identity: OpenIdentity,
        bindings: BindingSet,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            return Vec::new();
        };
        if attempt.phase != RemoteOpenPhase::Resolving {
            return Vec::new();
        }
        let client_id = attempt.client_id.clone();
        let pipe_id = attempt.pipe_id;
        let started_at = attempt.started_at;
        let Some(mapping) = bindings.entries().first().cloned() else {
            return self.fail_remote_attempt(
                open_identity,
                ErrorCode::Internal,
                PeerObservation::NotObserved,
                "RouteTable returned an empty BindingSet",
            );
        };
        if mapping.client_id().as_str() != client_id {
            return self.fail_remote_attempt(
                open_identity,
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "RouteTable returned a mapping for a different ClientId",
            );
        }

        let mapping_identity = mapping.identity();
        let listener_session_id =
            SessionId::from_uuid(mapping_identity.listener_session_id().as_uuid());
        let binding_id = BindingId::from_uuid(mapping_identity.binding_id().as_uuid());
        if Some(mapping_identity.gateway_id()) == self.gateway_id {
            let _ = self.take_remote_attempt(open_identity);
            let Some(binding) = self
                .registry
                .exact(listener_session_id, binding_id, &client_id)
            else {
                observe_open_result(Some(started_at), Some(ErrorCode::Unavailable));
                return self.open_failed(
                    pipe_id.connector_session_id(),
                    pipe_id.connection_id(),
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    "selected local ListenerBinding is stale",
                );
            };
            return self.offer_local_at(
                pipe_id.connector_session_id(),
                pipe_id,
                binding,
                client_id,
                Instant::now(),
                Some(started_at),
            );
        }

        let Some(attempt) = self.remote_open_attempts.get_mut(&open_identity) else {
            return Vec::new();
        };
        if attempt.phase != RemoteOpenPhase::Resolving {
            return Vec::new();
        }
        attempt.phase = RemoteOpenPhase::StartingPeer { binding_id };
        vec![GatewayAction::OpenPeer {
            open_identity,
            gateway_id: mapping_identity.gateway_id(),
            gateway_locator: mapping.gateway_locator().clone(),
            client_id,
            listener_session_id,
            binding_id,
        }]
    }

    pub(crate) fn route_failed(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        message: &str,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            return Vec::new();
        };
        if attempt.phase != RemoteOpenPhase::Resolving {
            return Vec::new();
        }
        self.fail_remote_attempt(open_identity, code, PeerObservation::NotObserved, message)
    }

    pub(crate) fn peer_open_committed(
        &mut self,
        open_identity: OpenIdentity,
        key: PeerStreamKey,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            if self.active_peer_opens.get(&open_identity) == Some(&key) {
                return Vec::new();
            }
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                PipeId::new(
                    open_identity.connector_session(),
                    open_identity.connection_id(),
                ),
                ErrorCode::Cancelled,
                "Open attempt ended before the peer OPEN committed",
            );
        };
        let binding_id = match attempt.phase {
            RemoteOpenPhase::StartingPeer { binding_id } => binding_id,
            RemoteOpenPhase::AwaitingPeer {
                key: current_key, ..
            } if current_key == key => return Vec::new(),
            RemoteOpenPhase::Resolving | RemoteOpenPhase::AwaitingPeer { .. } => {
                return self.endpoint_reset(
                    PipeEndpoint::Peer(key),
                    attempt.pipe_id,
                    ErrorCode::ProtocolError,
                    "peer OPEN committed in an invalid local phase",
                );
            }
        };
        if self.peer_pipes.contains_key(&key)
            || self
                .active_peer_opens
                .values()
                .any(|current| *current == key)
            || self.active_peer_opens.contains_key(&open_identity)
        {
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                attempt.pipe_id,
                ErrorCode::ProtocolError,
                "peer stream identity is already active",
            );
        }
        let previous = self.active_peer_opens.insert(open_identity, key);
        debug_assert!(previous.is_none());
        if let Some(attempt) = self.remote_open_attempts.get_mut(&open_identity) {
            attempt.phase = RemoteOpenPhase::AwaitingPeer { key, binding_id };
        }
        Vec::new()
    }

    pub(crate) fn peer_open_commit_failed(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            return Vec::new();
        };
        if !matches!(attempt.phase, RemoteOpenPhase::StartingPeer { .. }) {
            return Vec::new();
        }
        self.fail_remote_attempt(open_identity, code, observation, message)
    }

    pub(crate) fn receive_peer_open(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
    ) -> Vec<GatewayAction> {
        self.receive_peer_open_at(
            key,
            open_identity,
            client_id,
            listener_session_id,
            binding_id,
            Instant::now(),
        )
    }

    pub(crate) fn receive_peer_open_at(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
        now: Instant,
    ) -> Vec<GatewayAction> {
        if open_identity.entry_gateway() != key.peer_gateway_id() {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::PermissionDenied,
                    observation: PeerObservation::NotObserved,
                    message: "peer OPEN identity does not match the authenticated Gateway"
                        .to_owned(),
                }
                .into(),
            ];
        }
        if self.peer_pipes.contains_key(&key)
            || self.active_peer_opens.contains_key(&open_identity)
            || self.remote_open_attempts.contains_key(&open_identity)
        {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::AlreadyExists,
                    observation: PeerObservation::NotObserved,
                    message: "peer OPEN identity is already active".to_owned(),
                }
                .into(),
            ];
        }
        if client_id.is_empty() {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::InvalidArgument,
                    observation: PeerObservation::NotObserved,
                    message: "ClientId must not be empty".to_owned(),
                }
                .into(),
            ];
        }
        if self.live_pipe_count() >= self.limits.max_live_pipes || self.pending_capacity_reached() {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::ResourceExhausted,
                    observation: PeerObservation::NotObserved,
                    message: "Gateway Pipe limit reached".to_owned(),
                }
                .into(),
            ];
        }
        let listener_is_live = self
            .sessions
            .get(&listener_session_id)
            .is_some_and(|session| session.role == SessionRole::Listener);
        let Some(binding) = self
            .registry
            .exact(listener_session_id, binding_id, &client_id)
            .filter(|_| listener_is_live)
        else {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::Unavailable,
                    observation: PeerObservation::NotObserved,
                    message: "selected ListenerBinding is no longer current".to_owned(),
                }
                .into(),
            ];
        };

        let pipe_id = PipeId::new(
            open_identity.connector_session(),
            open_identity.connection_id(),
        );
        if self.pipes.contains_key(&pipe_id) {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::AlreadyExists,
                    observation: PeerObservation::NotObserved,
                    message: "Pipe identity is already active".to_owned(),
                }
                .into(),
            ];
        }
        self.insert_offer(
            pipe_id,
            PipeEntry {
                connector: PipeEndpoint::Peer(key),
                listener: PipeEndpoint::Sdk(listener_session_id),
                binding_id: binding.id,
                open_identity: Some(open_identity),
                phase: PipePhase::Offered,
                offered_at: now,
                open_started_at: None,
                connector_finished: false,
                listener_finished: false,
            },
        );
        self.to(
            listener_session_id,
            Frame::Offer {
                pipe_id,
                binding_id,
                client_id,
            },
        )
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    pub(crate) fn peer_opened(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
    ) -> Vec<GatewayAction> {
        self.peer_opened_at(key, open_identity, Instant::now())
    }

    pub(crate) fn peer_opened_at(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        now: Instant,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            if self.active_peer_opens.get(&open_identity) == Some(&key)
                && self.peer_pipes.contains_key(&key)
            {
                return Vec::new();
            }
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                PipeId::new(
                    open_identity.connector_session(),
                    open_identity.connection_id(),
                ),
                ErrorCode::Cancelled,
                "late peer OPENED cannot recreate an ended attempt",
            );
        };
        let binding_id = match attempt.phase {
            RemoteOpenPhase::StartingPeer { binding_id } => binding_id,
            RemoteOpenPhase::AwaitingPeer {
                key: current_key,
                binding_id,
            } if current_key == key => binding_id,
            RemoteOpenPhase::Resolving | RemoteOpenPhase::AwaitingPeer { .. } => {
                return self.endpoint_reset(
                    PipeEndpoint::Peer(key),
                    attempt.pipe_id,
                    ErrorCode::ProtocolError,
                    "peer OPENED does not match the current attempt",
                );
            }
        };
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        let connector = attempt.pipe_id.connector_session_id();
        let connector_is_live = self
            .sessions
            .get(&connector)
            .is_some_and(|session| session.role == SessionRole::Connector);
        if !connector_is_live || self.live_pipe_count() >= self.limits.max_live_pipes {
            let code = if connector_is_live {
                ErrorCode::ResourceExhausted
            } else {
                ErrorCode::Cancelled
            };
            let mut actions = self.endpoint_reset(
                PipeEndpoint::Peer(key),
                attempt.pipe_id,
                code,
                "peer OPENED after the local endpoint became unavailable",
            );
            if connector_is_live {
                observe_open_result(Some(attempt.started_at), Some(code));
                actions.extend(self.open_failed(
                    connector,
                    attempt.pipe_id.connection_id(),
                    code,
                    PeerObservation::MaybeObserved,
                    "Gateway live Pipe limit reached during remote admission",
                ));
            } else {
                observe_open_result(Some(attempt.started_at), Some(ErrorCode::Cancelled));
            }
            return actions;
        }

        observe_open_result(Some(attempt.started_at), None);
        self.insert_open(
            attempt.pipe_id,
            PipeEntry {
                connector: PipeEndpoint::Sdk(connector),
                listener: PipeEndpoint::Peer(key),
                binding_id,
                open_identity: Some(open_identity),
                phase: PipePhase::Open,
                offered_at: now,
                open_started_at: None,
                connector_finished: false,
                listener_finished: false,
            },
        );
        self.to(
            connector,
            Frame::Opened {
                pipe_id: attempt.pipe_id,
            },
        )
        .map(GatewayAction::SendSdkFrame)
        .into_iter()
        .collect()
    }

    pub(crate) fn peer_open_failed(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            return Vec::new();
        };
        let matches = match attempt.phase {
            RemoteOpenPhase::StartingPeer { .. } => true,
            RemoteOpenPhase::AwaitingPeer {
                key: current_key, ..
            } => current_key == key,
            RemoteOpenPhase::Resolving => false,
        };
        if !matches {
            return Vec::new();
        }
        self.fail_remote_attempt(open_identity, code, observation, message)
    }

    pub(crate) fn peer_transport_lost_stream(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        observation: PeerObservation,
    ) -> Vec<GatewayAction> {
        if let Some(attempt) = self.remote_open_attempts.get(&open_identity) {
            let matches = matches!(attempt.phase, RemoteOpenPhase::StartingPeer { .. })
                || matches!(
                    attempt.phase,
                    RemoteOpenPhase::AwaitingPeer {
                        key: current_key,
                        ..
                    } if current_key == key
                );
            if matches {
                return self.fail_remote_attempt(
                    open_identity,
                    ErrorCode::Unavailable,
                    observation,
                    "PeerTransport was lost during remote OPEN",
                );
            }
        }

        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        let exact = self
            .pipes
            .get(&pipe_id)
            .is_some_and(|pipe| pipe.open_identity == Some(open_identity));
        if !exact {
            return Vec::new();
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        [pipe.connector, pipe.listener]
            .into_iter()
            .filter_map(PipeEndpoint::sdk_session)
            .filter_map(|session_id| {
                self.to(
                    session_id,
                    Frame::Reset {
                        pipe_id,
                        code: ErrorCode::Unavailable,
                        message: "PeerTransport was lost".to_owned(),
                    },
                )
                .map(GatewayAction::SendSdkFrame)
            })
            .collect()
    }

    pub(super) fn cancel_remote_attempt(
        &mut self,
        connector: SessionId,
        pipe_id: PipeId,
    ) -> Vec<GatewayAction> {
        let Some(gateway_id) = self.gateway_id else {
            return Vec::new();
        };
        if pipe_id.connector_session_id() != connector {
            return Vec::new();
        }
        let open_identity = OpenIdentity::new(gateway_id, connector, pipe_id.connection_id());
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        observe_open_result(Some(attempt.started_at), Some(ErrorCode::Cancelled));
        match attempt.phase {
            RemoteOpenPhase::Resolving => Vec::new(),
            RemoteOpenPhase::StartingPeer { .. } => {
                vec![GatewayAction::CancelPeerOpen { open_identity }]
            }
            RemoteOpenPhase::AwaitingPeer { key, .. } => self.endpoint_reset(
                PipeEndpoint::Peer(key),
                pipe_id,
                ErrorCode::Cancelled,
                "Connector cancelled the remote OPEN",
            ),
        }
    }

    fn fail_remote_attempt(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        observe_open_result(Some(attempt.started_at), Some(code));
        self.open_failed(
            attempt.pipe_id.connector_session_id(),
            attempt.pipe_id.connection_id(),
            code,
            observation,
            message,
        )
    }

    fn take_remote_attempt(&mut self, open_identity: OpenIdentity) -> Option<RemoteOpenAttempt> {
        let attempt = self.remote_open_attempts.remove(&open_identity)?;
        self.active_peer_opens.remove(&open_identity);
        Some(attempt)
    }
}
