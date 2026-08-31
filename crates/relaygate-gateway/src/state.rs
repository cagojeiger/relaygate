use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::Bytes;
use relaygate_protocol::{
    BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};
use relaygate_route_table::{ClientId as RouteClientId, GatewayId, GatewayLocator};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    GatewaySnapshot,
    auth::ClientKeyStore,
    peer::{OpenIdentity, PeerStreamKey},
    registry::{Binding, LocalRegistry},
};

mod opening;
mod pipe;
mod registration;
mod remote;
mod session;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) target: SessionId,
    pub(crate) frame: Frame,
    sender: mpsc::Sender<Frame>,
    cancellation: CancellationToken,
}

impl Delivery {
    pub(crate) fn deliver(self) -> Option<SessionId> {
        if let Err(error) = self.sender.try_send(self.frame) {
            let queue_state = match error {
                mpsc::error::TrySendError::Full(_) => "full",
                mpsc::error::TrySendError::Closed(_) => "closed",
            };
            tracing::warn!(
                component = "gateway",
                event = "gateway.session.writer_queue_rejected",
                session_id = %self.target.as_uuid(),
                queue_state,
                error_code = ?ErrorCode::ResourceExhausted,
                "closing a session whose bounded writer queue cannot accept a frame"
            );
            self.cancellation.cancel();
            return Some(self.target);
        }
        None
    }
}

#[derive(Debug, Clone)]
pub(crate) enum GatewayAction {
    SendSdkFrame(Delivery),
    PublishRegistration {
        session_id: SessionId,
        bindings: Vec<Binding>,
    },
    ResolveRoute {
        open_identity: OpenIdentity,
        client_id: RouteClientId,
    },
    OpenPeer {
        open_identity: OpenIdentity,
        gateway_id: GatewayId,
        gateway_locator: GatewayLocator,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
    },
    CancelPeerOpen {
        open_identity: OpenIdentity,
    },
    SendPeerFrame(PeerDelivery),
}

impl From<Delivery> for GatewayAction {
    fn from(delivery: Delivery) -> Self {
        Self::SendSdkFrame(delivery)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PeerDelivery {
    Opened {
        key: PeerStreamKey,
    },
    Failed {
        key: PeerStreamKey,
        code: ErrorCode,
        observation: PeerObservation,
        message: String,
    },
    Data {
        key: PeerStreamKey,
        payload: Bytes,
    },
    Fin {
        key: PeerStreamKey,
    },
    Close {
        key: PeerStreamKey,
    },
    Reset {
        key: PeerStreamKey,
        code: ErrorCode,
        message: String,
    },
}

impl From<PeerDelivery> for GatewayAction {
    fn from(delivery: PeerDelivery) -> Self {
        Self::SendPeerFrame(delivery)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolViolation {
    #[error("frame {frame_name} is not valid for a {role:?} session")]
    InvalidFrameForRole {
        role: SessionRole,
        frame_name: &'static str,
    },
    #[error("session {sender:?} does not own existing Pipe {pipe_id:?} for frame {frame_name}")]
    PipeOwnership {
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    },
}

#[derive(Debug, Clone)]
struct SessionEntry {
    role: SessionRole,
    sender: mpsc::Sender<Frame>,
    cancellation: CancellationToken,
    highest_connection_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipePhase {
    Offered,
    Open,
}

#[derive(Debug, Clone)]
struct PipeEntry {
    connector: PipeEndpoint,
    listener: PipeEndpoint,
    binding_id: BindingId,
    open_identity: Option<OpenIdentity>,
    phase: PipePhase,
    offered_at: Instant,
    connector_finished: bool,
    listener_finished: bool,
}

impl PipeEntry {
    fn ensure_sdk_owner(
        &self,
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    ) -> Result<(), ProtocolViolation> {
        if self.connector == PipeEndpoint::Sdk(sender) || self.listener == PipeEndpoint::Sdk(sender)
        {
            return Ok(());
        }
        Err(ProtocolViolation::PipeOwnership {
            sender,
            pipe_id,
            frame_name,
        })
    }

    fn peer_key(&self) -> Option<PeerStreamKey> {
        match (self.connector, self.listener) {
            (PipeEndpoint::Peer(key), _) | (_, PipeEndpoint::Peer(key)) => Some(key),
            (PipeEndpoint::Sdk(_), PipeEndpoint::Sdk(_)) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipeEndpoint {
    Sdk(SessionId),
    Peer(PeerStreamKey),
}

impl PipeEndpoint {
    const fn sdk_session(self) -> Option<SessionId> {
        match self {
            Self::Sdk(session_id) => Some(session_id),
            Self::Peer(_) => None,
        }
    }

    const fn peer_key(self) -> Option<PeerStreamKey> {
        match self {
            Self::Sdk(_) => None,
            Self::Peer(key) => Some(key),
        }
    }
}

#[derive(Debug, Clone)]
struct RemoteOpenAttempt {
    pipe_id: PipeId,
    client_id: String,
    phase: RemoteOpenPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteOpenPhase {
    Resolving,
    StartingPeer {
        binding_id: BindingId,
    },
    AwaitingPeer {
        key: PeerStreamKey,
        binding_id: BindingId,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GatewayLimits {
    pub(crate) max_sessions: usize,
    pub(crate) max_bindings: usize,
    pub(crate) max_pending_offers: usize,
    pub(crate) max_live_pipes: usize,
    pub(crate) offer_timeout: Duration,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            max_sessions: crate::config::DEFAULT_MAX_SESSIONS,
            max_bindings: crate::config::DEFAULT_MAX_BINDINGS,
            max_pending_offers: crate::config::DEFAULT_MAX_PENDING_OFFERS,
            max_live_pipes: crate::config::DEFAULT_MAX_LIVE_PIPES,
            offer_timeout: crate::config::DEFAULT_OFFER_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub(crate) struct GatewayState {
    auth: ClientKeyStore,
    sessions: HashMap<SessionId, SessionEntry>,
    registry: LocalRegistry,
    pipes: HashMap<PipeId, PipeEntry>,
    peer_pipes: HashMap<PeerStreamKey, PipeId>,
    active_peer_opens: HashMap<OpenIdentity, PeerStreamKey>,
    remote_open_attempts: HashMap<OpenIdentity, RemoteOpenAttempt>,
    pending_offer_count: usize,
    live_pipe_count: usize,
    gateway_id: Option<GatewayId>,
    limits: GatewayLimits,
}

impl GatewayState {
    pub(crate) fn new(auth: ClientKeyStore, limits: GatewayLimits) -> Self {
        Self::build(auth, limits, None)
    }

    pub(crate) fn new_distributed(
        auth: ClientKeyStore,
        limits: GatewayLimits,
        gateway_id: GatewayId,
    ) -> Self {
        Self::build(auth, limits, Some(gateway_id))
    }

    fn build(auth: ClientKeyStore, limits: GatewayLimits, gateway_id: Option<GatewayId>) -> Self {
        Self {
            auth,
            sessions: HashMap::new(),
            registry: LocalRegistry::default(),
            pipes: HashMap::new(),
            peer_pipes: HashMap::new(),
            active_peer_opens: HashMap::new(),
            remote_open_attempts: HashMap::new(),
            pending_offer_count: 0,
            live_pipe_count: 0,
            gateway_id,
            limits,
        }
    }

    pub(crate) fn handle(
        &mut self,
        session_id: SessionId,
        frame: Frame,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        self.handle_at(session_id, frame, Instant::now())
    }

    pub(crate) fn handle_at(
        &mut self,
        session_id: SessionId,
        frame: Frame,
        now: Instant,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let Some(role) = self.sessions.get(&session_id).map(|session| session.role) else {
            return Ok(Vec::new());
        };
        if !frame_allowed(role, &frame) {
            return Err(ProtocolViolation::InvalidFrameForRole {
                role,
                frame_name: frame_name(&frame),
            });
        }

        let actions = match frame {
            Frame::Register {
                request_id,
                client_id,
                client_key,
            } => self.register(session_id, request_id, client_id, client_key),
            Frame::Unregister {
                request_id,
                binding_id,
            } => self.unregister(session_id, request_id, binding_id),
            Frame::Open {
                connection_id,
                client_id,
            } => self.open(session_id, connection_id, client_id, now),
            Frame::OfferAccepted { pipe_id } => {
                Self::send_actions(self.offer_accepted(session_id, pipe_id)?)
            }
            Frame::OfferRejected {
                pipe_id,
                code,
                message,
            } => Self::send_actions(self.offer_rejected(session_id, pipe_id, code, message)?),
            Frame::Data { pipe_id, payload } => {
                Self::send_actions(self.data(session_id, pipe_id, payload)?)
            }
            Frame::Fin { pipe_id } => Self::send_actions(self.fin(session_id, pipe_id)?),
            Frame::Close { pipe_id } => Self::send_actions(self.close(session_id, pipe_id)?),
            Frame::Reset {
                pipe_id,
                code,
                message,
            } => Self::send_actions(self.reset(session_id, pipe_id, code, message)?),
            Frame::Cancel { pipe_id } => Self::send_actions(self.cancel(session_id, pipe_id)?),
            Frame::Ping { nonce } => self
                .to(session_id, Frame::Pong { nonce })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            Frame::Pong { .. } => Vec::new(),
            Frame::Hello { .. }
            | Frame::Welcome { .. }
            | Frame::Registered { .. }
            | Frame::RegisterFailed { .. }
            | Frame::Unregistered { .. }
            | Frame::Offer { .. }
            | Frame::Opened { .. }
            | Frame::OpenFailed { .. } => Vec::new(),
        };
        Ok(actions)
    }

    pub(crate) fn snapshot(&self) -> GatewaySnapshot {
        let mut snapshot = GatewaySnapshot::from_parts(
            self.sessions.values().map(|session| session.role),
            self.registry.binding_count(),
            self.pending_offer_count,
            self.live_pipe_count,
        );
        snapshot.remote_open_attempts = self.remote_open_attempts.len();
        snapshot
    }

    #[cfg(test)]
    fn pipe_count(&self) -> usize {
        self.pipes.len()
    }

    fn pending_offer_count(&self) -> usize {
        self.pending_offer_count
    }

    fn live_pipe_count(&self) -> usize {
        self.live_pipe_count
    }

    #[cfg(test)]
    fn remote_open_attempt_count(&self) -> usize {
        self.remote_open_attempts.len()
    }

    #[cfg(test)]
    fn active_peer_open_count(&self) -> usize {
        self.active_peer_opens.len()
    }

    fn insert_offer(&mut self, pipe_id: PipeId, pipe: PipeEntry) {
        debug_assert_eq!(pipe.phase, PipePhase::Offered);
        self.index_peer_pipe(pipe_id, &pipe);
        let previous = self.pipes.insert(pipe_id, pipe);
        debug_assert!(previous.is_none());
        self.pending_offer_count += 1;
    }

    fn remove_pipe(&mut self, pipe_id: PipeId) -> Option<PipeEntry> {
        let pipe = self.pipes.remove(&pipe_id)?;
        if let Some(peer_key) = pipe.peer_key() {
            self.peer_pipes.remove(&peer_key);
        }
        if let Some(open_identity) = pipe.open_identity {
            self.active_peer_opens.remove(&open_identity);
        }
        match pipe.phase {
            PipePhase::Offered => self.pending_offer_count -= 1,
            PipePhase::Open => self.live_pipe_count -= 1,
        }
        Some(pipe)
    }

    fn promote_offer(&mut self, pipe_id: PipeId) -> Option<&mut PipeEntry> {
        let pipe = self.pipes.get_mut(&pipe_id)?;
        if pipe.phase != PipePhase::Offered {
            return None;
        }
        pipe.phase = PipePhase::Open;
        self.pending_offer_count -= 1;
        self.live_pipe_count += 1;
        Some(pipe)
    }

    fn insert_open(&mut self, pipe_id: PipeId, pipe: PipeEntry) {
        debug_assert_eq!(pipe.phase, PipePhase::Open);
        self.index_peer_pipe(pipe_id, &pipe);
        let previous = self.pipes.insert(pipe_id, pipe);
        debug_assert!(previous.is_none());
        self.live_pipe_count += 1;
    }

    fn index_peer_pipe(&mut self, pipe_id: PipeId, pipe: &PipeEntry) {
        let Some(peer_key) = pipe.peer_key() else {
            return;
        };
        let previous = self.peer_pipes.insert(peer_key, pipe_id);
        debug_assert!(previous.is_none());
        if let Some(open_identity) = pipe.open_identity {
            let previous = self.active_peer_opens.insert(open_identity, peer_key);
            debug_assert!(previous.is_none());
        }
    }

    #[cfg(test)]
    fn connection_high_watermark(&self, session_id: SessionId) -> Option<u64> {
        self.sessions
            .get(&session_id)
            .and_then(|session| session.highest_connection_id)
    }

    fn send_actions<T>(deliveries: Vec<T>) -> Vec<GatewayAction>
    where
        T: Into<GatewayAction>,
    {
        deliveries.into_iter().map(Into::into).collect()
    }

    fn registration_publication(&self, session_id: SessionId) -> GatewayAction {
        GatewayAction::PublishRegistration {
            session_id,
            bindings: self.registry.bindings_for_session(session_id),
        }
    }
}

fn frame_allowed(role: SessionRole, frame: &Frame) -> bool {
    matches!(frame, Frame::Ping { .. } | Frame::Pong { .. })
        || matches!(
            (role, frame),
            (
                SessionRole::Connector,
                Frame::Open { .. }
                    | Frame::Data { .. }
                    | Frame::Fin { .. }
                    | Frame::Close { .. }
                    | Frame::Reset { .. }
                    | Frame::Cancel { .. }
            ) | (
                SessionRole::Listener,
                Frame::Register { .. }
                    | Frame::Unregister { .. }
                    | Frame::OfferAccepted { .. }
                    | Frame::OfferRejected { .. }
                    | Frame::Data { .. }
                    | Frame::Fin { .. }
                    | Frame::Close { .. }
                    | Frame::Reset { .. }
            )
        )
}

fn frame_name(frame: &Frame) -> &'static str {
    match frame {
        Frame::Hello { .. } => "HELLO",
        Frame::Welcome { .. } => "WELCOME",
        Frame::Register { .. } => "REGISTER",
        Frame::Registered { .. } => "REGISTERED",
        Frame::RegisterFailed { .. } => "REGISTER_FAILED",
        Frame::Unregister { .. } => "UNREGISTER",
        Frame::Unregistered { .. } => "UNREGISTERED",
        Frame::Open { .. } => "OPEN",
        Frame::Offer { .. } => "OFFER",
        Frame::OfferAccepted { .. } => "OFFER_ACCEPTED",
        Frame::OfferRejected { .. } => "OFFER_REJECTED",
        Frame::Opened { .. } => "OPENED",
        Frame::OpenFailed { .. } => "OPEN_FAILED",
        Frame::Data { .. } => "DATA",
        Frame::Fin { .. } => "FIN",
        Frame::Close { .. } => "CLOSE",
        Frame::Reset { .. } => "RESET",
        Frame::Ping { .. } => "PING",
        Frame::Pong { .. } => "PONG",
        Frame::Cancel { .. } => "CANCEL",
    }
}
