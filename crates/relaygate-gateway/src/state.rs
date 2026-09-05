use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId};
use relaygate_route_table::{DestinationId as RouteDestinationId, GatewayId, GatewayLocator};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    GatewaySnapshot,
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
            metrics::counter!(
                "relaygate_gateway_writer_queue_rejections_total",
                "reason" => queue_state
            )
            .increment(1);
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
        destination_id: RouteDestinationId,
    },
    OpenPeer {
        open_identity: OpenIdentity,
        gateway_id: GatewayId,
        gateway_locator: GatewayLocator,
        destination_id: String,
        relay_session_id: SessionId,
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
    #[error("session {sender:?} does not own existing Pipe {pipe_id:?} for frame {frame_name}")]
    PipeOwnership {
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    },
}

#[derive(Debug, Clone)]
struct SessionEntry {
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
    open_started_at: Option<Instant>,
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
    destination_id: String,
    started_at: Instant,
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
    sessions: HashMap<SessionId, SessionEntry>,
    registry: LocalRegistry,
    pipes: HashMap<PipeId, PipeEntry>,
    peer_pipes: HashMap<PeerStreamKey, PipeId>,
    active_peer_opens: HashMap<OpenIdentity, PeerStreamKey>,
    remote_open_attempts: HashMap<OpenIdentity, RemoteOpenAttempt>,
    pending_offer_count: usize,
    live_pipe_count: usize,
    draining: bool,
    gateway_id: Option<GatewayId>,
    limits: GatewayLimits,
}

impl GatewayState {
    pub(crate) fn new(limits: GatewayLimits) -> Self {
        Self::build(limits, None)
    }

    pub(crate) fn new_distributed(limits: GatewayLimits, gateway_id: GatewayId) -> Self {
        Self::build(limits, Some(gateway_id))
    }

    fn build(limits: GatewayLimits, gateway_id: Option<GatewayId>) -> Self {
        Self {
            sessions: HashMap::new(),
            registry: LocalRegistry::default(),
            pipes: HashMap::new(),
            peer_pipes: HashMap::new(),
            active_peer_opens: HashMap::new(),
            remote_open_attempts: HashMap::new(),
            pending_offer_count: 0,
            live_pipe_count: 0,
            draining: false,
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
        if !self.sessions.contains_key(&session_id) {
            return Ok(Vec::new());
        }

        let actions = match frame {
            Frame::Publish {
                request_id,
                destination_id,
            } => self.publish(session_id, request_id, destination_id),
            Frame::Unpublish {
                request_id,
                binding_id,
            } => self.unpublish(session_id, request_id, binding_id),
            Frame::Dial {
                connection_id,
                destination_id,
            } => self.dial(session_id, connection_id, destination_id, now),
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
            | Frame::SessionRejected { .. }
            | Frame::Published { .. }
            | Frame::PublishFailed { .. }
            | Frame::Unpublished { .. }
            | Frame::Offer { .. }
            | Frame::Opened { .. }
            | Frame::DialFailed { .. } => Vec::new(),
        };
        Ok(actions)
    }

    pub(crate) fn snapshot(&self) -> GatewaySnapshot {
        let mut snapshot = GatewaySnapshot::from_parts(
            self.sessions.len(),
            self.registry.binding_count(),
            self.pending_offer_count,
            self.live_pipe_count,
            self.draining,
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

    pub(crate) fn begin_draining(&mut self) -> Vec<GatewayAction> {
        if self.draining {
            return Vec::new();
        }
        self.draining = true;
        self.sessions
            .keys()
            .map(|session_id| GatewayAction::PublishRegistration {
                session_id: *session_id,
                bindings: Vec::new(),
            })
            .collect()
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.pending_offer_count == 0
            && self.live_pipe_count == 0
            && self.remote_open_attempts.is_empty()
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

fn observe_dial_request() {
    metrics::counter!("relaygate_gateway_dial_requests_total").increment(1);
}

fn observe_dial_result(started_at: Option<Instant>, code: Option<ErrorCode>) {
    let Some(started_at) = started_at else {
        return;
    };
    let (outcome, code) = match code {
        None => ("success", "ok"),
        Some(ErrorCode::Cancelled) => ("cancelled", "cancelled"),
        Some(code) => ("error", error_code_name(code)),
    };
    metrics::counter!(
        "relaygate_gateway_dial_results_total",
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
    metrics::histogram!(
        "relaygate_gateway_dial_duration_seconds",
        "outcome" => outcome
    )
    .record(started_at.elapsed().as_secs_f64());
}

pub(super) const fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidArgument => "invalid_argument",
        ErrorCode::Unauthenticated => "unauthenticated",
        ErrorCode::PermissionDenied => "permission_denied",
        ErrorCode::NotFound => "not_found",
        ErrorCode::FailedPrecondition => "failed_precondition",
        ErrorCode::Unavailable => "unavailable",
        ErrorCode::DeadlineExceeded => "deadline_exceeded",
        ErrorCode::ResourceExhausted => "resource_exhausted",
        ErrorCode::Cancelled => "cancelled",
        ErrorCode::ProtocolError => "protocol_error",
        ErrorCode::Internal => "internal",
        ErrorCode::AlreadyExists => "already_exists",
    }
}
