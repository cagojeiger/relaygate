use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};

use super::identity::{OpenIdentity, PeerOpenProgress, PeerTransportId, StreamId};

/// Exact remote Gateway incarnation and routable location selected for one
/// request-local remote OPEN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerTarget {
    gateway_id: GatewayId,
    gateway_locator: GatewayLocator,
}

impl PeerTarget {
    #[must_use]
    pub(crate) const fn new(gateway_id: GatewayId, gateway_locator: GatewayLocator) -> Self {
        Self {
            gateway_id,
            gateway_locator,
        }
    }

    #[must_use]
    pub(crate) const fn gateway_id(&self) -> GatewayId {
        self.gateway_id
    }

    #[must_use]
    pub(crate) const fn gateway_locator(&self) -> &GatewayLocator {
        &self.gateway_locator
    }
}

/// Exact OPEN payload after RouteTable selection. It contains no fallback
/// candidate and is discarded when this attempt reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerOpenRequest {
    target: PeerTarget,
    open_identity: OpenIdentity,
    destination_id: String,
    relay_session_id: SessionId,
    binding_id: BindingId,
}

impl PeerOpenRequest {
    pub(crate) fn new(
        target: PeerTarget,
        open_identity: OpenIdentity,
        destination_id: impl Into<String>,
        relay_session_id: SessionId,
        binding_id: BindingId,
    ) -> Result<Self, PeerFailure> {
        let destination_id = destination_id.into();
        if destination_id.is_empty() {
            return Err(PeerFailure::not_observed(
                ErrorCode::InvalidArgument,
                "peer OPEN DestinationId must not be empty",
            ));
        }
        Ok(Self {
            target,
            open_identity,
            destination_id,
            relay_session_id,
            binding_id,
        })
    }

    #[must_use]
    pub(crate) const fn target(&self) -> &PeerTarget {
        &self.target
    }

    #[must_use]
    pub(crate) const fn open_identity(&self) -> OpenIdentity {
        self.open_identity
    }

    #[must_use]
    pub(crate) fn destination_id(&self) -> &str {
        &self.destination_id
    }

    #[must_use]
    pub(crate) const fn relay_session_id(&self) -> SessionId {
        self.relay_session_id
    }

    #[must_use]
    pub(crate) const fn binding_id(&self) -> BindingId {
        self.binding_id
    }
}

/// Transport-local identity returned only after OPEN is committed to the
/// ordered aggregate writer queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PeerStreamKey {
    peer_gateway_id: GatewayId,
    peer_transport_id: PeerTransportId,
    stream_id: StreamId,
}

impl PeerStreamKey {
    #[must_use]
    pub(crate) const fn new(
        peer_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        stream_id: StreamId,
    ) -> Self {
        Self {
            peer_gateway_id,
            peer_transport_id,
            stream_id,
        }
    }

    #[must_use]
    pub(crate) const fn peer_gateway_id(self) -> GatewayId {
        self.peer_gateway_id
    }

    #[must_use]
    pub(crate) const fn peer_transport_id(self) -> PeerTransportId {
        self.peer_transport_id
    }

    #[must_use]
    pub(crate) const fn stream_id(self) -> StreamId {
        self.stream_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerFailure {
    code: ErrorCode,
    observation: PeerObservation,
    message: String,
}

impl PeerFailure {
    #[must_use]
    pub(crate) fn new(
        code: ErrorCode,
        observation: PeerObservation,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            observation,
            message: message.into(),
        }
    }

    #[must_use]
    pub(crate) fn not_observed(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(code, PeerObservation::NotObserved, message)
    }

    #[must_use]
    pub(crate) fn maybe_observed(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(code, PeerObservation::MaybeObserved, message)
    }

    #[must_use]
    pub(crate) const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub(crate) const fn observation(&self) -> PeerObservation {
        self.observation
    }

    #[must_use]
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub(crate) const fn metric_code(&self) -> &'static str {
        match self.code {
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
}

impl std::fmt::Display for PeerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{:?}/{:?}: {}",
            self.code, self.observation, self.message
        )
    }
}

impl std::error::Error for PeerFailure {}

/// One stream affected by a transport-scoped loss. No item authorizes replay,
/// reroute, or resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LostPeerStream {
    pub(crate) key: PeerStreamKey,
    pub(crate) open_identity: OpenIdentity,
    pub(crate) progress: PeerOpenProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerEvent {
    IncomingOpen {
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        destination_id: String,
        relay_session_id: SessionId,
        binding_id: BindingId,
    },
    Opened {
        key: PeerStreamKey,
        open_identity: OpenIdentity,
    },
    Failed {
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        failure: PeerFailure,
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
    TransportLost {
        peer_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        streams: Vec<LostPeerStream>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PeerCounts {
    pub(crate) connecting: usize,
    pub(crate) ready: usize,
    pub(crate) streams: usize,
}
