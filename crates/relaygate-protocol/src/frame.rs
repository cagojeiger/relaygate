use std::fmt;

use bytes::Bytes;

use crate::{BearerToken, BindingId, Destination, PipeId, SessionId};

/// Stable wire error codes whose discriminants are part of protocol version 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    /// A request field is malformed or violates the contract.
    InvalidArgument = 1,
    /// The operation credential is missing, expired, or unverifiable.
    Unauthenticated = 2,
    /// The verified credential does not authorize the operation.
    PermissionDenied = 3,
    /// No Binding or Pipe matches the requested Destination or identifier.
    NotFound = 4,
    /// The current state does not allow the operation.
    FailedPrecondition = 5,
    /// The serving side cannot accept the operation at this time.
    Unavailable = 6,
    /// The operation did not reach a terminal result within its deadline.
    DeadlineExceeded = 7,
    /// Covers admission, capacity, queue, and size limits.
    ResourceExhausted = 8,
    /// The initiator abandoned the operation before it completed.
    Cancelled = 9,
    /// A peer violated the wire protocol or state-machine contract.
    ProtocolError = 10,
    /// A local invariant or task failed without a peer protocol violation.
    Internal = 11,
    /// The requested registration already exists.
    AlreadyExists = 12,
}

impl ErrorCode {
    /// Canonical snake_case name for metric labels and structured logs.
    #[must_use]
    pub const fn metric_name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::Unauthenticated => "unauthenticated",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::FailedPrecondition => "failed_precondition",
            Self::Unavailable => "unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Cancelled => "cancelled",
            Self::ProtocolError => "protocol_error",
            Self::Internal => "internal",
            Self::AlreadyExists => "already_exists",
        }
    }

    /// Decodes the wire byte; `None` for values outside the contract.
    #[must_use]
    pub fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::InvalidArgument),
            2 => Some(Self::Unauthenticated),
            3 => Some(Self::PermissionDenied),
            4 => Some(Self::NotFound),
            5 => Some(Self::FailedPrecondition),
            6 => Some(Self::Unavailable),
            7 => Some(Self::DeadlineExceeded),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::Cancelled),
            10 => Some(Self::ProtocolError),
            11 => Some(Self::Internal),
            12 => Some(Self::AlreadyExists),
            _ => None,
        }
    }
}

/// Whether a peer may have observed a failed control operation.
///
/// This is control-operation metadata, not an application payload receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PeerObservation {
    /// The operation did not reach the peer's observable state.
    NotObserved = 1,
    /// The sender cannot determine whether the peer observed the operation.
    MaybeObserved = 2,
    /// The peer observed or committed the operation before failure.
    Observed = 3,
}

impl PeerObservation {
    /// Decodes the wire byte; `None` for values outside the contract.
    #[must_use]
    pub fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::NotObserved),
            2 => Some(Self::MaybeObserved),
            3 => Some(Self::Observed),
            _ => None,
        }
    }
}

/// Wire messages exchanged within one SDK–Gateway session.
///
/// Credentials are operation-scoped and appear only in [`Frame::Publish`] and
/// [`Frame::Dial`]; application data remains opaque in [`Frame::Data`].
#[derive(Clone, PartialEq, Eq)]
pub enum Frame {
    /// SDK → Gateway: starts a session and carries no credential.
    Hello,
    /// Gateway → SDK: establishes the session and assigns its identifier.
    Welcome {
        /// Identifies the session incarnation the Gateway just established.
        session_id: SessionId,
    },
    /// Gateway → SDK: refuses the session before it is established.
    SessionRejected {
        /// Names the reason the session was refused.
        code: ErrorCode,
        /// Human-readable detail for the code.
        message: String,
    },
    /// SDK → Gateway: registers a Destination for this session.
    Publish {
        /// Correlates the reply with the request that carried the same id.
        request_id: u64,
        /// The exact routing address being registered.
        destination: Destination,
        /// Operation credential the Gateway verifies for this registration.
        access_token: BearerToken,
    },
    /// Gateway → SDK: reports that the registration succeeded.
    Published {
        /// Correlates the reply with the request that carried the same id.
        request_id: u64,
        /// Identifies the Binding the registration created.
        binding_id: BindingId,
    },
    /// Gateway → SDK: reports that the registration failed.
    PublishFailed {
        /// Correlates the reply with the request that carried the same id.
        request_id: u64,
        /// Names the reason the registration failed.
        code: ErrorCode,
        /// Human-readable detail for the code.
        message: String,
    },
    /// SDK → Gateway: releases a Binding this session owns.
    Unpublish {
        /// Correlates the reply with the request that carried the same id.
        request_id: u64,
        /// Identifies the Binding to release.
        binding_id: BindingId,
    },
    /// Gateway → SDK: confirms that the Binding is released.
    Unpublished {
        /// Correlates the reply with the request that carried the same id.
        request_id: u64,
    },
    /// SDK → Gateway: requests a Pipe to a Destination.
    Dial {
        /// Session-local id that correlates the dial with its terminal result.
        connection_id: u64,
        /// The exact routing address the dial targets.
        destination: Destination,
        /// Operation credential the Gateway verifies for this dial.
        access_token: BearerToken,
    },
    /// Offers an incoming Pipe to the session that owns a selected binding.
    Offer {
        /// Identifies the Pipe being offered.
        pipe_id: PipeId,
        /// Identifies the Binding the Gateway selected for the dial.
        binding_id: BindingId,
        /// The routing address the dial targeted.
        destination: Destination,
    },
    /// SDK → Gateway: the Listener admitted the offered Pipe to its queue.
    OfferAccepted {
        /// Identifies the offered Pipe.
        pipe_id: PipeId,
    },
    /// SDK → Gateway: the Listener did not admit the offered Pipe.
    OfferRejected {
        /// Identifies the offered Pipe.
        pipe_id: PipeId,
        /// Names the reason the offer was rejected.
        code: ErrorCode,
        /// Human-readable detail for the code.
        message: String,
    },
    /// Confirms that a Pipe is established for the dialing session.
    Opened {
        /// Identifies the established Pipe, carrying the origin session id and
        /// the connection id of the dial that created it.
        pipe_id: PipeId,
    },
    /// Gateway → SDK: ends a dial attempt without a Pipe.
    DialFailed {
        /// Correlates the failure with the dial that carried the same id.
        connection_id: u64,
        /// Names the reason the dial failed.
        code: ErrorCode,
        /// States whether the selected Listener may have observed the dial.
        observation: PeerObservation,
        /// Human-readable detail for the code.
        message: String,
    },
    /// Carries opaque application bytes over an established Pipe.
    Data {
        /// Identifies the Pipe the bytes belong to.
        pipe_id: PipeId,
        /// Application bytes RelayGate relays without interpreting them.
        payload: Bytes,
    },
    /// Half-closes the sender's write direction of a Pipe.
    Fin {
        /// Identifies the Pipe being half-closed.
        pipe_id: PipeId,
    },
    /// Closes a Pipe normally in both directions.
    Close {
        /// Identifies the Pipe being closed.
        pipe_id: PipeId,
    },
    /// Terminates a Pipe with an error.
    Reset {
        /// Identifies the Pipe being terminated.
        pipe_id: PipeId,
        /// Names the reason the Pipe was terminated.
        code: ErrorCode,
        /// Human-readable detail for the code.
        message: String,
    },
    /// Probes session liveness and expects a matching [`Frame::Pong`].
    Ping {
        /// Value the peer echoes in its reply.
        nonce: u64,
    },
    /// Answers a liveness probe.
    Pong {
        /// Echoes the nonce of the probe being answered.
        nonce: u64,
    },
    /// Cancels a pending Pipe establishment attempt.
    Cancel {
        /// Identifies the pending Pipe to release.
        pipe_id: PipeId,
    },
}

impl fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hello => formatter.write_str("Hello"),
            Self::Welcome { session_id } => formatter
                .debug_struct("Welcome")
                .field("session_id", session_id)
                .finish(),
            Self::SessionRejected { code, message } => formatter
                .debug_struct("SessionRejected")
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Publish {
                request_id,
                destination,
                access_token,
            } => formatter
                .debug_struct("Publish")
                .field("request_id", request_id)
                .field("destination", destination)
                .field("access_token", access_token)
                .finish(),
            Self::Published {
                request_id,
                binding_id,
            } => formatter
                .debug_struct("Published")
                .field("request_id", request_id)
                .field("binding_id", binding_id)
                .finish(),
            Self::PublishFailed {
                request_id,
                code,
                message,
            } => formatter
                .debug_struct("PublishFailed")
                .field("request_id", request_id)
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Unpublish {
                request_id,
                binding_id,
            } => formatter
                .debug_struct("Unpublish")
                .field("request_id", request_id)
                .field("binding_id", binding_id)
                .finish(),
            Self::Unpublished { request_id } => formatter
                .debug_struct("Unpublished")
                .field("request_id", request_id)
                .finish(),
            Self::Dial {
                connection_id,
                destination,
                access_token,
            } => formatter
                .debug_struct("Dial")
                .field("connection_id", connection_id)
                .field("destination", destination)
                .field("access_token", access_token)
                .finish(),
            Self::Offer {
                pipe_id,
                binding_id,
                destination,
            } => formatter
                .debug_struct("Offer")
                .field("pipe_id", pipe_id)
                .field("binding_id", binding_id)
                .field("destination", destination)
                .finish(),
            Self::OfferAccepted { pipe_id } => formatter
                .debug_struct("OfferAccepted")
                .field("pipe_id", pipe_id)
                .finish(),
            Self::OfferRejected {
                pipe_id,
                code,
                message,
            } => formatter
                .debug_struct("OfferRejected")
                .field("pipe_id", pipe_id)
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Opened { pipe_id } => formatter
                .debug_struct("Opened")
                .field("pipe_id", pipe_id)
                .finish(),
            Self::DialFailed {
                connection_id,
                code,
                observation,
                message,
            } => formatter
                .debug_struct("DialFailed")
                .field("connection_id", connection_id)
                .field("code", code)
                .field("observation", observation)
                .field("message", message)
                .finish(),
            Self::Data { pipe_id, payload } => formatter
                .debug_struct("Data")
                .field("pipe_id", pipe_id)
                .field("payload_len", &payload.len())
                .finish(),
            Self::Fin { pipe_id } => formatter
                .debug_struct("Fin")
                .field("pipe_id", pipe_id)
                .finish(),
            Self::Close { pipe_id } => formatter
                .debug_struct("Close")
                .field("pipe_id", pipe_id)
                .finish(),
            Self::Reset {
                pipe_id,
                code,
                message,
            } => formatter
                .debug_struct("Reset")
                .field("pipe_id", pipe_id)
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Ping { nonce } => formatter
                .debug_struct("Ping")
                .field("nonce", nonce)
                .finish(),
            Self::Pong { nonce } => formatter
                .debug_struct("Pong")
                .field("nonce", nonce)
                .finish(),
            Self::Cancel { pipe_id } => formatter
                .debug_struct("Cancel")
                .field("pipe_id", pipe_id)
                .finish(),
        }
    }
}
