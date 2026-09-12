use bytes::Bytes;

use crate::{BearerToken, BindingId, Destination, PipeId, SessionId};

/// Stable wire error codes whose discriminants are part of protocol version 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    InvalidArgument = 1,
    Unauthenticated = 2,
    PermissionDenied = 3,
    NotFound = 4,
    FailedPrecondition = 5,
    Unavailable = 6,
    DeadlineExceeded = 7,
    /// Covers admission, capacity, queue, and size limits.
    ResourceExhausted = 8,
    Cancelled = 9,
    /// A peer violated the wire protocol or state-machine contract.
    ProtocolError = 10,
    /// A local invariant or task failed without a peer protocol violation.
    Internal = 11,
    AlreadyExists = 12,
}

impl ErrorCode {
    pub(crate) fn from_wire(value: u8) -> Option<Self> {
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
    pub(crate) fn from_wire(value: u8) -> Option<Self> {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Hello,
    Welcome {
        session_id: SessionId,
    },
    SessionRejected {
        code: ErrorCode,
        message: String,
    },
    Publish {
        request_id: u64,
        destination: Destination,
        access_token: BearerToken,
    },
    Published {
        request_id: u64,
        binding_id: BindingId,
    },
    PublishFailed {
        request_id: u64,
        code: ErrorCode,
        message: String,
    },
    Unpublish {
        request_id: u64,
        binding_id: BindingId,
    },
    Unpublished {
        request_id: u64,
    },
    Dial {
        connection_id: u64,
        destination: Destination,
        access_token: BearerToken,
    },
    /// Offers an incoming Pipe to the session that owns a selected binding.
    Offer {
        pipe_id: PipeId,
        binding_id: BindingId,
        destination: Destination,
    },
    OfferAccepted {
        pipe_id: PipeId,
    },
    OfferRejected {
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    },
    /// Confirms that a Pipe is established for the dialing session.
    Opened {
        pipe_id: PipeId,
    },
    DialFailed {
        connection_id: u64,
        code: ErrorCode,
        observation: PeerObservation,
        message: String,
    },
    Data {
        pipe_id: PipeId,
        payload: Bytes,
    },
    /// Half-closes the sender's write direction of a Pipe.
    Fin {
        pipe_id: PipeId,
    },
    /// Closes a Pipe normally in both directions.
    Close {
        pipe_id: PipeId,
    },
    /// Terminates a Pipe with an error.
    Reset {
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    /// Cancels a pending Pipe establishment attempt.
    Cancel {
        pipe_id: PipeId,
    },
}
