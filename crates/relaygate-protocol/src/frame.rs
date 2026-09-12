use bytes::Bytes;

use crate::{BearerToken, BindingId, Destination, PipeId, SessionId};

/// Stable failure codes carried by protocol error frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    /// An argument or frame field is invalid.
    InvalidArgument = 1,
    /// The operation credential could not be authenticated.
    Unauthenticated = 2,
    /// The authenticated credential does not authorize the operation.
    PermissionDenied = 3,
    /// The requested current resource does not exist.
    NotFound = 4,
    /// A required operation precondition is not satisfied.
    FailedPrecondition = 5,
    /// A required transport or dependency is unavailable.
    Unavailable = 6,
    /// The operation did not complete before its deadline.
    DeadlineExceeded = 7,
    /// An admission, capacity, queue, or size limit was exceeded.
    ResourceExhausted = 8,
    /// The operation was cancelled by its owner.
    Cancelled = 9,
    /// A peer violated the wire protocol or state-machine contract.
    ProtocolError = 10,
    /// An internal invariant or task failed.
    Internal = 11,
    /// The requested resource already exists.
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

/// One SDK–Gateway protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Starts a credential-free SDK session handshake.
    Hello,
    /// Accepts the handshake and assigns a session incarnation.
    Welcome {
        /// Identifier assigned to this transport-session incarnation.
        session_id: SessionId,
    },
    /// Rejects the session handshake.
    SessionRejected {
        /// Stable rejection reason.
        code: ErrorCode,
        /// Human-readable diagnostic message.
        message: String,
    },
    /// Requests a live route binding for an exact destination.
    Publish {
        /// Session-local identifier used to correlate the response.
        request_id: u64,
        /// Exact destination to bind.
        destination: Destination,
        /// Operation-scoped authorization credential.
        access_token: BearerToken,
    },
    /// Confirms that a publish request created a binding.
    Published {
        /// Identifier of the corresponding publish request.
        request_id: u64,
        /// Identifier assigned to the live binding.
        binding_id: BindingId,
    },
    /// Rejects a publish request.
    PublishFailed {
        /// Identifier of the corresponding publish request.
        request_id: u64,
        /// Stable failure reason.
        code: ErrorCode,
        /// Human-readable diagnostic message.
        message: String,
    },
    /// Requests removal of a live binding.
    Unpublish {
        /// Session-local identifier used to correlate the response.
        request_id: u64,
        /// Binding to remove.
        binding_id: BindingId,
    },
    /// Confirms completion of an unpublish request.
    Unpublished {
        /// Identifier of the corresponding unpublish request.
        request_id: u64,
    },
    /// Requests a new Pipe to an exact destination.
    Dial {
        /// Session-local connection identifier for the attempted Pipe.
        connection_id: u64,
        /// Exact destination to resolve.
        destination: Destination,
        /// Operation-scoped authorization credential.
        access_token: BearerToken,
    },
    /// Offers an incoming Pipe to the session that owns a selected binding.
    Offer {
        /// Cluster-unique identifier of the offered Pipe.
        pipe_id: PipeId,
        /// Selected binding that receives the offer.
        binding_id: BindingId,
        /// Exact destination associated with the binding.
        destination: Destination,
    },
    /// Accepts an offered Pipe.
    OfferAccepted {
        /// Identifier of the offered Pipe.
        pipe_id: PipeId,
    },
    /// Rejects an offered Pipe.
    OfferRejected {
        /// Identifier of the offered Pipe.
        pipe_id: PipeId,
        /// Stable rejection reason.
        code: ErrorCode,
        /// Human-readable diagnostic message.
        message: String,
    },
    /// Confirms that a Pipe is established for the dialing session.
    Opened {
        /// Identifier of the established Pipe.
        pipe_id: PipeId,
    },
    /// Reports that a dial attempt failed.
    DialFailed {
        /// Session-local identifier of the corresponding dial attempt.
        connection_id: u64,
        /// Stable failure reason.
        code: ErrorCode,
        /// Whether the selected peer may have observed the attempt.
        observation: PeerObservation,
        /// Human-readable diagnostic message.
        message: String,
    },
    /// Carries opaque application bytes on an established Pipe.
    Data {
        /// Pipe that owns the payload.
        pipe_id: PipeId,
        /// Opaque application payload.
        payload: Bytes,
    },
    /// Half-closes the sender's write direction of a Pipe.
    Fin {
        /// Pipe whose write direction is finished.
        pipe_id: PipeId,
    },
    /// Closes a Pipe normally in both directions.
    Close {
        /// Pipe to close.
        pipe_id: PipeId,
    },
    /// Terminates a Pipe with an error.
    Reset {
        /// Pipe to terminate.
        pipe_id: PipeId,
        /// Stable termination reason.
        code: ErrorCode,
        /// Human-readable diagnostic message.
        message: String,
    },
    /// Requests a liveness response carrying the same nonce.
    Ping {
        /// Opaque value used to correlate the response.
        nonce: u64,
    },
    /// Responds to a liveness request.
    Pong {
        /// Nonce copied from the corresponding [`Frame::Ping`].
        nonce: u64,
    },
    /// Cancels a pending Pipe establishment attempt.
    Cancel {
        /// Identifier of the pending Pipe.
        pipe_id: PipeId,
    },
}
