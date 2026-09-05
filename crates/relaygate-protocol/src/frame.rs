use bytes::Bytes;

use crate::{BindingId, ClusterToken, DestinationId, PipeId, SessionId};

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
    ResourceExhausted = 8,
    Cancelled = 9,
    ProtocolError = 10,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PeerObservation {
    NotObserved = 1,
    MaybeObserved = 2,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Hello {
        cluster_token: ClusterToken,
    },
    Welcome {
        session_id: SessionId,
    },
    SessionRejected {
        code: ErrorCode,
        message: String,
    },
    Publish {
        request_id: u64,
        destination_id: DestinationId,
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
        destination_id: DestinationId,
    },
    Offer {
        pipe_id: PipeId,
        binding_id: BindingId,
        destination_id: DestinationId,
    },
    OfferAccepted {
        pipe_id: PipeId,
    },
    OfferRejected {
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    },
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
    Fin {
        pipe_id: PipeId,
    },
    Close {
        pipe_id: PipeId,
    },
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
    Cancel {
        pipe_id: PipeId,
    },
}
