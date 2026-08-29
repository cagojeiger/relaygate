use relaygate_protocol::{ErrorCode as WireErrorCode, PeerObservation as WirePeerObservation};

/// Stable SDK failure reason. This type deliberately does not expose the wire
/// protocol enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    FailedPrecondition,
    Unavailable,
    DeadlineExceeded,
    ResourceExhausted,
    Cancelled,
    ProtocolError,
    Internal,
    AlreadyExists,
}

/// Whether a failed operation can be proven to have reached its peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerObservation {
    NotObserved,
    MaybeObserved,
    Observed,
}

/// A terminal SDK operation error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message} ({observation:?})")]
pub struct Error {
    code: ErrorCode,
    observation: PeerObservation,
    message: String,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
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
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub const fn observation(&self) -> PeerObservation {
        self.observation
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::Unavailable | ErrorCode::DeadlineExceeded | ErrorCode::ResourceExhausted
        )
    }

    pub(crate) fn closed() -> Self {
        Self::new(
            ErrorCode::Cancelled,
            PeerObservation::NotObserved,
            "SDK runtime is closed",
        )
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::Unavailable,
            PeerObservation::NotObserved,
            message,
        )
    }

    pub(crate) fn maybe_observed(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::Unavailable,
            PeerObservation::MaybeObserved,
            message,
        )
    }

    pub(crate) fn deadline(observation: PeerObservation) -> Self {
        Self::new(
            ErrorCode::DeadlineExceeded,
            observation,
            "operation deadline exceeded",
        )
    }
}

impl ErrorCode {
    pub(crate) fn from_wire(value: WireErrorCode) -> Self {
        match value {
            WireErrorCode::InvalidArgument => Self::InvalidArgument,
            WireErrorCode::Unauthenticated => Self::Unauthenticated,
            WireErrorCode::PermissionDenied => Self::PermissionDenied,
            WireErrorCode::NotFound => Self::NotFound,
            WireErrorCode::FailedPrecondition => Self::FailedPrecondition,
            WireErrorCode::Unavailable => Self::Unavailable,
            WireErrorCode::DeadlineExceeded => Self::DeadlineExceeded,
            WireErrorCode::ResourceExhausted => Self::ResourceExhausted,
            WireErrorCode::Cancelled => Self::Cancelled,
            WireErrorCode::ProtocolError => Self::ProtocolError,
            WireErrorCode::Internal => Self::Internal,
            WireErrorCode::AlreadyExists => Self::AlreadyExists,
        }
    }
}

impl PeerObservation {
    pub(crate) fn from_wire(value: WirePeerObservation) -> Self {
        match value {
            WirePeerObservation::NotObserved => Self::NotObserved,
            WirePeerObservation::MaybeObserved => Self::MaybeObserved,
            WirePeerObservation::Observed => Self::Observed,
        }
    }
}
