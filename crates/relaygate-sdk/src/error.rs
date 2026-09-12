use relaygate_protocol::{ErrorCode as WireErrorCode, PeerObservation as WirePeerObservation};

/// Stable SDK failure reason. This type deliberately does not expose the wire
/// protocol enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// An input or configuration value was invalid.
    InvalidArgument,
    /// The supplied operation credential could not authenticate the caller.
    Unauthenticated,
    /// The authenticated caller is not authorized for the operation.
    PermissionDenied,
    /// The requested destination or resource was not found.
    NotFound,
    /// The operation conflicts with the current state.
    FailedPrecondition,
    /// A required session or service is temporarily unavailable.
    Unavailable,
    /// The operation did not finish before its deadline.
    DeadlineExceeded,
    /// A bounded local or Gateway resource was exhausted.
    ResourceExhausted,
    /// The operation or owning runtime was cancelled.
    Cancelled,
    /// A peer violated the RelayGate wire contract.
    ProtocolError,
    /// RelayGate encountered an internal failure.
    Internal,
    /// The requested registration already exists.
    AlreadyExists,
}

/// Peer observation for a connection or registration control operation.
///
/// On Pipe I/O errors this metadata is diagnostic only: it does not describe
/// payload delivery or revoke the fact that the Pipe was already established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerObservation {
    /// The operation was not committed to the peer.
    NotObserved,
    /// The SDK cannot determine whether the peer observed the operation.
    MaybeObserved,
    /// The peer observed and answered the operation.
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

/// SDK result type using [`Error`] for terminal operation failures.
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

    /// Returns the stable SDK failure category.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns control-operation observation metadata, not a payload receipt.
    /// Do not use this value to decide whether to replay failed Pipe I/O.
    #[must_use]
    pub const fn observation(&self) -> PeerObservation {
        self.observation
    }

    /// Returns a human-readable diagnostic message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Classifies transient, not-observed errors for a new connection or
    /// registration control operation. The caller still decides whether to
    /// start it; the SDK does not replay the failed operation.
    ///
    /// Do not apply this hint to Pipe I/O errors, including errors recovered
    /// from Tokio I/O adapters. It can be `true` after payload was exchanged:
    /// neither that value nor `false` determines delivery or safe payload retry.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.observation, PeerObservation::NotObserved)
            && matches!(
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
    pub(crate) const fn metric_name(self) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::{Error, ErrorCode, PeerObservation};

    #[test]
    fn retryable_requires_a_transient_not_observed_failure() {
        for code in [
            ErrorCode::Unavailable,
            ErrorCode::DeadlineExceeded,
            ErrorCode::ResourceExhausted,
        ] {
            let not_observed = Error::new(code, PeerObservation::NotObserved, "transient");
            let maybe_observed = Error::new(code, PeerObservation::MaybeObserved, "uncertain");
            let observed = Error::new(code, PeerObservation::Observed, "observed");
            assert!(not_observed.is_retryable());
            assert!(!maybe_observed.is_retryable());
            assert!(!observed.is_retryable());
        }

        for code in [ErrorCode::InvalidArgument, ErrorCode::PermissionDenied] {
            assert!(
                !Error::new(code, PeerObservation::NotObserved, "terminal").is_retryable(),
                "{code:?} must not produce a retry hint"
            );
        }
    }
}
