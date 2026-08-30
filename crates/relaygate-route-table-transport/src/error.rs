use std::fmt;

use relaygate_route_table::{ErrorCode as CoreErrorCode, RouteTableError};
use serde::{Deserialize, Serialize};

/// Stable error categories exposed by the private RouteTable transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    FailedPrecondition,
    Unavailable,
    DeadlineExceeded,
    ResourceExhausted,
    ProtocolError,
    Internal,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Unavailable => "UNAVAILABLE",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::ProtocolError => "PROTOCOL_ERROR",
            Self::Internal => "INTERNAL",
        })
    }
}

/// One terminal RouteTable transport failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct TransportError {
    code: ErrorCode,
    message: String,
}

impl TransportError {
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    pub(crate) fn unauthenticated() -> Self {
        Self::new(
            ErrorCode::Unauthenticated,
            "internal Gateway authentication failed",
        )
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }

    pub(crate) fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DeadlineExceeded, message)
    }

    pub(crate) fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ResourceExhausted, message)
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ProtocolError, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

impl From<RouteTableError> for TransportError {
    fn from(error: RouteTableError) -> Self {
        let code = match error.code() {
            CoreErrorCode::InvalidArgument => ErrorCode::InvalidArgument,
            CoreErrorCode::PermissionDenied => ErrorCode::PermissionDenied,
            CoreErrorCode::NotFound => ErrorCode::NotFound,
            CoreErrorCode::FailedPrecondition => ErrorCode::FailedPrecondition,
            CoreErrorCode::Internal => ErrorCode::Internal,
        };
        Self::new(code, error.to_string())
    }
}
