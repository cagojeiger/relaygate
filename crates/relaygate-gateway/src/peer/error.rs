use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerError {
    InvalidArgument(&'static str),
    AlreadyExists(&'static str),
    FailedPrecondition(&'static str),
    Protocol(&'static str),
    ResourceExhausted(&'static str),
}

impl fmt::Display for PeerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (code, message) = match self {
            Self::InvalidArgument(message) => ("INVALID_ARGUMENT", message),
            Self::AlreadyExists(message) => ("ALREADY_EXISTS", message),
            Self::FailedPrecondition(message) => ("FAILED_PRECONDITION", message),
            Self::Protocol(message) => ("PROTOCOL_ERROR", message),
            Self::ResourceExhausted(message) => ("RESOURCE_EXHAUSTED", message),
        };
        write!(formatter, "{code}: {message}")
    }
}

impl std::error::Error for PeerError {}
