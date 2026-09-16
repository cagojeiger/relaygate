/// A terminal RouteTable operation error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteTableError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("authenticated Gateway does not own the requested registration")]
    PermissionDenied,
    #[error("no live binding exists for the requested Destination")]
    NotFound,
    #[error("operation is not valid for the current RouteTable state: {0}")]
    FailedPrecondition(String),
    #[error("RouteTable deadline cannot be represented")]
    DeadlineOverflow,
}

impl From<relaygate_destination::DestinationError> for RouteTableError {
    fn from(error: relaygate_destination::DestinationError) -> Self {
        Self::InvalidArgument(error.to_string())
    }
}
