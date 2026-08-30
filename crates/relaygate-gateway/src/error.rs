#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid Gateway configuration: {0}")]
    InvalidConfig(String),
    #[error("Gateway I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Gateway protocol failed: {0}")]
    Protocol(#[from] relaygate_protocol::ProtocolError),
    #[error("Gateway RouteTable orchestration failed: {0}")]
    Routing(String),
    #[error("Gateway peer relay failed: {0}")]
    Peer(String),
    #[error("Gateway health check timed out")]
    HealthCheckTimeout,
    #[error("Gateway health check received an unexpected frame")]
    UnexpectedHealthResponse,
}

impl From<crate::routing::RoutingError> for GatewayError {
    fn from(error: crate::routing::RoutingError) -> Self {
        Self::Routing(error.to_string())
    }
}
