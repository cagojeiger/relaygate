use relaygate_route_table::RouteTableError;
use relaygate_route_table_transport::TransportError;

/// Gateway-local failure at the RouteTable orchestration boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RoutingError {
    #[error("invalid routing configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid routing projection: {0}")]
    InvalidProjection(String),
    #[allow(
        dead_code,
        reason = "request-local Resolve is retained for G005 remote OPEN integration"
    )]
    #[error("RouteTable shard {shard_id} is unavailable")]
    ShardUnavailable { shard_id: String },
    #[error("RouteTable shard {shard_id} worker stopped")]
    WorkerStopped { shard_id: String },
    #[error("RouteTable operation failed: {0}")]
    Transport(#[from] TransportError),
    #[error("RouteTable worker failed: {0}")]
    WorkerFailed(String),
}

impl From<RouteTableError> for RoutingError {
    fn from(error: RouteTableError) -> Self {
        Self::InvalidProjection(error.to_string())
    }
}
