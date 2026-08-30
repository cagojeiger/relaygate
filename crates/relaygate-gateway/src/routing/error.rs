use relaygate_route_table::RouteTableError;
use relaygate_route_table_transport::TransportError;

/// Gateway-local failure at the RouteTable orchestration boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RoutingError {
    #[error("invalid routing configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid routing projection: {0}")]
    InvalidProjection(String),
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

impl RoutingError {
    /// Maps a request-local Resolve failure onto the SDK-facing stable error
    /// taxonomy. Resolve failures happen before peer OPEN commit, so the
    /// caller assigns `NOT_OBSERVED` independently of this reason code.
    pub(crate) const fn open_error_code(&self) -> relaygate_protocol::ErrorCode {
        use relaygate_protocol::ErrorCode as SdkCode;
        use relaygate_route_table_transport::ErrorCode as RouteCode;

        match self {
            Self::InvalidConfig(_) | Self::InvalidProjection(_) | Self::WorkerFailed(_) => {
                SdkCode::Internal
            }
            Self::ShardUnavailable { .. } | Self::WorkerStopped { .. } => SdkCode::Unavailable,
            Self::Transport(error) => match error.code() {
                RouteCode::InvalidArgument => SdkCode::InvalidArgument,
                RouteCode::Unauthenticated => SdkCode::Unauthenticated,
                RouteCode::PermissionDenied => SdkCode::PermissionDenied,
                RouteCode::NotFound => SdkCode::NotFound,
                RouteCode::FailedPrecondition => SdkCode::FailedPrecondition,
                RouteCode::Unavailable => SdkCode::Unavailable,
                RouteCode::DeadlineExceeded => SdkCode::DeadlineExceeded,
                RouteCode::ResourceExhausted => SdkCode::ResourceExhausted,
                RouteCode::ProtocolError => SdkCode::ProtocolError,
                RouteCode::Internal => SdkCode::Internal,
            },
        }
    }
}
