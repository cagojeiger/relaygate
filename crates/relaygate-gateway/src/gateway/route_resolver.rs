use futures_util::future::BoxFuture;
use relaygate_protocol::ErrorCode;
use relaygate_route_table::{BindingSet, DestinationId};

use crate::routing::{RoutingError, RoutingHandle};

/// Request-local RouteTable lookup used by the Gateway OPEN path.
///
/// Registration publication deliberately remains on `RoutingHandle`; this
/// port owns only `DestinationId -> current BindingSet` resolution.
pub(super) trait RouteResolver: Send + Sync {
    fn resolve(
        &self,
        destination_id: DestinationId,
    ) -> BoxFuture<'_, Result<BindingSet, RouteResolveFailure>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RouteResolveFailure {
    code: ErrorCode,
    message: String,
}

impl RouteResolveFailure {
    pub(super) const fn code(&self) -> ErrorCode {
        self.code
    }

    pub(super) fn message(&self) -> &str {
        &self.message
    }

    fn from_routing(error: RoutingError) -> Self {
        Self {
            code: error.open_error_code(),
            message: error.to_string(),
        }
    }
}

impl RouteResolver for RoutingHandle {
    fn resolve(
        &self,
        destination_id: DestinationId,
    ) -> BoxFuture<'_, Result<BindingSet, RouteResolveFailure>> {
        Box::pin(async move {
            RoutingHandle::resolve(self, destination_id)
                .await
                .map_err(RouteResolveFailure::from_routing)
        })
    }
}
