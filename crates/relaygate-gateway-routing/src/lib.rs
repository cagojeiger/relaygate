//! Gateway-owned RouteTable orchestration.
//!
//! This crate projects local binding truth into shard-local soft state. It
//! deliberately does not own local bindings, cache Resolve results, or relay
//! peer payloads; the Gateway commits bindings through [`RoutingHandle`].

mod config;
mod error;
mod lifecycle;
mod projection;
mod runtime;

use relaygate_protocol::{BindingId, Destination, SessionId};

pub use config::GatewayRoutingConfig;
pub use error::RoutingError;
pub use runtime::{RoutingHandle, RoutingRuntime};

#[cfg(test)]
mod tests;

/// One live local binding as the Gateway publishes it for projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub id: BindingId,
    pub destination: Destination,
    pub session_id: SessionId,
}

/// Gateway-local summary of the RouteTable dependency's last observed state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RouteDependencyHealth {
    /// This Gateway runs without RouteTable orchestration.
    #[default]
    Disabled,
    /// Every configured shard is available and current desired registrations are synchronized.
    Ready,
    /// At least one shard is unavailable or a desired registration is not synchronized.
    Degraded,
    /// At least one shard or desired registration observed a non-retryable control failure.
    Terminal,
}

impl RouteDependencyHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Ready => "READY",
            Self::Degraded => "DEGRADED",
            Self::Terminal => "TERMINAL",
        }
    }
}
