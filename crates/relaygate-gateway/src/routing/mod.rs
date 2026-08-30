//! Gateway-owned RouteTable orchestration.
//!
//! This module projects local binding truth into shard-local soft state. It
//! deliberately does not own local bindings, cache Resolve results, or relay
//! peer payloads.

mod config;
mod error;
mod lifecycle;
mod projection;
mod runtime;

pub use config::GatewayRoutingConfig;
pub(crate) use error::RoutingError;
pub(crate) use runtime::{RoutingHandle, RoutingRuntime};

#[cfg(test)]
mod tests;
