//! Gateway runtime for local and one-hop relayed Pipes.
//!
//! The crate owns live SDK sessions, local listener bindings, OPEN admission,
//! byte relay, and cleanup. Process configuration and signal handling belong to
//! `relaygate-server`.

mod auth;
mod config;
mod error;
mod gateway;
mod observation;
mod peer;
mod registry;
mod routing;
mod state;

pub use config::GatewayConfig;
pub use error::GatewayError;
pub use gateway::{Gateway, check, check_insecure_for_tests};
pub use observation::{GatewaySnapshot, RouteDependencyHealth};
pub use peer::{GatewayPeerConfig, TrustedPeerConfig};
pub use routing::GatewayRoutingConfig;
