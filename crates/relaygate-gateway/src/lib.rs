//! Single-Gateway local relay runtime.
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
pub use gateway::{Gateway, check};
pub use observation::GatewaySnapshot;
pub use peer::{GatewayPeerConfig, TrustedPeerConfig};
pub use routing::GatewayRoutingConfig;
