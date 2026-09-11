//! Gateway runtime for local and one-hop relayed Pipes.
//!
//! The crate owns live SDK sessions, local route bindings, PUBLISH/DIAL
//! admission, byte relay, and cleanup. Process configuration and signal
//! handling belong to `relaygate-server`.

mod authorization;
mod config;
mod error;
mod gateway;
mod metrics;
mod observation;
mod peer;
mod rate_limit;
mod registry;
mod routing;
mod state;
#[cfg(test)]
mod test_support;

pub use authorization::{AuthorizationConfig, Es256PublicKey, TrustedIssuer};
pub use config::{
    DEFAULT_AUTHORIZATION_CONCURRENCY, DEFAULT_AUTHORIZATION_TIMEOUT, GatewayConfig,
    MAX_AUTHORIZATION_CONCURRENCY, MAX_AUTHORIZATION_TIMEOUT,
};
pub use error::GatewayError;
pub use gateway::{Gateway, check, check_insecure_for_tests};
pub use observation::{GatewaySnapshot, RouteDependencyHealth};
pub use peer::GatewayPeerConfig;
pub use routing::GatewayRoutingConfig;
