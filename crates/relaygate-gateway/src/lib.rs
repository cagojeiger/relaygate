//! Single-Gateway local relay runtime.
//!
//! The crate owns live SDK sessions, local listener bindings, OPEN admission,
//! byte relay, and cleanup. Process configuration and signal handling belong to
//! `relaygate-server`.

mod auth;
mod config;
mod error;
mod gateway;
mod registry;
mod state;

pub use config::GatewayConfig;
pub use error::GatewayError;
pub use gateway::{Gateway, check};
