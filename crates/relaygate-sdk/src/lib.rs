//! Public Rust SDK for RelayGate's Connector, Listener, and Pipe roles.
//!
//! Wire frames and Gateway-owned state stay private to this crate. Applications
//! work only with the SDK types re-exported here.

mod config;
mod connector;
mod error;
mod lifetime;
mod listener;
mod observability;
mod pipe;
mod session;

pub use config::Config;
pub use connector::Connector;
pub use error::{Error, ErrorCode, PeerObservation, Result};
pub use listener::{Listener, ListenerRuntime, ListenerStatus};
pub use pipe::{Pipe, PipeReadHalf, PipeWriteHalf};
