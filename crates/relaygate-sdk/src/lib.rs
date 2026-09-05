//! Public Rust SDK for one symmetric Relay session and its Listeners and Pipes.
//!
//! Wire frames and Gateway-owned state stay private to this crate. Applications
//! work only with the SDK types re-exported here.

mod config;
mod destination;
mod error;
mod lifetime;
mod listener;
mod observability;
mod pipe;
mod session;
mod transport;

pub use config::Config;
pub use destination::{DestinationId, DestinationIdError};
pub use error::{Error, ErrorCode, PeerObservation, Result};
pub use listener::{Listener, ListenerStatus, Relay};
pub use pipe::{Pipe, PipeReadHalf, PipeWriteHalf};
pub use relaygate_transport::{ClientTlsConfig, TlsConfigError};
pub use transport::GatewayTransportConfig;
