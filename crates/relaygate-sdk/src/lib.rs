//! Public Rust SDK for one symmetric Relay session and its Listeners and Pipes.
//!
//! Wire frames and Gateway-owned state stay private to this crate. Applications
//! work only with the SDK types re-exported here.
//!
//! ```no_run
//! use relaygate_sdk::{Config, Relay};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::new("relaygate.example.com:443")?
//!     .cluster_token(std::env::var("RELAYGATE_CLUSTER_TOKEN")?);
//! let relay = Relay::connect(config).await?;
//! relay.close();
//! # Ok(())
//! # }
//! ```
//!
//! A bare `host:port` or `tls://host:port` automatically uses public CA trust
//! and verifies the endpoint's DNS/IP identity. Providers may explicitly supply
//! `tcp://host:port` for plaintext, which does not encrypt credentials or data.
//! TLS errors never trigger plaintext fallback. Private deployments can supply
//! a CA with [`Config::with_ca_certificate`].

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
