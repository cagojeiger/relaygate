//! Public Rust SDK for one symmetric Relay session and its Listeners and Pipes.
//!
//! Wire frames and Gateway-owned state stay private to this crate. Applications
//! work only with the SDK types re-exported here.
//!
//! ```no_run
//! use relaygate_sdk::{AccessToken, AccessTokenSource, Config, Relay, RouteAddress};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::new("relaygate.example.com:443")?;
//! let relay = Relay::connect(config).await?;
//! let address: RouteAddress = "inference/stt.seoul".parse()?;
//! let token = AccessToken::new(std::env::var("RELAYGATE_ACCESS_TOKEN")?)?;
//! let listener = relay
//!     .listen(address, AccessTokenSource::static_token(token))
//!     .await?;
//! listener.close().await?;
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

mod access_token;
mod config;
mod error;
mod lifetime;
mod listener;
mod observability;
mod pipe;
mod session;
mod transport;

pub use access_token::{
    AccessAction, AccessToken, AccessTokenError, AccessTokenRequest, AccessTokenSource,
    AccessTokenSourceError,
};
pub use config::Config;
pub use error::{Error, ErrorCode, PeerObservation, Result};
pub use listener::{Listener, ListenerStatus, Relay};
pub use pipe::{Pipe, PipeReadHalf, PipeWriteHalf};
pub use relaygate_address::{DestinationName, NamespaceId, RouteAddress};
pub use relaygate_transport::{ClientTlsConfig, TlsConfigError};
pub use transport::GatewayTransportConfig;
