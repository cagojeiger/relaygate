//! Gateway-to-Gateway one-hop peer relay: transport, handshake, stream
//! multiplexing, and the peer wire codec.
//!
//! Deterministic peer state stays separate from async network I/O so the
//! one-hop relay contract can be tested without timing-sensitive sockets.
//! This crate knows nothing about Gateway session state; the Gateway drives
//! it through [`PeerHandle`] and consumes [`PeerEvents`].

mod codec;
mod config;
mod error;
mod event;
mod frame;
mod handshake;
mod identity;
pub mod jitter;
pub mod metrics;
mod pool;
mod runtime;
mod stream;
mod transport;

pub use config::GatewayPeerConfig;
pub use event::LostPeerStream;
pub use event::{PeerCounts, PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey, PeerTarget};
pub use identity::{OpenIdentity, PeerOpenProgress, PeerTransportId, StreamId};
pub use runtime::{PeerEvents, PeerHandle, PeerRuntime};

/// Invalid [`GatewayPeerConfig`] input; the message is the validation detail.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PeerConfigError(String);

#[cfg(test)]
mod liveness_runtime_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_support {
    use relaygate_protocol::Destination;

    #[allow(clippy::expect_used)]
    pub(crate) fn destination(destination: &str) -> Destination {
        format!("test/{destination}")
            .parse()
            .expect("valid test Destination")
    }
}
