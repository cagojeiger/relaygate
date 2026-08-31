//! Gateway-to-Gateway peer relay primitives.
//!
//! This module keeps the deterministic peer state separate from async network
//! I/O so the one-hop relay contract can be tested without timing-sensitive
//! sockets.
mod auth;
mod codec;
mod config;
mod error;
mod event;
mod frame;
mod handshake;
mod identity;
mod pool;
mod runtime;
mod stream;
mod transport;

#[cfg(test)]
pub(crate) use config::{ConnectGate, ResetCommitGate};
pub use config::{GatewayPeerConfig, TrustedPeerConfig};
pub(crate) use event::{PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey, PeerTarget};
pub(crate) use identity::OpenIdentity;
#[cfg(test)]
pub(crate) use identity::PeerTransportId;
pub(crate) use runtime::{PeerEvents, PeerHandle, PeerRuntime};

#[cfg(test)]
mod liveness_runtime_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;
