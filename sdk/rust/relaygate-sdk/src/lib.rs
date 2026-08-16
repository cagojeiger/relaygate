//! Public Rust client for RelayGate's exact-target relay data plane.
//!
//! Generated protobuf and tonic types are deliberately private. The SDK owns
//! one authenticated stream and exposes only client, listener, offer, and pipe
//! concepts.

mod client;
mod config;
mod error;
mod listener;
mod pipe;
mod runtime;

mod wire {
    tonic::include_proto!("relaygate.relay.v1");
}

pub use client::{Client, Session};
pub use config::Config;
pub use error::{
    AcceptError, BindError, CloseError, ConnectError, OpenError, OpenFailure, PipeError,
    RejectError, SessionError, UnbindError,
};
pub use listener::{Listener, Offer, OfferMetadata};
pub use pipe::Pipe;
