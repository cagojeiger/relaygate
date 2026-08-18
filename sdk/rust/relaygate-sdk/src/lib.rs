//! Public Rust client for RelayGate's exact-target relay data plane.
//!
//! Generated protobuf and tonic types are deliberately private. The SDK owns
//! one authenticated stream and exposes only client, listener, offer, and pipe
//! concepts.
//!
//! [`ManagedClient`] is an opt-in in-process supervisor. It reconnects a fresh
//! authenticated session and rebinds current logical listeners, but never
//! retries or resumes Open, Pipe, or payload work.

mod client;
mod config;
mod error;
mod listener;
mod managed;
mod pipe;
mod runtime;

mod wire {
    tonic::include_proto!("relaygate.relay.v1");
}

pub use client::{Client, Session};
pub use config::Config;
pub use error::{
    AcceptError, BindError, CloseError, ConnectError, ManagedError, OpenError, OpenFailure,
    PipeError, RejectError, SessionError, UnbindError,
};
pub use listener::{Listener, Offer, OfferMetadata};
pub use managed::{ManagedClient, ManagedListener, ManagedState};
pub use pipe::Pipe;
