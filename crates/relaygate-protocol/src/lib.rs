//! Internal SDK–Gateway wire contract.
//!
//! Published only as a dependency of `relaygate-sdk`; it is not a stable
//! application API.
//!
//! This crate owns framing and identifiers only. Socket ownership, reconnect,
//! admission, and routing policy belong to the SDK and Gateway crates.

mod codec;
mod error;
mod frame;
mod identity;
mod secret;

pub use codec::{DEFAULT_MAX_FRAME_LEN, FrameCodec, MAX_HELLO_FRAME_LEN};
pub use error::ProtocolError;
pub use frame::{ErrorCode, Frame, PeerObservation};
pub use identity::{BindingId, PipeId, SessionId};
pub use relaygate_destination::{Destination, DestinationName, Namespace};
pub use secret::{BearerToken, MAX_BEARER_TOKEN_BYTES};
