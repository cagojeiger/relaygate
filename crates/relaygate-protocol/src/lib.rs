//! Workspace-internal SDK–Gateway wire contract.
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
pub use relaygate_address::{DestinationName, NamespaceId, RouteAddress};
pub use secret::{BearerToken, MAX_BEARER_TOKEN_BYTES};
