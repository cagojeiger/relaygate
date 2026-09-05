//! Workspace-internal SDK–Gateway wire contract.
//!
//! This crate owns framing and identifiers only. Socket ownership, reconnect,
//! admission, and routing policy belong to the SDK and Gateway crates.

mod codec;
mod error;
mod frame;
mod identity;
mod secret;

pub use codec::{DEFAULT_MAX_FRAME_LEN, FrameCodec};
pub use error::ProtocolError;
pub use frame::{ErrorCode, Frame, PeerObservation};
pub use identity::{BindingId, DestinationId, PipeId, SessionId};
pub use secret::ClusterToken;
