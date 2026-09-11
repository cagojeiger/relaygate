//! Canonical logical destinations shared across RelayGate layers.
//!
//! This crate owns representation and validation, not routing or authorization.

mod destination;
mod error;
mod name;

pub use destination::Destination;
pub use error::DestinationError;
pub use name::{DestinationName, MAX_DESTINATION_NAME_BYTES, MAX_LABEL_BYTES, Namespace};
