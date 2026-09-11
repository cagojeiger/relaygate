//! Canonical logical route addresses shared across RelayGate layers.
//!
//! This crate owns representation and validation, not routing or authorization.

mod error;
mod name;
mod route;

pub use error::AddressError;
pub use name::{DestinationName, MAX_DESTINATION_BYTES, MAX_LABEL_BYTES, NamespaceId};
pub use route::RouteAddress;
