//! Private TCP adapter for the memory-only RouteTable core.
//!
//! This crate is workspace-internal. It owns the local/CI authentication
//! handshake, a bounded request/response client, and the network service actor.
//! It does not provide persistence, reconnect, request replay, or routing policy.

mod auth;
mod client;
mod codec;
mod dto;
mod error;
mod frame;
mod service;

pub use auth::{GatewayName, InternalGatewayKey, TrustedGatewayKeys};
pub use client::{RouteTableClient, RouteTableClientConfig};
pub use error::{ErrorCode, TransportError};
pub use service::{RouteTableService, RouteTableServiceConfig};
