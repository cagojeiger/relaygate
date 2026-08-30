//! Memory-only, shard-local RouteTable core.
//!
//! This crate owns immutable shard-directory parsing and the synchronous
//! registration lease state machine. Networking, authentication transport,
//! Gateway retry policy, replication, and persistence belong elsewhere.

mod directory;
mod error;
mod identity;
mod model;
mod shard;

pub use directory::{AUTHORITY_HASH_SHA256_MODULO_V1, ShardDirectory, ShardRecord};
pub use error::{ErrorCode, RouteTableError};
pub use identity::{
    AuthenticatedGatewayId, BindingId, ClientId, GatewayId, GatewayLocator, LeaseId,
    ListenerSessionId, RegistrationRevision, RequestContext, ShardDirectoryGeneration,
    ShardEndpoint, ShardId,
};
pub use model::{
    BindingSet, MappingEntry, MappingIdentity, MappingSnapshot, RegistrationAck, RegistrationKey,
    RouteTableStats,
};
pub use shard::{RouteTableConfig, RouteTableShard};
