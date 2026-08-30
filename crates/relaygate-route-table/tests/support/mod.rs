#![allow(dead_code)]

use std::time::Duration;

use relaygate_route_table::{
    AuthenticatedGatewayId, BindingId, ClientId, GatewayId, GatewayLocator, ListenerSessionId,
    MappingEntry, MappingSnapshot, RegistrationKey, RequestContext, RouteTableConfig,
    RouteTableError, RouteTableShard, ShardDirectory, ShardId,
};
use uuid::Uuid;

pub const ONE_SHARD_DIRECTORY: &[u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"http://rt-0:8080"}]}"#;

pub fn directory() -> Result<ShardDirectory, RouteTableError> {
    ShardDirectory::from_json_bytes(ONE_SHARD_DIRECTORY)
}

pub fn shard(ttl: Duration) -> Result<RouteTableShard, RouteTableError> {
    RouteTableShard::new(
        directory()?,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(ttl)?,
    )
}

pub const fn gateway(value: u128) -> GatewayId {
    GatewayId::from_uuid(Uuid::from_u128(value))
}

pub const fn session(value: u128) -> ListenerSessionId {
    ListenerSessionId::from_uuid(Uuid::from_u128(value))
}

pub const fn binding(value: u128) -> BindingId {
    BindingId::from_uuid(Uuid::from_u128(value))
}

pub const fn context(gateway_id: GatewayId) -> RequestContext {
    RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id))
}

pub fn key(
    gateway_id: GatewayId,
    listener_session_id: ListenerSessionId,
) -> Result<RegistrationKey, RouteTableError> {
    Ok(RegistrationKey::new(
        gateway_id,
        listener_session_id,
        ShardId::new("rt-0")?,
    ))
}

pub fn client(value: &str) -> Result<ClientId, RouteTableError> {
    ClientId::new(value)
}

pub fn mapping(
    client_id: &str,
    gateway_id: GatewayId,
    listener_session_id: ListenerSessionId,
    binding_id: BindingId,
) -> Result<MappingEntry, RouteTableError> {
    Ok(MappingEntry::new(
        ClientId::new(client_id)?,
        gateway_id,
        listener_session_id,
        binding_id,
        GatewayLocator::new(format!("gw-{gateway_id}"))?,
    ))
}

pub fn snapshot(
    entries: impl IntoIterator<Item = MappingEntry>,
) -> Result<MappingSnapshot, RouteTableError> {
    MappingSnapshot::new(entries)
}
