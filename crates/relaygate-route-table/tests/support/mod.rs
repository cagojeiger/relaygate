#![allow(dead_code)]

use std::time::Duration;

use relaygate_route_table::{
    AuthenticatedGatewayId, BindingId, DestinationId, GatewayId, GatewayLocator, MappingEntry,
    MappingSnapshot, RegistrationKey, RelaySessionId, RequestContext, RouteTableConfig,
    RouteTableError, RouteTableShard, ShardDirectory, ShardId,
};
use sha2::{Digest, Sha256};
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

pub const fn session(value: u128) -> RelaySessionId {
    RelaySessionId::from_uuid(Uuid::from_u128(value))
}

pub const fn binding(value: u128) -> BindingId {
    BindingId::from_uuid(Uuid::from_u128(value))
}

pub const fn context(gateway_id: GatewayId) -> RequestContext {
    RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id))
}

pub fn key(
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
) -> Result<RegistrationKey, RouteTableError> {
    Ok(RegistrationKey::new(
        gateway_id,
        relay_session_id,
        ShardId::new("rt-0")?,
    ))
}

pub fn client(value: &str) -> Result<DestinationId, RouteTableError> {
    DestinationId::new(test_destination(value))
}

pub fn mapping(
    destination_id: &str,
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
    binding_id: BindingId,
) -> Result<MappingEntry, RouteTableError> {
    Ok(MappingEntry::new(
        client(destination_id)?,
        gateway_id,
        relay_session_id,
        binding_id,
        GatewayLocator::new(format!("gw-{gateway_id}"))?,
    ))
}

fn test_destination(label: &str) -> String {
    if let Ok(parsed) = Uuid::parse_str(label)
        && parsed.get_version_num() == 4
    {
        return parsed.to_string();
    }
    let digest = Sha256::digest(label.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

pub fn snapshot(
    entries: impl IntoIterator<Item = MappingEntry>,
) -> Result<MappingSnapshot, RouteTableError> {
    MappingSnapshot::new(entries)
}
