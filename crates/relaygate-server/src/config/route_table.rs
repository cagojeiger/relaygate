use std::{env, fs, time::Duration};

use anyhow::{Context, Result};
use relaygate_route_table::{RouteTableConfig, RouteTableShard, ShardDirectory, ShardId};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableServiceConfig, TrustedGatewayKeys,
};
use relaygate_transport::ServerTlsConfig;

use super::{
    InternalTransport, internal_transport, load_internal_tls, optional_duration_millis,
    optional_usize, parse_gateway_credentials,
};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:27430";
const DEFAULT_SHARD_ID: &str = "rt-0";
const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 128;
const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 32;
const DEFAULT_MAX_CONNECTIONS: usize = 1_024;
const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) struct RouteTableRuntimeConfig {
    pub(crate) bind_address: String,
    pub(crate) shard: RouteTableShard,
    pub(crate) trusted_gateways: TrustedGatewayKeys,
    pub(crate) service: RouteTableServiceConfig,
    pub(crate) configured_gateways: usize,
    pub(crate) tls: Option<ServerTlsConfig>,
}

impl RouteTableRuntimeConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let insecure = internal_transport()? == InternalTransport::Plaintext;
        let bind_address =
            env::var("RELAYGATE_RT_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
        let directory_path = env::var("RELAYGATE_RT_SHARD_DIRECTORY_PATH")
            .context("RELAYGATE_RT_SHARD_DIRECTORY_PATH is required")?;
        let directory_bytes = fs::read(&directory_path).with_context(|| {
            format!("failed to read ShardDirectory artifact at {directory_path:?}")
        })?;
        let directory = ShardDirectory::from_json_bytes(directory_bytes)?;
        let shard_id = ShardId::new(
            env::var("RELAYGATE_RT_SHARD_ID").unwrap_or_else(|_| DEFAULT_SHARD_ID.to_owned()),
        )?;
        let lease_ttl =
            optional_duration_millis("RELAYGATE_RT_LEASE_TTL_MS")?.unwrap_or(DEFAULT_LEASE_TTL);
        let shard = RouteTableShard::new(directory, shard_id, RouteTableConfig::new(lease_ttl)?)?;

        let gateway_keys = parse_gateway_credentials(
            env::var("RELAYGATE_INTERNAL_GATEWAY_KEYS")
                .context("RELAYGATE_INTERNAL_GATEWAY_KEYS is required")?,
        )?
        .into_iter()
        .map(|credential| {
            Ok((
                GatewayName::new(credential.name)?,
                InternalGatewayKey::new(credential.key)?,
            ))
        })
        .collect::<Result<Vec<_>, relaygate_route_table_transport::TransportError>>()?;
        let configured_gateways = gateway_keys.len();
        let trusted_gateways = TrustedGatewayKeys::new(gateway_keys)?;
        let service = RouteTableServiceConfig::new(
            optional_usize("RELAYGATE_RT_REQUEST_QUEUE_CAPACITY")?
                .unwrap_or(DEFAULT_REQUEST_QUEUE_CAPACITY),
            optional_usize("RELAYGATE_RT_WRITER_QUEUE_CAPACITY")?
                .unwrap_or(DEFAULT_WRITER_QUEUE_CAPACITY),
            optional_usize("RELAYGATE_RT_MAX_CONNECTIONS")?.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            optional_usize("RELAYGATE_RT_MAX_FRAME_LEN")?.unwrap_or(DEFAULT_MAX_FRAME_LEN),
            optional_duration_millis("RELAYGATE_RT_HANDSHAKE_TIMEOUT_MS")?
                .unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT),
        )?;
        let tls = if insecure {
            None
        } else {
            let material = load_internal_tls()?;
            Some(ServerTlsConfig::mutually_authenticated(
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?)
        };

        Ok(Self {
            bind_address,
            shard,
            trusted_gateways,
            service,
            configured_gateways,
            tls,
        })
    }
}
