use std::{collections::HashMap, env, fs, time::Duration};

use anyhow::{Context, Result, bail};
use relaygate_gateway::{
    GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig, TrustedPeerConfig,
};
use relaygate_route_table::{GatewayLocator, ShardDirectory};
use relaygate_route_table_transport::{GatewayName, InternalGatewayKey, RouteTableClientConfig};

use super::{optional_duration_millis, optional_usize, parse_gateway_credentials};

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:27420";
const DEFAULT_PEER_BIND_ADDRESS: &str = "0.0.0.0:27421";
const DEFAULT_RT_CLIENT_QUEUE_CAPACITY: usize = 128;
const DEFAULT_RT_MAX_FRAME_LEN: usize = 1024 * 1024;
const DEFAULT_RT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

const DISTRIBUTED_ENVIRONMENT: [&str; 6] = [
    "RELAYGATE_RT_TRUSTED_LOCAL",
    "RELAYGATE_RT_SHARD_DIRECTORY_PATH",
    "RELAYGATE_GATEWAY_NAME",
    "RELAYGATE_GATEWAY_LOCATOR",
    "RELAYGATE_INTERNAL_GATEWAY_KEYS",
    "RELAYGATE_PEER_BIND_ADDR",
];

pub(crate) struct DistributedGatewayConfig {
    pub(crate) peer_bind_address: String,
    pub(crate) routing: GatewayRoutingConfig,
    pub(crate) peer: GatewayPeerConfig,
}

pub(crate) struct GatewayRuntimeConfig {
    pub(crate) bind_address: String,
    pub(crate) gateway: GatewayConfig,
    pub(crate) distributed: Option<DistributedGatewayConfig>,
    pub(crate) configured_clients: usize,
    pub(crate) stats_interval: Option<Duration>,
}

impl GatewayRuntimeConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let bind_address =
            env::var("RELAYGATE_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
        let client_keys = parse_client_keys(env::var("RELAYGATE_CLIENT_KEYS").unwrap_or_default())?;
        let configured_clients = client_keys.len();
        let mut gateway = GatewayConfig::new(client_keys);

        if let Some(capacity) = optional_usize("RELAYGATE_WRITER_QUEUE_CAPACITY")? {
            gateway = gateway.with_writer_queue_capacity(capacity);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_FRAME_LEN")? {
            gateway = gateway.with_max_frame_len(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_SESSIONS")? {
            gateway = gateway.with_max_sessions(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_BINDINGS")? {
            gateway = gateway.with_max_bindings(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_PENDING_OFFERS")? {
            gateway = gateway.with_max_pending_offers(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_LIVE_PIPES")? {
            gateway = gateway.with_max_live_pipes(maximum);
        }
        if let Some(timeout) = optional_duration_millis("RELAYGATE_OFFER_TIMEOUT_MS")? {
            gateway = gateway.with_offer_timeout(timeout);
        }

        Ok(Self {
            bind_address,
            gateway,
            distributed: distributed_from_env()?,
            configured_clients,
            stats_interval: optional_duration_millis("RELAYGATE_STATS_INTERVAL_MS")?,
        })
    }
}

fn distributed_from_env() -> Result<Option<DistributedGatewayConfig>> {
    if !DISTRIBUTED_ENVIRONMENT
        .iter()
        .any(|name| env::var_os(name).is_some())
    {
        return Ok(None);
    }
    if env::var("RELAYGATE_RT_TRUSTED_LOCAL").ok().as_deref() != Some("true") {
        bail!(
            "RELAYGATE_RT_TRUSTED_LOCAL must be `true` to enable distributed Gateway routing over the local/CI plain-TCP key adapter"
        );
    }

    let directory_path = env::var("RELAYGATE_RT_SHARD_DIRECTORY_PATH")
        .context("RELAYGATE_RT_SHARD_DIRECTORY_PATH is required for distributed Gateway mode")?;
    let directory =
        ShardDirectory::from_json_bytes(fs::read(&directory_path).with_context(|| {
            format!("failed to read ShardDirectory artifact at {directory_path:?}")
        })?)?;
    let gateway_name_value = env::var("RELAYGATE_GATEWAY_NAME")
        .context("RELAYGATE_GATEWAY_NAME is required for distributed Gateway mode")?;
    let gateway_name = GatewayName::new(gateway_name_value.clone())?;
    let gateway_locator = GatewayLocator::new(
        env::var("RELAYGATE_GATEWAY_LOCATOR")
            .context("RELAYGATE_GATEWAY_LOCATOR is required for distributed Gateway mode")?,
    )?;
    let credentials = parse_gateway_credentials(
        env::var("RELAYGATE_INTERNAL_GATEWAY_KEYS")
            .context("RELAYGATE_INTERNAL_GATEWAY_KEYS is required for distributed Gateway mode")?,
    )?;
    let local = credentials
        .iter()
        .find(|credential| credential.name == gateway_name_value)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "RELAYGATE_INTERNAL_GATEWAY_KEYS has no entry for local GatewayName {:?}",
                gateway_name_value
            )
        })?;
    let internal_gateway_key = InternalGatewayKey::new(local.key.clone())?;
    let trusted_peers = credentials
        .iter()
        .filter(|credential| credential.name != gateway_name_value)
        .map(|credential| TrustedPeerConfig::new(&credential.name, &credential.key))
        .collect::<Result<Vec<_>, _>>()?;
    let peer = GatewayPeerConfig::new(gateway_name_value, local.key.clone(), trusted_peers)?;
    let client = RouteTableClientConfig::new(
        DEFAULT_RT_CLIENT_QUEUE_CAPACITY,
        DEFAULT_RT_MAX_FRAME_LEN,
        DEFAULT_RT_CONNECT_TIMEOUT,
        DEFAULT_RT_HANDSHAKE_TIMEOUT,
        DEFAULT_RT_REQUEST_TIMEOUT,
    )?;
    Ok(Some(DistributedGatewayConfig {
        peer_bind_address: env::var("RELAYGATE_PEER_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_PEER_BIND_ADDRESS.to_owned()),
        routing: GatewayRoutingConfig::new(
            directory,
            gateway_name,
            internal_gateway_key,
            gateway_locator,
            client,
        ),
        peer,
    }))
}

fn parse_client_keys(value: String) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();
    if value.is_empty() {
        return Ok(keys);
    }
    for entry in value.split(',') {
        let Some((client_id, client_key)) = entry.split_once('=') else {
            bail!("RELAYGATE_CLIENT_KEYS entries must use ClientId=ClientKey");
        };
        if client_id.is_empty() || client_key.is_empty() {
            bail!("RELAYGATE_CLIENT_KEYS entries require non-empty ClientId and ClientKey");
        }
        if keys
            .insert(client_id.to_owned(), client_key.to_owned())
            .is_some()
        {
            bail!("RELAYGATE_CLIENT_KEYS contains duplicate ClientId {client_id:?}");
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::parse_client_keys;

    #[test]
    fn client_key_config_rejects_duplicate_client_ids() {
        assert!(parse_client_keys("echo.alpha=one,echo.alpha=two".to_owned()).is_err());
    }

    #[test]
    fn client_key_config_preserves_exact_values() -> Result<(), Box<dyn std::error::Error>> {
        let keys = parse_client_keys("echo.alpha=Key=With=Equals".to_owned())?;
        assert_eq!(
            keys.get("echo.alpha").map(String::as_str),
            Some("Key=With=Equals")
        );
        Ok(())
    }
}
