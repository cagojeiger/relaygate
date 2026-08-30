use std::{collections::HashMap, env, time::Duration};

use anyhow::{Result, bail};
use relaygate_gateway::GatewayConfig;

use super::{optional_duration_millis, optional_usize};

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:27420";

pub(crate) struct GatewayRuntimeConfig {
    pub(crate) bind_address: String,
    pub(crate) gateway: GatewayConfig,
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
            configured_clients,
            stats_interval: optional_duration_millis("RELAYGATE_STATS_INTERVAL_MS")?,
        })
    }
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
