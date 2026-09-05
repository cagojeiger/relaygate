use std::{env, fs, time::Duration};

use anyhow::{Context, Result, bail};
use relaygate_gateway::{
    GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig, TrustedPeerConfig,
};
use relaygate_route_table::{GatewayLocator, ShardDirectory};
use relaygate_route_table_transport::{GatewayName, InternalGatewayKey, RouteTableClientConfig};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};

use super::{
    insecure_test_transport, load_internal_tls, optional_duration_millis, optional_usize,
    parse_gateway_credentials,
};

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
    pub(crate) insecure_transport: bool,
}

pub(crate) struct GatewayRuntimeConfig {
    pub(crate) bind_address: String,
    pub(crate) gateway: GatewayConfig,
    pub(crate) distributed: Option<DistributedGatewayConfig>,
    pub(crate) stats_interval: Option<Duration>,
}

impl GatewayRuntimeConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let bind_address =
            env::var("RELAYGATE_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
        let cluster_token = env::var("RELAYGATE_CLUSTER_TOKEN")
            .context("RELAYGATE_CLUSTER_TOKEN is required for Gateway mode")?;
        let mut gateway = GatewayConfig::new(cluster_token);
        if !insecure_test_transport() {
            let certificate_path = env::var("RELAYGATE_SDK_TLS_CERT_PATH")
                .context("RELAYGATE_SDK_TLS_CERT_PATH is required for Gateway mode")?;
            let private_key_path = env::var("RELAYGATE_SDK_TLS_KEY_PATH")
                .context("RELAYGATE_SDK_TLS_KEY_PATH is required for Gateway mode")?;
            let tls = ServerTlsConfig::server_authenticated(
                &fs::read(&certificate_path).with_context(|| {
                    format!("failed to read SDK TLS certificate at {certificate_path:?}")
                })?,
                &fs::read(&private_key_path).with_context(|| {
                    format!("failed to read SDK TLS private key at {private_key_path:?}")
                })?,
            )?;
            gateway = gateway.with_sdk_tls(tls);
        }
        if let Ok(next) = env::var("RELAYGATE_NEXT_CLUSTER_TOKEN") {
            gateway = gateway.with_next_cluster_token(next);
        }

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
        if let Some(timeout) = optional_duration_millis("RELAYGATE_DRAIN_TIMEOUT_MS")? {
            gateway = gateway.with_drain_timeout(timeout);
        }
        let heartbeat_idle = optional_duration_millis("RELAYGATE_SDK_HEARTBEAT_IDLE_MS")?;
        let heartbeat_response = optional_duration_millis("RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS")?;
        if heartbeat_idle.is_some() || heartbeat_response.is_some() {
            let default_idle = gateway.heartbeat_idle_interval();
            let default_response = gateway.heartbeat_response_timeout();
            gateway = gateway.with_heartbeat(
                heartbeat_idle.unwrap_or(default_idle),
                heartbeat_response.unwrap_or(default_response),
            );
        }

        Ok(Self {
            bind_address,
            gateway,
            distributed: distributed_from_env()?,
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
    let insecure = insecure_test_transport();
    if insecure && env::var("RELAYGATE_RT_TRUSTED_LOCAL").ok().as_deref() != Some("true") {
        bail!(
            "RELAYGATE_RT_TRUSTED_LOCAL must be `true` to enable distributed Gateway routing over the local/CI plain-TCP key adapter"
        );
    }
    if !insecure && env::var_os("RELAYGATE_RT_TRUSTED_LOCAL").is_some() {
        bail!(
            "RELAYGATE_RT_TRUSTED_LOCAL is only valid with RELAYGATE_INSECURE_TEST_TRANSPORT=true"
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
    let mut peer = GatewayPeerConfig::new(gateway_name_value, local.key.clone(), trusted_peers)?;
    let peer_heartbeat_idle = optional_duration_millis("RELAYGATE_PEER_HEARTBEAT_IDLE_MS")?
        .unwrap_or_else(|| peer.heartbeat_idle_interval());
    let peer_heartbeat_response = optional_duration_millis("RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS")?
        .unwrap_or_else(|| peer.heartbeat_response_timeout());
    let peer_idle_retirement = optional_duration_millis("RELAYGATE_PEER_IDLE_TIMEOUT_MS")?
        .unwrap_or_else(|| peer.idle_retirement_timeout());
    peer = peer.with_liveness(
        peer_heartbeat_idle,
        peer_heartbeat_response,
        peer_idle_retirement,
    );
    let client = RouteTableClientConfig::new(
        DEFAULT_RT_CLIENT_QUEUE_CAPACITY,
        DEFAULT_RT_MAX_FRAME_LEN,
        DEFAULT_RT_CONNECT_TIMEOUT,
        DEFAULT_RT_HANDSHAKE_TIMEOUT,
        DEFAULT_RT_REQUEST_TIMEOUT,
    )?;
    let mut routing = GatewayRoutingConfig::new(
        directory,
        gateway_name,
        internal_gateway_key,
        gateway_locator,
        client,
    );
    if !insecure {
        let material = load_internal_tls()?;
        let peer_server_name = env::var("RELAYGATE_PEER_TLS_SERVER_NAME")
            .context("RELAYGATE_PEER_TLS_SERVER_NAME is required")?;
        let route_table_server_name = env::var("RELAYGATE_RT_TLS_SERVER_NAME")
            .context("RELAYGATE_RT_TLS_SERVER_NAME is required")?;
        peer = peer.with_tls(
            ClientTlsConfig::mutually_authenticated(
                peer_server_name,
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?,
            ServerTlsConfig::mutually_authenticated(
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?,
        );
        routing = routing.with_tls(ClientTlsConfig::mutually_authenticated(
            route_table_server_name,
            &material.ca,
            &material.certificate,
            &material.private_key,
        )?);
    }
    Ok(Some(DistributedGatewayConfig {
        peer_bind_address: env::var("RELAYGATE_PEER_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_PEER_BIND_ADDRESS.to_owned()),
        routing,
        peer,
        insecure_transport: insecure,
    }))
}
