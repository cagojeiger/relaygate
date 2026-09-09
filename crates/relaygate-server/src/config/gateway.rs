use std::{env, fs, time::Duration};

use anyhow::{Context, Result};
use relaygate_gateway::{GatewayConfig, GatewayPeerConfig, GatewayRoutingConfig};
use relaygate_route_table::{GatewayLocator, ShardDirectory};
use relaygate_route_table_transport::{GatewayName, RouteTableClientConfig};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};

use super::{
    InternalTransport, insecure_test_transport, internal_transport, load_internal_tls,
    optional_duration_millis, optional_usize,
};

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:27420";
const DEFAULT_PEER_BIND_ADDRESS: &str = "0.0.0.0:27421";
const DEFAULT_RT_CLIENT_QUEUE_CAPACITY: usize = 128;
const DEFAULT_RT_MAX_FRAME_LEN: usize = 1024 * 1024;
const DEFAULT_RT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_RT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

const DISTRIBUTED_ENVIRONMENT: [&str; 5] = [
    "RELAYGATE_RT_TRUSTED_LOCAL",
    "RELAYGATE_RT_SHARD_DIRECTORY_PATH",
    "RELAYGATE_GATEWAY_NAME",
    "RELAYGATE_GATEWAY_LOCATOR",
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
        if env::var_os("RELAYGATE_INTERNAL_TRANSPORT").is_some() {
            internal_transport()?;
        }
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
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_PENDING_HANDSHAKES")? {
            gateway = gateway.with_max_pending_handshakes(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_BINDINGS")? {
            gateway = gateway.with_max_bindings(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_PENDING_OFFERS")? {
            gateway = gateway.with_max_pending_offers(maximum);
        }
        if let Some(maximum) = optional_usize("RELAYGATE_MAX_REMOTE_DIAL_ATTEMPTS")? {
            gateway = gateway.with_max_remote_dial_attempts(maximum);
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
    let insecure = internal_transport()? == InternalTransport::Plaintext;

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
    let mut peer = GatewayPeerConfig::new(gateway_name_value)?;
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
    let mut routing = GatewayRoutingConfig::new(directory, gateway_name, gateway_locator, client);
    if !insecure {
        let material = load_internal_tls()?;
        let peer_server_name = env::var("RELAYGATE_PEER_TLS_SERVER_NAME")
            .context("RELAYGATE_PEER_TLS_SERVER_NAME is required")?;
        let route_table_server_name = env::var("RELAYGATE_RT_TLS_SERVER_NAME")
            .context("RELAYGATE_RT_TLS_SERVER_NAME is required")?;
        peer = peer.with_tls(
            ClientTlsConfig::mutually_authenticated(
                peer_server_name.clone(),
                &material.ca,
                &material.certificate,
                &material.private_key,
            )?,
            ServerTlsConfig::mutually_authenticated(
                peer_server_name,
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
