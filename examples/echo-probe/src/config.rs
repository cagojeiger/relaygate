use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, ensure};
use relaygate_sdk::{ClientTlsConfig, Config, GatewayTransportConfig};

pub(crate) const DESTINATION_IDS: [&str; 3] = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
];
pub(crate) const SHARED_DESTINATION_ID: &str = "44444444-4444-4444-8444-444444444444";
pub(crate) const CONCURRENT_PIPES_PER_PATH: usize = 32;
pub(crate) const ECHO_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const ROUTE_WAIT: Duration = Duration::from_secs(20);
pub(crate) const CONTINUITY_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const CONTINUITY_FRESHNESS: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_SOAK_DURATION: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_SOAK_CONCURRENCY: usize = 64;
pub(crate) const DEFAULT_STORM_SESSIONS: usize = 100;
pub(crate) const DEFAULT_STORM_PAUSE: Duration = Duration::from_secs(30);

const DEFAULT_GATEWAYS: &str = "gateway-a:27420,gateway-b:27420,gateway-c:27420";
const DEFAULT_CONTINUITY_STATE: &str = "/tmp/relaygate-continuity.state";
const DEFAULT_CLUSTER_TOKEN: &str = "relaygate-local-cluster-token";
const DEFAULT_TLS_CA_PATH: &str = "/etc/relaygate/tls/ca.crt";
const DEFAULT_TLS_SERVER_NAME: &str = "relaygate-gateway.internal";

pub(crate) fn environment(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

pub(crate) fn gateway_addresses() -> anyhow::Result<Vec<String>> {
    let value = environment("RELAYGATE_GATEWAYS", DEFAULT_GATEWAYS);
    let addresses = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    ensure!(
        addresses.len() == DESTINATION_IDS.len(),
        "RELAYGATE_GATEWAYS must contain exactly three comma-separated addresses"
    );
    Ok(addresses)
}

pub(crate) fn cluster_token() -> String {
    environment("RELAYGATE_CLUSTER_TOKEN", DEFAULT_CLUSTER_TOKEN)
}

pub(crate) fn sdk_config(address: impl Into<String>) -> anyhow::Result<Config> {
    let ca_path = environment("RELAYGATE_SDK_TLS_CA_PATH", DEFAULT_TLS_CA_PATH);
    let server_name = environment("RELAYGATE_SDK_TLS_SERVER_NAME", DEFAULT_TLS_SERVER_NAME);
    let ca = std::fs::read(&ca_path)
        .with_context(|| format!("failed to read SDK TLS CA at {ca_path:?}"))?;
    let tls = ClientTlsConfig::server_authenticated(server_name, &ca)?;
    Ok(Config::new(
        cluster_token(),
        GatewayTransportConfig::tls_tcp(address, tls),
    ))
}

pub(crate) fn continuity_state_path() -> PathBuf {
    PathBuf::from(environment(
        "RELAYGATE_CONTINUITY_STATE",
        DEFAULT_CONTINUITY_STATE,
    ))
}

pub(crate) fn soak_duration() -> anyhow::Result<Duration> {
    positive_integer(
        "RELAYGATE_SOAK_DURATION_SECS",
        DEFAULT_SOAK_DURATION.as_secs(),
    )
    .map(Duration::from_secs)
}

pub(crate) fn soak_concurrency() -> anyhow::Result<usize> {
    positive_integer(
        "RELAYGATE_SOAK_CONCURRENCY",
        DEFAULT_SOAK_CONCURRENCY as u64,
    )
    .and_then(|value| {
        usize::try_from(value)
            .map_err(|_| anyhow::anyhow!("RELAYGATE_SOAK_CONCURRENCY is too large"))
    })
}

pub(crate) fn storm_sessions() -> anyhow::Result<usize> {
    positive_integer("RELAYGATE_STORM_SESSIONS", DEFAULT_STORM_SESSIONS as u64).and_then(|value| {
        usize::try_from(value).map_err(|_| anyhow::anyhow!("RELAYGATE_STORM_SESSIONS is too large"))
    })
}

pub(crate) fn storm_pause() -> anyhow::Result<Duration> {
    positive_integer("RELAYGATE_STORM_PAUSE_SECS", DEFAULT_STORM_PAUSE.as_secs())
        .map(Duration::from_secs)
}

fn positive_integer(name: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) => parse_positive_integer(name, Some(&value), default),
        Err(env::VarError::NotPresent) => parse_positive_integer(name, None, default),
        Err(error) => Err(error.into()),
    }
}

fn parse_positive_integer(name: &str, value: Option<&str>, default: u64) -> anyhow::Result<u64> {
    let value = value.map_or(Ok(default), |value| {
        value
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name} must be a positive integer"))
    })?;
    ensure!(value > 0, "{name} must be greater than zero");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_integer_rejects_zero() -> anyhow::Result<()> {
        let error = match parse_positive_integer("TEST_VALUE", Some("0"), 1) {
            Ok(value) => anyhow::bail!("unexpected value: {value}"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("greater than zero"));
        Ok(())
    }

    #[test]
    fn positive_integer_uses_default_when_absent() -> anyhow::Result<()> {
        assert_eq!(parse_positive_integer("TEST_VALUE", None, 42)?, 42);
        Ok(())
    }
}
