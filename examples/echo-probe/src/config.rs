use std::{env, path::PathBuf, time::Duration};

use anyhow::ensure;

pub(crate) const CLIENT_IDS: [&str; 3] = ["echo.a", "echo.b", "echo.c"];
pub(crate) const SHARED_CLIENT_ID: &str = "echo.shared";
pub(crate) const CONCURRENT_PIPES_PER_PATH: usize = 32;
pub(crate) const ECHO_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const ROUTE_WAIT: Duration = Duration::from_secs(20);
pub(crate) const CONTINUITY_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const CONTINUITY_FRESHNESS: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_SOAK_DURATION: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_SOAK_CONCURRENCY: usize = 64;

const DEFAULT_GATEWAYS: &str = "gateway-a:27420,gateway-b:27420,gateway-c:27420";
const DEFAULT_CONTINUITY_STATE: &str = "/tmp/relaygate-continuity.state";

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
        addresses.len() == CLIENT_IDS.len(),
        "RELAYGATE_GATEWAYS must contain exactly three comma-separated addresses"
    );
    Ok(addresses)
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

fn positive_integer(name: &str, default: u64) -> anyhow::Result<u64> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name} must be a positive integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    ensure!(value > 0, "{name} must be greater than zero");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_integer_rejects_zero() -> anyhow::Result<()> {
        let error = match positive_integer("TEST_VALUE", 0) {
            Ok(value) => anyhow::bail!("unexpected value: {value}"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("greater than zero"));
        Ok(())
    }
}
