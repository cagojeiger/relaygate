use std::{env, path::PathBuf, time::Duration};

use anyhow::ensure;

pub(crate) const CLIENT_IDS: [&str; 3] = ["echo.a", "echo.b", "echo.c"];
pub(crate) const SHARED_CLIENT_ID: &str = "echo.shared";
pub(crate) const CONCURRENT_PIPES_PER_PATH: usize = 32;
pub(crate) const ECHO_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const ROUTE_WAIT: Duration = Duration::from_secs(20);
pub(crate) const CONTINUITY_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const CONTINUITY_FRESHNESS: Duration = Duration::from_secs(2);

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
