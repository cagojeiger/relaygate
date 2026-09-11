mod authorization;
mod gateway;
mod route_table;
mod transport;

use std::{env, fs, time::Duration};

use anyhow::{Context, Result, bail};
use tokio::time::Instant;

pub(crate) use gateway::GatewayRuntimeConfig;
pub(crate) use route_table::RouteTableRuntimeConfig;
pub(crate) use transport::sdk_tls_enabled;
use transport::{InternalTransport, internal_transport};

pub(super) struct InternalTlsMaterial {
    pub(super) ca: Vec<u8>,
    pub(super) certificate: Vec<u8>,
    pub(super) private_key: Vec<u8>,
}

pub(super) fn insecure_test_transport() -> bool {
    env::var("RELAYGATE_INSECURE_TEST_TRANSPORT")
        .ok()
        .as_deref()
        == Some("true")
}

pub(super) fn load_internal_tls() -> Result<InternalTlsMaterial> {
    let ca_path = env::var("RELAYGATE_INTERNAL_TLS_CA_PATH")
        .context("RELAYGATE_INTERNAL_TLS_CA_PATH is required")?;
    let certificate_path = env::var("RELAYGATE_INTERNAL_TLS_CERT_PATH")
        .context("RELAYGATE_INTERNAL_TLS_CERT_PATH is required")?;
    let private_key_path = env::var("RELAYGATE_INTERNAL_TLS_KEY_PATH")
        .context("RELAYGATE_INTERNAL_TLS_KEY_PATH is required")?;
    Ok(InternalTlsMaterial {
        ca: fs::read(&ca_path)
            .with_context(|| format!("failed to read internal TLS CA at {ca_path:?}"))?,
        certificate: fs::read(&certificate_path).with_context(|| {
            format!("failed to read internal TLS certificate at {certificate_path:?}")
        })?,
        private_key: fs::read(&private_key_path).with_context(|| {
            format!("failed to read internal TLS private key at {private_key_path:?}")
        })?,
    })
}

pub(crate) fn optional_usize(name: &str) -> Result<Option<usize>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(Some(parsed))
}

pub(crate) fn optional_duration_millis(name: &str) -> Result<Option<Duration>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    duration_millis(name, &value).map(Some)
}

fn duration_millis(name: &str, value: &str) -> Result<Duration> {
    let milliseconds = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer number of milliseconds"))?;
    if milliseconds == 0 {
        bail!("{name} must be greater than zero");
    }
    let duration = Duration::from_millis(milliseconds);
    if Instant::now().checked_add(duration).is_none() {
        bail!("{name} is too large to form a monotonic deadline");
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::duration_millis;

    #[test]
    fn duration_rejects_zero_milliseconds() {
        assert!(duration_millis("RELAYGATE_STATS_INTERVAL_MS", "0").is_err());
    }

    #[test]
    fn duration_matches_platform_deadline_representability() {
        let duration = Duration::from_millis(u64::MAX);
        assert_eq!(
            duration_millis("RELAYGATE_STATS_INTERVAL_MS", &u64::MAX.to_string()).is_ok(),
            Instant::now().checked_add(duration).is_some()
        );
    }
}
