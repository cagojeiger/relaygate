mod gateway;
mod route_table;

use std::{env, time::Duration};

use anyhow::{Context, Result, bail};
use relaygate_route_table_transport::{GatewayName, InternalGatewayKey};

pub(crate) use gateway::GatewayRuntimeConfig;
pub(crate) use route_table::RouteTableRuntimeConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GatewayCredential {
    pub(super) name: String,
    pub(super) key: String,
}

pub(super) fn parse_gateway_credentials(value: String) -> Result<Vec<GatewayCredential>> {
    if value.is_empty() {
        bail!("RELAYGATE_INTERNAL_GATEWAY_KEYS must contain at least one GatewayName=Key entry");
    }
    let mut parsed: Vec<GatewayCredential> = Vec::new();
    for entry in value.split(',') {
        let Some((name, key)) = entry.split_once('=') else {
            bail!("RELAYGATE_INTERNAL_GATEWAY_KEYS entries must use GatewayName=Key");
        };
        GatewayName::new(name.to_owned())?;
        InternalGatewayKey::new(key.to_owned())?;
        if parsed.iter().any(|credential| credential.name == name) {
            bail!("RELAYGATE_INTERNAL_GATEWAY_KEYS contains duplicate GatewayName {name:?}");
        }
        parsed.push(GatewayCredential {
            name: name.to_owned(),
            key: key.to_owned(),
        });
    }
    Ok(parsed)
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
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::{duration_millis, parse_gateway_credentials};

    #[test]
    fn duration_rejects_zero_milliseconds() {
        assert!(duration_millis("RELAYGATE_STATS_INTERVAL_MS", "0").is_err());
    }

    #[test]
    fn gateway_credentials_are_non_empty_unique_and_preserve_equals() -> anyhow::Result<()> {
        assert!(parse_gateway_credentials(String::new()).is_err());
        assert!(parse_gateway_credentials("gw-a".to_owned()).is_err());
        assert!(parse_gateway_credentials("gw-a=one,gw-a=two".to_owned()).is_err());
        let parsed = parse_gateway_credentials("gw-a=key=with=equals".to_owned())?;
        assert_eq!(parsed[0].name, "gw-a");
        assert_eq!(parsed[0].key, "key=with=equals");
        Ok(())
    }
}
