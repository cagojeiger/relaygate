mod gateway;
mod route_table;

use std::{env, time::Duration};

use anyhow::{Context, Result, bail};

pub(crate) use gateway::GatewayRuntimeConfig;
pub(crate) use route_table::RouteTableRuntimeConfig;

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
    use super::duration_millis;

    #[test]
    fn duration_rejects_zero_milliseconds() {
        assert!(duration_millis("RELAYGATE_STATS_INTERVAL_MS", "0").is_err());
    }
}
