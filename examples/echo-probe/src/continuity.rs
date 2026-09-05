use std::{
    fs,
    path::Path,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail, ensure};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use crate::{
    config::{
        CONTINUITY_FRESHNESS, CONTINUITY_INTERVAL, ECHO_DEADLINE, ROUTE_WAIT,
        continuity_state_path, environment,
    },
    probe::{connect, dial_when_available},
};

pub(crate) async fn run_continuity() -> anyhow::Result<()> {
    let address = environment("RELAYGATE_CONTINUITY_ADDR", "gateway-a:27420");
    let destination_id = environment(
        "RELAYGATE_CONTINUITY_DESTINATION_ID",
        crate::config::DESTINATION_IDS[2],
    );
    let state_path = continuity_state_path();
    let connector = connect(&address).await?;
    let mut pipe = dial_when_available(&connector, &destination_id, ROUTE_WAIT).await?;
    let mut sequence = 1_u64;

    loop {
        let payload = continuity_payload(sequence);
        timeout(ECHO_DEADLINE, async {
            pipe.write_all(&payload).await?;
            let mut echoed = [0_u8; 32];
            pipe.read_exact(&mut echoed).await?;
            if echoed != payload {
                return Err(std::io::Error::other("continuity echo mismatch"));
            }
            Ok::<_, std::io::Error>(())
        })
        .await
        .context("continuity Pipe round-trip timed out")??;
        write_continuity_state(&state_path, sequence)?;
        sequence = sequence
            .checked_add(1)
            .context("continuity sequence exhausted")?;
        tokio::time::sleep(CONTINUITY_INTERVAL).await;
    }
}

pub(crate) async fn check_continuity() -> anyhow::Result<()> {
    let state_path = continuity_state_path();
    let first = wait_for_continuity_state(&state_path).await?;
    ensure_fresh(first)?;

    let deadline = Instant::now() + CONTINUITY_FRESHNESS;
    loop {
        tokio::time::sleep(CONTINUITY_INTERVAL).await;
        let current = read_continuity_state(&state_path)?;
        ensure_fresh(current)?;
        if current.sequence > first.sequence {
            println!(
                "continuity Pipe advanced from {} to {}",
                first.sequence, current.sequence
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("continuity Pipe did not advance within the freshness deadline");
        }
    }
}

#[derive(Clone, Copy)]
struct ContinuityState {
    sequence: u64,
    unix_millis: u128,
}

fn continuity_payload(sequence: u64) -> [u8; 32] {
    let mut payload = [0_u8; 32];
    payload[..8].copy_from_slice(&sequence.to_be_bytes());
    for (index, byte) in payload[8..].iter_mut().enumerate() {
        *byte = ((sequence as usize).wrapping_add(index).wrapping_mul(37) % 256) as u8;
    }
    payload
}

fn write_continuity_state(path: &Path, sequence: u64) -> anyhow::Result<()> {
    let state = format!("{} {}\n", sequence, unix_millis()?);
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, state).with_context(|| {
        format!(
            "failed to write continuity state at {}",
            temporary.display()
        )
    })?;
    fs::rename(&temporary, path)
        .with_context(|| format!("failed to publish continuity state at {}", path.display()))
}

async fn wait_for_continuity_state(path: &Path) -> anyhow::Result<ContinuityState> {
    let deadline = Instant::now() + ROUTE_WAIT;
    loop {
        match read_continuity_state(path) {
            Ok(state) => return Ok(state),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(CONTINUITY_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn read_continuity_state(path: &Path) -> anyhow::Result<ContinuityState> {
    let value = fs::read_to_string(path)
        .with_context(|| format!("failed to read continuity state at {}", path.display()))?;
    let mut fields = value.split_whitespace();
    let sequence = fields
        .next()
        .context("continuity state is missing sequence")?
        .parse::<u64>()
        .context("continuity sequence is invalid")?;
    let unix_millis = fields
        .next()
        .context("continuity state is missing timestamp")?
        .parse::<u128>()
        .context("continuity timestamp is invalid")?;
    ensure!(
        fields.next().is_none(),
        "continuity state has trailing fields"
    );
    Ok(ContinuityState {
        sequence,
        unix_millis,
    })
}

fn ensure_fresh(state: ContinuityState) -> anyhow::Result<()> {
    let age = unix_millis()?.saturating_sub(state.unix_millis);
    ensure!(
        age <= CONTINUITY_FRESHNESS.as_millis(),
        "continuity state is stale by {age}ms"
    );
    Ok(())
}

fn unix_millis() -> anyhow::Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX epoch")?
        .as_millis())
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn continuity_state_parser_rejects_trailing_fields() -> anyhow::Result<()> {
        let path = env::temp_dir().join(format!(
            "relaygate-continuity-parser-{}",
            std::process::id()
        ));
        fs::write(&path, "1 2 trailing\n")?;
        let result = read_continuity_state(&path);
        let _ = fs::remove_file(path);
        assert!(result.is_err());
        Ok(())
    }
}
