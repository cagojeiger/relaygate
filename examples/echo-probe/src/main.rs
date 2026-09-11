mod chat;
mod config;
mod continuity;
mod latency;
mod overload;
mod probe;
mod soak_dial;

use std::env;

use anyhow::{Context, bail, ensure};

use crate::config::DESTINATIONS;

const SHARD_ISOLATION_USAGE: &str =
    "expect-shard-isolation <unavailable-destination> <local-owner-index> <available-destination>";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    match command()? {
        Command::Single => probe::run_single().await,
        Command::Chat => chat::run_chat().await,
        Command::Matrix => probe::run_matrix().await,
        Command::Soak => probe::run_soak().await,
        Command::Overload => overload::run().await,
        Command::Latency => latency::run().await,
        Command::ReconnectStorm => probe::run_reconnect_storm().await,
        Command::WaitDestination(destination) => {
            probe::wait_destination_available(&destination).await
        }
        Command::ExpectShardIsolation {
            unavailable_destination,
            local_owner_index,
            available_destination,
        } => {
            probe::expect_shard_isolation(
                &unavailable_destination,
                local_owner_index,
                &available_destination,
            )
            .await
        }
        Command::Continuity => continuity::run_continuity().await,
        Command::ContinuityCheck => continuity::check_continuity().await,
    }
}

#[derive(Debug)]
enum Command {
    Single,
    Chat,
    Matrix,
    Soak,
    Overload,
    Latency,
    ReconnectStorm,
    WaitDestination(String),
    ExpectShardIsolation {
        unavailable_destination: String,
        local_owner_index: usize,
        available_destination: String,
    },
    Continuity,
    ContinuityCheck,
}

fn command() -> anyhow::Result<Command> {
    command_from(env::args().skip(1))
}

fn command_from(args: impl IntoIterator<Item = String>) -> anyhow::Result<Command> {
    let mut args = args.into_iter();
    let command = match args.next().as_deref() {
        None | Some("single") => Command::Single,
        Some("chat") => Command::Chat,
        Some("matrix") => Command::Matrix,
        Some("soak") => Command::Soak,
        Some("overload") => Command::Overload,
        Some("latency") => Command::Latency,
        Some("reconnect-storm") => Command::ReconnectStorm,
        Some("wait-destination") => {
            let Some(destination) = args.next() else {
                bail!("wait-destination requires a Destination argument");
            };
            Command::WaitDestination(destination)
        }
        Some("expect-shard-isolation") => {
            let Some(unavailable_destination) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            let Some(local_owner_index) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            let local_owner_index = local_owner_index
                .parse::<usize>()
                .with_context(|| "local-owner-index must be a non-negative integer")?;
            ensure!(
                local_owner_index < DESTINATIONS.len(),
                "local-owner-index must be in 0..{} (one index per configured Gateway)",
                DESTINATIONS.len()
            );
            let Some(available_destination) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            Command::ExpectShardIsolation {
                unavailable_destination,
                local_owner_index,
                available_destination,
            }
        }
        Some("continuity") => Command::Continuity,
        Some("continuity-check") => Command::ContinuityCheck,
        Some(other) => bail!(
            "unknown command {other:?}; expected single, chat, matrix, soak, overload, latency, reconnect-storm, wait-destination, expect-shard-isolation, continuity, or continuity-check"
        ),
    };
    if args.next().is_some() {
        bail!("probe command does not accept extra arguments");
    }
    Ok(command)
}

fn init_tracing() -> anyhow::Result<()> {
    let filter = match env::var("RELAYGATE_LOG") {
        Ok(value) => tracing_subscriber::EnvFilter::try_new(value)?,
        Err(_) => tracing_subscriber::EnvFilter::new("info"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wait_destination_command_with_destination() -> anyhow::Result<()> {
        match command_from(["wait-destination".to_owned(), "examples/echo-b".to_owned()]) {
            Ok(Command::WaitDestination(destination)) => {
                anyhow::ensure!(
                    destination == "examples/echo-b",
                    "unexpected destination: {destination}"
                );
            }
            Ok(other) => anyhow::bail!("unexpected command: {other:?}"),
            Err(error) => anyhow::bail!("unexpected error: {error}"),
        }
        Ok(())
    }

    #[test]
    fn parses_soak_command() -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            command_from(["soak".to_owned()]),
            Ok(Command::Soak)
        ));
        Ok(())
    }

    #[test]
    fn parses_overload_command() -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            command_from(["overload".to_owned()]),
            Ok(Command::Overload)
        ));
        Ok(())
    }

    #[test]
    fn parses_chat_command() -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            command_from(["chat".to_owned()]),
            Ok(Command::Chat)
        ));
        Ok(())
    }

    #[test]
    fn parses_reconnect_storm_command() -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            command_from(["reconnect-storm".to_owned()]),
            Ok(Command::ReconnectStorm)
        ));
        Ok(())
    }

    #[test]
    fn rejects_wait_destination_without_destination() -> anyhow::Result<()> {
        let error = match command_from(["wait-destination".to_owned()]) {
            Ok(command) => anyhow::bail!("unexpected command: {command:?}"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error.to_string().contains("requires a Destination"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn parses_shard_isolation_command() -> anyhow::Result<()> {
        match command_from([
            "expect-shard-isolation".to_owned(),
            "examples/echo-b".to_owned(),
            "1".to_owned(),
            "examples/echo-c".to_owned(),
        ]) {
            Ok(Command::ExpectShardIsolation {
                unavailable_destination,
                local_owner_index,
                available_destination,
            }) => {
                anyhow::ensure!(unavailable_destination == "examples/echo-b");
                anyhow::ensure!(local_owner_index == 1);
                anyhow::ensure!(available_destination == "examples/echo-c");
            }
            Ok(other) => anyhow::bail!("unexpected command: {other:?}"),
            Err(error) => anyhow::bail!("unexpected error: {error}"),
        }
        Ok(())
    }

    #[test]
    fn rejects_shard_isolation_with_missing_arguments() -> anyhow::Result<()> {
        let error = match command_from([
            "expect-shard-isolation".to_owned(),
            "examples/echo-b".to_owned(),
            "1".to_owned(),
        ]) {
            Ok(command) => anyhow::bail!("unexpected command: {command:?}"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error.to_string().contains(SHARD_ISOLATION_USAGE),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn rejects_non_numeric_shard_isolation_owner_index() -> anyhow::Result<()> {
        let error = match command_from([
            "expect-shard-isolation".to_owned(),
            "examples/echo-b".to_owned(),
            "gateway-b".to_owned(),
            "examples/echo-c".to_owned(),
        ]) {
            Ok(command) => anyhow::bail!("unexpected command: {command:?}"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error
                .to_string()
                .contains("local-owner-index must be a non-negative integer"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn rejects_out_of_range_shard_isolation_owner_index() -> anyhow::Result<()> {
        let error = match command_from([
            "expect-shard-isolation".to_owned(),
            "examples/echo-b".to_owned(),
            DESTINATIONS.len().to_string(),
            "examples/echo-c".to_owned(),
        ]) {
            Ok(command) => anyhow::bail!("unexpected command: {command:?}"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error.to_string().contains("must be in 0..3"),
            "unexpected error: {error}"
        );
        Ok(())
    }
}
