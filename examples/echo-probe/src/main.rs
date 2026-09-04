mod config;
mod continuity;
mod probe;

use std::env;

use anyhow::{Context, bail, ensure};

use crate::config::CLIENT_IDS;

const SHARD_ISOLATION_USAGE: &str =
    "expect-shard-isolation <unavailable-client-id> <local-owner-index> <available-client-id>";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    match command()? {
        Command::Single => probe::run_single().await,
        Command::Matrix => probe::run_matrix().await,
        Command::Soak => probe::run_soak().await,
        Command::WaitClient(client_id) => probe::wait_client_registered(&client_id).await,
        Command::ExpectShardIsolation {
            unavailable_client_id,
            local_owner_index,
            available_client_id,
        } => {
            probe::expect_shard_isolation(
                &unavailable_client_id,
                local_owner_index,
                &available_client_id,
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
    Matrix,
    Soak,
    WaitClient(String),
    ExpectShardIsolation {
        unavailable_client_id: String,
        local_owner_index: usize,
        available_client_id: String,
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
        Some("matrix") => Command::Matrix,
        Some("soak") => Command::Soak,
        Some("wait-client") => {
            let Some(client_id) = args.next() else {
                bail!("wait-client requires a ClientId argument");
            };
            Command::WaitClient(client_id)
        }
        Some("expect-shard-isolation") => {
            let Some(unavailable_client_id) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            let Some(local_owner_index) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            let local_owner_index = local_owner_index
                .parse::<usize>()
                .with_context(|| "local-owner-index must be a non-negative integer")?;
            ensure!(
                local_owner_index < CLIENT_IDS.len(),
                "local-owner-index must be in 0..{} (one index per configured Gateway)",
                CLIENT_IDS.len()
            );
            let Some(available_client_id) = args.next() else {
                bail!("usage: {SHARD_ISOLATION_USAGE}");
            };
            Command::ExpectShardIsolation {
                unavailable_client_id,
                local_owner_index,
                available_client_id,
            }
        }
        Some("continuity") => Command::Continuity,
        Some("continuity-check") => Command::ContinuityCheck,
        Some(other) => bail!(
            "unknown command {other:?}; expected single, matrix, soak, wait-client, expect-shard-isolation, continuity, or continuity-check"
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
    fn parses_wait_client_command_with_client_id() -> anyhow::Result<()> {
        match command_from(["wait-client".to_owned(), "echo.b".to_owned()]) {
            Ok(Command::WaitClient(client_id)) => {
                anyhow::ensure!(client_id == "echo.b", "unexpected client id: {client_id}");
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
    fn rejects_wait_client_without_client_id() -> anyhow::Result<()> {
        let error = match command_from(["wait-client".to_owned()]) {
            Ok(command) => anyhow::bail!("unexpected command: {command:?}"),
            Err(error) => error,
        };
        anyhow::ensure!(
            error.to_string().contains("requires a ClientId"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn parses_shard_isolation_command() -> anyhow::Result<()> {
        match command_from([
            "expect-shard-isolation".to_owned(),
            "echo.b".to_owned(),
            "1".to_owned(),
            "echo.c".to_owned(),
        ]) {
            Ok(Command::ExpectShardIsolation {
                unavailable_client_id,
                local_owner_index,
                available_client_id,
            }) => {
                anyhow::ensure!(unavailable_client_id == "echo.b");
                anyhow::ensure!(local_owner_index == 1);
                anyhow::ensure!(available_client_id == "echo.c");
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
            "echo.b".to_owned(),
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
            "echo.b".to_owned(),
            "gateway-b".to_owned(),
            "echo.c".to_owned(),
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
            "echo.b".to_owned(),
            CLIENT_IDS.len().to_string(),
            "echo.c".to_owned(),
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
