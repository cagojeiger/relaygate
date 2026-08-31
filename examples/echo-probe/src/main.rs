mod config;
mod continuity;
mod probe;

use std::env;

use anyhow::bail;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    match command()? {
        Command::Single => probe::run_single().await,
        Command::Matrix => probe::run_matrix().await,
        Command::WaitClient(client_id) => probe::wait_client_registered(&client_id).await,
        Command::ExpectRouteTableUnavailable => probe::expect_route_table_unavailable().await,
        Command::Continuity => continuity::run_continuity().await,
        Command::ContinuityCheck => continuity::check_continuity().await,
    }
}

#[derive(Debug)]
enum Command {
    Single,
    Matrix,
    WaitClient(String),
    ExpectRouteTableUnavailable,
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
        Some("wait-client") => {
            let Some(client_id) = args.next() else {
                bail!("wait-client requires a ClientId argument");
            };
            Command::WaitClient(client_id)
        }
        Some("expect-rt-unavailable") => Command::ExpectRouteTableUnavailable,
        Some("continuity") => Command::Continuity,
        Some("continuity-check") => Command::ContinuityCheck,
        Some(other) => bail!(
            "unknown command {other:?}; expected single, matrix, wait-client, expect-rt-unavailable, continuity, or continuity-check"
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
}
