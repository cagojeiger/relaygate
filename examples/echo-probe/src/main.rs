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
        Command::ExpectRouteTableUnavailable => probe::expect_route_table_unavailable().await,
        Command::Continuity => continuity::run_continuity().await,
        Command::ContinuityCheck => continuity::check_continuity().await,
    }
}

#[derive(Clone, Copy)]
enum Command {
    Single,
    Matrix,
    ExpectRouteTableUnavailable,
    Continuity,
    ContinuityCheck,
}

fn command() -> anyhow::Result<Command> {
    let mut args = env::args().skip(1);
    let command = match args.next().as_deref() {
        None | Some("single") => Command::Single,
        Some("matrix") => Command::Matrix,
        Some("expect-rt-unavailable") => Command::ExpectRouteTableUnavailable,
        Some("continuity") => Command::Continuity,
        Some("continuity-check") => Command::ContinuityCheck,
        Some(other) => bail!(
            "unknown command {other:?}; expected single, matrix, expect-rt-unavailable, continuity, or continuity-check"
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
