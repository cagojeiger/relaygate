use std::collections::HashSet;

use anyhow::{Context, bail};
use relaygate_sdk::{
    ClientTlsConfig, Config, DestinationId, GatewayTransportConfig, Listener, Pipe, Relay,
};
use tokio::{
    io::{AsyncWriteExt, copy},
    task::JoinSet,
};

const DEFAULT_DESTINATION: &str = "11111111-1111-4111-8111-111111111111";
const DEFAULT_CLUSTER_TOKEN: &str = "relaygate-local-cluster-token";
const DEFAULT_TLS_CA_PATH: &str = "/etc/relaygate/tls/ca.crt";
const DEFAULT_TLS_SERVER_NAME: &str = "relaygate-gateway.internal";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let cluster_token = environment("RELAYGATE_CLUSTER_TOKEN", DEFAULT_CLUSTER_TOKEN);
    let destinations = destinations_from_env()?;
    let ca_path = environment("RELAYGATE_SDK_TLS_CA_PATH", DEFAULT_TLS_CA_PATH);
    let server_name = environment("RELAYGATE_SDK_TLS_SERVER_NAME", DEFAULT_TLS_SERVER_NAME);
    let tls = ClientTlsConfig::server_authenticated(
        server_name,
        &std::fs::read(&ca_path)
            .with_context(|| format!("failed to read SDK TLS CA at {ca_path:?}"))?,
    )?;
    let relay = Relay::connect(Config::new(
        cluster_token,
        GatewayTransportConfig::tls_tcp(address, tls),
    ))
    .await?;
    let mut tasks = JoinSet::new();

    for destination in destinations {
        let listener = relay
            .listen(destination)
            .await
            .with_context(|| format!("failed to listen on Destination {destination}"))?;
        tasks.spawn(serve_listener(destination, listener));
    }

    let result = tasks
        .join_next()
        .await
        .context("all Listener tasks exited unexpectedly")?;
    result.context("Listener task panicked")?
}

fn init_tracing() -> anyhow::Result<()> {
    let filter = match std::env::var("RELAYGATE_LOG") {
        Ok(value) => tracing_subscriber::EnvFilter::try_new(value)?,
        Err(_) => tracing_subscriber::EnvFilter::new("info"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
}

async fn echo(pipe: Pipe) -> std::io::Result<()> {
    let (mut reader, mut writer) = pipe.into_split();
    copy(&mut reader, &mut writer).await?;
    writer.shutdown().await
}

async fn serve_listener(destination: DestinationId, listener: Listener) -> anyhow::Result<()> {
    loop {
        let pipe = listener
            .accept()
            .await
            .with_context(|| format!("Destination {destination} accept loop stopped"))?;
        tokio::spawn(async move {
            if let Err(error) = echo(pipe).await {
                eprintln!("echo Pipe failed: {error}");
            }
        });
    }
}

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn destinations_from_env() -> anyhow::Result<Vec<DestinationId>> {
    let value = environment("RELAYGATE_DESTINATIONS", DEFAULT_DESTINATION);
    let mut seen = HashSet::new();
    let mut destinations = Vec::new();
    for entry in value.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            bail!("RELAYGATE_DESTINATIONS contains an empty entry");
        }
        let destination: DestinationId = entry
            .parse()
            .with_context(|| format!("invalid UUIDv4 DestinationId {entry:?}"))?;
        if !seen.insert(destination) {
            bail!("RELAYGATE_DESTINATIONS contains duplicate DestinationId {entry:?}");
        }
        destinations.push(destination);
    }
    Ok(destinations)
}

#[cfg(test)]
mod tests {
    use super::destinations_from_env;

    #[test]
    fn default_destination_is_valid() -> anyhow::Result<()> {
        // SAFETY: this test does not mutate the process environment.
        let destinations = destinations_from_env()?;
        anyhow::ensure!(!destinations.is_empty());
        Ok(())
    }
}
