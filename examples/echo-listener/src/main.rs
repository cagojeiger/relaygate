use anyhow::Context;
use relaygate_sdk::{
    AccessToken, AccessTokenSource, ClientTlsConfig, Config, Destination, GatewayTransportConfig,
    Listener, Pipe, Relay,
};
use tokio::io::{AsyncWriteExt, copy};

const DEFAULT_TLS_CA_PATH: &str = "/etc/relaygate/tls/ca.crt";
const DEFAULT_TLS_SERVER_NAME: &str = "relaygate-gateway.internal";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let destination = destination_from_env()?;
    let access_token = AccessToken::new(required_environment("RELAYGATE_ACCESS_TOKEN")?)?;
    let ca_path = environment("RELAYGATE_SDK_TLS_CA_PATH", DEFAULT_TLS_CA_PATH);
    let server_name = environment("RELAYGATE_SDK_TLS_SERVER_NAME", DEFAULT_TLS_SERVER_NAME);
    let tls = ClientTlsConfig::server_authenticated(
        server_name,
        &std::fs::read(&ca_path)
            .with_context(|| format!("failed to read SDK TLS CA at {ca_path:?}"))?,
    )?;
    let relay = Relay::connect(Config::with_transport(GatewayTransportConfig::tls_tcp(
        address, tls,
    )))
    .await?;
    let listener = relay
        .listen(
            destination.clone(),
            AccessTokenSource::static_token(access_token),
        )
        .await
        .with_context(|| format!("failed to listen on Destination {destination}"))?;
    serve_listener(destination, listener).await
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

async fn serve_listener(destination: Destination, listener: Listener) -> anyhow::Result<()> {
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

fn required_environment(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

fn destination_from_env() -> anyhow::Result<Destination> {
    parse_destination(&required_environment("RELAYGATE_DESTINATION")?)
}

fn parse_destination(value: &str) -> anyhow::Result<Destination> {
    value
        .parse()
        .with_context(|| format!("invalid Destination {value:?}"))
}

#[cfg(test)]
mod tests {
    use super::parse_destination;

    #[test]
    fn destination_requires_namespace_and_name() -> anyhow::Result<()> {
        assert_eq!(
            parse_destination("examples/echo")?.to_string(),
            "examples/echo"
        );
        assert!(parse_destination("echo").is_err());
        Ok(())
    }
}
