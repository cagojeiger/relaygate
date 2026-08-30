use relaygate_sdk::{Config, ListenerRuntime, Pipe};
use tokio::io::{AsyncWriteExt, copy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let client_id = environment("RELAYGATE_CLIENT_ID", "echo.alpha");
    let client_key = environment("RELAYGATE_CLIENT_KEY", "dev-echo-alpha-v1");

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    let listener = runtime.listen(client_id, client_key).await?;

    loop {
        let pipe = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = echo(pipe).await {
                eprintln!("echo Pipe failed: {error}");
            }
        });
    }
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

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
