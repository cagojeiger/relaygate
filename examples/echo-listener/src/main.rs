use relaygate_sdk::{Config, ListenerRuntime, Pipe};
use tokio::io::{AsyncWriteExt, copy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

async fn echo(pipe: Pipe) -> std::io::Result<()> {
    let (mut reader, mut writer) = pipe.into_split();
    copy(&mut reader, &mut writer).await?;
    writer.shutdown().await
}

fn environment(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
