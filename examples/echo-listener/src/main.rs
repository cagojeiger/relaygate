use std::collections::HashSet;

use anyhow::{Context, bail};
use relaygate_sdk::{Config, Listener, ListenerRuntime, Pipe};
use tokio::io::{AsyncWriteExt, copy};
use tokio::task::JoinSet;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ListenerConfig {
    client_id: String,
    client_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing()?;
    let address = environment("RELAYGATE_ADDR", "gateway:27420");
    let listeners = listener_configs_from_env()?;

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    let mut tasks = JoinSet::new();

    for listener_config in listeners {
        let client_id = listener_config.client_id;
        let listener = runtime
            .listen(client_id.clone(), listener_config.client_key)
            .await
            .with_context(|| format!("failed to listen for ClientId {client_id}"))?;
        tasks.spawn(serve_listener(client_id, listener));
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

async fn serve_listener(client_id: String, listener: Listener) -> anyhow::Result<()> {
    loop {
        let pipe = listener
            .accept()
            .await
            .with_context(|| format!("Listener {client_id} accept loop stopped"))?;
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

fn listener_configs_from_env() -> anyhow::Result<Vec<ListenerConfig>> {
    match std::env::var("RELAYGATE_LISTENERS") {
        Ok(value) => parse_listener_configs(&value),
        Err(_) => Ok(vec![ListenerConfig {
            client_id: environment("RELAYGATE_CLIENT_ID", "echo.alpha"),
            client_key: environment("RELAYGATE_CLIENT_KEY", "dev-echo-alpha-v1"),
        }]),
    }
}

fn parse_listener_configs(value: &str) -> anyhow::Result<Vec<ListenerConfig>> {
    let mut seen = HashSet::new();
    let mut listeners = Vec::new();

    for entry in value.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            bail!("RELAYGATE_LISTENERS contains an empty entry");
        }
        let (client_id, client_key) = entry.split_once('=').with_context(|| {
            format!("RELAYGATE_LISTENERS entry {entry:?} must be ClientId=ClientKey")
        })?;
        let client_id = client_id.trim();
        let client_key = client_key.trim();
        if client_id.is_empty() || client_key.is_empty() {
            bail!(
                "RELAYGATE_LISTENERS entry {entry:?} must not contain an empty ClientId or ClientKey"
            );
        }
        if !seen.insert(client_id.to_owned()) {
            bail!("RELAYGATE_LISTENERS contains duplicate ClientId {client_id:?}");
        }
        listeners.push(ListenerConfig {
            client_id: client_id.to_owned(),
            client_key: client_key.to_owned(),
        });
    }

    if listeners.is_empty() {
        bail!("RELAYGATE_LISTENERS must contain at least one ClientId=ClientKey entry");
    }

    Ok(listeners)
}

#[cfg(test)]
mod tests {
    use super::{ListenerConfig, parse_listener_configs};

    #[test]
    fn parses_multiple_listener_configs() -> Result<(), String> {
        let listeners = match parse_listener_configs(" echo.alpha = key-a , echo.beta=key-b ") {
            Ok(listeners) => listeners,
            Err(error) => return Err(format!("valid listener list failed: {error}")),
        };

        assert_eq!(
            listeners,
            vec![
                ListenerConfig {
                    client_id: "echo.alpha".to_owned(),
                    client_key: "key-a".to_owned(),
                },
                ListenerConfig {
                    client_id: "echo.beta".to_owned(),
                    client_key: "key-b".to_owned(),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn rejects_empty_entries() -> Result<(), String> {
        let error = parse_error("echo.alpha=key-a,")?;

        assert!(error.to_string().contains("empty entry"));
        Ok(())
    }

    #[test]
    fn rejects_missing_separator() -> Result<(), String> {
        let error = parse_error("echo.alpha")?;

        assert!(error.to_string().contains("ClientId=ClientKey"));
        Ok(())
    }

    #[test]
    fn rejects_empty_fields() -> Result<(), String> {
        let error = parse_error("echo.alpha=")?;

        assert!(error.to_string().contains("must not contain an empty"));
        Ok(())
    }

    #[test]
    fn rejects_duplicate_client_ids() -> Result<(), String> {
        let error = parse_error("echo.alpha=key-a,echo.alpha=key-b")?;

        assert!(error.to_string().contains("duplicate ClientId"));
        Ok(())
    }

    fn parse_error(value: &str) -> Result<anyhow::Error, String> {
        match parse_listener_configs(value) {
            Ok(listeners) => Err(format!("invalid listener list parsed as {listeners:?}")),
            Err(error) => Ok(error),
        }
    }
}
