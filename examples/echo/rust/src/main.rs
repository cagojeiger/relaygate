use std::{env, future::Future, time::Duration};

use relaygate_sdk::{Config, ManagedClient, Pipe};
use tokio::time::timeout;

const DEFAULT_ADDRESS: &str = "127.0.0.1:27420";
const DEFAULT_CLIENT_ID: &str = "local-development";
const DEFAULT_API_KEY_ID: &str = "primary";
const DEFAULT_ENDPOINT: &str = "/examples/echo";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(12);

type AppResult<T> = Result<T, String>;

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve { target: String },
    Send { target: String, message: String },
}

struct Settings {
    address: String,
    client_id: String,
    api_key_id: String,
    api_key: String,
    endpoint: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("relaygate echo: {error}");
        std::process::exit(1);
    }
}

async fn run() -> AppResult<()> {
    let command = parse_command(env::args().skip(1).collect())?;
    let settings = load_settings()?;
    match command {
        Command::Serve { target } => serve(settings, target).await,
        Command::Send { target, message } => send(settings, target, message.into_bytes()).await,
    }
}

fn parse_command(arguments: Vec<String>) -> AppResult<Command> {
    if arguments.len() < 2 {
        return Err("usage: relaygate-echo serve <target> | send <target> <message>".into());
    }
    if !valid_target(&arguments[1]) {
        return Err("target must contain only letters, digits, '.', '-', or '_'".into());
    }
    match arguments[0].as_str() {
        "serve" if arguments.len() == 2 => Ok(Command::Serve {
            target: arguments[1].clone(),
        }),
        "serve" => Err("usage: relaygate-echo serve <target>".into()),
        "send" if arguments.len() >= 3 => {
            let message = arguments[2..].join(" ");
            if message.is_empty() {
                return Err("message must not be empty".into());
            }
            Ok(Command::Send {
                target: arguments[1].clone(),
                message,
            })
        }
        "send" => Err("usage: relaygate-echo send <target> <message>".into()),
        command => Err(format!("unknown command {command:?}")),
    }
}

fn valid_target(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-' || byte == b'_'
        })
}

fn load_settings() -> AppResult<Settings> {
    Ok(Settings {
        address: env_or_default("RELAYGATE_ECHO_ADDRESS", DEFAULT_ADDRESS),
        client_id: env_or_default("RELAYGATE_ECHO_CLIENT_ID", DEFAULT_CLIENT_ID),
        api_key_id: env_or_default("RELAYGATE_ECHO_API_KEY_ID", DEFAULT_API_KEY_ID),
        api_key: required_env("RELAYGATE_ECHO_API_KEY")?,
        endpoint: env_or_default("RELAYGATE_ECHO_ENDPOINT", DEFAULT_ENDPOINT),
    })
}

fn env_or_default(name: &str, fallback: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn required_env(name: &str) -> AppResult<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

async fn connect(settings: &Settings) -> AppResult<ManagedClient> {
    let config = Config::new(
        format!("http://{}", settings.address),
        &settings.client_id,
        &settings.api_key_id,
        &settings.api_key,
    )
    .with_insecure_local();
    stage("connect", ManagedClient::connect(config)).await
}

async fn serve(settings: Settings, target: String) -> AppResult<()> {
    let client = connect(&settings).await?;
    let mut listener = stage("bind", client.bind(&settings.endpoint, &target)).await?;
    println!("ECHO_READY {target}");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        let offer = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            result = listener.next() => result
                .map_err(|error| format!("next offer: {error}"))?
                .ok_or_else(|| "listener offer stream ended".to_string())?,
        };
        let pipe = stage("accept", offer.accept()).await?;
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            result = echo_pipe(pipe) => result?,
        }
    }

    stage("unbind", listener.unbind()).await?;
    stage_value("close client", client.close()).await?;
    Ok(())
}

async fn echo_pipe(mut pipe: Pipe) -> AppResult<()> {
    loop {
        let payload = match pipe.recv().await {
            Ok(payload) => payload,
            Err(_) => return Ok(()),
        };
        stage("echo payload", pipe.send(payload)).await?;
    }
}

async fn send(settings: Settings, target: String, message: Vec<u8>) -> AppResult<()> {
    let client = connect(&settings).await?;
    let mut pipe = stage("open", client.open(&settings.endpoint, &target)).await?;
    stage("send", pipe.send(message.clone())).await?;
    let reply = stage("receive", pipe.recv()).await?;
    if reply != message {
        return Err(format!(
            "reply {:?} does not match message {:?}",
            String::from_utf8_lossy(&reply),
            String::from_utf8_lossy(&message)
        ));
    }
    stage("close pipe", pipe.close()).await?;
    let _terminal = stage_value("observe pipe terminal", pipe.done()).await?;
    println!("ECHO_REPLY {}", String::from_utf8_lossy(&reply));
    stage_value("close client", client.close()).await?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn stage<T, E>(name: &str, operation: impl Future<Output = Result<T, E>>) -> AppResult<T>
where
    E: std::fmt::Display,
{
    match timeout(OPERATION_TIMEOUT, operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(format!("{name}: {error}")),
        Err(_) => Err(format!("{name}: timeout")),
    }
}

async fn stage_value<T>(name: &str, operation: impl Future<Output = T>) -> AppResult<T> {
    timeout(OPERATION_TIMEOUT, operation)
        .await
        .map_err(|_| format!("{name}: timeout"))
}

#[cfg(test)]
mod tests {
    use super::{Command, parse_command};

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn parses_serve() {
        assert_eq!(
            parse_command(args(&["serve", "rust"])),
            Ok(Command::Serve {
                target: "rust".into()
            })
        );
    }

    #[test]
    fn parses_send_and_joins_message_arguments() {
        assert_eq!(
            parse_command(args(&["send", "go", "hello", "world"])),
            Ok(Command::Send {
                target: "go".into(),
                message: "hello world".into()
            })
        );
    }

    #[test]
    fn rejects_invalid_commands() {
        assert!(parse_command(args(&["send", "go"])).is_err());
        assert!(parse_command(args(&["serve", "bad target"])).is_err());
        assert!(parse_command(args(&["chat", "go"])).is_err());
    }
}
