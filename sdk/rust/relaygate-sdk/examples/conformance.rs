use std::{
    env,
    fmt::Display,
    future::Future,
    io::{self, Write},
    net::SocketAddr,
    time::Duration,
};

use relaygate_sdk::{Client, Config};
use tokio::time::timeout;

const DEFAULT_RELAY_ADDRESS: &str = "127.0.0.1:7200";
const STAGE_TIMEOUT: Duration = Duration::from_secs(12);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(45);

type AppResult<T> = Result<T, String>;

struct Settings {
    role: String,
    case_name: String,
    endpoint: String,
    target: &'static str,
    relay_address: String,
    client_id: String,
    api_key_id: String,
    api_key: String,
}

#[tokio::main]
async fn main() {
    let case_name = env::var("RELAYGATE_SDK_CASE").unwrap_or_default();
    let result = timeout(PROCESS_TIMEOUT, run()).await;
    match result {
        Ok(Ok(())) => println!("SDK_PASS {case_name}"),
        Ok(Err(error)) => {
            eprintln!("SDK_FAIL {case_name}: {error}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("SDK_FAIL {case_name}: conformance process timeout");
            std::process::exit(124);
        }
    }
}

async fn run() -> AppResult<()> {
    let settings = load_settings()?;
    let config = Config::new(
        format!("http://{}", settings.relay_address),
        &settings.client_id,
        &settings.api_key_id,
        &settings.api_key,
    )
    .with_insecure_local();
    let client = stage("connect", Client::connect(config)).await?;

    match settings.role.as_str() {
        "listener" => run_listener(client, &settings).await,
        "caller" => run_caller(client, &settings).await,
        _ => Err(format!("unsupported role {:?}", settings.role)),
    }
}

fn load_settings() -> AppResult<Settings> {
    let role = required_env("RELAYGATE_SDK_ROLE")?;
    if role != "listener" && role != "caller" {
        return Err("role must be listener or caller".into());
    }
    let case_name = required_env("RELAYGATE_SDK_CASE")?;
    if !safe_case_name(&case_name) {
        return Err("case must contain only lowercase letters, digits, or hyphens".into());
    }
    let relay_address = env::var("RELAYGATE_SDK_RELAY_ADDRESS")
        .unwrap_or_else(|_| DEFAULT_RELAY_ADDRESS.to_string());
    if !loopback_address(&relay_address) {
        return Err("relay address must be a loopback host and port".into());
    }
    Ok(Settings {
        role,
        endpoint: format!("/sdk/conformance/{case_name}"),
        case_name,
        target: "exact",
        relay_address,
        client_id: required_env("RELAYGATE_SDK_CLIENT_ID")?,
        api_key_id: required_env("RELAYGATE_SDK_API_KEY_ID")?,
        api_key: required_env("RELAYGATE_SDK_API_KEY")?,
    })
}

fn loopback_address(value: &str) -> bool {
    if let Ok(address) = value.parse::<SocketAddr>() {
        return address.ip().is_loopback() && address.port() != 0;
    }
    value.rsplit_once(':').is_some_and(|(host, port)| {
        (host.eq_ignore_ascii_case("localhost") || host.eq_ignore_ascii_case("localhost."))
            && port.parse::<u16>().is_ok_and(|port| port != 0)
    })
}

fn required_env(name: &str) -> AppResult<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn safe_case_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

async fn run_listener(client: Client, settings: &Settings) -> AppResult<()> {
    let mut listener = stage(
        "bind",
        client.bind(settings.endpoint.clone(), settings.target),
    )
    .await?;
    println!("SDK_READY {}", settings.case_name);
    io::stdout()
        .flush()
        .map_err(|error| format!("flush ready marker: {error}"))?;

    let offer = stage("next offer", listener.next())
        .await?
        .ok_or_else(|| "listener offer stream ended".to_string())?;
    {
        let metadata = offer.metadata();
        if metadata.attempt_id().is_empty()
            || metadata.listener_binding_id() != listener.binding_id()
            || metadata.caller_session_id().is_empty()
            || metadata.endpoint() != settings.endpoint
            || metadata.target_id() != settings.target
        {
            return Err("offer metadata did not match the exact binding".into());
        }
    }

    let mut pipe = stage("accept", offer.accept()).await?;
    for (index, expected) in caller_frames(&settings.case_name).into_iter().enumerate() {
        let payload = stage(&format!("receive caller frame {}", index + 1), pipe.recv()).await?;
        if payload != expected {
            return Err(format!(
                "caller frame {} = {:?}, want {:?}",
                index + 1,
                payload,
                expected
            ));
        }
    }
    for (index, payload) in listener_frames(&settings.case_name).into_iter().enumerate() {
        stage(
            &format!("send listener frame {}", index + 1),
            pipe.send(payload),
        )
        .await?;
    }
    let _terminal = stage_value("observe pipe terminal", pipe.done()).await?;
    stage("unbind", listener.unbind()).await?;
    stage_value("close client", client.close()).await?;
    Ok(())
}

async fn run_caller(client: Client, settings: &Settings) -> AppResult<()> {
    let mut pipe = stage(
        "open",
        client.open(settings.endpoint.clone(), settings.target),
    )
    .await?;
    for (index, payload) in caller_frames(&settings.case_name).into_iter().enumerate() {
        stage(
            &format!("send caller frame {}", index + 1),
            pipe.send(payload),
        )
        .await?;
    }
    for (index, expected) in listener_frames(&settings.case_name).into_iter().enumerate() {
        let payload = stage(
            &format!("receive listener frame {}", index + 1),
            pipe.recv(),
        )
        .await?;
        if payload != expected {
            return Err(format!(
                "listener frame {} = {:?}, want {:?}",
                index + 1,
                payload,
                expected
            ));
        }
    }
    stage("close pipe", pipe.close()).await?;
    let _terminal = stage_value("observe pipe terminal", pipe.done()).await?;
    stage_value("close client", client.close()).await?;
    Ok(())
}

async fn stage<T, E>(name: &str, operation: impl Future<Output = Result<T, E>>) -> AppResult<T>
where
    E: Display,
{
    match timeout(STAGE_TIMEOUT, operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(format!("{name}: {error}")),
        Err(_) => Err(format!("{name}: stage timeout")),
    }
}

async fn stage_value<T>(name: &str, operation: impl Future<Output = T>) -> AppResult<T> {
    timeout(STAGE_TIMEOUT, operation)
        .await
        .map_err(|_| format!("{name}: stage timeout"))
}

fn caller_frames(case_name: &str) -> [Vec<u8>; 2] {
    [
        format!("caller-frame-1:{case_name}").into_bytes(),
        format!("caller-frame-2:{case_name}").into_bytes(),
    ]
}

fn listener_frames(case_name: &str) -> [Vec<u8>; 2] {
    [
        format!("listener-frame-1:{case_name}").into_bytes(),
        format!("listener-frame-2:{case_name}").into_bytes(),
    ]
}
