use std::{collections::HashMap, env, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use relaygate_gateway::{Gateway, GatewayConfig, check};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const DEFAULT_BIND_ADDRESS: &str = "0.0.0.0:27420";
const DEFAULT_CHECK_DEADLINE: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;
    match command()? {
        Command::Serve => serve().await,
        Command::Check { address } => check(address, DEFAULT_CHECK_DEADLINE)
            .await
            .context("Gateway health check failed"),
    }
}

async fn serve() -> Result<()> {
    let bind_address =
        env::var("RELAYGATE_BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
    let client_keys = parse_client_keys(env::var("RELAYGATE_CLIENT_KEYS").unwrap_or_default())?;
    let client_count = client_keys.len();
    let writer_capacity = parse_optional_usize("RELAYGATE_WRITER_QUEUE_CAPACITY")?;
    let max_frame_len = parse_optional_usize("RELAYGATE_MAX_FRAME_LEN")?;
    let max_sessions = parse_optional_usize("RELAYGATE_MAX_SESSIONS")?;
    let max_bindings = parse_optional_usize("RELAYGATE_MAX_BINDINGS")?;
    let max_pending_offers = parse_optional_usize("RELAYGATE_MAX_PENDING_OFFERS")?;
    let max_live_pipes = parse_optional_usize("RELAYGATE_MAX_LIVE_PIPES")?;
    let offer_timeout = parse_optional_duration_millis("RELAYGATE_OFFER_TIMEOUT_MS")?;
    let stats_interval = parse_optional_duration_millis("RELAYGATE_STATS_INTERVAL_MS")?;

    let mut config = GatewayConfig::new(client_keys);
    if let Some(capacity) = writer_capacity {
        config = config.with_writer_queue_capacity(capacity);
    }
    if let Some(maximum) = max_frame_len {
        config = config.with_max_frame_len(maximum);
    }
    if let Some(maximum) = max_sessions {
        config = config.with_max_sessions(maximum);
    }
    if let Some(maximum) = max_bindings {
        config = config.with_max_bindings(maximum);
    }
    if let Some(maximum) = max_pending_offers {
        config = config.with_max_pending_offers(maximum);
    }
    if let Some(maximum) = max_live_pipes {
        config = config.with_max_live_pipes(maximum);
    }
    if let Some(timeout) = offer_timeout {
        config = config.with_offer_timeout(timeout);
    }
    let gateway = Gateway::new(config)?;
    let listener = TcpListener::bind(&bind_address)
        .await
        .with_context(|| format!("failed to bind Gateway at {bind_address}"))?;
    let local_address = listener.local_addr()?;
    tracing::info!(
        component = "server",
        event = "server.started",
        address = %local_address,
        configured_clients = client_count,
        "RelayGate Gateway started"
    );

    let shutdown = CancellationToken::new();
    if let Some(interval) = stats_interval {
        log_gateway_snapshot(&gateway);
        let stats_gateway = gateway.clone();
        let stats_shutdown = shutdown.clone();
        tokio::spawn(async move {
            log_gateway_stats(stats_gateway, stats_shutdown, interval).await;
        });
    }
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_shutdown.cancel();
    });
    gateway.serve(listener, shutdown).await?;
    tracing::info!(
        component = "server",
        event = "server.stopped",
        "RelayGate Gateway stopped"
    );
    Ok(())
}

fn command() -> Result<Command> {
    let mut args = env::args().skip(1);
    let command = match args.next() {
        None => Command::Serve,
        Some(value) if value == "check" => {
            let Some(address) = args.next() else {
                bail!("usage: relaygate-server check <address>");
            };
            if args.next().is_some() {
                bail!("usage: relaygate-server check <address>");
            }
            Command::Check { address }
        }
        Some(value) => bail!("unknown command {value:?}; expected `check <address>`"),
    };
    Ok(command)
}

fn parse_client_keys(value: String) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();
    if value.is_empty() {
        return Ok(keys);
    }
    for entry in value.split(',') {
        let Some((client_id, client_key)) = entry.split_once('=') else {
            bail!("RELAYGATE_CLIENT_KEYS entries must use ClientId=ClientKey");
        };
        if client_id.is_empty() || client_key.is_empty() {
            bail!("RELAYGATE_CLIENT_KEYS entries require non-empty ClientId and ClientKey");
        }
        if keys
            .insert(client_id.to_owned(), client_key.to_owned())
            .is_some()
        {
            bail!("RELAYGATE_CLIENT_KEYS contains duplicate ClientId {client_id:?}");
        }
    }
    Ok(keys)
}

fn parse_optional_usize(name: &str) -> Result<Option<usize>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(Some(parsed))
}

fn parse_optional_duration_millis(name: &str) -> Result<Option<Duration>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    parse_duration_millis_value(name, &value).map(Some)
}

fn parse_duration_millis_value(name: &str, value: &str) -> Result<Duration> {
    let milliseconds = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer number of milliseconds"))?;
    if milliseconds == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(Duration::from_millis(milliseconds))
}

fn init_tracing() -> Result<()> {
    let filter = match env::var("RELAYGATE_LOG") {
        Ok(value) => {
            EnvFilter::try_new(value).context("RELAYGATE_LOG is not a valid log filter")?
        }
        Err(_) => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };
    let log_format = parse_log_format(env::var("RELAYGATE_LOG_FORMAT").ok())?;
    match log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing: {error}")),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter)
            .with_target(false)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing: {error}")),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum LogFormat {
    #[default]
    Text,
    Json,
}

fn parse_log_format(value: Option<String>) -> Result<LogFormat> {
    match value.as_deref().unwrap_or("text") {
        "text" => Ok(LogFormat::Text),
        "json" => Ok(LogFormat::Json),
        other => bail!("RELAYGATE_LOG_FORMAT must be `text` or `json`, got {other:?}"),
    }
}

async fn log_gateway_stats(
    gateway: Gateway,
    shutdown: CancellationToken,
    interval_duration: Duration,
) {
    let start = tokio::time::Instant::now() + interval_duration;
    let mut interval = tokio::time::interval_at(start, interval_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                log_gateway_snapshot(&gateway);
            }
        }
    }
}

fn log_gateway_snapshot(gateway: &Gateway) {
    let snapshot = gateway.snapshot();
    tracing::info!(
        component = "gateway",
        event = "gateway.snapshot",
        sessions = snapshot.sessions,
        listener_sessions = snapshot.listener_sessions,
        connector_sessions = snapshot.connector_sessions,
        listener_bindings = snapshot.listener_bindings,
        pending_offers = snapshot.pending_offers,
        live_pipes = snapshot.live_pipes,
        "Gateway current-state snapshot"
    );
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::warn!(
                                component = "server",
                                event = "server.signal_listener_failed",
                                signal = "SIGINT",
                                %error,
                                "failed to listen for Ctrl-C"
                            );
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(
                    component = "server",
                    event = "server.signal_listener_failed",
                    signal = "SIGTERM",
                    %error,
                    "failed to listen for SIGTERM"
                );
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(
            component = "server",
            event = "server.signal_listener_failed",
            signal = "SIGINT",
            %error,
            "failed to listen for Ctrl-C"
        );
    }
}

enum Command {
    Serve,
    Check { address: String },
}

#[cfg(test)]
mod tests {
    use super::{LogFormat, parse_client_keys, parse_duration_millis_value, parse_log_format};

    #[test]
    fn client_key_config_rejects_duplicate_client_ids() {
        assert!(parse_client_keys("echo.alpha=one,echo.alpha=two".to_owned()).is_err());
    }

    #[test]
    fn client_key_config_preserves_exact_values() -> Result<(), Box<dyn std::error::Error>> {
        let keys = parse_client_keys("echo.alpha=Key=With=Equals".to_owned())?;
        assert_eq!(
            keys.get("echo.alpha").map(String::as_str),
            Some("Key=With=Equals")
        );
        Ok(())
    }

    #[test]
    fn log_format_accepts_text_and_json_only() {
        assert_eq!(parse_log_format(None).unwrap_or_default(), LogFormat::Text);
        assert_eq!(
            parse_log_format(Some("json".to_owned())).unwrap_or_default(),
            LogFormat::Json
        );
        assert!(parse_log_format(Some("xml".to_owned())).is_err());
    }

    #[test]
    fn stats_interval_rejects_zero_milliseconds() {
        assert!(parse_duration_millis_value("RELAYGATE_STATS_INTERVAL_MS", "0").is_err());
    }
}
