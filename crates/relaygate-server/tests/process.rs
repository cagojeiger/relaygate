use std::{
    error::Error,
    io,
    net::TcpListener,
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::io::Read;
#[cfg(unix)]
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use relaygate_route_table::{ClientId, GatewayId, ShardDirectory};
#[cfg(unix)]
use relaygate_route_table_transport::{
    ErrorCode as RouteTableErrorCode, GatewayName, InternalGatewayKey, RouteTableClient,
    RouteTableClientConfig,
};

const STARTUP_DEADLINE: Duration = Duration::from_secs(5);
#[cfg(unix)]
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[cfg(unix)]
#[test]
fn server_boots_health_checks_and_exits_on_sigterm() -> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let mut server = ChildGuard::spawn(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", "echo.alpha=test-key"),
    )?;

    wait_until_healthy(&address, &mut server)?;

    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");

    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "server did not exit before the shutdown deadline",
        )
    })?;
    assert!(
        exit_status.success(),
        "server exited unsuccessfully after SIGTERM: {exit_status}"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_table_role_starts_ready_empty_hides_key_and_exits_on_sigterm()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let directory = ShardDirectory::from_json_bytes(ShardDirectoryArtifact::BYTES)?;
    let secret = "must-not-appear-route-table-key";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .arg("route-table")
            .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
            .env("RELAYGATE_RT_BIND_ADDR", &address)
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
            .env("RELAYGATE_RT_SHARD_ID", "rt-0")
            .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", format!("gw-a={secret}"))
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json"),
    )?;

    let client = wait_until_route_table_ready(&address, secret, &mut server).await?;
    let error = match client
        .resolve(directory.generation(), &ClientId::new("missing")?)
        .await
    {
        Ok(_) => {
            return Err(io::Error::other(
                "a READY-empty RouteTable unexpectedly resolved a missing ClientId",
            )
            .into());
        }
        Err(error) => error,
    };
    assert_eq!(error.code(), RouteTableErrorCode::NotFound);

    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");
    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "RouteTable server did not exit before the shutdown deadline",
        )
    })?;
    assert!(
        exit_status.success(),
        "RouteTable server exited unsuccessfully after SIGTERM: {exit_status}"
    );

    let (stdout, stderr) = server.read_captured()?;
    assert!(!stdout.contains(secret));
    assert!(!stderr.contains(secret));
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let started = records
        .iter()
        .find(|record| record["event"] == "server.started" && record["role"] == "route_table")
        .ok_or("missing RouteTable server.started JSON event")?;
    assert_eq!(started["component"], "server");
    assert_eq!(started["shard_id"], "rt-0");
    assert_eq!(started["configured_gateways"], 1);
    let trusted_local_warning = records
        .iter()
        .find(|record| record["event"] == "route_table.trusted_local_enabled")
        .ok_or("missing RouteTable trusted-local warning event")?;
    assert_eq!(trusted_local_warning["component"], "route_table");
    assert_eq!(trusted_local_warning["transport"], "plain_tcp");
    Ok(())
}

#[cfg(unix)]
#[test]
fn distributed_gateway_starts_without_route_table_and_hides_internal_key()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let peer_address = unused_loopback_address()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let secret = "must-not-appear-gateway-route-table-key";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", "echo.alpha=client-key")
            .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
            .env("RELAYGATE_GATEWAY_NAME", "gw-a")
            .env("RELAYGATE_GATEWAY_LOCATOR", &peer_address)
            .env("RELAYGATE_PEER_BIND_ADDR", &peer_address)
            .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", format!("gw-a={secret}"))
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json"),
    )?;

    wait_until_healthy(&address, &mut server)?;
    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");
    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "distributed Gateway did not exit before the shutdown deadline",
        )
    })?;
    assert!(exit_status.success(), "distributed Gateway shutdown failed");

    let (stdout, stderr) = server.read_captured()?;
    assert!(!stdout.contains(secret));
    assert!(!stderr.contains(secret));
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let started = records
        .iter()
        .find(|record| record["event"] == "server.started" && record["role"] == "gateway")
        .ok_or("missing distributed Gateway server.started event")?;
    assert_eq!(started["distributed_enabled"], true);
    let warning = records
        .iter()
        .find(|record| record["event"] == "gateway.route_table.trusted_local_enabled")
        .ok_or("missing distributed Gateway trusted-local warning")?;
    assert_eq!(warning["transport"], "plain_tcp");
    Ok(())
}

#[test]
fn invalid_cli_arguments_fail_with_actionable_errors() -> Result<(), Box<dyn Error>> {
    assert_failure(&["unknown"], "unknown command")?;
    assert_failure(&["gateway", "extra"], "usage: relaygate-server gateway")?;
    assert_failure(
        &["route-table", "extra"],
        "usage: relaygate-server route-table",
    )?;
    assert_failure(&["check"], "usage: relaygate-server check <address>")?;
    assert_failure(
        &["check", "127.0.0.1:1", "extra"],
        "usage: relaygate-server check <address>",
    )?;
    Ok(())
}

#[test]
fn invalid_environment_configuration_fails_before_serving() -> Result<(), Box<dyn Error>> {
    let missing_trusted_local_opt_in = server_command().arg("route-table").output()?;
    assert_unsuccessful_output(
        &missing_trusted_local_opt_in,
        "RELAYGATE_RT_TRUSTED_LOCAL must be `true`",
    );

    let missing_route_table_directory = server_command()
        .arg("route-table")
        .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
        .output()?;
    assert_unsuccessful_output(
        &missing_route_table_directory,
        "RELAYGATE_RT_SHARD_DIRECTORY_PATH is required",
    );

    let incomplete_gateway_routing = server_command()
        .env("RELAYGATE_GATEWAY_NAME", "gw-a")
        .output()?;
    assert_unsuccessful_output(
        &incomplete_gateway_routing,
        "RELAYGATE_RT_TRUSTED_LOCAL must be `true`",
    );

    let missing_gateway_directory = server_command()
        .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
        .env("RELAYGATE_GATEWAY_NAME", "gw-a")
        .env("RELAYGATE_GATEWAY_LOCATOR", "gw-a.internal:27431")
        .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", "gw-a=secret")
        .output()?;
    assert_unsuccessful_output(
        &missing_gateway_directory,
        "RELAYGATE_RT_SHARD_DIRECTORY_PATH is required for distributed Gateway mode",
    );

    let malformed_keys = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_CLIENT_KEYS", "echo.alpha")
        .output()?;
    assert_unsuccessful_output(
        &malformed_keys,
        "RELAYGATE_CLIENT_KEYS entries must use ClientId=ClientKey",
    );

    let zero_capacity = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_WRITER_QUEUE_CAPACITY", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_capacity,
        "RELAYGATE_WRITER_QUEUE_CAPACITY must be greater than zero",
    );

    let invalid_log_format = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_LOG_FORMAT", "xml")
        .output()?;
    assert_unsuccessful_output(
        &invalid_log_format,
        "RELAYGATE_LOG_FORMAT must be `text` or `json`",
    );

    let zero_stats_interval = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_STATS_INTERVAL_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_stats_interval,
        "RELAYGATE_STATS_INTERVAL_MS must be greater than zero",
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn json_logs_expose_stable_startup_and_snapshot_fields_without_secrets()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let secret = "must-not-appear-in-observability-output";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", format!("echo.alpha={secret}"))
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json")
            .env("RELAYGATE_STATS_INTERVAL_MS", "250"),
    )?;

    wait_until_healthy(&address, &mut server)?;

    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");

    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "server did not exit before the shutdown deadline",
        )
    })?;
    assert!(
        exit_status.success(),
        "server exited unsuccessfully after SIGTERM: {exit_status}"
    );

    let (stdout, stderr) = server.read_captured()?;
    assert!(!stdout.contains(secret));
    assert!(!stderr.contains(secret));
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;

    let started = records
        .iter()
        .find(|record| record["event"] == "server.started")
        .ok_or("missing server.started JSON event")?;
    assert_eq!(started["component"], "server");
    assert_eq!(started["configured_clients"], 1);

    let snapshot = records
        .iter()
        .find(|record| record["event"] == "gateway.snapshot")
        .ok_or("missing gateway.snapshot JSON event")?;
    assert_eq!(snapshot["component"], "gateway");
    for field in [
        "sessions",
        "listener_sessions",
        "connector_sessions",
        "listener_bindings",
        "pending_offers",
        "live_pipes",
        "route_registrations_synced",
        "route_registrations_unsynced",
        "remote_open_attempts",
        "peer_transports_connecting",
        "peer_transports_ready",
        "peer_streams",
    ] {
        assert!(
            snapshot[field].is_number(),
            "snapshot field {field:?} must be numeric: {snapshot}"
        );
    }
    Ok(())
}

fn server_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relaygate-server"));
    for name in [
        "RELAYGATE_BIND_ADDR",
        "RELAYGATE_CLIENT_KEYS",
        "RELAYGATE_GATEWAY_LOCATOR",
        "RELAYGATE_GATEWAY_NAME",
        "RELAYGATE_PEER_BIND_ADDR",
        "RELAYGATE_LOG",
        "RELAYGATE_LOG_FORMAT",
        "RELAYGATE_MAX_BINDINGS",
        "RELAYGATE_MAX_FRAME_LEN",
        "RELAYGATE_MAX_LIVE_PIPES",
        "RELAYGATE_MAX_PENDING_OFFERS",
        "RELAYGATE_MAX_SESSIONS",
        "RELAYGATE_OFFER_TIMEOUT_MS",
        "RELAYGATE_INTERNAL_GATEWAY_KEYS",
        "RELAYGATE_RT_BIND_ADDR",
        "RELAYGATE_RT_HANDSHAKE_TIMEOUT_MS",
        "RELAYGATE_RT_LEASE_TTL_MS",
        "RELAYGATE_RT_MAX_CONNECTIONS",
        "RELAYGATE_RT_MAX_FRAME_LEN",
        "RELAYGATE_RT_REQUEST_QUEUE_CAPACITY",
        "RELAYGATE_RT_SHARD_DIRECTORY_PATH",
        "RELAYGATE_RT_SHARD_ID",
        "RELAYGATE_RT_TRUSTED_LOCAL",
        "RELAYGATE_RT_WRITER_QUEUE_CAPACITY",
        "RELAYGATE_STATS_INTERVAL_MS",
        "RELAYGATE_WRITER_QUEUE_CAPACITY",
    ] {
        command.env_remove(name);
    }
    command.env("RELAYGATE_LOG", "warn");
    command
}

fn assert_failure(arguments: &[&str], expected: &str) -> Result<(), Box<dyn Error>> {
    let output = server_command().args(arguments).output()?;
    assert_unsuccessful_output(&output, expected);
    Ok(())
}

fn assert_unsuccessful_output(output: &Output, expected: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded; stderr: {stderr}"
    );
    assert!(
        stderr.contains(expected),
        "stderr did not contain {expected:?}; stderr: {stderr}"
    );
}

fn unused_loopback_address() -> io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address.to_string())
}

fn wait_until_healthy(address: &str, server: &mut ChildGuard) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + STARTUP_DEADLINE;

    loop {
        if let Some(status) = server.try_wait()? {
            return Err(io::Error::other(format!(
                "server exited before becoming healthy: {status}"
            ))
            .into());
        }

        let check = server_command().args(["check", address]).output()?;
        if check.status.success() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            let last_check = String::from_utf8_lossy(&check.stderr);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "server did not become healthy before the startup deadline; last check: {last_check}"
                ),
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
async fn wait_until_route_table_ready(
    address: &str,
    secret: &str,
    server: &mut ChildGuard,
) -> Result<RouteTableClient, Box<dyn Error>> {
    let endpoint: std::net::SocketAddr = address.parse()?;
    let deadline = tokio::time::Instant::now() + STARTUP_DEADLINE;
    loop {
        if let Some(status) = server.try_wait()? {
            return Err(io::Error::other(format!(
                "RouteTable server exited before becoming ready: {status}"
            ))
            .into());
        }
        let config = RouteTableClientConfig::new(
            8,
            1024 * 1024,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_secs(1),
        )?;
        let connected = RouteTableClient::connect(
            endpoint,
            GatewayName::new("gw-a")?,
            GatewayId::new(),
            InternalGatewayKey::new(secret)?,
            config,
        )
        .await;
        if let Ok(client) = connected {
            return Ok(client);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "RouteTable server did not become ready before the startup deadline",
            )
            .into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(unix)]
struct ShardDirectoryArtifact {
    path: PathBuf,
}

#[cfg(unix)]
impl ShardDirectoryArtifact {
    const BYTES: &'static [u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"127.0.0.1:27430"}]}"#;

    fn create() -> io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "relaygate-route-table-directory-{}-{nonce}.json",
            std::process::id()
        ));
        fs::write(&path, Self::BYTES)?;
        Ok(Self { path })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(unix)]
impl Drop for ShardDirectoryArtifact {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }

    #[cfg(unix)]
    fn spawn_captured(command: &mut Command) -> io::Result<Self> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self { child })
    }

    #[cfg(unix)]
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    #[cfg(unix)]
    fn wait_until(&mut self, timeout: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    #[cfg(unix)]
    fn read_captured(&mut self) -> io::Result<(String, String)> {
        let mut stdout = String::new();
        let mut stderr = String::new();
        if let Some(mut stream) = self.child.stdout.take() {
            stream.read_to_string(&mut stdout)?;
        }
        if let Some(mut stream) = self.child.stderr.take() {
            stream.read_to_string(&mut stderr)?;
        }
        Ok((stdout, stderr))
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
