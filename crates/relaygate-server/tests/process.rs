use std::{
    error::Error,
    io,
    net::TcpListener,
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::{
    fs,
    net::TcpStream,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use futures_util::{SinkExt, StreamExt};
#[cfg(unix)]
use relaygate_protocol::{ClientKey, ErrorCode, Frame, FrameCodec, SessionRole};
#[cfg(unix)]
use relaygate_route_table::{
    BindingId, ClientId, GatewayId, GatewayLocator, ListenerSessionId, MappingEntry,
    MappingSnapshot, RegistrationKey, RegistrationRevision, ShardDirectory, ShardId,
};
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
const PROCESS_NOFILE_LIMIT: usize = 128;
#[cfg(unix)]
const PEER_FD_PRESSURE_ATTEMPTS: usize = 512;
#[cfg(unix)]
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);

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
    let metrics_address = unused_loopback_address()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let directory = ShardDirectory::from_json_bytes(ShardDirectoryArtifact::BYTES)?;
    let secret = "must-not-appear-route-table-key";
    let gateway_id = GatewayId::new();
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .arg("route-table")
            .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
            .env("RELAYGATE_RT_BIND_ADDR", &address)
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
            .env("RELAYGATE_RT_SHARD_ID", "rt-0")
            .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", format!("gw-a={secret}"))
            .env("RELAYGATE_METRICS_BIND_ADDR", &metrics_address)
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json"),
    )?;

    let client = wait_until_route_table_ready(&address, gateway_id, secret, &mut server).await?;
    let rejected = match RouteTableClient::connect(
        address.as_str(),
        GatewayName::new("gw-a")?,
        GatewayId::new(),
        InternalGatewayKey::new("wrong-key")?,
        RouteTableClientConfig::new(
            8,
            1024 * 1024,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_secs(1),
        )?,
    )
    .await
    {
        Ok(_) => return Err(io::Error::other("wrong internal key was accepted").into()),
        Err(error) => error,
    };
    assert_eq!(rejected.code(), RouteTableErrorCode::Unauthenticated);
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

    let listener_session_id = ListenerSessionId::new();
    let key = RegistrationKey::new(gateway_id, listener_session_id, ShardId::new("rt-0")?);
    let registration = client.register(directory.generation(), &key).await?;
    let snapshot = MappingSnapshot::new([MappingEntry::new(
        ClientId::new("metrics.listener")?,
        gateway_id,
        listener_session_id,
        BindingId::new(),
        GatewayLocator::new("127.0.0.1:27421")?,
    )])?;
    client
        .update(
            directory.generation(),
            &key,
            registration.lease_id(),
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    let metrics = wait_for_metrics(
        &metrics_address,
        &mut server,
        "relaygate_route_table_registrations",
    )?;
    assert!(metrics.contains("role=\"route_table\""));
    for metric in [
        "relaygate_route_table_registrations",
        "relaygate_route_table_mappings",
        "relaygate_route_table_routes",
        "relaygate_route_table_expiry_records",
    ] {
        assert!(
            metrics.contains(&format!("{metric}{{role=\"route_table\"}} 1")),
            "expected {metric} to report the one current registration/mapping/route/expiry record"
        );
    }
    assert!(metric_has_labels(
        &metrics,
        "relaygate_route_table_requests_total",
        &[
            "role=\"route_table\"",
            "operation=\"resolve\"",
            "outcome=\"error\"",
            "code=\"not_found\""
        ]
    ));
    assert!(metric_has_labels(
        &metrics,
        "relaygate_route_table_requests_total",
        &[
            "role=\"route_table\"",
            "operation=\"register\"",
            "outcome=\"success\"",
            "code=\"ok\""
        ]
    ));
    assert!(metric_has_labels(
        &metrics,
        "relaygate_route_table_handshakes_total",
        &["role=\"route_table\"", "outcome=\"success\"", "code=\"ok\""]
    ));
    assert!(metrics.contains("relaygate_route_table_request_duration_seconds_bucket"));
    assert!(!metrics.contains("quantile="));
    assert!(!metrics.contains(secret));

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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_table_expiry_metric_counts_removed_soft_state_once() -> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let metrics_address = unused_loopback_address()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let directory = ShardDirectory::from_json_bytes(ShardDirectoryArtifact::BYTES)?;
    let gateway_id = GatewayId::new();
    let secret = "expiry-test-key";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .arg("route-table")
            .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
            .env("RELAYGATE_RT_BIND_ADDR", &address)
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
            .env("RELAYGATE_RT_SHARD_ID", "rt-0")
            .env("RELAYGATE_RT_LEASE_TTL_MS", "100")
            .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", format!("gw-a={secret}"))
            .env("RELAYGATE_METRICS_BIND_ADDR", &metrics_address),
    )?;

    let client = wait_until_route_table_ready(&address, gateway_id, secret, &mut server).await?;
    let key = RegistrationKey::new(gateway_id, ListenerSessionId::new(), ShardId::new("rt-0")?);
    client.register(directory.generation(), &key).await?;

    let metrics = wait_for_metrics(
        &metrics_address,
        &mut server,
        "relaygate_route_table_expired_registrations_total{role=\"route_table\"} 1",
    )?;
    assert!(metrics.contains("relaygate_route_table_registrations{role=\"route_table\"} 0"));
    assert!(metrics.contains("relaygate_route_table_expiry_records{role=\"route_table\"} 0"));

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
    assert!(exit_status.success());
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

#[cfg(unix)]
#[test]
fn distributed_peer_accept_failure_exits_process_nonzero_within_bound() -> Result<(), Box<dyn Error>>
{
    let address = unused_loopback_address()?;
    let peer_address = unused_loopback_address()?;
    let peer_socket = peer_address.parse()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let secret = "must-not-appear-peer-failure-key";
    let mut server = ChildGuard::spawn_captured(
        server_command_with_open_file_limit(PROCESS_NOFILE_LIMIT)
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

    let mut idle_peers = Vec::with_capacity(PEER_FD_PRESSURE_ATTEMPTS);
    for _ in 0..PEER_FD_PRESSURE_ATTEMPTS {
        if server.try_wait()?.is_some() {
            break;
        }
        if let Ok(stream) = TcpStream::connect_timeout(&peer_socket, PEER_CONNECT_TIMEOUT) {
            idle_peers.push(stream);
        }
    }
    assert!(!idle_peers.is_empty(), "no peer connection was established");

    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "distributed Gateway did not fail closed before the shutdown deadline",
        )
    })?;
    assert!(
        !exit_status.success(),
        "distributed Gateway unexpectedly exited successfully"
    );

    drop(idle_peers);
    let (stdout, stderr) = server.read_captured()?;
    assert!(!stdout.contains(secret));
    assert!(!stderr.contains(secret));
    assert!(
        stderr.contains(
            "Gateway peer relay failed: Unavailable/NotObserved: peer listener accept failed"
        ),
        "unexpected distributed Gateway failure: {stderr}"
    );
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "server.started" && record["role"] == "gateway"),
        "distributed Gateway failed before reaching the running state"
    );
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

    let malformed_metrics_address = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_METRICS_BIND_ADDR", "not-a-socket-address")
        .output()?;
    assert_unsuccessful_output(
        &malformed_metrics_address,
        "RELAYGATE_METRICS_BIND_ADDR must be a socket address",
    );

    let metrics_interval_without_exporter = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_METRICS_INTERVAL_MS", "100")
        .output()?;
    assert_unsuccessful_output(
        &metrics_interval_without_exporter,
        "RELAYGATE_METRICS_BIND_ADDR is required",
    );

    let zero_metrics_interval = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_METRICS_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_METRICS_INTERVAL_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_metrics_interval,
        "RELAYGATE_METRICS_INTERVAL_MS must be greater than zero",
    );

    let zero_sdk_heartbeat_idle = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_SDK_HEARTBEAT_IDLE_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_sdk_heartbeat_idle,
        "RELAYGATE_SDK_HEARTBEAT_IDLE_MS must be greater than zero",
    );

    let zero_sdk_heartbeat_timeout = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_sdk_heartbeat_timeout,
        "RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS must be greater than zero",
    );

    let artifact = ShardDirectoryArtifact::create()?;
    let zero_peer_heartbeat_idle = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
        .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
        .env("RELAYGATE_GATEWAY_NAME", "gw-a")
        .env("RELAYGATE_GATEWAY_LOCATOR", "127.0.0.1:27421")
        .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", "gw-a=secret")
        .env("RELAYGATE_PEER_HEARTBEAT_IDLE_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_peer_heartbeat_idle,
        "RELAYGATE_PEER_HEARTBEAT_IDLE_MS must be greater than zero",
    );

    let zero_peer_heartbeat_timeout = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
        .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
        .env("RELAYGATE_GATEWAY_NAME", "gw-a")
        .env("RELAYGATE_GATEWAY_LOCATOR", "127.0.0.1:27421")
        .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", "gw-a=secret")
        .env("RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_peer_heartbeat_timeout,
        "RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS must be greater than zero",
    );

    let zero_peer_idle_retirement = server_command()
        .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
        .env("RELAYGATE_RT_TRUSTED_LOCAL", "true")
        .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path())
        .env("RELAYGATE_GATEWAY_NAME", "gw-a")
        .env("RELAYGATE_GATEWAY_LOCATOR", "127.0.0.1:27421")
        .env("RELAYGATE_INTERNAL_GATEWAY_KEYS", "gw-a=secret")
        .env("RELAYGATE_PEER_IDLE_TIMEOUT_MS", "0")
        .output()?;
    assert_unsuccessful_output(
        &zero_peer_idle_retirement,
        "RELAYGATE_PEER_IDLE_TIMEOUT_MS must be greater than zero",
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
    assert_eq!(snapshot["draining"], false);
    assert_eq!(snapshot["route_dependency_health"], "DISABLED");
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

#[cfg(unix)]
#[test]
fn default_json_logs_do_not_emit_gateway_snapshots() -> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", "echo.alpha=test-key")
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json"),
    )?;

    wait_until_healthy(&address, &mut server)?;
    thread::sleep(Duration::from_millis(300));

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
    assert!(exit_status.success(), "server shutdown failed");

    let (stdout, stderr) = server.read_captured()?;
    assert!(
        stderr.is_empty(),
        "server wrote unexpected stderr: {stderr}"
    );
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "server.started"),
        "missing server.started JSON event"
    );
    assert!(
        records
            .iter()
            .all(|record| record["event"] != "gateway.snapshot"),
        "default observability configuration emitted gateway.snapshot"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_metrics_expose_current_state_and_red_signals_without_secrets()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let metrics_address = unused_loopback_address()?;
    let secret = "must-not-appear-in-metrics-output";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", format!("echo.alpha={secret}"))
            .env("RELAYGATE_MAX_BINDINGS", "1")
            .env("RELAYGATE_METRICS_BIND_ADDR", &metrics_address)
            .env("RELAYGATE_METRICS_INTERVAL_MS", "20")
            .env("RELAYGATE_LOG", "info")
            .env("RELAYGATE_LOG_FORMAT", "json"),
    )?;

    wait_until_healthy(&address, &mut server)?;
    let mut listener = connect_sdk_session(&address, SessionRole::Listener).await?;
    listener
        .send(Frame::Register {
            request_id: 1,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new(secret),
        })
        .await?;
    let registered = tokio::time::timeout(Duration::from_secs(1), listener.next()).await?;
    assert!(
        matches!(
            registered,
            Some(Ok(Frame::Registered { request_id: 1, .. }))
        ),
        "Listener should register before the successful OPEN metric case: {registered:?}"
    );
    listener
        .send(Frame::Register {
            request_id: 2,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new("wrong-key"),
        })
        .await?;
    let rejected = tokio::time::timeout(Duration::from_secs(1), listener.next()).await?;
    assert!(
        matches!(
            rejected,
            Some(Ok(Frame::RegisterFailed {
                request_id: 2,
                code: ErrorCode::Unauthenticated,
                ..
            }))
        ),
        "wrong ClientKey should reject registration without removing the live binding: {rejected:?}"
    );
    let mut capacity_listener = connect_sdk_session(&address, SessionRole::Listener).await?;
    capacity_listener
        .send(Frame::Register {
            request_id: 1,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new(secret),
        })
        .await?;
    let exhausted = tokio::time::timeout(Duration::from_secs(1), capacity_listener.next()).await?;
    assert!(
        matches!(
            exhausted,
            Some(Ok(Frame::RegisterFailed {
                request_id: 1,
                code: ErrorCode::ResourceExhausted,
                ..
            }))
        ),
        "binding limit should reject another valid Listener registration: {exhausted:?}"
    );

    let mut sdk = connect_sdk_session(&address, SessionRole::Connector).await?;
    sdk.send(Frame::Open {
        connection_id: 1,
        client_id: "echo.alpha".to_owned(),
    })
    .await?;
    let offer = tokio::time::timeout(Duration::from_secs(1), listener.next()).await?;
    let pipe_id = match offer {
        Some(Ok(Frame::Offer { pipe_id, .. })) => pipe_id,
        other => return Err(io::Error::other(format!("expected OFFER: {other:?}")).into()),
    };
    listener.send(Frame::OfferAccepted { pipe_id }).await?;
    let opened = tokio::time::timeout(Duration::from_secs(1), sdk.next()).await?;
    assert!(
        matches!(opened, Some(Ok(Frame::Opened { pipe_id: opened })) if opened == pipe_id),
        "accepted Listener offer should complete OPEN: {opened:?}"
    );

    sdk.send(Frame::Open {
        connection_id: 2,
        client_id: "missing".to_owned(),
    })
    .await?;
    let result = tokio::time::timeout(Duration::from_secs(1), sdk.next()).await?;
    assert!(
        matches!(
            result,
            Some(Ok(Frame::OpenFailed {
                connection_id: 2,
                code: ErrorCode::NotFound,
                ..
            }))
        ),
        "missing local binding should produce terminal OPEN_FAILED: {result:?}"
    );
    let body = wait_for_metrics(
        &metrics_address,
        &mut server,
        "relaygate_gateway_open_results_total",
    )?;
    assert!(body.contains("role=\"gateway\""));
    assert!(body.contains("relaygate_gateway_sessions"));
    assert!(body.contains("relaygate_gateway_route_dependency"));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_open_requests_total",
        &["role=\"gateway\""]
    ));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_listener_registration_results_total",
        &[
            "role=\"gateway\"",
            "outcome=\"error\"",
            "code=\"resource_exhausted\""
        ]
    ));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_listener_registration_results_total",
        &["role=\"gateway\"", "outcome=\"success\"", "code=\"ok\""]
    ));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_listener_registration_results_total",
        &[
            "role=\"gateway\"",
            "outcome=\"error\"",
            "code=\"unauthenticated\""
        ]
    ));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_open_results_total",
        &["role=\"gateway\"", "outcome=\"success\"", "code=\"ok\""]
    ));
    assert!(metric_has_labels(
        &body,
        "relaygate_gateway_open_results_total",
        &[
            "role=\"gateway\"",
            "outcome=\"error\"",
            "code=\"not_found\""
        ]
    ));
    assert!(body.contains("relaygate_gateway_open_duration_seconds_bucket"));
    assert!(!body.contains("quantile="));
    assert!(!body.contains(secret));
    assert!(!body.contains("client_key"));
    assert!(!body.contains("payload"));

    let signal_status = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()?;
    assert!(signal_status.success(), "failed to send SIGTERM to server");
    let draining = wait_for_metrics(
        &metrics_address,
        &mut server,
        "relaygate_gateway_draining{role=\"gateway\"} 1",
    )?;
    assert!(draining.contains("relaygate_gateway_draining{role=\"gateway\"} 1"));

    sdk.send(Frame::Close { pipe_id }).await?;
    let closed = tokio::time::timeout(Duration::from_secs(1), listener.next()).await?;
    assert!(
        matches!(closed, Some(Ok(Frame::Close { pipe_id: closed })) if closed == pipe_id),
        "Listener should observe the test Pipe closing before process shutdown: {closed:?}"
    );

    let exit_status = server.wait_until(SHUTDOWN_DEADLINE)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "metrics-enabled server did not exit before the shutdown deadline",
        )
    })?;
    assert!(
        exit_status.success(),
        "metrics-enabled server shutdown failed"
    );
    let (stdout, stderr) = server.read_captured()?;
    assert!(
        stderr.is_empty(),
        "server wrote unexpected stderr: {stderr}"
    );
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "gateway.drain.started")
    );
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "gateway.drain.completed")
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn occupied_metrics_address_fails_before_gateway_serve() -> Result<(), Box<dyn Error>> {
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let metrics_address = occupied.local_addr()?.to_string();
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
            .env("RELAYGATE_METRICS_BIND_ADDR", metrics_address),
    )?;
    let status = server
        .wait_until(STARTUP_DEADLINE)?
        .ok_or("server did not fail after the metrics address bind conflict")?;
    assert!(
        !status.success(),
        "server ignored the metrics bind conflict"
    );
    let (_, stderr) = server.read_captured()?;
    assert!(
        stderr.contains("failed to start Prometheus metrics exporter"),
        "unexpected metrics bind error: {stderr}"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_sdk_heartbeat_timeout_is_observable_without_payload_or_secrets()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let secret = "must-not-appear-heartbeat-key";
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_CLIENT_KEYS", format!("echo.alpha={secret}"))
            .env("RELAYGATE_LOG", "debug")
            .env("RELAYGATE_LOG_FORMAT", "json")
            .env("RELAYGATE_SDK_HEARTBEAT_IDLE_MS", "40")
            .env("RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS", "40"),
    )?;

    wait_until_healthy(&address, &mut server)?;
    let mut sdk = connect_sdk_session(&address, SessionRole::Connector).await?;
    let heartbeat = tokio::time::timeout(Duration::from_secs(1), sdk.next()).await?;
    assert!(
        matches!(heartbeat, Some(Ok(Frame::Ping { .. }))),
        "Gateway should send a heartbeat PING to an idle SDK session: {heartbeat:?}"
    );
    let ended = tokio::time::timeout(Duration::from_secs(1), sdk.next()).await?;
    assert!(
        ended.is_none(),
        "Gateway should close the SDK session after heartbeat timeout: {ended:?}"
    );

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
    assert!(exit_status.success(), "Gateway shutdown failed");

    let (stdout, stderr) = server.read_captured()?;
    assert!(!stdout.contains(secret));
    assert!(!stderr.contains(secret));
    let records = stdout
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let timeout_event = records
        .iter()
        .find(|record| record["event"] == "gateway.session.heartbeat_timeout")
        .ok_or("missing gateway.session.heartbeat_timeout JSON event")?;
    assert_eq!(timeout_event["component"], "gateway");
    assert!(timeout_event["session_id"].is_string());
    assert!(timeout_event.get("payload").is_none());
    assert!(timeout_event.get("client_key").is_none());
    assert!(timeout_event.get("secret").is_none());
    Ok(())
}

fn server_command() -> Command {
    clean_server_command(Command::new(env!("CARGO_BIN_EXE_relaygate-server")))
}

#[cfg(unix)]
fn server_command_with_open_file_limit(limit: usize) -> Command {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("ulimit -n \"$1\" && shift && exec \"$0\" \"$@\"")
        .arg(env!("CARGO_BIN_EXE_relaygate-server"))
        .arg(limit.to_string());
    clean_server_command(command)
}

fn clean_server_command(mut command: Command) -> Command {
    for name in [
        "RELAYGATE_BIND_ADDR",
        "RELAYGATE_CLIENT_KEYS",
        "RELAYGATE_GATEWAY_LOCATOR",
        "RELAYGATE_GATEWAY_NAME",
        "RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS",
        "RELAYGATE_PEER_BIND_ADDR",
        "RELAYGATE_PEER_HEARTBEAT_IDLE_MS",
        "RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS",
        "RELAYGATE_PEER_IDLE_TIMEOUT_MS",
        "RELAYGATE_LOG",
        "RELAYGATE_LOG_FORMAT",
        "RELAYGATE_METRICS_BIND_ADDR",
        "RELAYGATE_METRICS_INTERVAL_MS",
        "RELAYGATE_MAX_BINDINGS",
        "RELAYGATE_MAX_FRAME_LEN",
        "RELAYGATE_MAX_LIVE_PIPES",
        "RELAYGATE_MAX_PENDING_OFFERS",
        "RELAYGATE_MAX_SESSIONS",
        "RELAYGATE_OFFER_TIMEOUT_MS",
        "RELAYGATE_SDK_HEARTBEAT_IDLE_MS",
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

#[cfg(unix)]
async fn connect_sdk_session(
    address: &str,
    role: SessionRole,
) -> Result<tokio_util::codec::Framed<tokio::net::TcpStream, FrameCodec>, Box<dyn Error>> {
    let stream = tokio::net::TcpStream::connect(address).await?;
    let mut framed = tokio_util::codec::Framed::new(stream, FrameCodec::default());
    framed.send(Frame::Hello { role }).await?;
    let welcome = tokio::time::timeout(Duration::from_secs(1), framed.next()).await?;
    assert!(
        matches!(welcome, Some(Ok(Frame::Welcome { .. }))),
        "Gateway should welcome the SDK session: {welcome:?}"
    );
    Ok(framed)
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
            let (stdout, stderr) = server.read_captured()?;
            return Err(io::Error::other(format!(
                "server exited before becoming healthy: {status}; stdout: {stdout}; stderr: {stderr}"
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
fn wait_for_metrics(
    address: &str,
    server: &mut ChildGuard,
    required_metric: &str,
) -> Result<String, Box<dyn Error>> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    loop {
        if let Some(status) = server.try_wait()? {
            let (stdout, stderr) = server.read_captured()?;
            return Err(io::Error::other(format!(
                "server exited before metrics became ready: {status}; stdout: {stdout}; stderr: {stderr}"
            ))
            .into());
        }
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream.set_read_timeout(Some(Duration::from_secs(1)))?;
            stream.write_all(
                b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )?;
            let mut response = String::new();
            stream.read_to_string(&mut response)?;
            if response.starts_with("HTTP/1.1 200") && response.contains(required_metric) {
                return Ok(response);
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "metrics endpoint did not become ready before the startup deadline",
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn metric_has_labels(body: &str, metric: &str, labels: &[&str]) -> bool {
    body.lines()
        .any(|line| line.starts_with(metric) && labels.iter().all(|label| line.contains(label)))
}

#[cfg(unix)]
async fn wait_until_route_table_ready(
    address: &str,
    gateway_id: GatewayId,
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
            gateway_id,
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
