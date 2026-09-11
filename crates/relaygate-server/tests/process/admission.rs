use super::*;

use tokio::io::AsyncReadExt;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_admission_rejection_metrics_distinguish_transport_capacity()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let metrics_address = unused_loopback_address()?;
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_MAX_SESSIONS", "2")
            .env("RELAYGATE_MAX_PENDING_HANDSHAKES", "1")
            .env("RELAYGATE_METRICS_BIND_ADDR", &metrics_address)
            .env("RELAYGATE_METRICS_INTERVAL_MS", "10"),
    )?;
    wait_until_healthy(&address, &mut server)?;
    wait_handshake_usage(&metrics_address, &mut server, 0)?;
    let stalled = tokio::net::TcpStream::connect(&address).await?;
    wait_handshake_usage(&metrics_address, &mut server, 1)?;
    assert_socket_rejected(&address).await?;
    assert_rejection(&metrics_address, &mut server, "handshake_limit")?;
    drop(stalled);
    wait_handshake_usage(&metrics_address, &mut server, 0)?;

    let first = connect_sdk_session(&address).await?;
    let second = connect_sdk_session(&address).await?;
    assert_socket_rejected(&address).await?;
    assert_rejection(&metrics_address, &mut server, "session_limit")?;
    drop((first, second));
    Ok(())
}

async fn assert_socket_rejected(address: &str) -> Result<(), Box<dyn Error>> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    let mut byte = [0_u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte)).await??,
        0
    );
    Ok(())
}

#[test]
fn invalid_connection_rate_environment_fails_before_serving() -> Result<(), Box<dyn Error>> {
    for name in [
        "RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND",
        "RELAYGATE_SDK_CONNECTION_BURST",
    ] {
        for value in ["0", "invalid", "-1"] {
            let output = server_command()
                .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
                .env(name, value)
                .output()?;
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains(name));
        }
    }
    Ok(())
}

#[test]
fn invalid_control_rate_environment_fails_before_serving() -> Result<(), Box<dyn Error>> {
    for name in [
        "RELAYGATE_CONTROL_RATE_PER_SECOND",
        "RELAYGATE_CONTROL_BURST",
        "RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND",
        "RELAYGATE_SESSION_CONTROL_BURST",
    ] {
        for value in ["0", "invalid", "-1"] {
            let output = server_command()
                .env("RELAYGATE_BIND_ADDR", "127.0.0.1:0")
                .env(name, value)
                .output()?;
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains(name));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_rate_environment_rejects_and_reports_without_session_state()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let metrics_address = unused_loopback_address()?;
    let mut server = ChildGuard::spawn_captured(
        server_command()
            .env("RELAYGATE_BIND_ADDR", &address)
            .env("RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND", "1")
            .env("RELAYGATE_SDK_CONNECTION_BURST", "1")
            .env("RELAYGATE_METRICS_BIND_ADDR", &metrics_address)
            .env("RELAYGATE_METRICS_INTERVAL_MS", "10"),
    )?;
    wait_until_healthy(&address, &mut server)?;
    // Probe admission already spent the initial burst; recover between attempts.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut active = connect_sdk_session(&address).await?;
    // A refill may admit one of these sockets. Verify rejection over the burst,
    // rather than assuming a specific socket arrives before the next refill.
    futures_util::future::try_join_all((0..16).map(|_| async {
        let mut stream = tokio::net::TcpStream::connect(&address).await?;
        let mut byte = [0];
        match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut byte)).await {
            Ok(Ok(0)) | Err(_) => Ok(()),
            Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionReset => Ok(()),
            Ok(Err(error)) => Err(error),
            Ok(Ok(_)) => Err(io::Error::other("unexpected data before HELLO")),
        }
    }))
    .await?;
    let body = wait_for_metrics(&metrics_address, &mut server, "reason=\"rate_limit\"")?;
    assert!(body.lines().any(|line| {
        line.starts_with("relaygate_gateway_sdk_transport_rejections_total{")
            && line.contains("reason=\"rate_limit\"")
            && line
                .split_whitespace()
                .last()
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|count| count > 0)
    }));
    active.send(Frame::Ping { nonce: 88 }).await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), active.next()).await?,
        Some(Ok(Frame::Pong { nonce: 88 }))
    ));
    wait_handshake_usage(&metrics_address, &mut server, 0)?;
    Ok(())
}

fn assert_rejection(
    metrics_address: &str,
    server: &mut ChildGuard,
    reason: &str,
) -> Result<String, Box<dyn Error>> {
    let label = format!("reason=\"{reason}\"");
    let body = wait_for_metrics(metrics_address, server, &label)?;
    assert!(body.lines().any(|line| {
        line.starts_with("relaygate_gateway_sdk_transport_rejections_total{")
            && line.contains(&label)
            && line.split_whitespace().last() == Some("1")
    }));
    Ok(body)
}

fn wait_handshake_usage(
    address: &str,
    server: &mut ChildGuard,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    let expected = count.to_string();
    loop {
        let body = wait_for_metrics(address, server, "resource=\"sdk_handshakes\"")?;
        if body.lines().any(|line| {
            line.starts_with("relaygate_gateway_resource_used{")
                && line.contains("resource=\"sdk_handshakes\"")
                && line.split_whitespace().last() == Some(expected.as_str())
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("handshake usage did not converge").into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}
