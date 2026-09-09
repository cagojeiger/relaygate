use super::*;

use tokio::io::AsyncReadExt;
use tokio_util::codec::Framed;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_admission_rejection_metrics_distinguish_capacity_and_credentials()
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
    wait_until_healthy_with_token(&address, TEST_CLUSTER_TOKEN, &mut server)?;
    wait_handshake_usage(&metrics_address, &mut server, 0)?;
    let stalled = tokio::net::TcpStream::connect(&address).await?;
    wait_handshake_usage(&metrics_address, &mut server, 1)?;
    assert_socket_rejected(&address).await?;
    assert_rejection(&metrics_address, &mut server, "handshake_limit")?;
    drop(stalled);
    wait_handshake_usage(&metrics_address, &mut server, 0)?;

    let invalid_token = "must-not-appear-in-admission-metrics";
    let mut invalid = Framed::new(
        tokio::net::TcpStream::connect(&address).await?,
        FrameCodec::default(),
    );
    invalid
        .send(Frame::Hello {
            cluster_token: ClusterToken::new(invalid_token),
        })
        .await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), invalid.next()).await?,
        Some(Ok(Frame::SessionRejected {
            code: ErrorCode::Unauthenticated,
            ..
        }))
    ));
    drop(invalid);
    let body = assert_rejection(&metrics_address, &mut server, "cluster_token")?;
    assert!(!body.contains(invalid_token));
    assert!(!body.contains(TEST_CLUSTER_TOKEN));
    wait_handshake_usage(&metrics_address, &mut server, 0)?;

    let first = connect_sdk_session(&address, TEST_CLUSTER_TOKEN).await?;
    let second = connect_sdk_session(&address, TEST_CLUSTER_TOKEN).await?;
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
