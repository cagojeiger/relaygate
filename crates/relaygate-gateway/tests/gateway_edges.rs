mod support;

use futures_util::{SinkExt, StreamExt};
use relaygate_gateway::GatewayConfig;
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode, Frame, PeerObservation, PipeId, SessionRole,
};
use tokio::{
    net::TcpStream,
    time::{Duration, sleep, timeout},
};

use support::{TestGateway, TestResult, next_frame, sdk_session};

#[tokio::test]
async fn invalid_client_key_creates_no_binding() -> TestResult {
    let gateway = TestGateway::start(&[("echo.alpha", "secret")]).await?;
    let mut listener = sdk_session(gateway.address, SessionRole::Listener).await?;
    listener
        .send(Frame::Register {
            request_id: 1,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new("invalid"),
        })
        .await?;
    assert!(matches!(
        next_frame(&mut listener).await?,
        Frame::RegisterFailed {
            code: ErrorCode::Unauthenticated,
            ..
        }
    ));

    let mut connector = sdk_session(gateway.address, SessionRole::Connector).await?;
    connector
        .send(Frame::Open {
            connection_id: 1,
            client_id: "echo.alpha".to_owned(),
        })
        .await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::OpenFailed {
            code: ErrorCode::NotFound,
            observation: PeerObservation::NotObserved,
            ..
        }
    ));

    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn same_client_id_is_offered_to_exactly_one_listener() -> TestResult {
    let gateway = TestGateway::start(&[("echo.shared", "secret")]).await?;
    let mut first = sdk_session(gateway.address, SessionRole::Listener).await?;
    let mut second = sdk_session(gateway.address, SessionRole::Listener).await?;
    let first_binding = register(&mut first, 1).await?;
    let _second_binding = register(&mut second, 2).await?;
    let mut connector = sdk_session(gateway.address, SessionRole::Connector).await?;

    connector
        .send(Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        })
        .await?;
    let Frame::Offer {
        pipe_id,
        binding_id,
        ..
    } = next_frame(&mut first).await?
    else {
        return Err("first listener did not receive the selected offer".into());
    };
    assert_eq!(binding_id, first_binding);
    assert!(
        timeout(Duration::from_millis(30), second.next())
            .await
            .is_err()
    );

    first.send(Frame::OfferAccepted { pipe_id }).await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Opened { pipe_id: opened } if opened == pipe_id
    ));

    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn listener_disconnect_resets_its_pipe_and_preserves_sibling_binding() -> TestResult {
    let gateway = TestGateway::start(&[("echo.shared", "secret")]).await?;
    let mut first = sdk_session(gateway.address, SessionRole::Listener).await?;
    let mut second = sdk_session(gateway.address, SessionRole::Listener).await?;
    let _first_binding = register(&mut first, 1).await?;
    let second_binding = register(&mut second, 2).await?;
    let mut connector = sdk_session(gateway.address, SessionRole::Connector).await?;

    let first_pipe = request_offer(&mut connector, &mut first, 1).await?;
    first
        .send(Frame::OfferAccepted {
            pipe_id: first_pipe,
        })
        .await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Opened { pipe_id } if pipe_id == first_pipe
    ));

    drop(first);
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Reset {
            pipe_id,
            code: ErrorCode::Unavailable,
            ..
        } if pipe_id == first_pipe
    ));

    connector
        .send(Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        })
        .await?;
    let Frame::Offer {
        pipe_id: second_pipe,
        binding_id,
        ..
    } = next_frame(&mut second).await?
    else {
        return Err("surviving listener did not receive a new offer".into());
    };
    assert_eq!(binding_id, second_binding);
    second
        .send(Frame::OfferAccepted {
            pipe_id: second_pipe,
        })
        .await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Opened { pipe_id } if pipe_id == second_pipe
    ));

    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn unanswered_offer_expires_and_late_accept_cannot_reopen_it() -> TestResult {
    let gateway = TestGateway::start_with_config(
        GatewayConfig::new([("echo.shared".to_owned(), "secret".to_owned())])
            .with_offer_timeout(Duration::from_millis(100)),
    )
    .await?;
    let mut listener = sdk_session(gateway.address, SessionRole::Listener).await?;
    register(&mut listener, 1).await?;
    let mut connector = sdk_session(gateway.address, SessionRole::Connector).await?;

    let expired_pipe = request_offer(&mut connector, &mut listener, 1).await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::OpenFailed {
            connection_id: 1,
            code: ErrorCode::DeadlineExceeded,
            observation: PeerObservation::MaybeObserved,
            ..
        }
    ));
    assert!(matches!(
        next_frame(&mut listener).await?,
        Frame::Reset {
            pipe_id,
            code: ErrorCode::DeadlineExceeded,
            ..
        } if pipe_id == expired_pipe
    ));

    listener
        .send(Frame::OfferAccepted {
            pipe_id: expired_pipe,
        })
        .await?;
    let live_pipe = request_offer(&mut connector, &mut listener, 2).await?;
    listener
        .send(Frame::OfferAccepted { pipe_id: live_pipe })
        .await?;
    assert!(matches!(
        next_frame(&mut connector).await?,
        Frame::Opened { pipe_id } if pipe_id == live_pipe
    ));

    gateway.stop().await?;
    Ok(())
}

#[tokio::test]
async fn session_limit_includes_connections_waiting_for_hello() -> TestResult {
    let gateway =
        TestGateway::start_without_check(GatewayConfig::new([]).with_max_sessions(1)).await?;
    let first = TcpStream::connect(gateway.address).await?;
    sleep(Duration::from_millis(20)).await;

    let second = TcpStream::connect(gateway.address).await?;
    let mut second =
        tokio_util::codec::Framed::new(second, relaygate_protocol::FrameCodec::default());
    second
        .send(Frame::Hello {
            role: SessionRole::Connector,
        })
        .await?;
    assert!(matches!(
        timeout(Duration::from_secs(1), second.next()).await,
        Ok(None) | Ok(Some(Err(_)))
    ));

    drop(first);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        match timeout(
            Duration::from_millis(100),
            sdk_session(gateway.address, SessionRole::Connector),
        )
        .await
        {
            Ok(Ok(_session)) => break,
            Ok(Err(_)) | Err(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(10)).await;
            }
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(error.into()),
        }
    }

    gateway.stop().await?;
    Ok(())
}

async fn register(
    listener: &mut tokio_util::codec::Framed<tokio::net::TcpStream, relaygate_protocol::FrameCodec>,
    request_id: u64,
) -> TestResult<BindingId> {
    listener
        .send(Frame::Register {
            request_id,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        })
        .await?;
    match next_frame(listener).await? {
        Frame::Registered { binding_id, .. } => Ok(binding_id),
        _ => Err("listener registration failed".into()),
    }
}

async fn request_offer(
    connector: &mut tokio_util::codec::Framed<
        tokio::net::TcpStream,
        relaygate_protocol::FrameCodec,
    >,
    listener: &mut tokio_util::codec::Framed<tokio::net::TcpStream, relaygate_protocol::FrameCodec>,
    connection_id: u64,
) -> TestResult<PipeId> {
    connector
        .send(Frame::Open {
            connection_id,
            client_id: "echo.shared".to_owned(),
        })
        .await?;
    match next_frame(listener).await? {
        Frame::Offer { pipe_id, .. } => Ok(pipe_id),
        _ => Err("listener did not receive offer".into()),
    }
}
