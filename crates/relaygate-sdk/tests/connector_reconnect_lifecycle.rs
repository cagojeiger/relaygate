mod support;

use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, PipeId, SessionId, SessionRole};
use relaygate_sdk::{Config, Connector, ErrorCode};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::codec::Framed;

use support::{
    TestResult, TestTransport, accept_session, bind_gateway, next_application_frame, unexpected,
};

const RECONNECT_BACKOFF: Duration = Duration::from_millis(150);
const MINIMUM_OBSERVED_BACKOFF: Duration = Duration::from_millis(20);
const NO_RECONNECT_WINDOW: Duration = Duration::from_millis(450);

#[tokio::test]
async fn connector_runtime_cycles_through_loss_failed_reconnect_recovery_and_close() -> TestResult {
    timeout(Duration::from_secs(3), lifecycle_case()).await??;
    Ok(())
}

async fn lifecycle_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut initial, initial_session_id) =
            accept_session(&gateway, SessionRole::Connector).await?;
        let initial_pipe_id = accept_open(&mut initial, initial_session_id, "echo.initial").await?;
        initial
            .send(Frame::Data {
                pipe_id: initial_pipe_id,
                payload: Bytes::from_static(b"first"),
            })
            .await?;
        drop(initial);

        reject_next_connector_handshake(&gateway).await?;
        let (mut replacement, replacement_session_id) =
            accept_session(&gateway, SessionRole::Connector).await?;
        assert_ne!(initial_session_id, replacement_session_id);

        let replacement_pipe_id =
            accept_open(&mut replacement, replacement_session_id, "echo.recovered").await?;
        replacement
            .send(Frame::Data {
                pipe_id: replacement_pipe_id,
                payload: Bytes::from_static(b"second"),
            })
            .await?;
        assert_clean_session_end_and_no_reconnect(&gateway, &mut replacement).await
    });

    let connector = Connector::connect(
        Config::new(address).with_reconnect_backoff(RECONNECT_BACKOFF, RECONNECT_BACKOFF),
    )
    .await?;

    let mut first = connector.open("echo.initial").await?;
    let mut first_payload = [0_u8; 5];
    assert_eq!(
        first.read_into(&mut first_payload).await?,
        first_payload.len()
    );
    assert_eq!(&first_payload, b"first");
    let first_error = timeout(Duration::from_secs(1), first.read_into(&mut first_payload))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("lost ConnectorSession did not fail old Pipe"))?;
    assert_eq!(first_error.code(), ErrorCode::Unavailable);

    let mut replacement = connector.open("echo.recovered").await?;
    let mut replacement_payload = [0_u8; 6];
    assert_eq!(
        replacement.read_into(&mut replacement_payload).await?,
        replacement_payload.len()
    );
    assert_eq!(&replacement_payload, b"second");

    connector.close();
    let replacement_error = timeout(
        Duration::from_secs(1),
        replacement.read_into(&mut replacement_payload),
    )
    .await?
    .err()
    .ok_or_else(|| io::Error::other("runtime close did not fail recovered Pipe"))?;
    assert_eq!(replacement_error.code(), ErrorCode::Unavailable);
    let after_close_error = connector
        .open("echo.after-close")
        .await
        .err()
        .ok_or_else(|| io::Error::other("closed Connector reopened a Pipe"))?;
    assert_eq!(after_close_error.code(), ErrorCode::Cancelled);

    server.await??;
    Ok(())
}

async fn accept_open(
    transport: &mut TestTransport,
    session_id: SessionId,
    expected_client_id: &str,
) -> TestResult<PipeId> {
    let open = next_application_frame(transport).await?;
    let connection_id = match open {
        Frame::Open {
            connection_id,
            client_id,
        } if client_id == expected_client_id => connection_id,
        other => return Err(unexpected(other).into()),
    };
    let pipe_id = PipeId::new(session_id, connection_id);
    transport.send(Frame::Opened { pipe_id }).await?;
    Ok(pipe_id)
}

async fn reject_next_connector_handshake(gateway: &TcpListener) -> TestResult {
    let (stream, _) = gateway.accept().await?;
    let mut transport = Framed::new(stream, FrameCodec::default());
    let hello = transport
        .next()
        .await
        .ok_or_else(|| io::Error::other("SDK closed before failed reconnect HELLO"))??;
    match hello {
        Frame::Hello {
            role: SessionRole::Connector,
        } => {}
        other => return Err(unexpected(other).into()),
    }
    transport.send(Frame::Pong { nonce: 7 }).await?;
    match timeout(Duration::from_secs(1), transport.next()).await? {
        None => {}
        Some(Ok(frame)) => {
            return Err(io::Error::other(format!(
                "SDK emitted a frame after rejecting the handshake: {frame:?}"
            ))
            .into());
        }
        Some(Err(error)) => {
            return Err(io::Error::other(format!(
                "SDK did not close the rejected handshake with clean TCP EOF: {error}"
            ))
            .into());
        }
    }
    if timeout(MINIMUM_OBSERVED_BACKOFF, gateway.accept())
        .await
        .is_ok()
    {
        return Err(io::Error::other("Connector retried without bounded backoff").into());
    }
    Ok(())
}

async fn assert_clean_session_end_and_no_reconnect(
    gateway: &TcpListener,
    transport: &mut TestTransport,
) -> TestResult {
    match timeout(Duration::from_secs(1), transport.next()).await? {
        None => {}
        Some(Ok(frame)) => {
            return Err(io::Error::other(format!(
                "SDK session emitted a frame after runtime termination: {frame:?}"
            ))
            .into());
        }
        Some(Err(error)) => {
            return Err(io::Error::other(format!(
                "SDK session did not close with clean TCP EOF: {error}"
            ))
            .into());
        }
    }

    if timeout(NO_RECONNECT_WINDOW, gateway.accept()).await.is_ok() {
        return Err(io::Error::other("SDK reconnected after runtime termination").into());
    }
    Ok(())
}
