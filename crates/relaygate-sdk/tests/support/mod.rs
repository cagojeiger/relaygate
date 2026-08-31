use std::{error::Error as StdError, io};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionId, SessionRole};
use tokio::net::TcpListener;
use tokio::time::{Duration, Instant, timeout};
use tokio_util::codec::Framed;

pub type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
pub type TestTransport = Framed<tokio::net::TcpStream, FrameCodec>;

pub async fn bind_gateway() -> TestResult<(TcpListener, String)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    Ok((listener, address))
}

pub async fn accept_session(
    listener: &TcpListener,
    expected_role: SessionRole,
) -> TestResult<(TestTransport, SessionId)> {
    let (stream, _) = listener.accept().await?;
    let mut transport = Framed::new(stream, FrameCodec::default());
    let hello = transport
        .next()
        .await
        .ok_or_else(|| io::Error::other("SDK closed before HELLO"))??;
    match hello {
        Frame::Hello { role } if role == expected_role => {}
        other => {
            return Err(io::Error::other(format!("unexpected first frame: {other:?}")).into());
        }
    }
    let session_id = SessionId::new();
    transport.send(Frame::Welcome { session_id }).await?;
    Ok((transport, session_id))
}

#[allow(dead_code)]
pub async fn next_application_frame(transport: &mut TestTransport) -> TestResult<Frame> {
    loop {
        let frame = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("SDK session closed"))??;
        match frame {
            Frame::Ping { nonce } => {
                transport.send(Frame::Pong { nonce }).await?;
            }
            Frame::Pong { .. } => {}
            other => return Ok(other),
        }
    }
}

#[allow(dead_code)]
pub async fn answer_heartbeats_for(
    transport: &mut TestTransport,
    duration: Duration,
) -> TestResult {
    let deadline = Instant::now() + duration;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        match timeout(remaining, transport.next()).await {
            Ok(Some(Ok(Frame::Ping { nonce }))) => {
                transport.send(Frame::Pong { nonce }).await?;
            }
            Ok(Some(Ok(Frame::Pong { .. }))) => {}
            Ok(Some(Ok(frame))) => return Err(unexpected(frame).into()),
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(None) => return Err(io::Error::other("SDK session closed").into()),
            Err(_) => return Ok(()),
        }
    }
}

pub fn unexpected(frame: Frame) -> io::Error {
    io::Error::other(format!("unexpected frame: {frame:?}"))
}
