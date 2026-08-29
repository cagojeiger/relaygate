use std::{error::Error as StdError, io};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionId, SessionRole};
use tokio::net::TcpListener;
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

pub fn unexpected(frame: Frame) -> io::Error {
    io::Error::other(format!("unexpected frame: {frame:?}"))
}
