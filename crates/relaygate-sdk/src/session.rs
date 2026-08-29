use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, SessionId, SessionRole};
use tokio::{net::TcpStream, time::timeout};
use tokio_util::codec::Framed;

use crate::{Config, Error, ErrorCode, PeerObservation, Result};

pub(crate) type WireTransport = Framed<TcpStream, FrameCodec>;

pub(crate) struct EstablishedSession {
    pub(crate) id: SessionId,
    pub(crate) transport: WireTransport,
}

pub(crate) async fn establish(config: &Config, role: SessionRole) -> Result<EstablishedSession> {
    let stream = timeout(
        config.connect_timeout,
        TcpStream::connect(&config.gateway_addr),
    )
    .await
    .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
    .map_err(|error| Error::unavailable(format!("Gateway connection failed: {error}")))?;
    let _ = stream.set_nodelay(true);
    let mut transport = Framed::new(stream, FrameCodec::new(config.max_frame_len));
    transport
        .send(Frame::Hello { role })
        .await
        .map_err(|error| Error::unavailable(format!("session hello failed: {error}")))?;
    let frame = timeout(config.connect_timeout, transport.next())
        .await
        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
        .ok_or_else(|| Error::unavailable("Gateway closed before WELCOME"))?
        .map_err(|error| {
            Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::NotObserved,
                format!("WELCOME decode failed: {error}"),
            )
        })?;
    let Frame::Welcome { session_id } = frame else {
        return Err(Error::new(
            ErrorCode::ProtocolError,
            PeerObservation::NotObserved,
            "first Gateway response was not WELCOME",
        ));
    };
    Ok(EstablishedSession {
        id: session_id,
        transport,
    })
}

pub(crate) fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

pub(crate) fn valid_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= u16::MAX as usize
}
