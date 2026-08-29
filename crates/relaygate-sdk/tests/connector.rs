mod support;

use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, PipeId, SessionRole};
use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation};
use tokio::time::timeout;

use support::{TestResult, accept_session, bind_gateway, unexpected};

#[tokio::test]
async fn pipe_preserves_bytes_and_directional_fin() -> TestResult {
    timeout(
        Duration::from_secs(3),
        pipe_preserves_bytes_and_directional_fin_case(),
    )
    .await??;
    Ok(())
}

async fn pipe_preserves_bytes_and_directional_fin_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let frame = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OPEN"))??;
        let connection_id = match frame {
            Frame::Open {
                connection_id,
                client_id,
            } if client_id == "echo.alpha" => connection_id,
            other => return Err(unexpected(other).into()),
        };
        let pipe_id = PipeId::new(session_id, connection_id);
        transport.send(Frame::Opened { pipe_id }).await?;
        transport
            .send(Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"hello"),
            })
            .await?;
        transport.send(Frame::Fin { pipe_id }).await?;

        let outbound = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing DATA"))??;
        match outbound {
            Frame::Data {
                pipe_id: id,
                payload,
            } if id == pipe_id && payload == Bytes::from_static(b"reply") => {}
            other => return Err(unexpected(other).into()),
        }
        let fin = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing FIN"))??;
        if !matches!(fin, Frame::Fin { pipe_id: id } if id == pipe_id) {
            return Err(unexpected(fin).into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(Config::new(address)).await?;
    let mut pipe = connector.open("echo.alpha").await?;
    let mut buffer = [0_u8; 5];
    assert_eq!(pipe.read(&mut buffer).await?, 5);
    assert_eq!(&buffer, b"hello");
    assert_eq!(pipe.read(&mut buffer).await?, 0);
    pipe.write_all(b"reply").await?;
    pipe.shutdown_write().await?;
    pipe.shutdown_write().await?;
    server.await??;
    connector.close();
    Ok(())
}

#[tokio::test]
async fn committed_open_is_not_replayed_and_reports_maybe_observed() -> TestResult {
    timeout(
        Duration::from_secs(3),
        committed_open_is_not_replayed_and_reports_maybe_observed_case(),
    )
    .await??;
    Ok(())
}

async fn committed_open_is_not_replayed_and_reports_maybe_observed_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&listener, SessionRole::Connector).await?;
        let frame = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OPEN"))??;
        if !matches!(frame, Frame::Open { .. }) {
            return Err(unexpected(frame).into());
        }
        drop(transport);

        // The managed Connector may reconnect, but the old OPEN must not be
        // replayed on that session.
        let (mut replacement, _) = accept_session(&listener, SessionRole::Connector).await?;
        assert!(
            timeout(Duration::from_millis(150), replacement.next())
                .await
                .is_err()
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let error = connector
        .open("echo.alpha")
        .await
        .err()
        .ok_or_else(|| io::Error::other("OPEN unexpectedly succeeded"))?;
    assert_eq!(error.observation(), PeerObservation::MaybeObserved);
    server.await??;
    connector.close();
    Ok(())
}

#[tokio::test]
async fn opened_pipe_fails_when_its_session_is_replaced() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let frame = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OPEN"))??;
        let connection_id = match frame {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Opened {
                pipe_id: PipeId::new(session_id, connection_id),
            })
            .await?;
        drop(transport);

        let _ = timeout(
            Duration::from_secs(1),
            accept_session(&listener, SessionRole::Connector),
        )
        .await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let mut pipe = connector.open("echo.alpha").await?;
    let mut buffer = [0_u8; 1];
    let error = timeout(Duration::from_secs(1), pipe.read(&mut buffer))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("old Pipe did not fail after session replacement"))?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    connector.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn full_pipe_buffer_resets_only_that_pipe() -> TestResult {
    timeout(
        Duration::from_secs(3),
        full_pipe_buffer_resets_only_that_pipe_case(),
    )
    .await??;
    Ok(())
}

async fn full_pipe_buffer_resets_only_that_pipe_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let frame = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OPEN"))??;
        let connection_id = match frame {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        let pipe_id = PipeId::new(session_id, connection_id);
        transport.send(Frame::Opened { pipe_id }).await?;
        for value in [b"one".as_slice(), b"two".as_slice()] {
            transport
                .send(Frame::Data {
                    pipe_id,
                    payload: Bytes::copy_from_slice(value),
                })
                .await?;
        }
        let reset = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing RESET"))??;
        if !matches!(reset, Frame::Reset { pipe_id: id, .. } if id == pipe_id) {
            return Err(unexpected(reset).into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(Config::new(address).with_pipe_inbound_capacity(1)).await?;
    let mut pipe = connector.open("echo.alpha").await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut buffer = [0_u8; 3];
    assert_eq!(pipe.read(&mut buffer).await?, 3);
    assert_eq!(&buffer, b"one");
    let error = pipe
        .read(&mut buffer)
        .await
        .err()
        .ok_or_else(|| io::Error::other("buffer overflow did not fail Pipe"))?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    server.await??;
    connector.close();
    Ok(())
}
