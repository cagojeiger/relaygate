mod support;

use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, PipeId, SessionRole};
use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation};
use tokio::time::{Instant, timeout};

use support::{
    TestResult, accept_session, answer_heartbeats_for, bind_gateway, next_application_frame,
    unexpected,
};

#[tokio::test]
async fn idle_connector_session_uses_heartbeat_and_pipe_read_idle_is_not_a_timeout() -> TestResult {
    timeout(
        Duration::from_secs(3),
        idle_connector_session_uses_heartbeat_and_pipe_read_idle_is_not_a_timeout_case(),
    )
    .await??;
    Ok(())
}

async fn idle_connector_session_uses_heartbeat_and_pipe_read_idle_is_not_a_timeout_case()
-> TestResult {
    let (listener, address) = bind_gateway().await?;
    let (idle_checked_tx, idle_checked_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let heartbeat = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing heartbeat PING"))??;
        let nonce = match heartbeat {
            Frame::Ping { nonce } => nonce,
            other => return Err(unexpected(other).into()),
        };
        transport.send(Frame::Pong { nonce }).await?;
        let _ = idle_checked_tx.send(());

        let open = next_application_frame(&mut transport).await?;
        let connection_id = match open {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        let pipe_id = PipeId::new(session_id, connection_id);
        transport.send(Frame::Opened { pipe_id }).await?;
        answer_heartbeats_for(&mut transport, Duration::from_millis(150)).await?;
        transport
            .send(Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"x"),
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(50))
            .with_heartbeat(Duration::from_millis(40), Duration::from_millis(40)),
    )
    .await?;
    idle_checked_rx.await?;
    let mut pipe = connector.open("echo.idle").await?;
    let mut byte = [0_u8; 1];
    assert_eq!(pipe.read_into(&mut byte).await?, 1);
    assert_eq!(&byte, b"x");
    connector.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn connector_heartbeat_timeout_closes_session_and_reconnects() -> TestResult {
    timeout(
        Duration::from_secs(3),
        connector_heartbeat_timeout_closes_session_and_reconnects_case(),
    )
    .await??;
    Ok(())
}

async fn connector_heartbeat_timeout_closes_session_and_reconnects_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, initial_session_id) =
            accept_session(&listener, SessionRole::Connector).await?;
        let heartbeat = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing heartbeat PING"))??;
        if !matches!(heartbeat, Frame::Ping { .. }) {
            return Err(unexpected(heartbeat).into());
        }
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(
                io::Error::other("heartbeat timeout did not close ConnectorSession").into(),
            );
        }

        let (_replacement, replacement_session_id) =
            accept_session(&listener, SessionRole::Connector).await?;
        assert_ne!(initial_session_id, replacement_session_id);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
            .with_heartbeat(Duration::from_millis(40), Duration::from_millis(40)),
    )
    .await?;
    server.await??;
    connector.close();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_pong_is_not_starved_by_sustained_outbound_frames() -> TestResult {
    timeout(
        Duration::from_secs(3),
        matching_pong_is_not_starved_by_sustained_outbound_frames_case(),
    )
    .await??;
    Ok(())
}

async fn matching_pong_is_not_starved_by_sustained_outbound_frames_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let open = next_application_frame(&mut transport).await?;
        let connection_id = match open {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        let pipe_id = PipeId::new(session_id, connection_id);
        transport.send(Frame::Opened { pipe_id }).await?;

        let mut answered_heartbeats = 0_usize;
        loop {
            let frame = transport
                .next()
                .await
                .ok_or_else(|| io::Error::other("SDK session ended during outbound load"))??;
            match frame {
                Frame::Ping { nonce } => {
                    transport.send(Frame::Pong { nonce }).await?;
                    answered_heartbeats += 1;
                }
                Frame::Data {
                    pipe_id: received, ..
                } if received == pipe_id => {}
                Frame::Close { pipe_id: closed } if closed == pipe_id => break,
                other => return Err(unexpected(other).into()),
            }
        }
        assert!(
            answered_heartbeats > 0,
            "test must overlap sustained outbound traffic with a heartbeat"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(500))
            .with_heartbeat(Duration::from_millis(30), Duration::from_millis(100))
            .with_outbound_capacity(256),
    )
    .await?;
    let mut pipe = connector.open("echo.loaded").await?;
    let finish = Instant::now() + Duration::from_millis(300);
    while Instant::now() < finish {
        pipe.write_all_bytes(b"x").await?;
    }
    pipe.close().await?;
    server.await??;
    connector.close();
    Ok(())
}

#[tokio::test]
async fn dropping_last_connector_owner_stops_the_session() -> TestResult {
    timeout(
        Duration::from_secs(3),
        dropping_last_connector_owner_stops_the_session_case(),
    )
    .await??;
    Ok(())
}

async fn dropping_last_connector_owner_stops_the_session_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&listener, SessionRole::Connector).await?;
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("dropped Connector kept its session alive").into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(Config::new(address)).await?;
    drop(connector);
    server.await??;
    Ok(())
}

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
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
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
        let _ = received_tx.send(());
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(
                io::Error::other("Pipe drop did not stop the last SDK runtime owner").into(),
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(Config::new(address)).await?;
    let mut pipe = connector.open("echo.alpha").await?;
    drop(connector);
    let mut buffer = [0_u8; 5];
    assert_eq!(pipe.read_into(&mut buffer).await?, 5);
    assert_eq!(&buffer, b"hello");
    assert_eq!(pipe.read_into(&mut buffer).await?, 0);
    pipe.write_all_bytes(b"reply").await?;
    pipe.shutdown_write().await?;
    pipe.shutdown_write().await?;
    received_rx.await?;
    drop(pipe);
    server.await??;
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
        let (mut transport, initial_session_id) =
            accept_session(&listener, SessionRole::Connector).await?;
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
        let (mut replacement, replacement_session_id) =
            accept_session(&listener, SessionRole::Connector).await?;
        assert_ne!(initial_session_id, replacement_session_id);
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
async fn committed_open_timeout_closes_session_and_existing_pipes() -> TestResult {
    timeout(
        Duration::from_secs(3),
        committed_open_timeout_closes_session_and_existing_pipes_case(),
    )
    .await??;
    Ok(())
}

async fn committed_open_timeout_closes_session_and_existing_pipes_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let first = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing first OPEN"))??;
        let first_connection = match first {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Opened {
                pipe_id: PipeId::new(session_id, first_connection),
            })
            .await?;

        let second = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing timed-out OPEN"))??;
        if !matches!(second, Frame::Open { .. }) {
            return Err(unexpected(second).into());
        }
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("timed-out ConnectorSession remained open").into());
        }

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
            .with_operation_timeout(Duration::from_millis(80))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let mut existing = connector.open("echo.alpha").await?;
    let error = connector
        .open("echo.beta")
        .await
        .err()
        .ok_or_else(|| io::Error::other("timed-out OPEN unexpectedly succeeded"))?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(error.observation(), PeerObservation::MaybeObserved);

    let mut byte = [0_u8; 1];
    let pipe_error = timeout(Duration::from_secs(1), existing.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("existing Pipe survived session timeout"))?;
    assert_eq!(pipe_error.code(), ErrorCode::Unavailable);
    server.await??;
    connector.close();
    Ok(())
}

#[tokio::test]
async fn stalled_transport_send_times_out_and_cleans_up_session() -> TestResult {
    timeout(
        Duration::from_secs(5),
        stalled_transport_send_times_out_and_cleans_up_session_case(),
    )
    .await??;
    Ok(())
}

async fn stalled_transport_send_times_out_and_cleans_up_session_case() -> TestResult {
    let (listener, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&listener, SessionRole::Connector).await?;
        let open = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OPEN"))??;
        let connection_id = match open {
            Frame::Open { connection_id, .. } => connection_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Opened {
                pipe_id: PipeId::new(session_id, connection_id),
            })
            .await?;

        // Deliberately stop reading. The SDK must bound the socket send and
        // terminate the session instead of leaving the actor blocked forever.
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(transport);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(20))
            .with_outbound_capacity(1)
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let mut pipe = connector.open("echo.alpha").await?;
    let payload = vec![0_u8; 32 * 1024 * 1024];
    let write_error = timeout(Duration::from_secs(2), pipe.write_all_bytes(&payload))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("stalled transport write unexpectedly succeeded"))?;
    assert_eq!(write_error.code(), ErrorCode::Unavailable);

    let mut byte = [0_u8; 1];
    let read_error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("stalled session Pipe did not fail"))?;
    assert_eq!(read_error.code(), ErrorCode::Unavailable);
    connector.close();
    server.await??;
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
    let error = timeout(Duration::from_secs(1), pipe.read_into(&mut buffer))
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
    assert_eq!(pipe.read_into(&mut buffer).await?, 3);
    assert_eq!(&buffer, b"one");
    let error = pipe
        .read_into(&mut buffer)
        .await
        .err()
        .ok_or_else(|| io::Error::other("buffer overflow did not fail Pipe"))?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    server.await??;
    connector.close();
    Ok(())
}
