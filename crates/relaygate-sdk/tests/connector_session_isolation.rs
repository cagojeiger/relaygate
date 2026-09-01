mod support;

use std::{io, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, PipeId, SessionRole};
use relaygate_sdk::{Config, Connector, ErrorCode, PeerObservation};
use tokio::{sync::oneshot, time::timeout};

use support::{
    TestResult, TestTransport, accept_session, bind_gateway, next_application_frame, unexpected,
};

#[tokio::test]
async fn timed_out_session_late_opened_cannot_complete_reused_connection_id() -> TestResult {
    timeout(
        Duration::from_secs(5),
        timed_out_session_late_opened_cannot_complete_reused_connection_id_case(),
    )
    .await??;
    Ok(())
}

async fn timed_out_session_late_opened_cannot_complete_reused_connection_id_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (replacement_ready_tx, replacement_ready_rx) = oneshot::channel();
    let (stale_processed_tx, stale_processed_rx) = oneshot::channel();
    let (release_current_tx, release_current_rx) = oneshot::channel();
    let (client_done_tx, client_done_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (mut initial, initial_session_id) =
            accept_session(&gateway, SessionRole::Connector).await?;
        let existing_connection_id =
            open_connection_id(next_application_frame(&mut initial).await?, "echo.existing")?;
        initial
            .send(Frame::Opened {
                pipe_id: PipeId::new(initial_session_id, existing_connection_id),
            })
            .await?;

        let timed_out_connection_id =
            open_connection_id(next_application_frame(&mut initial).await?, "echo.timeout")?;
        assert_eq!(existing_connection_id, 1);
        assert_eq!(timed_out_connection_id, 2);
        let ended = timeout(Duration::from_secs(1), initial.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("timed-out ConnectorSession remained open").into());
        }

        let (mut replacement, replacement_session_id) =
            accept_session(&gateway, SessionRole::Connector).await?;
        assert_ne!(initial_session_id, replacement_session_id);
        let _ = replacement_ready_tx.send(());

        let warmup_connection_id = open_connection_id(
            next_application_frame(&mut replacement).await?,
            "echo.warmup",
        )?;
        assert_eq!(warmup_connection_id, 1);
        replacement
            .send(Frame::OpenFailed {
                connection_id: warmup_connection_id,
                code: relaygate_protocol::ErrorCode::NotFound,
                observation: relaygate_protocol::PeerObservation::NotObserved,
                message: "warm-up failure".to_owned(),
            })
            .await?;

        let replacement_connection_id = open_connection_id(
            next_application_frame(&mut replacement).await?,
            "echo.replacement",
        )?;
        assert_eq!(replacement_connection_id, timed_out_connection_id);
        send_opened_with_barrier(
            &mut replacement,
            PipeId::new(initial_session_id, timed_out_connection_id),
            0x1326,
        )
        .await?;
        let _ = stale_processed_tx.send(());
        release_current_rx
            .await
            .map_err(|_| io::Error::other("current OPENED release signal was dropped"))?;

        let current_pipe_id = PipeId::new(replacement_session_id, replacement_connection_id);
        replacement
            .send(Frame::Opened {
                pipe_id: current_pipe_id,
            })
            .await?;
        replacement
            .send(Frame::Data {
                pipe_id: current_pipe_id,
                payload: b"fresh".as_slice().into(),
            })
            .await?;
        client_done_rx
            .await
            .map_err(|_| io::Error::other("replacement Pipe result was dropped"))?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let connector = Connector::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(500))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let mut existing = connector.open("echo.existing").await?;
    let timeout_error = connector
        .open("echo.timeout")
        .await
        .err()
        .ok_or_else(|| io::Error::other("timed-out OPEN unexpectedly succeeded"))?;
    assert_eq!(timeout_error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(timeout_error.observation(), PeerObservation::MaybeObserved);

    let mut byte = [0_u8; 1];
    let existing_error = timeout(Duration::from_secs(1), existing.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("existing Pipe survived session timeout"))?;
    assert_eq!(existing_error.code(), ErrorCode::Unavailable);
    replacement_ready_rx
        .await
        .map_err(|_| io::Error::other("replacement session readiness signal was dropped"))?;

    let warmup_error = connector
        .open("echo.warmup")
        .await
        .err()
        .ok_or_else(|| io::Error::other("warm-up OPEN unexpectedly succeeded"))?;
    assert_eq!(warmup_error.code(), ErrorCode::NotFound);
    assert_eq!(warmup_error.observation(), PeerObservation::NotObserved);

    let mut replacement_open = Box::pin(connector.open("echo.replacement"));
    tokio::select! {
        biased;
        result = &mut replacement_open => {
            return Err(io::Error::other(format!(
                "stale OPENED completed the replacement attempt: {result:?}"
            )).into());
        }
        processed = stale_processed_rx => {
            processed.map_err(|_| io::Error::other(
                "stale OPENED processing signal was dropped"
            ))?;
        }
    }
    let _ = release_current_tx.send(());

    let mut pipe = replacement_open.await?;
    let mut payload = [0_u8; 5];
    assert_eq!(pipe.read_into(&mut payload).await?, payload.len());
    assert_eq!(&payload, b"fresh");
    let _ = client_done_tx.send(());
    connector.close();
    server.await??;
    Ok(())
}

fn open_connection_id(frame: Frame, expected_client_id: &str) -> TestResult<u64> {
    match frame {
        Frame::Open {
            connection_id,
            client_id,
        } if client_id == expected_client_id => Ok(connection_id),
        other => Err(unexpected(other).into()),
    }
}

async fn send_opened_with_barrier(
    transport: &mut TestTransport,
    pipe_id: PipeId,
    nonce: u64,
) -> TestResult {
    transport.send(Frame::Opened { pipe_id }).await?;
    transport.send(Frame::Ping { nonce }).await?;
    loop {
        let frame = timeout(Duration::from_secs(1), transport.next())
            .await?
            .ok_or_else(|| io::Error::other("SDK session closed before OPENED barrier"))??;
        match frame {
            Frame::Pong { nonce: observed } if observed == nonce => return Ok(()),
            Frame::Ping { nonce } => transport.send(Frame::Pong { nonce }).await?,
            Frame::Pong { .. } => {}
            other => return Err(unexpected(other).into()),
        }
    }
}
