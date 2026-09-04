mod support;

use std::{io, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{
    BindingId, ErrorCode as WireErrorCode, Frame, PipeId, SessionId, SessionRole,
};
use relaygate_sdk::{Config, ErrorCode, ListenerRuntime, ListenerStatus, PeerObservation};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::timeout;

use support::{
    TestResult, TestTransport, accept_session, bind_gateway, next_application_frame, unexpected,
};

const CLIENT_ID: &str = "echo.alpha";
const OLD_KEY: &str = "old-key";
const NEW_KEY: &str = "new-key";

#[tokio::test]
async fn session_loss_then_permanent_rejection_blocks_listener_until_recreated() -> TestResult {
    timeout(
        Duration::from_secs(5),
        session_loss_then_permanent_rejection_blocks_listener_until_recreated_case(),
    )
    .await??;
    Ok(())
}

async fn session_loss_then_permanent_rejection_blocks_listener_until_recreated_case() -> TestResult
{
    let (gateway, address) = bind_gateway().await?;
    let (first_offer_tx, first_offer_rx) = oneshot::channel();
    let (first_accepted_tx, first_accepted_rx) = oneshot::channel();
    let (queued_offer_tx, queued_offer_rx) = oneshot::channel();
    let (end_first_tx, end_first_rx) = oneshot::channel();
    let (recovery_committed_tx, recovery_committed_rx) = oneshot::channel();
    let (reject_recovery_tx, reject_recovery_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (mut first, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let initial_request = expect_register(&mut first, OLD_KEY).await?;
        let first_binding = BindingId::new();
        first
            .send(Frame::Registered {
                request_id: initial_request,
                binding_id: first_binding,
            })
            .await?;

        let accepted_pipe_id = offer_pipe(&mut first, first_binding, 40).await?;
        first
            .send(Frame::Data {
                pipe_id: accepted_pipe_id,
                payload: b"first".as_slice().into(),
            })
            .await?;
        let _ = first_offer_tx.send(());
        let _ = first_accepted_rx.await;
        match next_application_frame(&mut first).await? {
            Frame::Data { pipe_id, payload }
                if pipe_id == accepted_pipe_id && payload.as_ref() == b"written once" => {}
            other => return Err(unexpected(other).into()),
        }

        offer_pipe(&mut first, first_binding, 41).await?;
        let _ = queued_offer_tx.send(());
        let _ = end_first_rx.await;
        drop(first);

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let recovery_request = expect_register(&mut replacement, OLD_KEY).await?;
        let _ = recovery_committed_tx.send(());
        let _ = reject_recovery_rx.await;
        replacement
            .send(Frame::RegisterFailed {
                request_id: recovery_request,
                code: WireErrorCode::Unauthenticated,
                message: "replacement Gateway rejected the configured key".to_owned(),
            })
            .await?;

        let new_request = expect_register(&mut replacement, NEW_KEY).await?;
        replacement
            .send(Frame::Registered {
                request_id: new_request,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = done_rx.await;
        match timeout(Duration::from_secs(1), replacement.next()).await? {
            None => Ok::<_, Box<dyn std::error::Error + Send + Sync>>(()),
            Some(frame) => Err(io::Error::other(format!(
                "replacement ListenerSession replayed data or failed to close cleanly: {frame:?}"
            ))
            .into()),
        }
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_secs(1))
            .with_listener_queue_capacity(2)
            .with_offer_timeout(Duration::from_millis(100))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let listener = Arc::new(runtime.listen(CLIENT_ID, OLD_KEY).await?);

    first_offer_rx.await?;
    let mut accepted_pipe = listener.accept().await?;
    let mut payload = [0; 5];
    accepted_pipe.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"first");
    accepted_pipe.write_all_bytes(b"written once").await?;
    let _ = first_accepted_tx.send(());
    queued_offer_rx.await?;
    let _ = end_first_tx.send(());

    let mut byte = [0_u8; 1];
    let accepted_error = timeout(Duration::from_secs(1), accepted_pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("accepted old-session Pipe did not fail"))?;
    assert_eq!(accepted_error.code(), ErrorCode::Unavailable);
    assert_eq!(accepted_error.observation(), PeerObservation::NotObserved);
    assert!(accepted_error.is_retryable());
    recovery_committed_rx.await?;
    assert_eq!(listener.status(), ListenerStatus::Registering);

    let mut pending_accept = {
        let listener = Arc::clone(&listener);
        tokio::spawn(async move { listener.accept().await })
    };
    assert!(
        timeout(Duration::from_millis(50), &mut pending_accept)
            .await
            .is_err(),
        "old queued Pipe was returned before recovery completed"
    );

    let _ = reject_recovery_tx.send(());
    let pending_result = timeout(Duration::from_secs(1), &mut pending_accept).await??;
    let pending_error = pending_result
        .err()
        .ok_or_else(|| io::Error::other("blocked Listener returned the old queued Pipe"))?;
    assert_eq!(pending_error.code(), ErrorCode::Unauthenticated);
    assert_eq!(listener.status(), ListenerStatus::Blocked);

    let future_error = listener
        .accept()
        .await
        .err()
        .ok_or_else(|| io::Error::other("blocked Listener accepted a future Pipe"))?;
    assert_eq!(future_error, pending_error);

    listener.close().await?;
    assert_eq!(listener.status(), ListenerStatus::Closed);
    let replacement = runtime.listen(CLIENT_ID, NEW_KEY).await?;
    assert_eq!(replacement.status(), ListenerStatus::Active);
    let (mut old_reader, mut old_writer) = accepted_pipe.into_split();
    let read_error = old_reader
        .read(&mut byte)
        .await
        .err()
        .ok_or_else(|| io::Error::other("old Listener Pipe resumed reading after recovery"))?;
    let write_error = old_writer
        .write_all(b"must not replay")
        .await
        .err()
        .ok_or_else(|| io::Error::other("old Listener Pipe resumed writing after recovery"))?;
    for error in [read_error, write_error] {
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<relaygate_sdk::Error>()),
            Some(&accepted_error)
        );
    }
    assert!(
        timeout(Duration::from_millis(50), replacement.accept())
            .await
            .is_err(),
        "old queued Pipe leaked into the corrected-key Listener"
    );

    runtime.close();
    let _ = done_tx.send(());
    server.await??;
    Ok(())
}

async fn expect_register(transport: &mut TestTransport, expected_key: &str) -> TestResult<u64> {
    match next_application_frame(transport).await? {
        Frame::Register {
            request_id,
            client_id,
            client_key,
        } if client_id == CLIENT_ID && client_key.expose_secret() == expected_key => Ok(request_id),
        other => Err(unexpected(other).into()),
    }
}

async fn offer_pipe(
    transport: &mut TestTransport,
    binding_id: BindingId,
    connection_id: u64,
) -> TestResult<PipeId> {
    let pipe_id = PipeId::new(SessionId::new(), connection_id);
    transport
        .send(Frame::Offer {
            pipe_id,
            binding_id,
            client_id: CLIENT_ID.to_owned(),
        })
        .await?;
    match next_application_frame(transport).await? {
        Frame::OfferAccepted { pipe_id: accepted } if accepted == pipe_id => Ok(pipe_id),
        other => Err(unexpected(other).into()),
    }
}
