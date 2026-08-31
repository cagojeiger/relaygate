mod support;

use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{
    BindingId, ErrorCode as WireErrorCode, Frame, PipeId, SessionId, SessionRole,
};
use relaygate_sdk::{Config, ErrorCode, ListenerRuntime, ListenerStatus};
use tokio::sync::oneshot;
use tokio::time::timeout;

use support::{TestResult, accept_session, bind_gateway, unexpected};

#[tokio::test]
async fn session_loss_drains_unaccepted_pipe_before_recovery_admission() -> TestResult {
    timeout(
        Duration::from_secs(5),
        session_loss_drains_unaccepted_pipe_before_recovery_admission_case(),
    )
    .await??;
    Ok(())
}

async fn session_loss_drains_unaccepted_pipe_before_recovery_admission_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (first_admitted_tx, first_admitted_rx) = oneshot::channel();
    let (first_accepted_tx, first_accepted_rx) = oneshot::channel();
    let (second_admitted_tx, second_admitted_rx) = oneshot::channel();
    let (allow_recovery_tx, allow_recovery_rx) = oneshot::channel();
    let (recovered_admitted_tx, recovered_admitted_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = first
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing initial REGISTER"))??;
        let request_id = match register {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        let first_binding = BindingId::new();
        first
            .send(Frame::Registered {
                request_id,
                binding_id: first_binding,
            })
            .await?;

        let accepted_pipe_id = PipeId::new(SessionId::new(), 20);
        first
            .send(Frame::Offer {
                pipe_id: accepted_pipe_id,
                binding_id: first_binding,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let accepted = first
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing first OFFER_ACCEPTED"))??;
        if !matches!(accepted, Frame::OfferAccepted { pipe_id } if pipe_id == accepted_pipe_id) {
            return Err(unexpected(accepted).into());
        }
        let _ = first_admitted_tx.send(());
        let _ = first_accepted_rx.await;

        let queued_pipe_id = PipeId::new(SessionId::new(), 21);
        first
            .send(Frame::Offer {
                pipe_id: queued_pipe_id,
                binding_id: first_binding,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let queued = first
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing queued OFFER_ACCEPTED"))??;
        if !matches!(queued, Frame::OfferAccepted { pipe_id } if pipe_id == queued_pipe_id) {
            return Err(unexpected(queued).into());
        }
        let _ = second_admitted_tx.send(());
        drop(first);

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let recovery = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing recovery REGISTER"))??;
        let recovery_request = match recovery {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        let _ = allow_recovery_rx.await;
        let replacement_binding = BindingId::new();
        replacement
            .send(Frame::Registered {
                request_id: recovery_request,
                binding_id: replacement_binding,
            })
            .await?;

        let recovered_pipe_id = PipeId::new(SessionId::new(), 22);
        replacement
            .send(Frame::Offer {
                pipe_id: recovered_pipe_id,
                binding_id: replacement_binding,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let recovered = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing recovered OFFER_ACCEPTED"))??;
        if !matches!(recovered, Frame::OfferAccepted { pipe_id } if pipe_id == recovered_pipe_id) {
            return Err(unexpected(recovered).into());
        }
        let _ = recovered_admitted_tx.send(());
        let _ = done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_listener_queue_capacity(1)
            .with_offer_timeout(Duration::from_millis(100))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    first_admitted_rx.await?;
    let mut accepted_pipe = listener.accept().await?;
    let _ = first_accepted_tx.send(());
    second_admitted_rx.await?;

    timeout(Duration::from_secs(1), async {
        while listener.status() == ListenerStatus::Active {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut byte = [0_u8; 1];
    let accepted_error = timeout(Duration::from_secs(1), accepted_pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("accepted old-session Pipe did not fail"))?;
    assert_eq!(accepted_error.code(), ErrorCode::Unavailable);
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "old unaccepted Pipe was returned after ListenerSession loss"
    );

    let _ = allow_recovery_tx.send(());
    let recovered_pipe = timeout(Duration::from_secs(1), listener.accept()).await??;
    recovered_admitted_rx.await?;
    assert_eq!(listener.status(), ListenerStatus::Active);
    drop(recovered_pipe);
    drop(accepted_pipe);
    let _ = done_tx.send(());
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn terminal_queued_pipe_is_discarded_before_accept() -> TestResult {
    timeout(
        Duration::from_secs(3),
        terminal_queued_pipe_is_discarded_before_accept_case(),
    )
    .await??;
    Ok(())
}

async fn terminal_queued_pipe_is_discarded_before_accept_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        let request_id = match register {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        let binding_id = BindingId::new();
        transport
            .send(Frame::Registered {
                request_id,
                binding_id,
            })
            .await?;

        let terminal_pipe_id = PipeId::new(SessionId::new(), 30);
        transport
            .send(Frame::Offer {
                pipe_id: terminal_pipe_id,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let terminal_admitted = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing terminal Pipe OFFER_ACCEPTED"))??;
        if !matches!(terminal_admitted, Frame::OfferAccepted { pipe_id } if pipe_id == terminal_pipe_id)
        {
            return Err(unexpected(terminal_admitted).into());
        }
        transport
            .send(Frame::Reset {
                pipe_id: terminal_pipe_id,
                code: WireErrorCode::Cancelled,
                message: "Connector cancelled before Listener accept".to_owned(),
            })
            .await?;

        let live_pipe_id = PipeId::new(SessionId::new(), 31);
        transport
            .send(Frame::Offer {
                pipe_id: live_pipe_id,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let live_admitted = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing live Pipe OFFER_ACCEPTED"))??;
        if !matches!(live_admitted, Frame::OfferAccepted { pipe_id } if pipe_id == live_pipe_id) {
            return Err(unexpected(live_admitted).into());
        }
        transport
            .send(Frame::Data {
                pipe_id: live_pipe_id,
                payload: Bytes::from_static(b"live"),
            })
            .await?;
        transport
            .send(Frame::Fin {
                pipe_id: live_pipe_id,
            })
            .await?;
        let _ = ready_tx.send(());
        let _ = done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime =
        ListenerRuntime::connect(Config::new(address).with_listener_queue_capacity(1)).await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    ready_rx.await?;
    let mut pipe = listener.accept().await?;
    let mut payload = [0_u8; 4];
    assert_eq!(pipe.read_into(&mut payload).await?, payload.len());
    assert_eq!(&payload, b"live");
    assert_eq!(pipe.read_into(&mut payload).await?, 0);
    drop(pipe);
    let _ = done_tx.send(());
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn stalled_listener_transport_send_times_out_and_cleans_up_session() -> TestResult {
    timeout(
        Duration::from_secs(5),
        stalled_listener_transport_send_times_out_and_cleans_up_session_case(),
    )
    .await??;
    Ok(())
}

async fn stalled_listener_transport_send_times_out_and_cleans_up_session_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        let request_id = match register {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        let binding_id = BindingId::new();
        transport
            .send(Frame::Registered {
                request_id,
                binding_id,
            })
            .await?;

        let pipe_id = PipeId::new(SessionId::new(), 1);
        transport
            .send(Frame::Offer {
                pipe_id,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let accepted = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OFFER_ACCEPTED"))??;
        if !matches!(accepted, Frame::OfferAccepted { pipe_id: id } if id == pipe_id) {
            return Err(unexpected(accepted).into());
        }

        // Deliberately stop reading. The Listener actor must bound the socket
        // send and fail its session instead of retaining the Pipe forever.
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(transport);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(20))
            .with_outbound_capacity(1)
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    let mut pipe = listener.accept().await?;
    let payload = vec![0_u8; 32 * 1024 * 1024];
    let write_error = timeout(Duration::from_secs(2), pipe.write_all_bytes(&payload))
        .await?
        .err()
        .ok_or_else(|| {
            io::Error::other("stalled Listener transport write unexpectedly succeeded")
        })?;
    assert_eq!(write_error.code(), ErrorCode::Unavailable);

    let mut byte = [0_u8; 1];
    let read_error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("stalled ListenerSession Pipe did not fail"))?;
    assert_eq!(read_error.code(), ErrorCode::Unavailable);
    assert_eq!(listener.status(), ListenerStatus::Suspended);
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn cancelling_runtime_during_stalled_listener_send_cleans_up_session() -> TestResult {
    timeout(
        Duration::from_secs(5),
        cancelling_runtime_during_stalled_listener_send_cleans_up_session_case(),
    )
    .await??;
    Ok(())
}

async fn cancelling_runtime_during_stalled_listener_send_cleans_up_session_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        let request_id = match register {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        let binding_id = BindingId::new();
        transport
            .send(Frame::Registered {
                request_id,
                binding_id,
            })
            .await?;

        let pipe_id = PipeId::new(SessionId::new(), 1);
        transport
            .send(Frame::Offer {
                pipe_id,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let accepted = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing OFFER_ACCEPTED"))??;
        if !matches!(accepted, Frame::OfferAccepted { pipe_id: id } if id == pipe_id) {
            return Err(unexpected(accepted).into());
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(transport);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_secs(1))
            .with_outbound_capacity(1)
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    let mut pipe = listener.accept().await?;
    let mut writer = tokio::spawn(async move {
        let payload = vec![0_u8; 32 * 1024 * 1024];
        pipe.write_all_bytes(&payload).await
    });

    if let Ok(result) = timeout(Duration::from_millis(50), &mut writer).await {
        return Err(io::Error::other(format!(
            "listener write did not reach stalled state before cancellation: {result:?}"
        ))
        .into());
    }

    runtime.close();
    let result = timeout(Duration::from_secs(2), writer).await??;
    let error = result
        .err()
        .ok_or_else(|| io::Error::other("cancelled runtime allowed stalled write to succeed"))?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(listener.status(), ListenerStatus::Closed);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn stale_and_duplicate_offers_never_enqueue_a_second_pipe() -> TestResult {
    timeout(
        Duration::from_secs(3),
        stale_and_duplicate_offers_never_enqueue_a_second_pipe_case(),
    )
    .await??;
    Ok(())
}

async fn stale_and_duplicate_offers_never_enqueue_a_second_pipe_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (offers_checked_tx, offers_checked_rx) = oneshot::channel();
    let (test_done_tx, test_done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        let request_id = match register {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        let binding_id = BindingId::new();
        transport
            .send(Frame::Registered {
                request_id,
                binding_id,
            })
            .await?;

        let pipe_id = PipeId::new(SessionId::new(), 11);
        transport
            .send(Frame::Offer {
                pipe_id,
                binding_id: BindingId::new(),
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let stale = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing stale Offer rejection"))??;
        if !matches!(
            stale,
            Frame::OfferRejected {
                pipe_id: id,
                code: WireErrorCode::FailedPrecondition,
                ..
            } if id == pipe_id
        ) {
            return Err(unexpected(stale).into());
        }

        for _ in 0..2 {
            transport
                .send(Frame::Offer {
                    pipe_id,
                    binding_id,
                    client_id: "echo.alpha".to_owned(),
                })
                .await?;
            let accepted = transport
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing Offer acceptance"))??;
            if !matches!(accepted, Frame::OfferAccepted { pipe_id: id } if id == pipe_id) {
                return Err(unexpected(accepted).into());
            }
        }
        let _ = offers_checked_tx.send(());
        let _ = test_done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    offers_checked_rx.await?;
    let pipe = listener.accept().await?;
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    drop(pipe);
    let _ = test_done_tx.send(());
    server.await??;
    runtime.close();
    Ok(())
}

#[tokio::test]
async fn full_listener_queue_rejects_new_offer_without_replacing_old_pipe() -> TestResult {
    timeout(
        Duration::from_secs(3),
        full_listener_queue_rejects_new_offer_without_replacing_old_pipe_case(),
    )
    .await??;
    Ok(())
}

async fn full_listener_queue_rejects_new_offer_without_replacing_old_pipe_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (offers_checked_tx, offers_checked_rx) = oneshot::channel();
    let (test_done_tx, test_done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        let request_id = match register {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        let binding_id = BindingId::new();
        transport
            .send(Frame::Registered {
                request_id,
                binding_id,
            })
            .await?;

        let first_pipe = PipeId::new(SessionId::new(), 1);
        transport
            .send(Frame::Offer {
                pipe_id: first_pipe,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let first = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing first Offer result"))??;
        if !matches!(first, Frame::OfferAccepted { pipe_id } if pipe_id == first_pipe) {
            return Err(unexpected(first).into());
        }

        let second_pipe = PipeId::new(SessionId::new(), 2);
        transport
            .send(Frame::Offer {
                pipe_id: second_pipe,
                binding_id,
                client_id: "echo.alpha".to_owned(),
            })
            .await?;
        let second = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing second Offer rejection"))??;
        if !matches!(
            second,
            Frame::OfferRejected {
                pipe_id,
                code: WireErrorCode::ResourceExhausted,
                ..
            } if pipe_id == second_pipe
        ) {
            return Err(unexpected(second).into());
        }
        let _ = offers_checked_tx.send(());
        let _ = test_done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_listener_queue_capacity(1)
            .with_offer_timeout(Duration::from_millis(30)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    offers_checked_rx.await?;
    let pipe = listener.accept().await?;
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    drop(pipe);
    let _ = test_done_tx.send(());
    server.await??;
    runtime.close();
    Ok(())
}
