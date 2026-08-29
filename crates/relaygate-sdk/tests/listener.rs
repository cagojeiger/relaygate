mod support;

use std::{io, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{
    BindingId, ErrorCode as WireErrorCode, Frame, PipeId, SessionId, SessionRole,
};
use relaygate_sdk::{Config, ErrorCode, ListenerRuntime, ListenerStatus};
use tokio::sync::oneshot;
use tokio::time::timeout;

use support::{TestResult, accept_session, bind_gateway, unexpected};

#[tokio::test]
async fn dropping_last_listener_owner_stops_the_session() -> TestResult {
    timeout(
        Duration::from_secs(3),
        dropping_last_listener_owner_stops_the_session_case(),
    )
    .await??;
    Ok(())
}

async fn dropping_last_listener_owner_stops_the_session_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("dropped Listener runtime kept its session alive").into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    drop(runtime);
    server.await??;
    Ok(())
}

#[tokio::test]
async fn concurrent_duplicate_listener_is_rejected_and_closed_id_can_be_reused() -> TestResult {
    timeout(
        Duration::from_secs(3),
        duplicate_listener_is_rejected_locally_and_closed_id_can_be_reused_case(),
    )
    .await??;
    Ok(())
}

async fn duplicate_listener_is_rejected_locally_and_closed_id_can_be_reused_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (duplicate_checked_tx, duplicate_checked_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let first = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing first REGISTER"))??;
        let first_request = match first {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Registered {
                request_id: first_request,
                binding_id: BindingId::new(),
            })
            .await?;

        if let Ok(Some(frame)) = timeout(Duration::from_millis(100), transport.next()).await {
            return match frame {
                Ok(frame) => Err(unexpected(frame).into()),
                Err(error) => Err(error.into()),
            };
        }
        let _ = duplicate_checked_tx.send(());

        let unregister = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing UNREGISTER"))??;
        if !matches!(unregister, Frame::Unregister { .. }) {
            return Err(unexpected(unregister).into());
        }
        let second = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing replacement REGISTER"))??;
        let second_request = match second {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Registered {
                request_id: second_request,
                binding_id: BindingId::new(),
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    let (first, second) = tokio::join!(
        runtime.listen("echo.alpha", "dev-key"),
        runtime.listen("echo.alpha", "dev-key")
    );
    let (listener, duplicate) = match (first, second) {
        (Ok(listener), Err(error)) | (Err(error), Ok(listener)) => (listener, error),
        (Ok(_), Ok(_)) => {
            return Err(io::Error::other("both duplicate Listeners unexpectedly succeeded").into());
        }
        (Err(first), Err(second)) => {
            return Err(io::Error::other(format!(
                "both duplicate Listeners failed: {first}; {second}"
            ))
            .into());
        }
    };
    assert_eq!(listener.status(), ListenerStatus::Active);
    assert_eq!(duplicate.code(), ErrorCode::AlreadyExists);
    duplicate_checked_rx.await?;
    listener.close().await?;
    let replacement = runtime.listen("echo.alpha", "new-key").await?;
    assert_eq!(replacement.status(), ListenerStatus::Active);
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn desired_listener_is_registered_again_on_new_session() -> TestResult {
    timeout(
        Duration::from_secs(3),
        desired_listener_is_registered_again_on_new_session_case(),
    )
    .await??;
    Ok(())
}

async fn desired_listener_is_registered_again_on_new_session_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
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
            transport
                .send(Frame::Registered {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
            if request_id == 1 {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    timeout(Duration::from_secs(1), async {
        loop {
            if listener.status() == ListenerStatus::Active {
                tokio::time::sleep(Duration::from_millis(80)).await;
                if listener.status() == ListenerStatus::Active {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn closing_listener_during_recovery_register_rebuilds_session_for_sibling() -> TestResult {
    timeout(
        Duration::from_secs(5),
        closing_listener_during_recovery_register_rebuilds_session_for_sibling_case(),
    )
    .await??;
    Ok(())
}

async fn closing_listener_during_recovery_register_rebuilds_session_for_sibling_case() -> TestResult
{
    let (gateway, address) = bind_gateway().await?;
    let (initial_ready_tx, initial_ready_rx) = oneshot::channel();
    let (recovery_committed_tx, recovery_committed_rx) = oneshot::channel();
    let (sibling_recovered_tx, sibling_recovered_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut initial, _) = accept_session(&gateway, SessionRole::Listener).await?;
        for expected_client in ["echo.alpha", "echo.beta"] {
            let register = initial
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing initial REGISTER"))??;
            let request_id = match register {
                Frame::Register {
                    request_id,
                    client_id,
                    ..
                } if client_id == expected_client => request_id,
                other => return Err(unexpected(other).into()),
            };
            initial
                .send(Frame::Registered {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
        }
        let _ = initial_ready_rx.await;
        drop(initial);

        let (mut recovery, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let mut recovery_clients = Vec::new();
        for _ in 0..2 {
            let register = recovery
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing recovery REGISTER"))??;
            match register {
                Frame::Register { client_id, .. } => recovery_clients.push(client_id),
                other => return Err(unexpected(other).into()),
            }
        }
        recovery_clients.sort();
        assert_eq!(
            recovery_clients,
            vec!["echo.alpha".to_owned(), "echo.beta".to_owned()]
        );
        let _ = recovery_committed_tx.send(());
        let ended = timeout(Duration::from_secs(1), recovery.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other(
                "abandoned recovery REGISTER did not rebuild ListenerSession",
            )
            .into());
        }

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing sibling recovery REGISTER"))??;
        let request_id = match register {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.beta" => request_id,
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::Registered {
                request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = sibling_recovered_tx.send(());
        let _ = done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_secs(1))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let alpha = runtime.listen("echo.alpha", "dev-key").await?;
    let beta = runtime.listen("echo.beta", "dev-key").await?;
    let _ = initial_ready_tx.send(());
    recovery_committed_rx.await?;

    drop(alpha);
    sibling_recovered_rx.await?;
    timeout(Duration::from_secs(1), async {
        while beta.status() != ListenerStatus::Active {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let _ = done_tx.send(());
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn returned_listener_retries_transient_recovery_failure() -> TestResult {
    timeout(
        Duration::from_secs(3),
        returned_listener_retries_transient_recovery_failure_case(),
    )
    .await??;
    Ok(())
}

async fn returned_listener_retries_transient_recovery_failure_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (returned_tx, returned_rx) = oneshot::channel();
    let (recovered_tx, recovered_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let initial = first
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing initial REGISTER"))??;
        let initial_request = match initial {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        first
            .send(Frame::Registered {
                request_id: initial_request,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = returned_rx.await;
        drop(first);

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let recovery = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing recovery REGISTER"))??;
        let recovery_request = match recovery {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::RegisterFailed {
                request_id: recovery_request,
                code: WireErrorCode::Unavailable,
                message: "temporary recovery failure".to_owned(),
            })
            .await?;

        let retried = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing managed recovery retry"))??;
        let retry_request = match retried {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::Registered {
                request_id: retry_request,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = recovered_tx.send(());
        tokio::time::sleep(Duration::from_millis(40)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    let _ = returned_tx.send(());
    recovered_rx.await?;
    timeout(Duration::from_secs(1), async {
        while listener.status() != ListenerStatus::Active {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn initial_register_failure_is_terminal_until_application_retries() -> TestResult {
    timeout(
        Duration::from_secs(3),
        initial_register_failure_is_terminal_until_application_retries_case(),
    )
    .await??;
    Ok(())
}

async fn initial_register_failure_is_terminal_until_application_retries_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let first = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing first REGISTER"))??;
        let first_request = match first {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::RegisterFailed {
                request_id: first_request,
                code: WireErrorCode::Unavailable,
                message: "temporary registration failure".to_owned(),
            })
            .await?;

        let retried = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing application retry REGISTER"))??;
        let retry_request = match retried {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Registered {
                request_id: retry_request,
                binding_id: BindingId::new(),
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(Config::new(address)).await?;
    let error = runtime
        .listen("echo.alpha", "dev-key")
        .await
        .err()
        .ok_or_else(|| io::Error::other("failed initial listen unexpectedly succeeded"))?;
    assert_eq!(error.code(), ErrorCode::Unavailable);

    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    assert_eq!(listener.status(), ListenerStatus::Active);
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn committed_register_timeout_closes_session_without_replay() -> TestResult {
    timeout(
        Duration::from_secs(3),
        committed_register_timeout_closes_session_without_replay_case(),
    )
    .await??;
    Ok(())
}

async fn committed_register_timeout_closes_session_without_replay_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing REGISTER"))??;
        if !matches!(register, Frame::Register { .. }) {
            return Err(unexpected(register).into());
        }
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("timed-out ListenerSession remained open").into());
        }

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        assert!(
            timeout(Duration::from_millis(150), replacement.next())
                .await
                .is_err()
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(80))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let error = runtime
        .listen("echo.alpha", "dev-key")
        .await
        .err()
        .ok_or_else(|| io::Error::other("timed-out listen unexpectedly succeeded"))?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(
        error.observation(),
        relaygate_sdk::PeerObservation::MaybeObserved
    );
    server.await??;
    runtime.close();
    Ok(())
}

#[tokio::test]
async fn cancelled_committed_listen_closes_session_and_client_id_can_be_reused() -> TestResult {
    timeout(
        Duration::from_secs(3),
        cancelled_committed_listen_closes_session_and_client_id_can_be_reused_case(),
    )
    .await??;
    Ok(())
}

async fn cancelled_committed_listen_closes_session_and_client_id_can_be_reused_case() -> TestResult
{
    let (gateway, address) = bind_gateway().await?;
    let (committed_tx, committed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing cancelled REGISTER"))??;
        if !matches!(register, Frame::Register { .. }) {
            return Err(unexpected(register).into());
        }
        let _ = committed_tx.send(());
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("cancelled ListenerSession remained open").into());
        }

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let retry = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing replacement REGISTER"))??;
        let request_id = match retry {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.alpha" => request_id,
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::Registered {
                request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(40)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_secs(1))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.listen("echo.alpha", "dev-key").await }
    });
    committed_rx.await?;
    pending.abort();
    assert!(pending.await.is_err());

    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    assert_eq!(listener.status(), ListenerStatus::Active);
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn reconnect_replays_all_desired_listeners_with_capacity_one() -> TestResult {
    timeout(
        Duration::from_secs(5),
        reconnect_replays_all_desired_listeners_with_capacity_one_case(),
    )
    .await??;
    Ok(())
}

async fn reconnect_replays_all_desired_listeners_with_capacity_one_case() -> TestResult {
    const CLIENTS: [&str; 3] = ["echo.one", "echo.two", "echo.three"];
    let (gateway, address) = bind_gateway().await?;
    let (replayed_tx, replayed_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        for generation in 0..2 {
            let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
            let mut observed = Vec::new();
            for _ in CLIENTS {
                let frame = transport
                    .next()
                    .await
                    .ok_or_else(|| io::Error::other("missing replayed REGISTER"))??;
                let (request_id, client_id) = match frame {
                    Frame::Register {
                        request_id,
                        client_id,
                        ..
                    } => (request_id, client_id),
                    other => return Err(unexpected(other).into()),
                };
                observed.push(client_id);
                transport
                    .send(Frame::Registered {
                        request_id,
                        binding_id: BindingId::new(),
                    })
                    .await?;
            }
            observed.sort();
            let mut expected = CLIENTS.map(str::to_owned).to_vec();
            expected.sort();
            assert_eq!(observed, expected);
            if generation == 0 {
                // All three successful listen calls must return before this
                // test exercises managed recovery of returned handles.
                tokio::time::sleep(Duration::from_millis(40)).await;
            } else {
                let _ = replayed_tx.send(());
                let _ = done_rx.await;
                break;
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_outbound_capacity(1)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let mut listeners = Vec::new();
    for client_id in CLIENTS {
        listeners.push(runtime.listen(client_id, "dev-key").await?);
    }
    replayed_rx.await?;
    timeout(Duration::from_secs(1), async {
        loop {
            if listeners
                .iter()
                .all(|listener| listener.status() == ListenerStatus::Active)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
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
    let write_error = timeout(Duration::from_secs(2), pipe.write_all(&payload))
        .await?
        .err()
        .ok_or_else(|| {
            io::Error::other("stalled Listener transport write unexpectedly succeeded")
        })?;
    assert_eq!(write_error.code(), ErrorCode::Unavailable);

    let mut byte = [0_u8; 1];
    let read_error = timeout(Duration::from_secs(1), pipe.read(&mut byte))
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
