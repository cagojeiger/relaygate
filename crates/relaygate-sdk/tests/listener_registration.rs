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
async fn committed_initial_register_timeout_recovers_only_returned_listeners() -> TestResult {
    timeout(
        Duration::from_secs(5),
        committed_initial_register_timeout_recovers_only_returned_listeners_case(),
    )
    .await??;
    Ok(())
}

async fn committed_initial_register_timeout_recovers_only_returned_listeners_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (timed_out_tx, timed_out_rx) = oneshot::channel();
    let (recovered_tx, recovered_rx) = oneshot::channel();
    let (no_replay_tx, no_replay_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, first_session_id) =
            accept_session(&gateway, SessionRole::Listener).await?;
        let mut alpha_binding = None;
        for expected_client in ["echo.alpha", "echo.beta"] {
            let register = transport
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing returned Listener REGISTER"))??;
            let request_id = match register {
                Frame::Register {
                    request_id,
                    client_id,
                    ..
                } if client_id == expected_client => request_id,
                other => return Err(unexpected(other).into()),
            };
            let binding_id = BindingId::new();
            if expected_client == "echo.alpha" {
                alpha_binding = Some(binding_id);
            }
            transport
                .send(Frame::Registered {
                    request_id,
                    binding_id,
                })
                .await?;
        }
        let alpha_binding =
            alpha_binding.ok_or_else(|| io::Error::other("missing alpha binding"))?;
        let pipe_id = PipeId::new(SessionId::new(), 40);
        transport
            .send(Frame::Offer {
                pipe_id,
                binding_id: alpha_binding,
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

        let register = transport
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing initial Listener REGISTER"))??;
        let timed_out_request = match register {
            Frame::Register {
                request_id,
                client_id,
                ..
            } if client_id == "echo.gamma" => request_id,
            other => return Err(unexpected(other).into()),
        };

        timed_out_rx.await?;
        transport
            .send(Frame::Registered {
                request_id: timed_out_request,
                binding_id: BindingId::new(),
            })
            .await?;
        let ended = timeout(Duration::from_secs(1), transport.next()).await?;
        if ended.is_some() {
            return Err(io::Error::other("timed-out ListenerSession remained open").into());
        }

        let (mut replacement, replacement_session_id) =
            accept_session(&gateway, SessionRole::Listener).await?;
        assert_ne!(first_session_id, replacement_session_id);
        let mut recovered_clients = Vec::new();
        for _ in 0..2 {
            let register = replacement
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing recovery REGISTER"))??;
            let (request_id, client_id) = match register {
                Frame::Register {
                    request_id,
                    client_id,
                    ..
                } => (request_id, client_id),
                other => return Err(unexpected(other).into()),
            };
            recovered_clients.push(client_id);
            replacement
                .send(Frame::Registered {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
        }
        recovered_clients.sort();
        assert_eq!(recovered_clients, ["echo.alpha", "echo.beta"]);
        let _ = recovered_tx.send(());

        match timeout(Duration::from_millis(150), replacement.next()).await {
            Err(_) => {}
            Ok(None) => {
                return Err(io::Error::other("replacement ListenerSession closed").into());
            }
            Ok(Some(Ok(frame))) => return Err(unexpected(frame).into()),
            Ok(Some(Err(error))) => return Err(error.into()),
        }
        let _ = no_replay_tx.send(());

        let retry = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing application retry REGISTER"))??;
        let request_id = match retry {
            Frame::Register {
                request_id,
                client_id,
                client_key,
            } if client_id == "echo.gamma" && client_key.expose_secret() == "retry-key" => {
                request_id
            }
            Frame::Register {
                client_id,
                client_key,
                ..
            } if client_id == "echo.gamma" && client_key.expose_secret() == "dev-key" => {
                return Err(io::Error::other(
                    "timed-out initial REGISTER was replayed before application retry",
                )
                .into());
            }
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::Registered {
                request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = done_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(80))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let alpha = runtime.listen("echo.alpha", "dev-key").await?;
    let beta = runtime.listen("echo.beta", "dev-key").await?;
    let mut alpha_pipe = alpha.accept().await?;
    let error = runtime
        .listen("echo.gamma", "dev-key")
        .await
        .err()
        .ok_or_else(|| io::Error::other("timed-out listen unexpectedly succeeded"))?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(
        error.observation(),
        relaygate_sdk::PeerObservation::MaybeObserved
    );
    timed_out_tx
        .send(())
        .map_err(|_| io::Error::other("server stopped before late REGISTERED"))?;
    let mut byte = [0_u8; 1];
    let pipe_error = timeout(Duration::from_secs(1), alpha_pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("old-session Pipe remained readable after timeout"))?;
    assert_eq!(pipe_error.code(), ErrorCode::Unavailable);

    recovered_rx.await?;
    timeout(Duration::from_secs(1), async {
        while alpha.status() != ListenerStatus::Active || beta.status() != ListenerStatus::Active {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    no_replay_rx.await?;

    let gamma = runtime.listen("echo.gamma", "retry-key").await?;
    assert_eq!(gamma.status(), ListenerStatus::Active);
    let _ = done_tx.send(());
    runtime.close();
    server.await??;
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
