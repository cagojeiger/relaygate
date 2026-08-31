mod support;

use std::{io, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, ErrorCode as WireErrorCode, Frame, SessionRole};
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
