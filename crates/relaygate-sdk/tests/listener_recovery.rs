mod support;

use std::{io, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, ErrorCode as WireErrorCode, Frame, SessionRole};
use relaygate_sdk::{Config, ErrorCode, ListenerRuntime, ListenerStatus};
use tokio::sync::oneshot;
use tokio::time::timeout;

use support::{TestResult, accept_session, bind_gateway, unexpected};

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
async fn permanent_recovery_rejection_blocks_only_affected_listener() -> TestResult {
    timeout(
        Duration::from_secs(3),
        permanent_recovery_rejection_blocks_only_affected_listener_case(),
    )
    .await??;
    Ok(())
}

async fn permanent_recovery_rejection_blocks_only_affected_listener_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (returned_tx, returned_rx) = oneshot::channel();
    let (recovered_tx, recovered_rx) = oneshot::channel();
    let (checked_tx, checked_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = accept_session(&gateway, SessionRole::Listener).await?;
        for _ in 0..2 {
            let register = first
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing initial REGISTER"))??;
            let request_id = match register {
                Frame::Register {
                    request_id,
                    client_id,
                    client_key,
                } if (client_id == "echo.alpha" && client_key.expose_secret() == "old-key")
                    || (client_id == "echo.beta" && client_key.expose_secret() == "stable-key") =>
                {
                    request_id
                }
                other => return Err(unexpected(other).into()),
            };
            first
                .send(Frame::Registered {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
        }
        let _ = returned_rx.await;
        drop(first);

        let (mut replacement, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let mut saw_alpha = false;
        let mut saw_beta = false;
        for _ in 0..2 {
            let register = replacement
                .next()
                .await
                .ok_or_else(|| io::Error::other("missing recovery REGISTER"))??;
            match register {
                Frame::Register {
                    request_id,
                    client_id,
                    client_key,
                } if client_id == "echo.alpha"
                    && client_key.expose_secret() == "old-key"
                    && !saw_alpha =>
                {
                    saw_alpha = true;
                    replacement
                        .send(Frame::RegisterFailed {
                            request_id,
                            code: WireErrorCode::Unauthenticated,
                            message: "replacement Gateway rejected the fixed key".to_owned(),
                        })
                        .await?;
                }
                Frame::Register {
                    request_id,
                    client_id,
                    client_key,
                } if client_id == "echo.beta"
                    && client_key.expose_secret() == "stable-key"
                    && !saw_beta =>
                {
                    saw_beta = true;
                    replacement
                        .send(Frame::Registered {
                            request_id,
                            binding_id: BindingId::new(),
                        })
                        .await?;
                }
                other => return Err(unexpected(other).into()),
            }
        }
        if !saw_alpha || !saw_beta {
            return Err(io::Error::other("recovery set was incomplete").into());
        }
        let _ = recovered_tx.send(());

        match timeout(Duration::from_millis(100), replacement.next()).await {
            Err(_) => {}
            Ok(None) => {
                return Err(io::Error::other("replacement ListenerSession closed").into());
            }
            Ok(Some(Ok(frame))) => return Err(unexpected(frame).into()),
            Ok(Some(Err(error))) => return Err(error.into()),
        }
        let _ = checked_tx.send(());

        let replacement_register = replacement
            .next()
            .await
            .ok_or_else(|| io::Error::other("missing new-key REGISTER after blocked close"))??;
        let replacement_request = match replacement_register {
            Frame::Register {
                request_id,
                client_id,
                client_key,
            } if client_id == "echo.alpha" && client_key.expose_secret() == "new-key" => request_id,
            other => return Err(unexpected(other).into()),
        };
        replacement
            .send(Frame::Registered {
                request_id: replacement_request,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = finish_rx.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    let alpha = runtime.listen("echo.alpha", "old-key").await?;
    let beta = runtime.listen("echo.beta", "stable-key").await?;
    let _ = returned_tx.send(());
    recovered_rx.await?;
    timeout(Duration::from_secs(1), async {
        while alpha.status() != ListenerStatus::Blocked || beta.status() != ListenerStatus::Active {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let error = alpha
        .accept()
        .await
        .err()
        .ok_or_else(|| io::Error::other("blocked Listener unexpectedly accepted a Pipe"))?;
    assert_eq!(error.code(), ErrorCode::Unauthenticated);
    checked_rx.await?;
    alpha.close().await?;
    assert_eq!(alpha.status(), ListenerStatus::Closed);
    let replacement_alpha = runtime.listen("echo.alpha", "new-key").await?;
    assert_eq!(replacement_alpha.status(), ListenerStatus::Active);
    assert_eq!(beta.status(), ListenerStatus::Active);
    runtime.close();
    let _ = finish_tx.send(());
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
