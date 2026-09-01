mod support;

use std::{io, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, Frame, PipeId, SessionId, SessionRole};
use relaygate_sdk::{Config, ListenerRuntime, ListenerStatus};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

use support::{TestResult, accept_session, bind_gateway, next_application_frame, unexpected};

#[tokio::test]
async fn idle_listener_session_uses_heartbeat_before_register() -> TestResult {
    timeout(
        Duration::from_secs(3),
        idle_listener_session_uses_heartbeat_before_register_case(),
    )
    .await??;
    Ok(())
}

async fn idle_listener_session_uses_heartbeat_before_register_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (idle_checked_tx, idle_checked_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
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

        let register = next_application_frame(&mut transport).await?;
        let request_id = match register {
            Frame::Register { request_id, .. } => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport
            .send(Frame::Registered {
                request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(200))
            .with_heartbeat(Duration::from_millis(40), Duration::from_millis(40)),
    )
    .await?;
    idle_checked_rx.await?;
    let listener = runtime.listen("echo.alpha", "dev-key").await?;
    assert_eq!(listener.status(), ListenerStatus::Active);
    runtime.close();
    server.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_matching_pong_is_not_starved_by_sustained_outbound_frames() -> TestResult {
    timeout(
        Duration::from_secs(3),
        listener_matching_pong_is_not_starved_by_sustained_outbound_frames_case(),
    )
    .await??;
    Ok(())
}

async fn listener_matching_pong_is_not_starved_by_sustained_outbound_frames_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _) = accept_session(&gateway, SessionRole::Listener).await?;
        let register = next_application_frame(&mut transport).await?;
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
                client_id: "echo.loaded".to_owned(),
            })
            .await?;
        let accepted = next_application_frame(&mut transport).await?;
        if !matches!(accepted, Frame::OfferAccepted { pipe_id: accepted } if accepted == pipe_id) {
            return Err(unexpected(accepted).into());
        }

        let mut answered_heartbeats = 0_usize;
        loop {
            let frame = transport
                .next()
                .await
                .ok_or_else(|| io::Error::other("Listener session ended during outbound load"))??;
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

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_operation_timeout(Duration::from_millis(500))
            .with_heartbeat(Duration::from_millis(30), Duration::from_millis(100))
            .with_outbound_capacity(256),
    )
    .await?;
    let listener = runtime.listen("echo.loaded", "dev-key").await?;
    let mut pipe = listener.accept().await?;
    let finish = Instant::now() + Duration::from_millis(300);
    while Instant::now() < finish {
        pipe.write_all_bytes(b"x").await?;
    }
    pipe.close().await?;
    server.await??;
    runtime.close();
    Ok(())
}

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
        if timeout(Duration::from_millis(120), gateway.accept())
            .await
            .is_ok()
        {
            return Err(io::Error::other("dropped Listener runtime reconnected").into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    drop(runtime);
    server.await??;
    Ok(())
}
