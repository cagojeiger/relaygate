use std::{error::Error, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, MAX_HELLO_FRAME_LEN, ProtocolError};
use tokio::{
    io::{AsyncWriteExt, DuplexStream, duplex},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::{
    codec::{Encoder, Framed},
    sync::CancellationToken,
};

use super::{Gateway, GatewayConfig, session::SessionError};
use crate::test_support::{TestAction, authorization_config, bearer_token, unique_destination};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type SessionTask = JoinHandle<Result<(), SessionError>>;
mod backpressure;

fn start_session(
    gateway: &Gateway,
    capacity: usize,
) -> TestResult<(DuplexStream, CancellationToken, SessionTask)> {
    let transport_slot = Arc::clone(&gateway.inner.session_slots).try_acquire_owned()?;
    let handshake_slot = Arc::clone(&gateway.inner.handshake_slots).try_acquire_owned()?;
    let inner = Arc::clone(&gateway.inner);
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let (client, server) = duplex(capacity);
    let task = tokio::spawn(async move {
        let _transport_slot = transport_slot;
        inner
            .run_session(Box::new(server), shutdown, handshake_slot)
            .await
    });
    Ok((client, cancel, task))
}

fn assert_empty(gateway: &Gateway) {
    let snapshot = gateway.snapshot();
    assert_eq!(snapshot.sessions, 0);
    assert_eq!(snapshot.bindings, 0);
    assert_eq!(snapshot.pending_offers, 0);
    assert_eq!(snapshot.live_pipes, 0);
    assert_eq!(snapshot.session_slots_used, 0);
    assert_eq!(snapshot.pending_handshakes, 0);
    assert!(snapshot.sdk_admission_ready);
}

#[tokio::test]
async fn handshake_capacity_is_separate_and_released_after_welcome() -> TestResult {
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config())
            .with_max_sessions(3)
            .with_max_pending_handshakes(1),
    )?;
    let (client, cancel, task) = start_session(&gateway, 4096)?;
    assert!(!gateway.snapshot().sdk_admission_ready);
    assert_eq!(gateway.snapshot().pending_handshakes, 1);
    assert!(
        Arc::clone(&gateway.inner.handshake_slots)
            .try_acquire_owned()
            .is_err()
    );
    let mut client = Framed::new(client, FrameCodec::default());
    client.send(Frame::Hello).await?;
    assert!(matches!(
        client.next().await,
        Some(Ok(Frame::Welcome { .. }))
    ));
    assert_eq!(gateway.snapshot().pending_handshakes, 0);
    assert_eq!(gateway.snapshot().session_slots_used, 1);
    assert!(gateway.snapshot().sdk_admission_ready);

    let (_stalled, other_cancel, other_task) = start_session(&gateway, 8)?;
    client.send(Frame::Ping { nonce: 42 }).await?;
    assert!(matches!(
        client.next().await,
        Some(Ok(Frame::Pong { nonce: 42 }))
    ));
    other_cancel.cancel();
    other_task.await??;
    assert_eq!(gateway.snapshot().sessions, 1);
    cancel.cancel();
    task.await??;
    assert_empty(&gateway);
    Ok(())
}

#[tokio::test]
async fn oversized_pre_auth_header_is_rejected_without_waiting_for_payload() -> TestResult {
    let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
    let (mut client, _cancel, task) = start_session(&gateway, 8)?;
    let mut header = vec![b'R', b'G', 3, 1];
    header.extend_from_slice(&((MAX_HELLO_FRAME_LEN + 1) as u32).to_be_bytes());
    client.write_all(&header).await?;
    assert!(matches!(
        timeout(Duration::from_secs(1), task).await??,
        Err(SessionError::Protocol(ProtocolError::FrameTooLarge {
            maximum: MAX_HELLO_FRAME_LEN,
            ..
        }))
    ));
    assert_empty(&gateway);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_hello_and_blocked_handshake_responses_release_all_capacity() -> TestResult {
    for send_hello in [false, true] {
        let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        // Neither WELCOME nor SESSION_REJECTED fits unless the client reads.
        let (mut client, _cancel, task) = start_session(&gateway, 8)?;
        if send_hello {
            let mut hello = BytesMut::new();
            FrameCodec::default().encode(Frame::Hello, &mut hello)?;
            client.write_all(&hello).await?;
        }
        assert!(matches!(
            timeout(Duration::from_secs(6), task).await??,
            Err(SessionError::HandshakeTimeout)
        ));
        assert_empty(&gateway);
    }
    Ok(())
}

#[tokio::test]
async fn admission_preserves_pipelined_authorized_frame() -> TestResult {
    let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
    let (mut client, cancel, task) = start_session(&gateway, 128 * 1024)?;
    let mut frames = BytesMut::new();
    let mut codec = FrameCodec::default();
    codec.encode(Frame::Hello, &mut frames)?;
    let destination = unique_destination();
    codec.encode(
        Frame::Publish {
            request_id: 1,
            destination: destination.clone(),
            access_token: bearer_token(&destination, TestAction::Publish),
        },
        &mut frames,
    )?;
    client.write_all(&frames).await?;
    let mut client = Framed::new(client, codec);
    assert!(matches!(
        client.next().await,
        Some(Ok(Frame::Welcome { .. }))
    ));
    assert!(matches!(
        client.next().await,
        Some(Ok(Frame::Published { request_id: 1, .. }))
    ));
    cancel.cancel();
    task.await??;
    assert_empty(&gateway);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn late_hello_response_uses_only_the_remaining_handshake_budget() -> TestResult {
    let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
    let (mut client, _cancel, task) = start_session(&gateway, 8)?;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    let mut hello = BytesMut::new();
    FrameCodec::default().encode(Frame::Hello, &mut hello)?;
    client.write_all(&hello).await?;
    assert!(matches!(
        timeout(Duration::from_secs(2), task).await??,
        Err(SessionError::HandshakeTimeout)
    ));
    assert_empty(&gateway);
    Ok(())
}

#[tokio::test]
async fn malformed_first_frame_and_cancellation_release_handshake_capacity() -> TestResult {
    for input in [Some(Frame::Ping { nonce: 1 }), None] {
        let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        let (client, cancel, task) = start_session(&gateway, 4096)?;
        let mut client = Framed::new(client, FrameCodec::default());
        if let Some(frame) = input {
            client.send(frame).await?;
            let result = task.await?;
            assert!(
                matches!(
                    result,
                    Err(SessionError::Protocol(ProtocolError::FrameTooLarge {
                        maximum: MAX_HELLO_FRAME_LEN,
                        ..
                    }))
                ),
                "unexpected handshake result: {result:?}"
            );
        } else {
            cancel.cancel();
            task.await??;
        }
        assert_empty(&gateway);
    }
    Ok(())
}

#[tokio::test]
async fn handshake_limit_is_capped_by_total_session_limit() -> TestResult {
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config())
            .with_max_sessions(1)
            .with_max_pending_handshakes(10),
    )?;
    assert_eq!(gateway.snapshot().max_pending_handshakes, 1);
    Ok(())
}

#[tokio::test]
async fn tls_handshake_saturation_rejects_before_tls_and_recovers() -> TestResult {
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
    };

    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let tls = ServerTlsConfig::server_authenticated(
        certificate.as_bytes(),
        signing_key.serialize_pem().as_bytes(),
    )?;
    let client_tls =
        ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config())
            .with_sdk_tls(tls)
            .with_max_sessions(3)
            .with_max_pending_handshakes(1)
            .with_drain_timeout(Duration::from_millis(100)),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let runtime = gateway.clone();
    let stop = shutdown.clone();
    let server = tokio::spawn(async move { runtime.serve(listener, stop).await });

    let result: TestResult = async {
        let connect = || async {
            let stream = TcpStream::connect(address).await?;
            let stream = client_tls.connect_boxed(stream).await?;
            let mut client = Framed::new(stream, FrameCodec::default());
            client.send(Frame::Hello).await?;
            assert!(matches!(
                client.next().await,
                Some(Ok(Frame::Welcome { .. }))
            ));
            TestResult::Ok(client)
        };
        let mut admitted = timeout(Duration::from_secs(2), connect()).await??;
        wait_handshakes(&gateway, 0).await?;
        let mut stalled = TcpStream::connect(address).await?;
        wait_handshakes(&gateway, 1).await?;
        assert_eq!(gateway.snapshot().sessions, 1);
        assert!(!gateway.snapshot().sdk_admission_ready);

        let mut rejected = TcpStream::connect(address).await?;
        let mut byte = [0_u8; 1];
        // A socket which sends no TLS bytes is closed immediately at capacity.
        assert_eq!(
            timeout(Duration::from_secs(2), rejected.read(&mut byte)).await??,
            0
        );
        admitted.send(Frame::Ping { nonce: 9 }).await?;
        assert!(matches!(
            timeout(Duration::from_secs(2), admitted.next()).await?,
            Some(Ok(Frame::Pong { nonce: 9 }))
        ));
        // With no ClientHello, the actual TLS timeout must free both permits.
        assert_eq!(
            timeout(Duration::from_secs(7), stalled.read(&mut byte)).await??,
            0
        );
        drop(stalled);
        wait_handshakes(&gateway, 0).await?;
        let recovered = timeout(Duration::from_secs(2), connect()).await??;
        drop(recovered);
        drop(admitted);
        Ok(())
    }
    .await;

    shutdown.cancel();
    timeout(Duration::from_secs(2), server).await???;
    result?;
    let snapshot = gateway.snapshot();
    assert_eq!(snapshot.sessions, 0);
    assert_eq!(snapshot.session_slots_used, 0);
    assert_eq!(snapshot.pending_handshakes, 0);
    Ok(())
}

async fn wait_handshakes(gateway: &Gateway, count: usize) -> TestResult {
    timeout(Duration::from_secs(2), async {
        while gateway.snapshot().pending_handshakes != count {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}
