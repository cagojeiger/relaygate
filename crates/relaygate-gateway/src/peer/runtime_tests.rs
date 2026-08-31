use std::{error::Error, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use super::{
    GatewayPeerConfig, OpenIdentity, PeerEvent, PeerFailure, PeerHandle, PeerOpenRequest,
    PeerRuntime, PeerStreamKey, PeerTarget, TrustedPeerConfig,
    codec::PeerFrameCodec,
    event::PeerCounts,
    frame::PeerFrame,
    identity::{PeerGatewayKey, PeerGatewayName, PeerHandshake, PeerTransportId, StreamId},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct RuntimePair {
    gateway_a: GatewayId,
    gateway_b: GatewayId,
    locator_b: GatewayLocator,
    handle_a: PeerHandle,
    handle_b: PeerHandle,
    events_a: super::PeerEvents,
    events_b: super::PeerEvents,
    shutdown_a: CancellationToken,
    shutdown_b: CancellationToken,
    serve_a: JoinHandle<Result<(), PeerFailure>>,
    serve_b: JoinHandle<Result<(), PeerFailure>>,
}

impl RuntimePair {
    async fn start() -> TestResult<Self> {
        Self::start_with_event_capacity(64).await
    }

    async fn start_with_event_capacity(event_capacity: usize) -> TestResult<Self> {
        let listener_a = TcpListener::bind("127.0.0.1:0").await?;
        let listener_b = TcpListener::bind("127.0.0.1:0").await?;
        let locator_b = GatewayLocator::new(listener_b.local_addr()?.to_string())?;
        let gateway_a = GatewayId::new();
        let gateway_b = GatewayId::new();
        let config_a = test_config_with_event_capacity(
            "gateway-a",
            "key-a",
            "gateway-b",
            "key-b",
            event_capacity,
        )?;
        let config_b = test_config_with_event_capacity(
            "gateway-b",
            "key-b",
            "gateway-a",
            "key-a",
            event_capacity,
        )?;
        let shutdown_a = CancellationToken::new();
        let shutdown_b = CancellationToken::new();
        let (handle_a, events_a, runtime_a) =
            PeerRuntime::start(config_a, gateway_a, shutdown_a.clone())?;
        let (handle_b, events_b, runtime_b) =
            PeerRuntime::start(config_b, gateway_b, shutdown_b.clone())?;
        let serve_a = tokio::spawn(runtime_a.serve(listener_a));
        let serve_b = tokio::spawn(runtime_b.serve(listener_b));
        Ok(Self {
            gateway_a,
            gateway_b,
            locator_b,
            handle_a,
            handle_b,
            events_a,
            events_b,
            shutdown_a,
            shutdown_b,
            serve_a,
            serve_b,
        })
    }

    fn request_a_to_b(&self, connection_id: u64) -> TestResult<PeerOpenRequest> {
        Ok(PeerOpenRequest::new(
            PeerTarget::new(self.gateway_b, self.locator_b.clone()),
            OpenIdentity::new(self.gateway_a, SessionId::new(), connection_id),
            "echo.b",
            SessionId::new(),
            BindingId::new(),
        )?)
    }

    async fn shutdown(self) -> TestResult {
        self.shutdown_a.cancel();
        self.shutdown_b.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.serve_a).await???;
        tokio::time::timeout(Duration::from_secs(2), self.serve_b).await???;
        Ok(())
    }
}

fn test_config(
    local_name: &str,
    local_key: &str,
    peer_name: &str,
    peer_key: &str,
) -> Result<GatewayPeerConfig, crate::GatewayError> {
    test_config_with_event_capacity(local_name, local_key, peer_name, peer_key, 64)
}

fn test_config_with_event_capacity(
    local_name: &str,
    local_key: &str,
    peer_name: &str,
    peer_key: &str,
    event_capacity: usize,
) -> Result<GatewayPeerConfig, crate::GatewayError> {
    Ok(GatewayPeerConfig::new(
        local_name,
        local_key,
        [TrustedPeerConfig::new(peer_name, peer_key)?],
    )?
    .with_queue_bounds(64, event_capacity, 64, 64, 8)
    .with_resource_limits(64, 64, 16, 64 * 1024)
    .with_timeouts(
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(1),
    ))
}

async fn next_event(events: &mut super::PeerEvents) -> TestResult<PeerEvent> {
    tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await?
        .ok_or_else(|| "peer event stream closed".into())
}

async fn wait_for_counts(handle: &PeerHandle, expected: PeerCounts) -> TestResult {
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.counts() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn accept_one(
    events: &mut super::PeerEvents,
    owner: &PeerHandle,
) -> TestResult<(PeerStreamKey, OpenIdentity)> {
    let event = next_event(events).await?;
    let PeerEvent::IncomingOpen {
        key, open_identity, ..
    } = event
    else {
        return Err(format!("expected IncomingOpen, got {event:?}").into());
    };
    owner.send_opened(key).await?;
    Ok((key, open_identity))
}

#[tokio::test]
async fn fast_opened_event_and_open_commit_reply_keep_exact_correlation() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let request = pair.request_a_to_b(1)?;
    let identity = request.open_identity();
    let handle_a = pair.handle_a.clone();
    let open = tokio::spawn(async move { handle_a.open(request).await });

    let (_owner_key, incoming_identity) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    assert_eq!(incoming_identity, identity);

    // The commit reply and PeerEvent use independent bounded channels. The
    // caller may observe either first, but both carry the same stream key.
    let opened = next_event(&mut pair.events_a).await?;
    let PeerEvent::Opened {
        key: event_key,
        open_identity,
    } = opened
    else {
        return Err(format!("expected Opened, got {opened:?}").into());
    };
    let committed_key = open.await??;
    assert_eq!(event_key, committed_key);
    assert_eq!(open_identity, identity);
    pair.shutdown().await
}

#[tokio::test]
async fn post_commit_cancel_ignores_late_opened_and_converges_through_transport_loss() -> TestResult
{
    let mut pair = RuntimePair::start().await?;
    let request = pair.request_a_to_b(1)?;
    let identity = request.open_identity();
    let handle_a = pair.handle_a.clone();
    let open = tokio::spawn(async move { handle_a.open(request).await });

    let incoming = next_event(&mut pair.events_b).await?;
    let PeerEvent::IncomingOpen {
        key: owner_key,
        open_identity,
        ..
    } = incoming
    else {
        return Err(format!("expected IncomingOpen, got {incoming:?}").into());
    };
    assert_eq!(open_identity, identity);
    let entry_key = open.await??;

    pair.handle_a.cancel_open(identity).await?;
    let reset = next_event(&mut pair.events_b).await?;
    assert!(matches!(
        reset,
        PeerEvent::Reset {
            key,
            code: ErrorCode::Cancelled,
            ..
        } if key == owner_key
    ));

    pair.handle_b.send_opened(owner_key).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), pair.events_a.recv())
            .await
            .is_err(),
        "late OPENED recreated the cancelled Entry stream"
    );
    pair.handle_a.cancel_open(identity).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), pair.events_b.recv())
            .await
            .is_err(),
        "duplicate cancel emitted a second Owner event"
    );
    let idle_transport = PeerCounts {
        connecting: 0,
        ready: 1,
        streams: 0,
    };
    wait_for_counts(&pair.handle_a, idle_transport).await?;
    wait_for_counts(&pair.handle_b, idle_transport).await?;

    assert!(pair.handle_a.close_transport(entry_key));
    let loss = next_event(&mut pair.events_a).await?;
    assert!(matches!(
        loss,
        PeerEvent::TransportLost {
            peer_transport_id,
            streams,
            ..
        } if peer_transport_id == entry_key.peer_transport_id() && streams.is_empty()
    ));
    let remote_loss = next_event(&mut pair.events_b).await?;
    assert!(matches!(
        remote_loss,
        PeerEvent::TransportLost { streams, .. } if streams.is_empty()
    ));
    wait_for_counts(&pair.handle_a, PeerCounts::default()).await?;
    wait_for_counts(&pair.handle_b, PeerCounts::default()).await?;
    pair.handle_a.cancel_open(identity).await?;

    let reused = PeerOpenRequest::new(
        PeerTarget::new(pair.gateway_b, pair.locator_b.clone()),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let reopened = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(reused).await })
    };
    let (new_owner_key, new_identity) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let new_entry_key = reopened.await??;
    assert_eq!(new_identity, identity);
    assert_ne!(
        new_entry_key.peer_transport_id(),
        entry_key.peer_transport_id()
    );
    let reopened_event = next_event(&mut pair.events_a).await?;
    assert!(matches!(
        reopened_event,
        PeerEvent::Opened {
            key: event_key,
            open_identity,
        } if event_key == new_entry_key && open_identity == identity
    ));
    pair.handle_b.send_close(new_owner_key).await?;
    pair.shutdown().await
}

#[tokio::test]
async fn ready_transport_is_reused_and_preserves_per_transport_data_order() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let handle_a = pair.handle_a.clone();
    let first_request = pair.request_a_to_b(1)?;
    let first_open = tokio::spawn(async move { handle_a.open(first_request).await });
    let (first_owner_key, _) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let first_key = first_open.await??;
    let _ = next_event(&mut pair.events_a).await?;

    let handle_a = pair.handle_a.clone();
    let second_request = pair.request_a_to_b(2)?;
    let second_open = tokio::spawn(async move { handle_a.open(second_request).await });
    let (second_owner_key, _) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let second_key = second_open.await??;
    let _ = next_event(&mut pair.events_a).await?;

    assert_eq!(
        first_key.peer_transport_id(),
        second_key.peer_transport_id()
    );
    assert_ne!(first_key.stream_id(), second_key.stream_id());
    assert_eq!(pair.handle_a.counts().ready, 1);
    assert_eq!(pair.handle_a.counts().streams, 2);

    pair.handle_a
        .send_data(first_key, Bytes::from_static(b"first"))
        .await?;
    pair.handle_a
        .send_data(first_key, Bytes::from_static(b"second"))
        .await?;
    for expected in [Bytes::from_static(b"first"), Bytes::from_static(b"second")] {
        let event = next_event(&mut pair.events_b).await?;
        let PeerEvent::Data { key, payload } = event else {
            return Err(format!("expected Data, got {event:?}").into());
        };
        assert_eq!(key, first_owner_key);
        assert_eq!(payload, expected);
    }

    pair.handle_b.send_close(first_owner_key).await?;
    pair.handle_b.send_close(second_owner_key).await?;
    pair.shutdown().await
}

#[tokio::test]
async fn force_close_emits_one_transport_scoped_loss_and_releases_all_streams() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let handle_a = pair.handle_a.clone();
    let request = pair.request_a_to_b(1)?;
    let identity = request.open_identity();
    let open = tokio::spawn(async move { handle_a.open(request).await });
    let _ = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let key = open.await??;
    let _ = next_event(&mut pair.events_a).await?;

    assert!(pair.handle_a.close_transport(key));
    let loss = loop {
        let event = next_event(&mut pair.events_a).await?;
        if let PeerEvent::TransportLost { .. } = event {
            break event;
        }
    };
    let PeerEvent::TransportLost {
        peer_gateway_id,
        peer_transport_id,
        streams,
    } = loss
    else {
        return Err("expected transport loss".into());
    };
    assert_eq!(peer_gateway_id, pair.gateway_b);
    assert_eq!(peer_transport_id, key.peer_transport_id());
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].key, key);
    assert_eq!(
        streams[0].progress.failure_observation(),
        PeerObservation::MaybeObserved
    );

    let remote_loss = next_event(&mut pair.events_b).await?;
    assert!(matches!(
        remote_loss,
        PeerEvent::TransportLost { streams, .. } if streams.len() == 1
    ));
    wait_for_counts(&pair.handle_a, PeerCounts::default()).await?;
    wait_for_counts(&pair.handle_b, PeerCounts::default()).await?;

    pair.handle_a.cancel_open(identity).await?;
    let reused = PeerOpenRequest::new(
        PeerTarget::new(pair.gateway_b, pair.locator_b.clone()),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let reopened = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(reused).await })
    };
    let (new_owner_key, new_identity) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let new_entry_key = reopened.await??;
    assert_eq!(new_identity, identity);
    assert_ne!(new_entry_key.peer_transport_id(), key.peer_transport_id());
    let reopened_event = next_event(&mut pair.events_a).await?;
    assert!(matches!(
        reopened_event,
        PeerEvent::Opened {
            key: event_key,
            open_identity,
        } if event_key == new_entry_key && open_identity == identity
    ));
    pair.handle_b.send_close(new_owner_key).await?;
    pair.shutdown().await
}

#[tokio::test]
async fn wrong_trusted_key_fails_before_open_commit_without_ready_transport() -> TestResult {
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let listener_b = TcpListener::bind("127.0.0.1:0").await?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let locator_b = GatewayLocator::new(listener_b.local_addr()?.to_string())?;
    let shutdown_a = CancellationToken::new();
    let shutdown_b = CancellationToken::new();
    let (handle_a, _events_a, runtime_a) = PeerRuntime::start(
        test_config("gateway-a", "key-a", "gateway-b", "key-b")?,
        gateway_a,
        shutdown_a.clone(),
    )?;
    let (_handle_b, _events_b, runtime_b) = PeerRuntime::start(
        test_config("gateway-b", "key-b", "gateway-a", "wrong-key")?,
        gateway_b,
        shutdown_b.clone(),
    )?;
    let serve_a = tokio::spawn(runtime_a.serve(listener_a));
    let serve_b = tokio::spawn(runtime_b.serve(listener_b));
    let request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b),
        OpenIdentity::new(gateway_a, SessionId::new(), 1),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let failure = handle_a.open(request).await.err();
    assert!(failure.is_some());
    let failure = failure.ok_or("expected handshake failure")?;
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert_eq!(handle_a.counts().ready, 0);
    assert_eq!(handle_a.counts().streams, 0);

    shutdown_a.cancel();
    shutdown_b.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve_a).await???;
    tokio::time::timeout(Duration::from_secs(2), serve_b).await???;
    Ok(())
}

#[tokio::test]
async fn handshake_timeout_fails_before_open_commit_and_leaves_no_transport_state() -> TestResult {
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let blackhole = TcpListener::bind("127.0.0.1:0").await?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let locator_b = GatewayLocator::new(blackhole.local_addr()?.to_string())?;
    let shutdown_a = CancellationToken::new();
    let config_a = test_config("gateway-a", "key-a", "gateway-b", "key-b")?.with_timeouts(
        Duration::from_millis(500),
        Duration::from_millis(40),
        Duration::from_secs(1),
    );
    let (handle_a, _events_a, runtime_a) =
        PeerRuntime::start(config_a, gateway_a, shutdown_a.clone())?;
    let serve_a = tokio::spawn(runtime_a.serve(listener_a));
    let blackhole_task = tokio::spawn(async move {
        let (_stream, _) = blackhole.accept().await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });

    let request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b),
        OpenIdentity::new(gateway_a, SessionId::new(), 1),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let failure = tokio::time::timeout(Duration::from_secs(1), handle_a.open(request))
        .await?
        .err()
        .ok_or("expected peer handshake timeout")?;
    assert_eq!(failure.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert_eq!(handle_a.counts(), Default::default());

    shutdown_a.cancel();
    blackhole_task.await??;
    tokio::time::timeout(Duration::from_secs(2), serve_a).await???;
    Ok(())
}

#[tokio::test]
async fn cancel_during_handshake_never_flushes_cancelled_open_and_retires_idle_transport()
-> TestResult {
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let delayed_peer = TcpListener::bind("127.0.0.1:0").await?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let locator_b = GatewayLocator::new(delayed_peer.local_addr()?.to_string())?;
    let shutdown_a = CancellationToken::new();
    let config_a = test_config("gateway-a", "key-a", "gateway-b", "key-b")?
        .with_timeouts(
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .with_liveness(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
    let (handle_a, mut events_a, runtime_a) =
        PeerRuntime::start(config_a, gateway_a, shutdown_a.clone())?;
    let serve_a = tokio::spawn(runtime_a.serve(listener_a));
    let (hello_seen_tx, hello_seen_rx) = oneshot::channel();
    let (welcome_release_tx, welcome_release_rx) = oneshot::channel();
    let delayed_peer_task = tokio::spawn(async move {
        let (stream, _) = delayed_peer.accept().await?;
        stream.set_nodelay(true)?;
        let mut framed = Framed::new(stream, PeerFrameCodec::new(64 * 1024));
        let hello = tokio::time::timeout(Duration::from_secs(1), framed.next())
            .await?
            .ok_or("peer closed before HELLO")??;
        let PeerFrame::Hello(hello) = hello else {
            return Err(format!("expected HELLO, got {hello:?}").into());
        };
        let _ = hello_seen_tx.send(());
        welcome_release_rx
            .await
            .map_err(|_| "WELCOME release signal was dropped")?;
        framed
            .send(PeerFrame::Welcome(PeerHandshake {
                gateway_name: PeerGatewayName::new("gateway-b")?,
                internal_gateway_key: PeerGatewayKey::new("key-b")?,
                gateway_id: gateway_b,
                expected_peer_gateway_id: gateway_a,
                dialer_gateway_id: gateway_a,
                peer_transport_id: hello.peer_transport_id,
            }))
            .await?;

        let next = tokio::time::timeout(Duration::from_secs(1), framed.next()).await?;
        match next {
            None => Ok::<_, Box<dyn Error + Send + Sync>>(()),
            Some(Ok(frame)) => {
                Err(format!("cancelled OPEN was flushed after WELCOME: {frame:?}").into())
            }
            Some(Err(error)) => Err(error.into()),
        }
    });

    let identity = OpenIdentity::new(gateway_a, SessionId::new(), 1);
    let request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let open = {
        let handle = handle_a.clone();
        tokio::spawn(async move { handle.open(request).await })
    };
    hello_seen_rx.await?;
    wait_for_counts(
        &handle_a,
        PeerCounts {
            connecting: 1,
            ready: 0,
            streams: 0,
        },
    )
    .await?;

    handle_a.cancel_open(identity).await?;
    let failure = open
        .await?
        .err()
        .ok_or("handshake-pending OPEN unexpectedly succeeded")?;
    assert_eq!(failure.code(), ErrorCode::Cancelled);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    let _ = welcome_release_tx.send(());
    wait_for_counts(
        &handle_a,
        PeerCounts {
            connecting: 0,
            ready: 1,
            streams: 0,
        },
    )
    .await?;

    let retired = next_event(&mut events_a).await?;
    assert!(matches!(
        retired,
        PeerEvent::TransportLost { streams, .. } if streams.is_empty()
    ));
    delayed_peer_task.await??;
    wait_for_counts(&handle_a, PeerCounts::default()).await?;
    shutdown_a.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve_a).await???;
    Ok(())
}

#[tokio::test]
async fn authenticated_peer_cannot_spoof_open_entry_gateway() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let locator = GatewayLocator::new(listener.local_addr()?.to_string())?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let shutdown = CancellationToken::new();
    let (handle_b, mut events_b, runtime_b) = PeerRuntime::start(
        test_config("gateway-b", "key-b", "gateway-a", "key-a")?,
        gateway_b,
        shutdown.clone(),
    )?;
    let serve = tokio::spawn(runtime_b.serve(listener));

    let stream = TcpStream::connect(locator.as_str()).await?;
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(stream, PeerFrameCodec::new(64 * 1024));
    let transport_id = PeerTransportId::new();
    framed
        .send(PeerFrame::Hello(PeerHandshake {
            gateway_name: PeerGatewayName::new("gateway-a")?,
            internal_gateway_key: PeerGatewayKey::new("key-a")?,
            gateway_id: gateway_a,
            expected_peer_gateway_id: gateway_b,
            dialer_gateway_id: gateway_a,
            peer_transport_id: transport_id,
        }))
        .await?;
    let welcome = tokio::time::timeout(Duration::from_secs(1), framed.next())
        .await?
        .ok_or("peer closed before WELCOME")??;
    assert!(matches!(welcome, PeerFrame::Welcome(_)));

    let spoofed_gateway = GatewayId::new();
    framed
        .send(PeerFrame::Open {
            stream_id: StreamId::from_raw(0),
            open_identity: OpenIdentity::new(spoofed_gateway, SessionId::new(), 1),
            client_id: "echo.b".to_owned(),
            listener_session_id: SessionId::new(),
            binding_id: BindingId::new(),
        })
        .await?;
    let rejected = tokio::time::timeout(Duration::from_secs(1), framed.next())
        .await?
        .ok_or("peer closed before spoof rejection")??;
    assert!(matches!(
        rejected,
        PeerFrame::Failed {
            stream_id,
            code: ErrorCode::PermissionDenied,
            observation: PeerObservation::NotObserved,
            ..
        } if stream_id == StreamId::from_raw(0)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), events_b.recv())
            .await
            .is_err(),
        "spoofed OPEN must not create current stream state or an event"
    );
    assert_eq!(handle_b.counts().streams, 0);

    // A later strictly-increasing, correctly bound OPEN is still admitted.
    let valid_identity = OpenIdentity::new(gateway_a, SessionId::new(), 2);
    framed
        .send(PeerFrame::Open {
            stream_id: StreamId::from_raw(2),
            open_identity: valid_identity,
            client_id: "echo.b".to_owned(),
            listener_session_id: SessionId::new(),
            binding_id: BindingId::new(),
        })
        .await?;
    let incoming = next_event(&mut events_b).await?;
    let PeerEvent::IncomingOpen {
        key, open_identity, ..
    } = incoming
    else {
        return Err(format!("expected IncomingOpen, got {incoming:?}").into());
    };
    assert_eq!(open_identity, valid_identity);

    // CLOSE in OPENING is a stream-local protocol violation, not a normal
    // close and not a transport-wide failure.
    framed
        .send(PeerFrame::Close {
            stream_id: StreamId::from_raw(2),
        })
        .await?;
    let reset_event = next_event(&mut events_b).await?;
    assert!(matches!(
        reset_event,
        PeerEvent::Reset {
            key: event_key,
            code: ErrorCode::ProtocolError,
            ..
        } if event_key == key
    ));
    let reset_frame = tokio::time::timeout(Duration::from_secs(1), framed.next())
        .await?
        .ok_or("peer closed before protocol RESET")??;
    assert!(matches!(
        reset_frame,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::ProtocolError,
            ..
        } if stream_id == StreamId::from_raw(2)
    ));

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve).await???;
    Ok(())
}

#[tokio::test]
async fn local_opening_reset_emits_failed_for_attempt_cleanup() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let handle_a = pair.handle_a.clone();
    let request = pair.request_a_to_b(1)?;
    let open = tokio::spawn(async move { handle_a.open(request).await });
    let incoming = next_event(&mut pair.events_b).await?;
    let PeerEvent::IncomingOpen { key: owner_key, .. } = incoming else {
        return Err(format!("expected IncomingOpen, got {incoming:?}").into());
    };
    let entry_key = open.await??;

    pair.handle_b.send_close(owner_key).await?;
    let failed = next_event(&mut pair.events_a).await?;
    assert!(matches!(
        failed,
        PeerEvent::Failed {
            key,
            open_identity: _,
            failure,
            ..
        } if key == entry_key
            && failure.code() == ErrorCode::ProtocolError
            && failure.observation() == PeerObservation::MaybeObserved
    ));
    pair.shutdown().await
}

#[tokio::test]
async fn saturated_event_queue_fails_closed_without_cyclic_wait() -> TestResult {
    let mut pair = RuntimePair::start_with_event_capacity(1).await?;
    let handle_a = pair.handle_a.clone();
    let request = pair.request_a_to_b(1)?;
    let open = tokio::spawn(async move { handle_a.open(request).await });
    let (owner_key, _) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let _entry_key = open.await??;

    // Keep A's only event slot occupied by OPENED. A second event must fail
    // closed instead of blocking the manager while the consumer could be
    // awaiting a PeerHandle command reply from that same manager.
    pair.handle_b
        .send_data(owner_key, Bytes::from_static(b"blocked"))
        .await?;
    let serve_result = tokio::time::timeout(Duration::from_secs(2), &mut pair.serve_a).await??;
    let failure = serve_result
        .err()
        .ok_or("expected event queue saturation failure")?;
    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(pair.handle_a.counts(), Default::default());

    pair.shutdown_a.cancel();
    pair.shutdown_b.cancel();
    tokio::time::timeout(Duration::from_secs(2), pair.serve_b).await???;
    Ok(())
}

#[tokio::test]
async fn closed_event_receiver_shuts_down_and_clears_shared_counts() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let request = pair.request_a_to_b(1)?;
    drop(pair.events_a);

    let handle_a = pair.handle_a.clone();
    let open = tokio::spawn(async move { handle_a.open(request).await });
    let _ = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let _ = open.await??;

    let serve_result = tokio::time::timeout(Duration::from_secs(2), &mut pair.serve_a).await??;
    let failure = serve_result
        .err()
        .ok_or("expected closed event receiver failure")?;
    assert_eq!(failure.code(), ErrorCode::Unavailable);
    assert_eq!(pair.handle_a.counts(), Default::default());

    pair.shutdown_a.cancel();
    pair.shutdown_b.cancel();
    tokio::time::timeout(Duration::from_secs(2), pair.serve_b).await???;
    Ok(())
}

#[tokio::test]
async fn cancelled_runtime_ignores_receiver_drop_during_transport_loss() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let handle_a = pair.handle_a.clone();
    let request = pair.request_a_to_b(1)?;
    let open = tokio::spawn(async move { handle_a.open(request).await });
    let _ = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let _ = open.await??;
    let _ = next_event(&mut pair.events_a).await?;

    // RunningGateway.stop cancels first; its event-loop task can then drop the
    // receiver before the peer manager selects its shutdown branch. A racing
    // TransportLost must remain graceful shutdown, not UNAVAILABLE.
    pair.shutdown_a.cancel();
    drop(pair.events_a);
    let serve_result = tokio::time::timeout(Duration::from_secs(2), &mut pair.serve_a).await??;
    assert!(serve_result.is_ok());
    assert_eq!(pair.handle_a.counts(), Default::default());

    pair.shutdown_b.cancel();
    tokio::time::timeout(Duration::from_secs(2), pair.serve_b).await???;
    Ok(())
}

#[tokio::test]
async fn open_identity_is_unique_only_while_current_stream_is_active() -> TestResult {
    let mut pair = RuntimePair::start().await?;
    let identity = OpenIdentity::new(pair.gateway_a, SessionId::new(), 7);
    let request = PeerOpenRequest::new(
        PeerTarget::new(pair.gateway_b, pair.locator_b.clone()),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let first_open = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(request).await })
    };
    let (owner_key, _) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let first_key = first_open.await??;
    let _ = next_event(&mut pair.events_a).await?;

    let duplicate = PeerOpenRequest::new(
        PeerTarget::new(pair.gateway_b, pair.locator_b.clone()),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let failure = pair
        .handle_a
        .open(duplicate)
        .await
        .err()
        .ok_or("expected active identity conflict")?;
    assert_eq!(failure.code(), ErrorCode::AlreadyExists);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);

    pair.handle_b.send_close(owner_key).await?;
    let closed = next_event(&mut pair.events_a).await?;
    assert!(matches!(closed, PeerEvent::Close { key } if key == first_key));

    let reused = PeerOpenRequest::new(
        PeerTarget::new(pair.gateway_b, pair.locator_b.clone()),
        identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let reopened = {
        let handle = pair.handle_a.clone();
        tokio::spawn(async move { handle.open(reused).await })
    };
    let (new_owner_key, new_identity) = accept_one(&mut pair.events_b, &pair.handle_b).await?;
    let new_entry_key = reopened.await??;
    assert_eq!(new_identity, identity);
    assert_ne!(new_entry_key.stream_id(), first_key.stream_id());
    let _ = next_event(&mut pair.events_a).await?;
    pair.handle_a.cancel_open(identity).await?;
    let cancelled = next_event(&mut pair.events_b).await?;
    assert!(matches!(
        cancelled,
        PeerEvent::Reset {
            key,
            code: ErrorCode::Cancelled,
            ..
        } if key == new_owner_key
    ));
    pair.shutdown().await
}

#[test]
fn config_debug_redacts_all_peer_keys() -> TestResult {
    let config = test_config("gateway-a", "top-secret-a", "gateway-b", "top-secret-b")?;
    let rendered = format!("{config:?}");
    assert!(!rendered.contains("top-secret-a"));
    assert!(!rendered.contains("top-secret-b"));
    Ok(())
}
