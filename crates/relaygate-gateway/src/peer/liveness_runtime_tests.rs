use std::{error::Error, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use super::{
    GatewayPeerConfig, OpenIdentity, PeerEvent, PeerHandle, PeerOpenRequest, PeerRuntime,
    PeerTarget, TrustedPeerConfig,
    codec::PeerFrameCodec,
    event::PeerCounts,
    frame::PeerFrame,
    identity::{PeerGatewayKey, PeerGatewayName, PeerHandshake, PeerTransportId},
};
use observation::{assert_transport_lifecycle_event, captured_dispatch};

mod observation;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn test_config_with_liveness(
    heartbeat_idle_interval: Duration,
    heartbeat_response_timeout: Duration,
    idle_retirement_timeout: Duration,
) -> Result<GatewayPeerConfig, crate::GatewayError> {
    Ok(GatewayPeerConfig::new(
        "gateway-a",
        "key-a",
        [TrustedPeerConfig::new("gateway-b", "key-b")?],
    )?
    .with_queue_bounds(64, 64, 64, 64, 8)
    .with_resource_limits(64, 64, 16, 64 * 1024)
    .with_timeouts(
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(1),
    )
    .with_liveness(
        heartbeat_idle_interval,
        heartbeat_response_timeout,
        idle_retirement_timeout,
    ))
}

async fn next_event(events: &mut super::PeerEvents) -> TestResult<PeerEvent> {
    tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await?
        .ok_or_else(|| "peer event stream closed".into())
}

async fn next_fake_frame(framed: &mut Framed<TcpStream, PeerFrameCodec>) -> TestResult<PeerFrame> {
    tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await?
        .ok_or_else(|| "fake peer socket closed".to_owned())?
        .map_err(Into::into)
}

async fn accept_fake_peer(
    listener: &TcpListener,
    gateway_a: GatewayId,
    gateway_b: GatewayId,
) -> TestResult<(Framed<TcpStream, PeerFrameCodec>, PeerTransportId)> {
    let (stream, _) = listener.accept().await?;
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(stream, PeerFrameCodec::new(64 * 1024));
    let hello = next_fake_frame(&mut framed).await?;
    let PeerFrame::Hello(hello) = hello else {
        return Err(format!("expected peer HELLO, got {hello:?}").into());
    };
    assert_eq!(hello.gateway_id, gateway_a);
    assert_eq!(hello.expected_peer_gateway_id, gateway_b);
    assert_eq!(hello.dialer_gateway_id, gateway_a);

    let peer_transport_id = hello.peer_transport_id;
    framed
        .send(PeerFrame::Welcome(PeerHandshake {
            gateway_name: PeerGatewayName::new("gateway-b")?,
            internal_gateway_key: PeerGatewayKey::new("key-b")?,
            gateway_id: gateway_b,
            expected_peer_gateway_id: gateway_a,
            dialer_gateway_id: gateway_a,
            peer_transport_id,
        }))
        .await?;
    Ok((framed, peer_transport_id))
}

async fn wait_peer_counts(handle: &PeerHandle, expected: PeerCounts) -> TestResult {
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.counts() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn active_transport_heartbeat_timeout_releases_streams_and_reconnects_lazily() -> TestResult {
    let (captured, dispatch) = captured_dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let listener_b = TcpListener::bind("127.0.0.1:0").await?;
    let locator_b = GatewayLocator::new(listener_b.local_addr()?.to_string())?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let shutdown_a = CancellationToken::new();
    let (handle_a, mut events_a, runtime_a) = PeerRuntime::start(
        test_config_with_liveness(
            Duration::from_millis(20),
            Duration::from_millis(300),
            Duration::from_secs(1),
        )?,
        gateway_a,
        shutdown_a.clone(),
    )?;
    let serve_a = tokio::spawn(runtime_a.serve(listener_a));
    let (release_first, release_first_rx) = oneshot::channel::<()>();
    let fake_peer = tokio::spawn(async move {
        let (mut framed, first_transport_id) =
            accept_fake_peer(&listener_b, gateway_a, gateway_b).await?;
        let first_open = next_fake_frame(&mut framed).await?;
        let PeerFrame::Open {
            stream_id: first_stream_id,
            ..
        } = first_open
        else {
            return Err(format!("expected first OPEN, got {first_open:?}").into());
        };
        framed
            .send(PeerFrame::Opened {
                stream_id: first_stream_id,
            })
            .await?;
        let heartbeat_nonce = loop {
            if let PeerFrame::Ping { nonce } = next_fake_frame(&mut framed).await? {
                break nonce;
            }
        };
        let unrelated_nonce = heartbeat_nonce ^ u64::MAX;
        framed
            .send(PeerFrame::Ping {
                nonce: unrelated_nonce,
            })
            .await?;
        let pong = next_fake_frame(&mut framed).await?;
        assert!(matches!(
            pong,
            PeerFrame::Pong { nonce } if nonce == unrelated_nonce
        ));
        let _ = release_first_rx.await;
        drop(framed);

        let (mut framed, second_transport_id) =
            accept_fake_peer(&listener_b, gateway_a, gateway_b).await?;
        let second_open = next_fake_frame(&mut framed).await?;
        let PeerFrame::Open {
            stream_id: second_stream_id,
            ..
        } = second_open
        else {
            return Err(format!("expected second OPEN, got {second_open:?}").into());
        };
        framed
            .send(PeerFrame::Opened {
                stream_id: second_stream_id,
            })
            .await?;
        let close = next_fake_frame(&mut framed).await?;
        assert!(matches!(
            close,
            PeerFrame::Close { stream_id } if stream_id == second_stream_id
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>((first_transport_id, second_transport_id))
    });

    let first_request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b.clone()),
        OpenIdentity::new(gateway_a, SessionId::new(), 1),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let first_key = handle_a.open(first_request).await?;
    let opened = next_event(&mut events_a).await?;
    assert!(matches!(opened, PeerEvent::Opened { key, .. } if key == first_key));

    let loss = loop {
        let event = next_event(&mut events_a).await?;
        if matches!(event, PeerEvent::TransportLost { .. }) {
            break event;
        }
    };
    let PeerEvent::TransportLost {
        peer_gateway_id,
        peer_transport_id,
        streams,
    } = loss
    else {
        return Err("expected active heartbeat transport loss".into());
    };
    assert_eq!(peer_gateway_id, gateway_b);
    assert_eq!(peer_transport_id, first_key.peer_transport_id());
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].key, first_key);
    assert_eq!(
        streams[0].progress.failure_observation(),
        PeerObservation::MaybeObserved
    );
    assert_transport_lifecycle_event(
        &captured,
        "gateway.peer.transport.heartbeat_timeout",
        gateway_b,
        first_key.peer_transport_id(),
        1,
    )?;
    wait_peer_counts(&handle_a, Default::default()).await?;
    let _ = release_first.send(());

    let second_request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b),
        OpenIdentity::new(gateway_a, SessionId::new(), 2),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let second_key = handle_a.open(second_request).await?;
    assert_ne!(
        first_key.peer_transport_id(),
        second_key.peer_transport_id()
    );
    let opened = next_event(&mut events_a).await?;
    assert!(matches!(opened, PeerEvent::Opened { key, .. } if key == second_key));
    assert_eq!(handle_a.counts().ready, 1);
    assert_eq!(handle_a.counts().streams, 1);
    handle_a.send_close(second_key).await?;

    let (first_transport_id, second_transport_id) = fake_peer.await??;
    assert_eq!(first_transport_id, first_key.peer_transport_id());
    assert_eq!(second_transport_id, second_key.peer_transport_id());
    shutdown_a.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve_a).await???;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn zero_stream_idle_retirement_removes_ready_transport_and_reconnects_lazily() -> TestResult {
    let (captured, dispatch) = captured_dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let listener_b = TcpListener::bind("127.0.0.1:0").await?;
    let locator_b = GatewayLocator::new(listener_b.local_addr()?.to_string())?;
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let shutdown_a = CancellationToken::new();
    let (handle_a, mut events_a, runtime_a) = PeerRuntime::start(
        test_config_with_liveness(
            Duration::from_secs(1),
            Duration::from_millis(50),
            Duration::from_millis(40),
        )?,
        gateway_a,
        shutdown_a.clone(),
    )?;
    let serve_a = tokio::spawn(runtime_a.serve(listener_a));
    let fake_peer = tokio::spawn(async move {
        let (mut framed, first_transport_id) =
            accept_fake_peer(&listener_b, gateway_a, gateway_b).await?;
        let first_open = next_fake_frame(&mut framed).await?;
        let PeerFrame::Open {
            stream_id: first_stream_id,
            ..
        } = first_open
        else {
            return Err(format!("expected first OPEN, got {first_open:?}").into());
        };
        framed
            .send(PeerFrame::Opened {
                stream_id: first_stream_id,
            })
            .await?;
        let close = next_fake_frame(&mut framed).await?;
        assert!(matches!(
            close,
            PeerFrame::Close { stream_id } if stream_id == first_stream_id
        ));
        let eof = tokio::time::timeout(Duration::from_secs(2), framed.next()).await?;
        assert!(matches!(eof, None | Some(Err(_))));

        let (mut framed, second_transport_id) =
            accept_fake_peer(&listener_b, gateway_a, gateway_b).await?;
        let second_open = next_fake_frame(&mut framed).await?;
        let PeerFrame::Open {
            stream_id: second_stream_id,
            ..
        } = second_open
        else {
            return Err(format!("expected second OPEN, got {second_open:?}").into());
        };
        framed
            .send(PeerFrame::Opened {
                stream_id: second_stream_id,
            })
            .await?;
        let close = next_fake_frame(&mut framed).await?;
        assert!(matches!(
            close,
            PeerFrame::Close { stream_id } if stream_id == second_stream_id
        ));
        Ok::<_, Box<dyn Error + Send + Sync>>((first_transport_id, second_transport_id))
    });

    let first_request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b.clone()),
        OpenIdentity::new(gateway_a, SessionId::new(), 1),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let first_key = handle_a.open(first_request).await?;
    let opened = next_event(&mut events_a).await?;
    assert!(matches!(opened, PeerEvent::Opened { key, .. } if key == first_key));
    handle_a.send_close(first_key).await?;
    wait_peer_counts(
        &handle_a,
        PeerCounts {
            ready: 1,
            ..Default::default()
        },
    )
    .await?;

    let loss = loop {
        let event = next_event(&mut events_a).await?;
        if matches!(event, PeerEvent::TransportLost { .. }) {
            break event;
        }
    };
    let PeerEvent::TransportLost {
        peer_gateway_id,
        peer_transport_id,
        streams,
    } = loss
    else {
        return Err("expected idle retirement transport loss".into());
    };
    assert_eq!(peer_gateway_id, gateway_b);
    assert_eq!(peer_transport_id, first_key.peer_transport_id());
    assert!(streams.is_empty());
    assert_transport_lifecycle_event(
        &captured,
        "gateway.peer.transport.idle_retired",
        gateway_b,
        first_key.peer_transport_id(),
        0,
    )?;
    wait_peer_counts(&handle_a, Default::default()).await?;

    let second_request = PeerOpenRequest::new(
        PeerTarget::new(gateway_b, locator_b),
        OpenIdentity::new(gateway_a, SessionId::new(), 2),
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let second_key = handle_a.open(second_request).await?;
    assert_ne!(
        first_key.peer_transport_id(),
        second_key.peer_transport_id()
    );
    let opened = next_event(&mut events_a).await?;
    assert!(matches!(opened, PeerEvent::Opened { key, .. } if key == second_key));
    handle_a.send_close(second_key).await?;

    let (first_transport_id, second_transport_id) = fake_peer.await??;
    assert_eq!(first_transport_id, first_key.peer_transport_id());
    assert_eq!(second_transport_id, second_key.peer_transport_id());
    shutdown_a.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve_a).await???;
    Ok(())
}
