use super::*;

#[tokio::test]
async fn same_direction_duplicate_preserves_ready_transport_for_open_and_data() -> TestResult {
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

    let first_transport_id = PeerTransportId::new();
    let mut first =
        connect_raw_peer(locator.as_str(), gateway_a, gateway_b, first_transport_id).await?;
    wait_for_counts(
        &handle_b,
        PeerCounts {
            connecting: 0,
            ready: 1,
            streams: 0,
        },
    )
    .await?;

    let duplicate_transport_id = PeerTransportId::new();
    assert_ne!(duplicate_transport_id, first_transport_id);
    let stream = TcpStream::connect(locator.as_str()).await?;
    stream.set_nodelay(true)?;
    let mut duplicate = Framed::new(stream, PeerFrameCodec::new(64 * 1024));
    duplicate
        .send(PeerFrame::Hello(handshake(
            gateway_a,
            gateway_b,
            duplicate_transport_id,
        )?))
        .await?;
    let rejection = next_frame(&mut duplicate, "duplicate rejection").await?;
    assert!(matches!(
        rejection,
        PeerFrame::HandshakeRejected {
            code: ErrorCode::AlreadyExists,
            ..
        }
    ));
    match tokio::time::timeout(Duration::from_secs(1), duplicate.next()).await? {
        None => {}
        Some(Ok(frame)) => {
            return Err(
                format!("duplicate peer remained active after rejection: {frame:?}").into(),
            );
        }
        Some(Err(error)) => {
            return Err(format!("duplicate peer did not close cleanly: {error}").into());
        }
    }

    wait_for_counts(
        &handle_b,
        PeerCounts {
            connecting: 0,
            ready: 1,
            streams: 0,
        },
    )
    .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), events_b.recv())
            .await
            .is_err(),
        "duplicate handshake emitted a peer event"
    );

    let stream_id = StreamId::from_raw(0);
    let open_identity = OpenIdentity::new(gateway_a, SessionId::new(), 1);
    first
        .send(PeerFrame::Open {
            stream_id,
            open_identity,
            client_id: "echo.b".to_owned(),
            listener_session_id: SessionId::new(),
            binding_id: BindingId::new(),
        })
        .await?;
    let incoming = next_event(&mut events_b).await?;
    let PeerEvent::IncomingOpen {
        key,
        open_identity: observed_identity,
        ..
    } = incoming
    else {
        return Err(format!("expected IncomingOpen, got {incoming:?}").into());
    };
    assert_eq!(key.peer_transport_id(), first_transport_id);
    assert_eq!(key.stream_id(), stream_id);
    assert_eq!(observed_identity, open_identity);
    handle_b.send_opened(key).await?;
    assert!(matches!(
        next_frame(&mut first, "OPENED").await?,
        PeerFrame::Opened { stream_id: opened } if opened == stream_id
    ));

    let toward_b = Bytes::from_static(b"first-transport-still-ready");
    first
        .send(PeerFrame::Data {
            stream_id,
            payload: toward_b.clone(),
        })
        .await?;
    assert!(matches!(
        next_event(&mut events_b).await?,
        PeerEvent::Data { key: event_key, payload }
            if event_key == key && payload == toward_b
    ));

    let toward_a = Bytes::from_static(b"owner-reply");
    handle_b.send_data(key, toward_a.clone()).await?;
    assert!(matches!(
        next_frame(&mut first, "DATA reply").await?,
        PeerFrame::Data { stream_id: received, payload }
            if received == stream_id && payload == toward_a
    ));

    first.send(PeerFrame::Close { stream_id }).await?;
    assert!(matches!(
        next_event(&mut events_b).await?,
        PeerEvent::Close { key: event_key } if event_key == key
    ));
    wait_for_counts(
        &handle_b,
        PeerCounts {
            connecting: 0,
            ready: 1,
            streams: 0,
        },
    )
    .await?;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), serve).await???;
    assert_eq!(handle_b.counts(), PeerCounts::default());
    Ok(())
}

async fn connect_raw_peer(
    locator: &str,
    gateway_a: GatewayId,
    gateway_b: GatewayId,
    transport_id: PeerTransportId,
) -> TestResult<Framed<TcpStream, PeerFrameCodec>> {
    let stream = TcpStream::connect(locator).await?;
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(stream, PeerFrameCodec::new(64 * 1024));
    framed
        .send(PeerFrame::Hello(handshake(
            gateway_a,
            gateway_b,
            transport_id,
        )?))
        .await?;
    assert!(matches!(
        next_frame(&mut framed, "WELCOME").await?,
        PeerFrame::Welcome(PeerHandshake {
            gateway_id,
            expected_peer_gateway_id,
            dialer_gateway_id,
            peer_transport_id,
            ..
        }) if gateway_id == gateway_b
            && expected_peer_gateway_id == gateway_a
            && dialer_gateway_id == gateway_a
            && peer_transport_id == transport_id
    ));
    Ok(framed)
}

fn handshake(
    gateway_a: GatewayId,
    gateway_b: GatewayId,
    transport_id: PeerTransportId,
) -> TestResult<PeerHandshake> {
    Ok(PeerHandshake {
        gateway_name: PeerGatewayName::new("gateway-a")?,
        internal_gateway_key: PeerGatewayKey::new("key-a")?,
        gateway_id: gateway_a,
        expected_peer_gateway_id: gateway_b,
        dialer_gateway_id: gateway_a,
        peer_transport_id: transport_id,
    })
}

async fn next_frame(
    framed: &mut Framed<TcpStream, PeerFrameCodec>,
    expected: &str,
) -> TestResult<PeerFrame> {
    let frame = tokio::time::timeout(Duration::from_secs(1), framed.next())
        .await?
        .ok_or_else(|| std::io::Error::other(format!("peer closed before {expected}")))??;
    Ok(frame)
}
