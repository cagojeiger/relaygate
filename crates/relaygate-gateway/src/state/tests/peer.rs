#[test]
fn peer_open_identity_must_match_authenticated_peer_gateway()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let claimed_entry = GatewayId::new();
    let authenticated_peer = GatewayId::new();

    let actions = state.receive_peer_open(
        peer_key(authenticated_peer, 0),
        OpenIdentity::new(claimed_entry, SessionId::new(), 1),
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );

    assert!(peer_deliveries(&actions).any(|delivery| matches!(
        delivery,
        PeerDelivery::Failed {
            code: ErrorCode::PermissionDenied,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn stale_peer_frame_cannot_mutate_a_replacement_gateway_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let stale_owner = GatewayId::new();
    let current_owner = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            current_owner,
            SessionId::new(),
            BindingId::new(),
            "reused.internal:27421",
        )?,
    );
    let current_key = PeerStreamKey::for_test(current_owner, PeerTransportId::new(), 0);
    assert!(state.peer_open_committed(identity, current_key).is_empty());
    let opened = state.peer_opened(current_key, identity);
    assert!(sdk_deliveries(&opened).any(|delivery| matches!(
        delivery.frame,
        Frame::Opened { pipe_id }
            if pipe_id == PipeId::new(connector, 1)
    )));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);

    let stale_key = PeerStreamKey::for_test(stale_owner, PeerTransportId::new(), 0);
    assert_ne!(stale_key, current_key);
    assert!(
        state
            .peer_data(stale_key, Bytes::from_static(b"stale"))
            .is_empty()
    );
    assert!(
        state
            .peer_reset(stale_key, ErrorCode::Cancelled, "stale".to_owned())
            .is_empty()
    );
    assert!(
        state
            .peer_transport_lost_stream(stale_key, identity, PeerObservation::MaybeObserved)
            .is_empty()
    );
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.active_peer_open_count(), 1);

    let relay = state.handle(
        connector,
        Frame::Data {
            pipe_id: PipeId::new(connector, 1),
            payload: Bytes::from_static(b"current"),
        },
    )?;
    assert!(peer_deliveries(&relay).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { key, payload }
            if *key == current_key && payload.as_ref() == b"current"
    )));

    state.peer_close(current_key);
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.active_peer_open_count(), 0);
    Ok(())
}

#[test]
fn early_opened_then_late_commit_callback_is_idempotent() -> Result<(), Box<dyn std::error::Error>>
{
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    let target_binding = BindingId::new();
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            target_binding,
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let opened = state.peer_opened(key, identity);
    assert!(sdk_deliveries(&opened).any(|delivery| matches!(delivery.frame, Frame::Opened { .. })));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.remote_open_attempt_count(), 0);

    assert!(state.peer_open_committed(identity, key).is_empty());
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn early_failed_then_late_commit_callback_never_resurrects_state()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let failed = state.peer_open_failed(
        key,
        identity,
        ErrorCode::Unavailable,
        PeerObservation::NotObserved,
        "owner unavailable",
    );
    assert!(
        sdk_deliveries(&failed).any(|delivery| matches!(delivery.frame, Frame::OpenFailed { .. }))
    );
    assert_eq!(state.remote_open_attempt_count(), 0);

    let late = state.peer_open_committed(identity, key);
    assert!(peer_deliveries(&late).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            code: ErrorCode::Cancelled,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn early_transport_loss_then_late_commit_callback_never_leaves_an_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let lost = state.peer_transport_lost_stream(key, identity, PeerObservation::MaybeObserved);
    assert!(sdk_deliveries(&lost).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert_eq!(state.remote_open_attempt_count(), 0);

    let _late = state.peer_open_committed(identity, key);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn committed_open_transport_loss_fails_once_and_late_opened_cannot_resurrect()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let target_owner = GatewayId::new();
    let sibling_owner = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let target_connector = add_session(&mut state, SessionRole::Connector);
    let sibling_connector = add_session(&mut state, SessionRole::Connector);

    let target_resolve = state.handle(
        target_connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let target_identity = resolve_identity(&target_resolve).ok_or("missing target resolve")?;
    state.route_resolved(
        target_identity,
        binding_set(
            "echo.shared",
            target_owner,
            SessionId::new(),
            BindingId::new(),
            "target.internal:27421",
        )?,
    );
    let target_key = peer_key(target_owner, 0);
    assert!(
        state
            .peer_open_committed(target_identity, target_key)
            .is_empty()
    );

    let sibling_resolve = state.handle(
        sibling_connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let sibling_identity = resolve_identity(&sibling_resolve).ok_or("missing sibling resolve")?;
    state.route_resolved(
        sibling_identity,
        binding_set(
            "echo.shared",
            sibling_owner,
            SessionId::new(),
            BindingId::new(),
            "sibling.internal:27421",
        )?,
    );
    let sibling_key = peer_key(sibling_owner, 0);
    assert!(
        state
            .peer_open_committed(sibling_identity, sibling_key)
            .is_empty()
    );
    assert_eq!(state.remote_open_attempt_count(), 2);
    assert_eq!(state.active_peer_open_count(), 2);
    assert_eq!(state.pipe_count(), 0);

    let lost = state.peer_transport_lost_stream(
        target_key,
        target_identity,
        PeerObservation::MaybeObserved,
    );
    assert_eq!(lost.len(), 1);
    assert!(sdk_deliveries(&lost).any(|delivery| matches!(
        delivery,
        Delivery {
            target,
            frame: Frame::OpenFailed {
                connection_id: 1,
                code: ErrorCode::Unavailable,
                observation: PeerObservation::MaybeObserved,
                ..
            },
            ..
        } if *target == target_connector
    )));
    assert_eq!(peer_deliveries(&lost).count(), 0);
    assert_eq!(publications(&lost).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.pipe_count(), 0);

    let duplicate_loss = state.peer_transport_lost_stream(
        target_key,
        target_identity,
        PeerObservation::MaybeObserved,
    );
    assert!(duplicate_loss.is_empty());
    assert_eq!(state.remote_open_attempt_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);

    let late_opened = state.peer_opened(target_key, target_identity);
    assert_eq!(late_opened.len(), 1);
    assert!(peer_deliveries(&late_opened).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key,
            code: ErrorCode::Cancelled,
            ..
        } if *key == target_key
    )));
    assert_eq!(sdk_deliveries(&late_opened).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.pipe_count(), 0);

    let sibling_opened = state.peer_opened(sibling_key, sibling_identity);
    assert_eq!(sibling_opened.len(), 1);
    assert!(sdk_deliveries(&sibling_opened).any(|delivery| matches!(
        delivery,
        Delivery {
            target,
            frame: Frame::Opened { .. },
            ..
        } if *target == sibling_connector
    )));
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn connector_cancel_before_peer_open_commit_sends_cancel_and_late_commit_only_resets_peer()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let pipe_id = PipeId::new(connector, 1);

    let cancelled = state.handle(connector, Frame::Cancel { pipe_id })?;

    assert!(cancelled.iter().any(|action| {
        matches!(action, GatewayAction::CancelPeerOpen { open_identity } if *open_identity == identity)
    }));
    assert_eq!(sdk_deliveries(&cancelled).count(), 0);
    assert_eq!(peer_deliveries(&cancelled).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);

    let key = peer_key(owner_gateway, 0);
    let late_commit = state.peer_open_committed(identity, key);
    assert!(peer_deliveries(&late_commit).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == key
    )));
    assert_eq!(sdk_deliveries(&late_commit).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn connector_cancel_after_peer_open_commit_resets_peer_and_late_opened_cannot_recreate_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);
    state.peer_open_committed(identity, key);
    let pipe_id = PipeId::new(connector, 1);

    let cancelled = state.handle(connector, Frame::Cancel { pipe_id })?;

    assert!(peer_deliveries(&cancelled).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == key
    )));
    assert_eq!(sdk_deliveries(&cancelled).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);

    let late_opened = state.peer_opened(key, identity);
    assert!(peer_deliveries(&late_opened).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == key
    )));
    assert_eq!(sdk_deliveries(&late_opened).count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn connector_cancel_after_peer_opened_resets_only_target_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let mut identities = Vec::new();
    let mut keys = Vec::new();
    let mut pipe_ids = Vec::new();

    for (connection_id, raw_stream_id) in [(1, 0), (2, 2)] {
        let resolve = state.handle(
            connector,
            Frame::Open {
                connection_id,
                client_id: "echo.shared".to_owned(),
            },
        )?;
        let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
        let pipe_id = PipeId::new(connector, connection_id);
        state.route_resolved(
            identity,
            binding_set(
                "echo.shared",
                owner_gateway,
                SessionId::new(),
                BindingId::new(),
                "owner.internal:27421",
            )?,
        );
        let key = PeerStreamKey::for_test(owner_gateway, peer_transport_id, raw_stream_id);
        assert!(state.peer_open_committed(identity, key).is_empty());
        let opened = state.peer_opened(key, identity);
        assert_eq!(opened.len(), 1);
        assert!(sdk_deliveries(&opened).any(|delivery| {
            delivery.target == connector
                && matches!(delivery.frame, Frame::Opened { pipe_id: opened_id } if opened_id == pipe_id)
        }));
        identities.push(identity);
        keys.push(key);
        pipe_ids.push(pipe_id);
    }
    assert_eq!(state.pipe_count(), 2);
    assert_eq!(state.peer_pipe_count(), 2);
    assert_eq!(state.active_peer_open_count(), 2);
    assert_eq!(state.live_pipe_count(), 2);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);

    let cancelled = state.handle(
        connector,
        Frame::Cancel {
            pipe_id: pipe_ids[0],
        },
    )?;

    assert_eq!(cancelled.len(), 1);
    assert!(peer_deliveries(&cancelled).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key,
            code: ErrorCode::Cancelled,
            ..
        } if *key == keys[0]
    )));
    assert!(!peer_deliveries(&cancelled).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset { key, .. } if *key == keys[1]
    )));
    assert_eq!(sdk_deliveries(&cancelled).count(), 0);
    assert_eq!(publications(&cancelled).count(), 0);
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.live_pipe_count(), 1);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);

    let late_opened = state.peer_opened(keys[0], identities[0]);
    assert_eq!(late_opened.len(), 1);
    assert!(peer_deliveries(&late_opened).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key,
            code: ErrorCode::Cancelled,
            ..
        } if *key == keys[0]
    )));
    assert_eq!(sdk_deliveries(&late_opened).count(), 0);
    assert_eq!(publications(&late_opened).count(), 0);
    assert!(state
        .peer_data(keys[0], Bytes::from_static(b"late target data"))
        .is_empty());
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.live_pipe_count(), 1);

    let sibling = state.handle(
        connector,
        Frame::Data {
            pipe_id: pipe_ids[1],
            payload: Bytes::from_static(b"sibling survives cancel"),
        },
    )?;
    assert!(peer_deliveries(&sibling).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { key, payload }
            if *key == keys[1] && payload.as_ref() == b"sibling survives cancel"
    )));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.live_pipe_count(), 1);
    Ok(())
}

#[test]
fn connector_session_cleanup_resets_committed_peer_stream_as_cancelled()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);
    state.peer_open_committed(identity, key);
    state.peer_opened(key, identity);

    let cleanup = state.remove_session(connector);

    assert!(peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == key
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn owner_transport_loss_preserves_listener_binding_and_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let peer_transport_id = PeerTransportId::new();
    let identities = [
        OpenIdentity::new(entry_gateway, SessionId::new(), 1),
        OpenIdentity::new(entry_gateway, SessionId::new(), 1),
    ];
    let keys = [
        PeerStreamKey::for_test(entry_gateway, peer_transport_id, 0),
        PeerStreamKey::for_test(entry_gateway, peer_transport_id, 2),
    ];
    for (key, identity) in keys.into_iter().zip(identities) {
        let offered = state.receive_peer_open(
            key,
            identity,
            "echo.shared".to_owned(),
            listener,
            binding.id,
        );
        let pipe_id = PipeId::new(identity.connector_session(), identity.connection_id());
        assert!(sdk_deliveries(&offered).any(|delivery| matches!(
            delivery.frame,
            Frame::Offer { pipe_id: offered, .. } if offered == pipe_id
        )));
        let accepted = state.handle(listener, Frame::OfferAccepted { pipe_id })?;
        assert!(peer_deliveries(&accepted).any(|delivery| matches!(
            delivery,
            PeerDelivery::Opened { key: opened } if *opened == key
        )));
    }
    assert_eq!(state.pipe_count(), 2);
    assert_eq!(state.snapshot().listener_bindings, 1);

    for (key, identity) in keys.into_iter().zip(identities) {
        let lost = state.peer_transport_lost_stream(
            key,
            identity,
            PeerObservation::MaybeObserved,
        );
        assert!(sdk_deliveries(&lost).any(|delivery| delivery.target == listener));
        assert_eq!(publications(&lost).count(), 0);
    }
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.active_peer_open_count(), 0);
    assert_eq!(state.snapshot().listener_bindings, 1);
    assert_eq!(state.snapshot().listener_sessions, 1);
    Ok(())
}

#[test]
fn owner_listener_session_loss_resets_only_owned_peer_stream_and_preserves_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let lost_listener = add_session(&mut state, SessionRole::Listener);
    let sibling_listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, lost_listener)?;
    register_listener(&mut state, sibling_listener)?;
    let lost_binding = state
        .registry
        .bindings_for_session(lost_listener)
        .into_iter()
        .next()
        .ok_or("missing lost listener binding")?;
    let sibling_binding = state
        .registry
        .bindings_for_session(sibling_listener)
        .into_iter()
        .next()
        .ok_or("missing sibling listener binding")?;
    let peer_transport_id = PeerTransportId::new();
    let identities = [
        OpenIdentity::new(entry_gateway, SessionId::new(), 1),
        OpenIdentity::new(entry_gateway, SessionId::new(), 1),
    ];
    let keys = [
        PeerStreamKey::for_test(entry_gateway, peer_transport_id, 0),
        PeerStreamKey::for_test(entry_gateway, peer_transport_id, 2),
    ];
    let listeners = [lost_listener, sibling_listener];
    let binding_ids = [lost_binding.id, sibling_binding.id];
    let mut pipe_ids = Vec::new();

    for (((listener, binding_id), key), identity) in listeners
        .into_iter()
        .zip(binding_ids)
        .zip(keys)
        .zip(identities)
    {
        let offered = state.receive_peer_open(
            key,
            identity,
            "echo.shared".to_owned(),
            listener,
            binding_id,
        );
        let pipe_id = PipeId::new(identity.connector_session(), identity.connection_id());
        assert!(sdk_deliveries(&offered).any(|delivery| matches!(
            delivery,
            Delivery {
                target,
                frame: Frame::Offer {
                    pipe_id: offered_pipe,
                    ..
                },
                ..
            } if *target == listener && *offered_pipe == pipe_id
        )));
        let accepted = state.handle(listener, Frame::OfferAccepted { pipe_id })?;
        assert!(peer_deliveries(&accepted).any(|delivery| matches!(
            delivery,
            PeerDelivery::Opened { key: opened_key } if *opened_key == key
        )));
        pipe_ids.push(pipe_id);
    }
    assert_eq!(state.pipe_count(), 2);
    assert_eq!(state.peer_pipe_count(), 2);
    assert_eq!(state.active_peer_open_count(), 2);
    assert_eq!(state.live_pipe_count(), 2);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.snapshot().listener_bindings, 2);

    let cleanup = state.remove_session(lost_listener);

    assert!(peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key,
            code: ErrorCode::Unavailable,
            ..
        } if *key == keys[0]
    )));
    assert!(!peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset { key, .. } if *key == keys[1]
    )));
    assert_eq!(sdk_deliveries(&cleanup).count(), 0);
    let published = publications(&cleanup).collect::<Vec<_>>();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, lost_listener);
    assert!(published[0].1.is_empty());
    assert!(!state.sessions.contains_key(&lost_listener));
    assert!(state.sessions.contains_key(&sibling_listener));
    assert_eq!(state.registry.session_binding_count(lost_listener), 0);
    assert_eq!(state.registry.session_binding_count(sibling_listener), 1);
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.live_pipe_count(), 1);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.snapshot().listener_bindings, 1);

    assert!(state
        .peer_data(keys[0], Bytes::from_static(b"late target data"))
        .is_empty());
    let sibling = state.peer_data(
        keys[1],
        Bytes::from_static(b"sibling survives listener loss"),
    );
    assert!(sdk_deliveries(&sibling).any(|delivery| matches!(
        delivery,
        Delivery {
            target,
            frame: Frame::Data { pipe_id, payload },
            ..
        } if *target == sibling_listener
            && *pipe_id == pipe_ids[1]
            && payload.as_ref() == b"sibling survives listener loss"
    )));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);
    assert_eq!(state.live_pipe_count(), 1);
    Ok(())
}

#[test]
fn owner_offer_timeout_clears_peer_indexes_and_preserves_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = routed_state(GatewayId::new());
    let selected_cancellation = CancellationToken::new();
    let selected = add_session_with_cancellation(
        &mut state,
        SessionRole::Listener,
        selected_cancellation.clone(),
    );
    let sibling_cancellation = CancellationToken::new();
    let sibling = add_session_with_cancellation(
        &mut state,
        SessionRole::Listener,
        sibling_cancellation.clone(),
    );
    register_listener(&mut state, selected)?;
    register_listener(&mut state, sibling)?;
    let binding = state
        .registry
        .bindings_for_session(selected)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let entry_gateway = GatewayId::new();
    let key = peer_key(entry_gateway, 0);
    let identity = OpenIdentity::new(entry_gateway, SessionId::new(), 1);
    let offered_at = Instant::now();

    let offered = state.receive_peer_open_at(
        key,
        identity,
        "echo.shared".to_owned(),
        selected,
        binding.id,
        offered_at,
    );
    assert!(sdk_deliveries(&offered).any(|delivery| {
        delivery.target == selected && matches!(delivery.frame, Frame::Offer { .. })
    }));
    assert_eq!(state.pending_offer_count(), 1);
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.peer_pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);

    let offer_timeout = state.limits.offer_timeout;
    let expired = state.expire_offers(offered_at + offer_timeout);
    assert!(peer_deliveries(&expired).any(|delivery| matches!(
        delivery,
        PeerDelivery::Failed {
            key: failed_key,
            code: ErrorCode::DeadlineExceeded,
            observation: PeerObservation::MaybeObserved,
            ..
        } if *failed_key == key
    )));
    assert!(!sdk_deliveries(&expired).any(|delivery| {
        delivery.target == sibling && matches!(delivery.frame, Frame::Offer { .. })
    }));
    assert!(selected_cancellation.is_cancelled());
    assert!(!sibling_cancellation.is_cancelled());
    assert!(!state.sessions.contains_key(&selected));
    assert!(state.sessions.contains_key(&sibling));
    assert_eq!(state.registry.binding_count(), 1);
    assert_eq!(state.registry.session_binding_count(sibling), 1);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.live_pipe_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.peer_pipe_count(), 0);
    assert_eq!(state.active_peer_open_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert!(
        state
            .handle(
                selected,
                Frame::OfferAccepted {
                    pipe_id: PipeId::new(
                        identity.connector_session(),
                        identity.connection_id(),
                    ),
                },
            )?
            .is_empty()
    );
    Ok(())
}

#[test]
fn connector_cleanup_and_transport_loss_are_scoped_to_current_streams()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let mut state = routed_state(entry_gateway);
    let first_connector = add_session(&mut state, SessionRole::Connector);
    let second_connector = add_session(&mut state, SessionRole::Connector);
    let mut identities = Vec::new();
    let mut keys = Vec::new();
    for (connector, connection_id, raw_stream_id) in
        [(first_connector, 1, 0), (second_connector, 1, 2)]
    {
        let resolve = state.handle(
            connector,
            Frame::Open {
                connection_id,
                client_id: "echo.shared".to_owned(),
            },
        )?;
        let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
        state.route_resolved(
            identity,
            binding_set(
                "echo.shared",
                owner_gateway,
                SessionId::new(),
                BindingId::new(),
                "owner.internal:27421",
            )?,
        );
        let key = PeerStreamKey::for_test(owner_gateway, peer_transport_id, raw_stream_id);
        state.peer_open_committed(identity, key);
        state.peer_opened(key, identity);
        identities.push(identity);
        keys.push(key);
    }
    assert_eq!(state.pipe_count(), 2);

    let cleanup = state.remove_session(first_connector);
    assert!(peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == keys[0]
    )));
    assert!(!peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset { key, .. } if *key == keys[1]
    )));
    assert_eq!(publications(&cleanup).count(), 0);
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.active_peer_open_count(), 1);

    let surviving_pipe = PipeId::new(second_connector, 1);
    let relay = state.handle(
        second_connector,
        Frame::Data {
            pipe_id: surviving_pipe,
            payload: Bytes::from_static(b"still-live"),
        },
    )?;
    assert!(peer_deliveries(&relay).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { key, payload }
            if *key == keys[1] && payload.as_ref() == b"still-live"
    )));

    let stale_loss =
        state.peer_transport_lost_stream(keys[0], identities[0], PeerObservation::MaybeObserved);
    assert!(stale_loss.is_empty());
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.active_peer_open_count(), 1);

    let lost =
        state.peer_transport_lost_stream(keys[1], identities[1], PeerObservation::MaybeObserved);
    assert!(sdk_deliveries(&lost).any(|delivery| delivery.target == second_connector));
    assert_eq!(publications(&lost).count(), 0);
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.active_peer_open_count(), 0);
    Ok(())
}

#[test]
fn owner_peer_reset_cancels_an_offered_pipe_without_bouncing_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let identity = OpenIdentity::new(entry_gateway, SessionId::new(), 1);
    let key = peer_key(entry_gateway, 0);
    state.receive_peer_open(
        key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );

    let actions = state.peer_reset(key, ErrorCode::Cancelled, "entry cancelled".to_owned());

    assert!(sdk_deliveries(&actions).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::Cancelled,
            ..
        }
    )));
    assert_eq!(peer_deliveries(&actions).count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn remote_pipe_data_fin_close_and_reset_are_symmetric() -> Result<(), Box<dyn std::error::Error>> {
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);
    state.peer_open_committed(identity, key);
    state.peer_opened(key, identity);
    let pipe_id = PipeId::new(connector, 1);

    let sdk_data = state.handle(
        connector,
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"sdk-to-peer"),
        },
    )?;
    assert!(peer_deliveries(&sdk_data).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { payload, .. } if payload.as_ref() == b"sdk-to-peer"
    )));
    let peer_data = state.peer_data(key, Bytes::from_static(b"peer-to-sdk"));
    assert!(sdk_deliveries(&peer_data).any(|delivery| matches!(
        &delivery.frame,
        Frame::Data { payload, .. } if payload.as_ref() == b"peer-to-sdk"
    )));

    let sdk_fin = state.handle(connector, Frame::Fin { pipe_id })?;
    assert!(peer_deliveries(&sdk_fin).any(|delivery| matches!(delivery, PeerDelivery::Fin { .. })));
    let peer_data_after_sdk_fin =
        state.peer_data(key, Bytes::from_static(b"peer direction survives sdk FIN"));
    assert_eq!(peer_data_after_sdk_fin.len(), 1);
    assert!(sdk_deliveries(&peer_data_after_sdk_fin).any(|delivery| matches!(
        &delivery.frame,
        Frame::Data { payload, .. } if payload.as_ref() == b"peer direction survives sdk FIN"
    )));
    let peer_fin = state.peer_fin(key);
    assert_eq!(peer_fin.len(), 1);
    assert!(sdk_deliveries(&peer_fin).any(|delivery| matches!(delivery.frame, Frame::Fin { .. })));
    assert_eq!(state.pipe_count(), 0);

    let second_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let second_identity = resolve_identity(&second_resolve).ok_or("missing second resolve")?;
    state.route_resolved(
        second_identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let second_key = peer_key(owner_gateway, 2);
    state.peer_open_committed(second_identity, second_key);
    state.peer_opened(second_key, second_identity);
    let close = state.peer_close(second_key);
    assert!(sdk_deliveries(&close).any(|delivery| matches!(delivery.frame, Frame::Close { .. })));

    let third_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 3,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let third_identity = resolve_identity(&third_resolve).ok_or("missing third resolve")?;
    state.route_resolved(
        third_identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let third_key = peer_key(owner_gateway, 4);
    state.peer_open_committed(third_identity, third_key);
    state.peer_opened(third_key, third_identity);
    let reset = state.peer_reset(third_key, ErrorCode::Unavailable, "peer failed".to_owned());
    assert!(sdk_deliveries(&reset).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::Unavailable,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);

    let fourth_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 4,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let fourth_identity = resolve_identity(&fourth_resolve).ok_or("missing fourth resolve")?;
    state.route_resolved(
        fourth_identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let fourth_key = peer_key(owner_gateway, 6);
    state.peer_open_committed(fourth_identity, fourth_key);
    state.peer_opened(fourth_key, fourth_identity);
    let fourth_pipe_id = PipeId::new(connector, 4);

    let peer_fin = state.peer_fin(fourth_key);
    assert_eq!(peer_fin.len(), 1);
    assert!(sdk_deliveries(&peer_fin).any(|delivery| matches!(
        delivery.frame,
        Frame::Fin { pipe_id } if pipe_id == fourth_pipe_id
    )));
    let sdk_data_after_peer_fin = state.handle(
        connector,
        Frame::Data {
            pipe_id: fourth_pipe_id,
            payload: Bytes::from_static(b"sdk direction survives peer FIN"),
        },
    )?;
    assert_eq!(sdk_data_after_peer_fin.len(), 1);
    assert!(peer_deliveries(&sdk_data_after_peer_fin).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { key, payload }
            if *key == fourth_key && payload.as_ref() == b"sdk direction survives peer FIN"
    )));
    let data_after_peer_fin =
        state.peer_data(fourth_key, Bytes::from_static(b"invalid after peer FIN"));
    assert_eq!(data_after_peer_fin.len(), 2);
    assert!(sdk_deliveries(&data_after_peer_fin).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            pipe_id,
            code: ErrorCode::ProtocolError,
            ..
        } if pipe_id == fourth_pipe_id
    )));
    assert!(peer_deliveries(&data_after_peer_fin).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key,
            code: ErrorCode::ProtocolError,
            ..
        } if *key == fourth_key
    )));
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}
