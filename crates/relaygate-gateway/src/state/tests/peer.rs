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
    let peer_fin = state.peer_fin(key);
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
    Ok(())
}
