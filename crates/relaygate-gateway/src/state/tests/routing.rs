#[test]
fn routed_local_hit_never_resolves_or_opens_a_peer() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;

    assert!(actions.iter().all(|action| {
        !matches!(
            action,
            GatewayAction::ResolveRoute { .. } | GatewayAction::OpenPeer { .. }
        )
    }));
    assert!(sdk_deliveries(&actions).any(|delivery| {
        delivery.target == listener && matches!(delivery.frame, Frame::Offer { .. })
    }));
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn routed_miss_creates_one_request_local_resolve() -> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);

    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 7,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&actions).ok_or("missing ResolveRoute action")?;

    assert_eq!(identity.entry_gateway(), entry_gateway);
    assert_eq!(identity.connector_session(), connector);
    assert_eq!(identity.connection_id(), 7);
    assert_eq!(state.remote_open_attempt_count(), 1);
    assert!(
        state
            .handle(
                connector,
                Frame::Open {
                    connection_id: 7,
                    client_id: "echo.shared".to_owned(),
                },
            )?
            .is_empty()
    );
    assert_eq!(state.remote_open_attempt_count(), 1);
    Ok(())
}

#[test]
fn late_resolve_cannot_resurrect_a_closed_connector_session()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&actions).ok_or("missing ResolveRoute action")?;
    state.remove_session(connector);

    let late = state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            GatewayId::new(),
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );

    assert!(late.is_empty());
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn selected_stale_local_mapping_fails_without_fallback() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_id = GatewayId::new();
    let mut state = routed_state(gateway_id);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;

    let actions = state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            gateway_id,
            SessionId::new(),
            BindingId::new(),
            "self.internal:27421",
        )?,
    );

    assert!(sdk_deliveries(&actions).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::Unavailable,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    assert!(
        actions
            .iter()
            .all(|action| !matches!(action, GatewayAction::OpenPeer { .. }))
    );
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn owner_rejects_only_active_duplicate_open_identity() -> Result<(), Box<dyn std::error::Error>> {
    let owner_gateway = GatewayId::new();
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(owner_gateway);
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let identity = OpenIdentity::new(entry_gateway, SessionId::new(), 1);
    let first_key = peer_key(entry_gateway, 0);
    let duplicate_key = peer_key(entry_gateway, 2);

    let first = state.receive_peer_open(
        first_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(sdk_deliveries(&first).any(|delivery| matches!(delivery.frame, Frame::Offer { .. })));
    assert_eq!(state.remote_open_attempt_count(), 0);

    let duplicate = state.receive_peer_open(
        duplicate_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(peer_deliveries(&duplicate).any(|delivery| matches!(
        delivery,
        PeerDelivery::Failed {
            code: ErrorCode::AlreadyExists,
            ..
        }
    )));
    let pipe_id = PipeId::new(identity.connector_session(), identity.connection_id());
    state.handle(
        listener,
        Frame::OfferRejected {
            pipe_id,
            code: ErrorCode::Unavailable,
            message: "not ready".to_owned(),
        },
    )?;
    assert_eq!(state.pipe_count(), 0);

    let reused_after_terminal = state.receive_peer_open(
        duplicate_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(
        sdk_deliveries(&reused_after_terminal)
            .any(|delivery| matches!(delivery.frame, Frame::Offer { .. }))
    );
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}
