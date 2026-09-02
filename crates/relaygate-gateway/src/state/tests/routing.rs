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
fn concurrent_remote_opens_consume_only_their_request_local_resolve()
-> Result<(), Box<dyn std::error::Error>> {
    type Candidates = [(SessionId, BindingId); 2];
    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    let client_id = "echo.shared";
    let entry_gateway = GatewayId::new();
    let owner_a = GatewayId::new();
    let owner_b = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);

    let resolve_a = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: client_id.to_owned(),
        },
    )?;
    let resolve_b = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: client_id.to_owned(),
        },
    )?;
    let identity_a = resolve_identity(&resolve_a).ok_or("missing first resolve")?;
    let identity_b = resolve_identity(&resolve_b).ok_or("missing second resolve")?;
    assert_ne!(identity_a, identity_b);
    assert_eq!(state.remote_open_attempt_count(), 2);

    let make_bindings = |owner: GatewayId, locator: &str| -> TestResult<(BindingSet, Candidates)> {
        let candidates = [
            (SessionId::new(), BindingId::new()),
            (SessionId::new(), BindingId::new()),
        ];
        let entries = candidates
            .iter()
            .map(|(listener_session_id, binding_id)| {
                Ok(MappingEntry::new(
                    RouteClientId::new(client_id)?,
                    owner,
                    ListenerSessionId::from_uuid(listener_session_id.as_uuid()),
                    RouteBindingId::from_uuid(binding_id.as_uuid()),
                    GatewayLocator::new(locator)?,
                ))
            })
            .collect::<TestResult<Vec<_>>>()?;
        Ok((BindingSet::from_entries(entries)?, candidates))
    };
    let (bindings_a, a_candidates) = make_bindings(owner_a, "owner-a.internal:27421")?;
    let (bindings_b, b_candidates) = make_bindings(owner_b, "owner-b.internal:27421")?;

    let assert_one_peer = |actions: &[GatewayAction],
                           identity: OpenIdentity,
                           owner: GatewayId,
                           candidates: &Candidates|
     -> TestResult<()> {
        let [GatewayAction::OpenPeer {
            open_identity,
            gateway_id,
            listener_session_id,
            binding_id,
            ..
        }] = actions
        else {
            return Err("Resolve must select exactly one peer binding".into());
        };
        assert_eq!(*open_identity, identity);
        assert_eq!(*gateway_id, owner);
        assert!(candidates.contains(&(*listener_session_id, *binding_id)));
        Ok(())
    };

    let open_b = state.route_resolved(identity_b, bindings_b.clone());
    assert_one_peer(&open_b, identity_b, owner_b, &b_candidates)?;

    let open_a = state.route_resolved(identity_a, bindings_a.clone());
    assert_one_peer(&open_a, identity_a, owner_a, &a_candidates)?;

    assert!(state.route_resolved(identity_b, bindings_b).is_empty());
    assert!(state.route_resolved(identity_a, bindings_a).is_empty());

    let resolve_c = state.handle(
        connector,
        Frame::Open {
            connection_id: 3,
            client_id: client_id.to_owned(),
        },
    )?;
    let [GatewayAction::ResolveRoute {
        open_identity: identity_c,
        client_id: resolved_client_id,
    }] = resolve_c.as_slice()
    else {
        return Err("a new OPEN must issue exactly one request-local Resolve".into());
    };
    let identity_c = *identity_c;
    assert_eq!(resolved_client_id.as_str(), client_id);
    assert_eq!(state.remote_open_attempt_count(), 3);

    for identity in [identity_a, identity_b] {
        state.peer_open_commit_failed(
            identity,
            ErrorCode::Unavailable,
            PeerObservation::NotObserved,
            "test cleanup",
        );
    }
    state.route_failed(identity_c, ErrorCode::Unavailable, "test cleanup");
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn terminated_remote_open_does_not_cache_successful_resolve_for_next_open()
-> Result<(), Box<dyn std::error::Error>> {
    let client_id = "echo.shared";
    let mut state = routed_state(GatewayId::new());
    let connector = add_session(&mut state, SessionRole::Connector);

    let first_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: client_id.to_owned(),
        },
    )?;
    let first_identity = resolve_identity(&first_resolve).ok_or("missing first resolve")?;
    let first_peer = state.route_resolved(
        first_identity,
        binding_set(
            client_id,
            GatewayId::new(),
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    assert!(matches!(
        first_peer.as_slice(),
        [GatewayAction::OpenPeer { .. }]
    ));

    let failed = state.peer_open_commit_failed(
        first_identity,
        ErrorCode::Unavailable,
        PeerObservation::NotObserved,
        "peer unavailable",
    );
    assert!(sdk_deliveries(&failed).any(|delivery| {
        delivery.target == connector
            && matches!(
                delivery.frame,
                Frame::OpenFailed {
                    connection_id: 1,
                    code: ErrorCode::Unavailable,
                    observation: PeerObservation::NotObserved,
                    ..
                }
            )
    }));
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);

    let second_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: client_id.to_owned(),
        },
    )?;
    let [GatewayAction::ResolveRoute {
        open_identity: second_identity,
        client_id: resolved_client_id,
    }] = second_resolve.as_slice()
    else {
        return Err("new OPEN must issue one fresh Resolve".into());
    };
    assert_ne!(*second_identity, first_identity);
    assert_eq!(resolved_client_id.as_str(), client_id);
    assert_eq!(state.remote_open_attempt_count(), 1);

    state.route_failed(
        *second_identity,
        ErrorCode::Unavailable,
        "test cleanup",
    );
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
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
