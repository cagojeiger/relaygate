#[test]
fn session_and_binding_limits_preserve_existing_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits {
        max_sessions: 1,
        max_bindings: 1,
        ..GatewayLimits::default()
    });
    let listener = add_session(&mut state, SessionRole::Listener);
    let (sender, _receiver) = mpsc::channel(8);
    assert!(
        state
            .add_session(SessionRole::Connector, sender, CancellationToken::new())
            .is_none()
    );

    let registered = state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    let binding_id = sdk_deliveries(&registered)
        .find_map(|delivery| match delivery.frame {
            Frame::Registered { binding_id, .. } => Some(binding_id),
            _ => None,
        })
        .ok_or("missing binding")?;
    let repeated = state.handle(
        listener,
        Frame::Register {
            request_id: 2,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&repeated).map(|delivery| &delivery.frame),
        Some(Frame::Registered {
            binding_id: repeated_id,
            ..
        }) if *repeated_id == binding_id
    ));

    let exhausted = state.handle(
        listener,
        Frame::Register {
            request_id: 3,
            client_id: "echo.other".to_owned(),
            client_key: ClientKey::new("other-secret"),
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&exhausted).map(|delivery| &delivery.frame),
        Some(Frame::RegisterFailed {
            code: ErrorCode::ResourceExhausted,
            ..
        })
    ));
    assert_eq!(state.registry.binding_count(), 1);
    Ok(())
}

#[test]
fn pending_offer_limit_rejects_without_observation_or_state_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits {
        max_pending_offers: 1,
        ..GatewayLimits::default()
    });
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let first = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&first).map(|delivery| &delivery.frame),
        Some(Frame::Offer { .. })
    ));
    let second = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&second).map(|delivery| &delivery.frame),
        Some(Frame::OpenFailed {
            code: ErrorCode::ResourceExhausted,
            observation: relaygate_protocol::PeerObservation::NotObserved,
            ..
        })
    ));
    assert_eq!(state.pending_offer_count(), 1);
    assert_eq!(state.live_pipe_count(), 0);
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn live_pipe_limit_before_opened_reports_maybe_observed_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits {
        max_live_pipes: 1,
        ..GatewayLimits::default()
    });
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let first_offer = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let second_offer = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let first_pipe = sdk_deliveries(&first_offer)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing first offer")?;
    let second_pipe = sdk_deliveries(&second_offer)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing second offer")?;

    let opened = state.handle(
        listener,
        Frame::OfferAccepted {
            pipe_id: first_pipe,
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&opened).map(|delivery| &delivery.frame),
        Some(Frame::Opened { .. })
    ));
    let exhausted = state.handle(
        listener,
        Frame::OfferAccepted {
            pipe_id: second_pipe,
        },
    )?;
    assert!(sdk_deliveries(&exhausted).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::ResourceExhausted,
            observation: relaygate_protocol::PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert!(sdk_deliveries(&exhausted).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::ResourceExhausted,
            ..
        }
    )));
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.live_pipe_count(), 1);
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn offer_deadline_closes_selected_listener_session_and_preserves_sibling()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits {
        offer_timeout: Duration::from_millis(10),
        ..GatewayLimits::default()
    });
    let listener_cancellation = CancellationToken::new();
    let listener = add_session_with_cancellation(
        &mut state,
        SessionRole::Listener,
        listener_cancellation.clone(),
    );
    let sibling = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    register_listener(&mut state, sibling)?;
    state.handle(
        listener,
        Frame::Register {
            request_id: 2,
            client_id: "echo.other".to_owned(),
            client_key: ClientKey::new("other-secret"),
        },
    )?;

    let live_offer = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.other".to_owned(),
        },
    )?;
    let live_pipe = sdk_deliveries(&live_offer)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } if delivery.target == listener => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing live offer")?;
    state.handle(listener, Frame::OfferAccepted { pipe_id: live_pipe })?;
    let offered_at = Instant::now();
    let offered = state.handle_at(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
        offered_at,
    )?;
    let expired_pipe = sdk_deliveries(&offered)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing offer")?;

    let not_expired = state.expire_offers(offered_at + Duration::from_millis(9));
    assert!(not_expired.is_empty());
    assert_eq!(state.pending_offer_count(), 1);

    let expired = state.expire_offers(offered_at + Duration::from_millis(10));
    assert!(sdk_deliveries(&expired).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            connection_id: 2,
            code: ErrorCode::DeadlineExceeded,
            observation: relaygate_protocol::PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert!(sdk_deliveries(&expired).any(|delivery| {
        delivery.target == connector
            && matches!(
                delivery.frame,
                Frame::Reset {
                    pipe_id,
                    code: ErrorCode::Unavailable,
                    ..
                } if pipe_id == live_pipe
            )
    }));
    assert!(listener_cancellation.is_cancelled());
    assert!(!state.sessions.contains_key(&listener));
    assert!(state.sessions.contains_key(&sibling));
    assert_eq!(state.registry.binding_count(), 1);
    assert_eq!(state.registry.session_binding_count(sibling), 1);
    assert_eq!(state.pending_offer_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    assert!(
        state
            .handle(
                listener,
                Frame::OfferAccepted {
                    pipe_id: expired_pipe,
                },
            )?
            .is_empty()
    );

    let next = state.handle(
        connector,
        Frame::Open {
            connection_id: 3,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    assert!(sdk_deliveries(&next).any(|delivery| {
        delivery.target == sibling && matches!(delivery.frame, Frame::Offer { .. })
    }));
    Ok(())
}
