#[test]
fn invalid_key_creates_no_binding() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);

    let deliveries = state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("invalid"),
        },
    )?;

    assert_eq!(state.registry.binding_count(), 0);
    assert_eq!(publications(&deliveries).count(), 0);
    assert!(matches!(
        first_sdk_delivery(&deliveries).map(|delivery| &delivery.frame),
        Some(Frame::RegisterFailed {
            code: ErrorCode::Unauthenticated,
            ..
        })
    ));
    Ok(())
}

#[test]
fn created_registration_publishes_one_complete_session_snapshot_after_response()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits::default());
    let listener = add_session(&mut state, SessionRole::Listener);

    let first = state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    assert!(matches!(
        first.first(),
        Some(GatewayAction::SendSdkFrame(_))
    ));
    let first_publications = publications(&first).collect::<Vec<_>>();
    assert_eq!(first_publications.len(), 1);
    assert_eq!(first_publications[0].0, listener);
    assert_eq!(first_publications[0].1.len(), 1);
    assert_eq!(first_publications[0].1[0].client_id, "echo.shared");

    let second = state.handle(
        listener,
        Frame::Register {
            request_id: 2,
            client_id: "echo.other".to_owned(),
            client_key: ClientKey::new("other-secret"),
        },
    )?;
    assert!(matches!(
        second.first(),
        Some(GatewayAction::SendSdkFrame(_))
    ));
    let second_publications = publications(&second).collect::<Vec<_>>();
    assert_eq!(second_publications.len(), 1);
    assert_eq!(second_publications[0].0, listener);
    assert_eq!(second_publications[0].1.len(), 2);
    assert!(
        second_publications[0]
            .1
            .iter()
            .any(|binding| binding.client_id == "echo.shared")
    );
    assert!(
        second_publications[0]
            .1
            .iter()
            .any(|binding| binding.client_id == "echo.other")
    );
    Ok(())
}

#[test]
fn idempotent_registration_does_not_republish_unchanged_state()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;

    let repeated = state.handle(
        listener,
        Frame::Register {
            request_id: 2,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;

    assert_eq!(sdk_deliveries(&repeated).count(), 1);
    assert_eq!(publications(&repeated).count(), 0);
    Ok(())
}

#[test]
fn unregister_publishes_remaining_then_empty_complete_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = limited_state(GatewayLimits::default());
    let listener = add_session(&mut state, SessionRole::Listener);
    let first = state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    let first_id = sdk_deliveries(&first)
        .find_map(|delivery| match delivery.frame {
            Frame::Registered { binding_id, .. } => Some(binding_id),
            _ => None,
        })
        .ok_or("missing first binding")?;
    let second = state.handle(
        listener,
        Frame::Register {
            request_id: 2,
            client_id: "echo.other".to_owned(),
            client_key: ClientKey::new("other-secret"),
        },
    )?;
    let second_id = sdk_deliveries(&second)
        .find_map(|delivery| match delivery.frame {
            Frame::Registered { binding_id, .. } => Some(binding_id),
            _ => None,
        })
        .ok_or("missing second binding")?;

    let remaining = state.handle(
        listener,
        Frame::Unregister {
            request_id: 3,
            binding_id: first_id,
        },
    )?;
    assert!(matches!(
        remaining.first(),
        Some(GatewayAction::SendSdkFrame(_))
    ));
    let remaining_publication = publications(&remaining)
        .next()
        .ok_or("missing publication")?;
    assert_eq!(remaining_publication.1.len(), 1);
    assert_eq!(remaining_publication.1[0].id, second_id);

    let empty = state.handle(
        listener,
        Frame::Unregister {
            request_id: 4,
            binding_id: second_id,
        },
    )?;
    assert!(matches!(
        empty.first(),
        Some(GatewayAction::SendSdkFrame(_))
    ));
    let empty_publication = publications(&empty).next().ok_or("missing publication")?;
    assert!(empty_publication.1.is_empty());
    Ok(())
}

#[test]
fn only_listener_session_cleanup_publishes_an_empty_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let listener_cleanup = state.remove_session(listener);
    let publication = publications(&listener_cleanup)
        .next()
        .ok_or("missing listener cleanup publication")?;
    assert_eq!(publication.0, listener);
    assert!(publication.1.is_empty());
    assert_eq!(publications(&listener_cleanup).count(), 1);

    let connector_cleanup = state.remove_session(connector);
    assert_eq!(publications(&connector_cleanup).count(), 0);
    Ok(())
}

#[test]
fn stale_binding_cleanup_emits_failure_before_the_current_empty_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    state.sessions.remove(&listener).ok_or("missing listener")?;

    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;

    assert!(matches!(
        actions.first(),
        Some(GatewayAction::SendSdkFrame(_))
    ));
    assert!(matches!(
        first_sdk_delivery(&actions).map(|delivery| &delivery.frame),
        Some(Frame::OpenFailed {
            code: ErrorCode::Unavailable,
            ..
        })
    ));
    let publication = publications(&actions).next().ok_or("missing publication")?;
    assert_eq!(publication.0, listener);
    assert!(publication.1.is_empty());
    assert_eq!(state.registry.binding_count(), 0);
    Ok(())
}

#[test]
fn n_to_m_registry_offers_each_open_to_only_one_listener() -> Result<(), Box<dyn std::error::Error>>
{
    let mut state = state();
    let first_listener = add_session(&mut state, SessionRole::Listener);
    let second_listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    for listener in [first_listener, second_listener] {
        state.handle(
            listener,
            Frame::Register {
                request_id: 1,
                client_id: "echo.shared".to_owned(),
                client_key: ClientKey::new("secret"),
            },
        )?;
    }

    let first = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let second = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    let first_delivery = first_sdk_delivery(&first).ok_or("missing first offer")?;
    let second_delivery = first_sdk_delivery(&second).ok_or("missing second offer")?;
    assert_ne!(first_delivery.target, second_delivery.target);
    assert!(matches!(first_delivery.frame, Frame::Offer { .. }));
    assert!(matches!(second_delivery.frame, Frame::Offer { .. }));
    assert_eq!(state.pipe_count(), 2);
    Ok(())
}

#[test]
fn same_connection_id_is_isolated_across_connector_sessions()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let first_connector = add_session(&mut state, SessionRole::Connector);
    let second_connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let first = offer_pipe(&mut state, first_connector, listener, 1)?;
    let second = offer_pipe(&mut state, second_connector, listener, 1)?;

    assert_ne!(first, second);
    assert_eq!(first.connector_session_id(), first_connector);
    assert_eq!(second.connector_session_id(), second_connector);
    assert_eq!(first.connection_id(), 1);
    assert_eq!(second.connection_id(), 1);
    assert_eq!(state.pipe_count(), 2);
    Ok(())
}

#[test]
fn disconnect_removes_only_owned_state_and_terminates_pending_open()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let first_listener = add_session(&mut state, SessionRole::Listener);
    let second_listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    for listener in [first_listener, second_listener] {
        state.handle(
            listener,
            Frame::Register {
                request_id: 1,
                client_id: "echo.shared".to_owned(),
                client_key: ClientKey::new("secret"),
            },
        )?;
    }
    state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;

    let failures = state.remove_session(first_listener);

    assert_eq!(state.registry.binding_count(), 1);
    assert_eq!(state.pipe_count(), 0);
    assert!(sdk_deliveries(&failures).any(|delivery| {
        delivery.target == connector
            && matches!(
                delivery.frame,
                Frame::OpenFailed {
                    connection_id: 1,
                    code: ErrorCode::Unavailable,
                    observation: relaygate_protocol::PeerObservation::MaybeObserved,
                    ..
                }
            )
    }));
    Ok(())
}

#[test]
fn observability_snapshot_reports_current_live_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let pipe_id = offer_pipe(&mut state, connector, listener, 1)?;

    let offered = state.snapshot();

    assert_eq!(offered.sessions, 2);
    assert_eq!(offered.listener_sessions, 1);
    assert_eq!(offered.connector_sessions, 1);
    assert_eq!(offered.listener_bindings, 1);
    assert_eq!(offered.pending_offers, 1);
    assert_eq!(offered.live_pipes, 0);

    state.handle(listener, Frame::OfferAccepted { pipe_id })?;
    let opened = state.snapshot();

    assert_eq!(opened.pending_offers, 0);
    assert_eq!(opened.live_pipes, 1);

    state.remove_session(listener);
    let cleaned = state.snapshot();

    assert_eq!(cleaned.sessions, 1);
    assert_eq!(cleaned.listener_sessions, 0);
    assert_eq!(cleaned.connector_sessions, 1);
    assert_eq!(cleaned.listener_bindings, 0);
    assert_eq!(cleaned.pending_offers, 0);
    assert_eq!(cleaned.live_pipes, 0);
    Ok(())
}

#[test]
fn structured_events_use_stable_ids_without_frame_secrets_or_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer_output = Arc::clone(&output);
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_target(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || CapturedWriter {
            output: Arc::clone(&writer_output),
        })
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);

    let session_id = tracing::dispatcher::with_default(
        &dispatch,
        || -> Result<_, Box<dyn std::error::Error>> {
            let (sender, _receiver) = mpsc::channel(1);
            sender.try_send(Frame::Ping { nonce: 1 })?;
            let cancellation = CancellationToken::new();
            let session_id = relaygate_protocol::SessionId::new();
            let rejected_registration = Delivery {
                target: session_id,
                frame: Frame::Register {
                    request_id: 7,
                    client_id: "echo.shared".to_owned(),
                    client_key: ClientKey::new("top-secret-client-key"),
                },
                sender: sender.clone(),
                cancellation: cancellation.clone(),
            };
            let rejected_data = Delivery {
                target: session_id,
                frame: Frame::Data {
                    pipe_id: PipeId::new(session_id, 42),
                    payload: Bytes::from_static(b"payload-sentinel"),
                },
                sender,
                cancellation: cancellation.clone(),
            };

            assert_eq!(rejected_registration.deliver(), Some(session_id));
            assert_eq!(rejected_data.deliver(), Some(session_id));
            assert!(cancellation.is_cancelled());
            Ok(session_id)
        },
    )?;

    let bytes = match output.lock() {
        Ok(output) => output.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let logs = String::from_utf8(bytes)?;

    assert!(logs.contains(r#""component":"gateway""#), "{logs}");
    assert_eq!(
        logs.matches(r#""event":"gateway.session.writer_queue_rejected""#)
            .count(),
        2,
        "{logs}"
    );
    assert!(logs.contains(&format!(r#""session_id":"{}""#, session_id.as_uuid())));
    assert!(logs.contains(r#""queue_state":"full""#));
    assert!(logs.contains(r#""error_code":"ResourceExhausted""#));
    assert!(!logs.contains("top-secret-client-key"));
    assert!(!logs.contains("payload-sentinel"));
    Ok(())
}
