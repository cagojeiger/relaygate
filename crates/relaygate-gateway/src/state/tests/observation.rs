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
    let (output, dispatch) = captured_dispatch();

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

    let logs = String::from_utf8(captured_bytes(&output))?;

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

#[test]
fn pipe_opened_event_reports_full_identity_without_secrets_or_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let deliveries = state.handle(
        connector,
        Frame::Open {
            connection_id: 42,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let (pipe_id, binding_id) = sdk_deliveries(&deliveries)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer {
                pipe_id,
                binding_id,
                ..
            } if delivery.target == listener => Some((pipe_id, binding_id)),
            _ => None,
        })
        .ok_or("missing offer")?;
    let (output, dispatch) = captured_dispatch();

    tracing::dispatcher::with_default(&dispatch, || -> Result<_, Box<dyn std::error::Error>> {
        state.handle(listener, Frame::OfferAccepted { pipe_id })?;
        Ok(())
    })?;

    let logs = String::from_utf8(captured_bytes(&output))?;
    let records = logs
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let matching = records
        .iter()
        .filter(|record| record["fields"]["event"] == "gateway.pipe.opened")
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "{logs}");
    let fields = &matching[0]["fields"];

    assert_eq!(fields["component"], "gateway", "{fields}");
    assert_eq!(fields["event"], "gateway.pipe.opened", "{fields}");
    assert_eq!(
        fields["connector_session_id"],
        connector.as_uuid().to_string(),
        "{fields}"
    );
    assert_eq!(
        fields["listener_session_id"],
        listener.as_uuid().to_string(),
        "{fields}"
    );
    assert_eq!(fields["connection_id"], 42, "{fields}");
    assert_eq!(
        fields["binding_id"],
        binding_id.as_uuid().to_string(),
        "{fields}"
    );
    for forbidden in [
        "client_key",
        "internal_gateway_key",
        "payload",
        "application_data",
        "delivery_acknowledgement",
    ] {
        assert!(fields.get(forbidden).is_none(), "{fields}");
    }
    assert!(!logs.contains("secret"), "{logs}");
    Ok(())
}

#[test]
fn successful_data_relay_emits_no_per_frame_event_or_payload()
-> Result<(), Box<dyn std::error::Error>> {
    const PAYLOAD: &[u8] = b"data-hot-path-payload-sentinel";
    const FRAME_COUNT: usize = 32;

    let mut state = state();
    let (listener_sender, mut listener_frames) = mpsc::channel(1);
    let listener = state
        .add_session(
            SessionRole::Listener,
            listener_sender,
            CancellationToken::new(),
        )
        .ok_or("missing ListenerSession")?;
    let (connector_sender, mut connector_frames) = mpsc::channel(1);
    let connector = state
        .add_session(
            SessionRole::Connector,
            connector_sender,
            CancellationToken::new(),
        )
        .ok_or("missing ConnectorSession")?;
    register_listener(&mut state, listener)?;
    let pipe_id = open_pipe(&mut state, connector, listener, 1)?;
    let (output, dispatch) = captured_dispatch();

    tracing::dispatcher::with_default(
        &dispatch,
        || -> Result<_, Box<dyn std::error::Error>> {
            for _ in 0..FRAME_COUNT {
                for (sender, target) in [(connector, listener), (listener, connector)] {
                    let mut actions = state.handle(
                        sender,
                        Frame::Data {
                            pipe_id,
                            payload: Bytes::from_static(PAYLOAD),
                        },
                    )?;
                    assert_eq!(actions.len(), 1);
                    let Some(GatewayAction::SendSdkFrame(delivery)) = actions.pop() else {
                        return Err("DATA was not relayed to an SDK session".into());
                    };
                    assert_eq!(delivery.target, target);
                    assert_eq!(delivery.deliver(), None);

                    let received = if target == listener {
                        listener_frames.try_recv()?
                    } else {
                        connector_frames.try_recv()?
                    };
                    assert!(matches!(
                        received,
                        Frame::Data {
                            pipe_id: relayed_pipe_id,
                            payload,
                        } if relayed_pipe_id == pipe_id && payload.as_ref() == PAYLOAD
                    ));
                }
            }
            Ok(())
        },
    )?;

    let logs = String::from_utf8(captured_bytes(&output))?;
    assert!(logs.is_empty(), "{logs}");
    assert!(!logs.contains("data-hot-path-payload-sentinel"));
    Ok(())
}
