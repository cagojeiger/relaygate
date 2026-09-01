#[test]
fn late_cancel_is_idempotent_and_cannot_create_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let connector = add_session(&mut state, SessionRole::Connector);
    let pipe_id = PipeId::new(connector, 7);

    assert!(
        state
            .handle(connector, Frame::Cancel { pipe_id })?
            .is_empty()
    );
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}
#[test]
fn foreign_fin_on_offered_pipe_terminates_only_offender_and_preserves_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    let offender_cancellation = CancellationToken::new();
    let offender = add_session_with_cancellation(
        &mut state,
        SessionRole::Connector,
        offender_cancellation.clone(),
    );
    register_listener(&mut state, listener)?;
    let pipe_id = offer_pipe(&mut state, connector, listener, 1)?;

    let violation = match state.handle(offender, Frame::Fin { pipe_id }) {
        Err(violation) => violation,
        Ok(_) => return Err("a current Pipe frame from a non-owner must fail the session".into()),
    };

    assert!(matches!(
        violation,
        ProtocolViolation::PipeOwnership {
            sender,
            pipe_id: violated_pipe,
            frame_name: "FIN",
        } if sender == offender && violated_pipe == pipe_id
    ));
    assert_eq!(state.pending_offer_count(), 1);
    assert_eq!(state.live_pipe_count(), 0);
    assert_eq!(state.pipe_count(), 1);

    // `run_session` performs this cleanup immediately after propagating the violation.
    assert!(state.remove_session(offender).is_empty());
    assert!(offender_cancellation.is_cancelled());
    assert!(!state.sessions.contains_key(&offender));
    assert!(state.sessions.contains_key(&connector));
    assert!(state.sessions.contains_key(&listener));
    assert_eq!(state.pending_offer_count(), 1);

    let opened = state.handle(listener, Frame::OfferAccepted { pipe_id })?;
    assert!(matches!(
        first_sdk_delivery(&opened).map(|delivery| (&delivery.target, &delivery.frame)),
        Some((target, Frame::Opened { pipe_id: opened_pipe }))
            if *target == connector && *opened_pipe == pipe_id
    ));
    assert_eq!(state.live_pipe_count(), 1);
    Ok(())
}

#[test]
fn foreign_open_pipe_frames_terminate_each_offender_without_mutating_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let pipe_id = open_pipe(&mut state, connector, listener, 1)?;

    let frames = [
        (
            "DATA",
            Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"foreign"),
            },
        ),
        ("FIN", Frame::Fin { pipe_id }),
        ("CLOSE", Frame::Close { pipe_id }),
        (
            "RESET",
            Frame::Reset {
                pipe_id,
                code: ErrorCode::Cancelled,
                message: "foreign".to_owned(),
            },
        ),
    ];

    for (frame_name, frame) in frames {
        let offender_cancellation = CancellationToken::new();
        let offender = add_session_with_cancellation(
            &mut state,
            SessionRole::Connector,
            offender_cancellation.clone(),
        );
        let violation = match state.handle(offender, frame) {
            Err(violation) => violation,
            Ok(_) => {
                return Err("a current Pipe frame from a non-owner must fail the session".into());
            }
        };
        assert!(matches!(
            violation,
            ProtocolViolation::PipeOwnership {
                sender,
                pipe_id: violated_pipe,
                frame_name: actual_frame,
            } if sender == offender && violated_pipe == pipe_id && actual_frame == frame_name
        ));
        assert_eq!(state.pipe_count(), 1);
        assert_eq!(state.live_pipe_count(), 1);

        assert!(state.remove_session(offender).is_empty());
        assert!(offender_cancellation.is_cancelled());
        assert!(!state.sessions.contains_key(&offender));
        assert!(state.sessions.contains_key(&connector));
        assert!(state.sessions.contains_key(&listener));
        assert_eq!(state.pipe_count(), 1);
    }

    assert_eq!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id,
                    payload: Bytes::from_static(b"still-open"),
                },
            )?
            .len(),
        1
    );
    assert_eq!(state.handle(connector, Frame::Fin { pipe_id })?.len(), 1);
    assert_eq!(state.handle(listener, Frame::Fin { pipe_id })?.len(), 1);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn foreign_offer_responses_and_cancel_preserve_the_pending_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let pipe_id = offer_pipe(&mut state, connector, listener, 1)?;
    let cases = [
        (
            SessionRole::Listener,
            "OFFER_ACCEPTED",
            Frame::OfferAccepted { pipe_id },
        ),
        (
            SessionRole::Listener,
            "OFFER_REJECTED",
            Frame::OfferRejected {
                pipe_id,
                code: ErrorCode::Cancelled,
                message: "foreign".to_owned(),
            },
        ),
        (SessionRole::Connector, "CANCEL", Frame::Cancel { pipe_id }),
    ];

    for (role, frame_name, frame) in cases {
        let offender_cancellation = CancellationToken::new();
        let offender =
            add_session_with_cancellation(&mut state, role, offender_cancellation.clone());
        let violation = match state.handle(offender, frame) {
            Err(violation) => violation,
            Ok(_) => {
                return Err("a current Pipe frame from a non-owner must fail the session".into());
            }
        };
        assert!(matches!(
            violation,
            ProtocolViolation::PipeOwnership {
                sender,
                pipe_id: violated_pipe,
                frame_name: actual_frame,
            } if sender == offender && violated_pipe == pipe_id && actual_frame == frame_name
        ));
        assert_eq!(state.pending_offer_count(), 1);
        assert_eq!(state.pipe_count(), 1);

        let cleanup = state.remove_session(offender);
        assert_eq!(sdk_deliveries(&cleanup).count(), 0);
        assert_eq!(
            publications(&cleanup).count(),
            usize::from(role == SessionRole::Listener)
        );
        assert!(offender_cancellation.is_cancelled());
        assert_eq!(state.pending_offer_count(), 1);
    }

    assert_eq!(
        state
            .handle(listener, Frame::OfferAccepted { pipe_id })?
            .len(),
        1
    );
    assert_eq!(state.live_pipe_count(), 1);
    Ok(())
}

#[test]
fn unknown_pipe_frames_are_no_op_for_every_pipe_operation() -> Result<(), Box<dyn std::error::Error>>
{
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    let unknown = PipeId::new(connector, 99);
    let cases = [
        (listener, Frame::OfferAccepted { pipe_id: unknown }),
        (
            listener,
            Frame::OfferRejected {
                pipe_id: unknown,
                code: ErrorCode::Cancelled,
                message: "late".to_owned(),
            },
        ),
        (connector, Frame::Cancel { pipe_id: unknown }),
        (
            connector,
            Frame::Data {
                pipe_id: unknown,
                payload: Bytes::from_static(b"late"),
            },
        ),
        (connector, Frame::Fin { pipe_id: unknown }),
        (connector, Frame::Close { pipe_id: unknown }),
        (
            connector,
            Frame::Reset {
                pipe_id: unknown,
                code: ErrorCode::Cancelled,
                message: "late".to_owned(),
            },
        ),
    ];

    for (sender, frame) in cases {
        assert!(state.handle(sender, frame)?.is_empty());
        assert_eq!(state.pipe_count(), 0);
    }
    assert!(state.sessions.contains_key(&listener));
    assert!(state.sessions.contains_key(&connector));
    Ok(())
}

#[test]
fn owner_invalid_phase_resets_only_the_target_pipe() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let healthy = open_pipe(&mut state, connector, listener, 1)?;

    for invalid_case in 0..4 {
        let pipe_id = offer_pipe(&mut state, connector, listener, 10 + invalid_case)?;
        let frame = match invalid_case {
            0 => Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"too-early"),
            },
            1 => Frame::Fin { pipe_id },
            2 => Frame::Close { pipe_id },
            3 => Frame::Reset {
                pipe_id,
                code: ErrorCode::Cancelled,
                message: "too early".to_owned(),
            },
            _ => return Err("invalid offered-phase test case".into()),
        };
        let resets = state.handle(connector, frame)?;
        assert_eq!(resets.len(), 2);
        assert!(sdk_deliveries(&resets).all(|delivery| matches!(
            delivery.frame,
            Frame::Reset {
                pipe_id: reset_pipe,
                code: ErrorCode::ProtocolError,
                ..
            } if reset_pipe == pipe_id
        )));
        assert_eq!(state.pipe_count(), 1);
    }

    for invalid_case in 0..2 {
        let pipe_id = open_pipe(&mut state, connector, listener, 20 + invalid_case)?;
        let (sender, frame) = match invalid_case {
            0 => (listener, Frame::OfferAccepted { pipe_id }),
            1 => (
                listener,
                Frame::OfferRejected {
                    pipe_id,
                    code: ErrorCode::Cancelled,
                    message: "too late".to_owned(),
                },
            ),
            _ => return Err("invalid open-phase test case".into()),
        };
        let resets = state.handle(sender, frame)?;
        assert_eq!(resets.len(), 2);
        assert!(sdk_deliveries(&resets).all(|delivery| matches!(
            delivery.frame,
            Frame::Reset {
                pipe_id: reset_pipe,
                code: ErrorCode::ProtocolError,
                ..
            } if reset_pipe == pipe_id
        )));
        assert_eq!(state.pipe_count(), 1);
    }

    assert_eq!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id: healthy,
                    payload: Bytes::from_static(b"healthy"),
                },
            )?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn cancel_after_offer_admission_closes_only_the_cancelled_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let healthy = open_pipe(&mut state, connector, listener, 1)?;
    let cancelled = open_pipe(&mut state, connector, listener, 2)?;

    let resets = state.handle(connector, Frame::Cancel { pipe_id: cancelled })?;

    assert!(matches!(
        first_sdk_delivery(&resets).map(|delivery| (&delivery.target, &delivery.frame)),
        Some((target, Frame::Reset {
            pipe_id,
            code: ErrorCode::Cancelled,
            ..
        })) if *target == listener && *pipe_id == cancelled
    ));
    assert_eq!(resets.len(), 1);
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id: healthy,
                    payload: Bytes::from_static(b"healthy"),
                },
            )?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn connection_history_is_a_single_high_watermark() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let connector = add_session(&mut state, SessionRole::Connector);

    for connection_id in 1..=10_000 {
        let deliveries = state.handle(
            connector,
            Frame::Open {
                connection_id,
                client_id: String::new(),
            },
        )?;
        assert!(matches!(
            first_sdk_delivery(&deliveries).map(|delivery| &delivery.frame),
            Some(Frame::OpenFailed {
                code: ErrorCode::InvalidArgument,
                ..
            })
        ));
    }

    assert_eq!(state.connection_high_watermark(connector), Some(10_000));
    assert_eq!(state.pipe_count(), 0);
    assert!(
        state
            .handle(
                connector,
                Frame::Open {
                    connection_id: 9_999,
                    client_id: String::new(),
                },
            )?
            .is_empty()
    );
    assert_eq!(state.connection_high_watermark(connector), Some(10_000));
    Ok(())
}

#[test]
fn repeated_terminal_opens_leave_only_connection_high_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    for connection_id in 1..=64 {
        let offered = state.handle(
            connector,
            Frame::Open {
                connection_id,
                client_id: "echo.shared".to_owned(),
            },
        )?;
        let offers: Vec<_> = sdk_deliveries(&offered)
            .filter_map(|delivery| match delivery.frame {
                Frame::Offer { pipe_id, .. } if delivery.target == listener => Some(pipe_id),
                _ => None,
            })
            .collect();
        assert_eq!(offers.len(), 1);
        let pipe_id = offers[0];
        assert_eq!(pipe_id.connection_id(), connection_id);

        if connection_id % 2 == 0 {
            let opened = state.handle(listener, Frame::OfferAccepted { pipe_id })?;
            assert!(matches!(
                first_sdk_delivery(&opened).map(|delivery| &delivery.frame),
                Some(Frame::Opened { pipe_id: opened_pipe }) if *opened_pipe == pipe_id
            ));
            let closed = state.handle(connector, Frame::Close { pipe_id })?;
            assert!(matches!(
                first_sdk_delivery(&closed).map(|delivery| &delivery.frame),
                Some(Frame::Close { pipe_id: closed_pipe }) if *closed_pipe == pipe_id
            ));
        } else {
            let rejected = state.handle(
                listener,
                Frame::OfferRejected {
                    pipe_id,
                    code: ErrorCode::Unavailable,
                    message: "listener refused".to_owned(),
                },
            )?;
            assert!(matches!(
                first_sdk_delivery(&rejected).map(|delivery| &delivery.frame),
                Some(Frame::OpenFailed {
                    connection_id: failed,
                    code: ErrorCode::Unavailable,
                    observation: PeerObservation::NotObserved,
                    ..
                }) if *failed == connection_id
            ));
        }

        assert_eq!(state.pending_offer_count(), 0);
        assert_eq!(state.live_pipe_count(), 0);
        assert_eq!(state.pipe_count(), 0);
    }

    assert_eq!(state.connection_high_watermark(connector), Some(64));
    assert_eq!(state.registry.binding_count(), 1);
    assert_eq!(state.registry.session_binding_count(listener), 1);
    Ok(())
}

#[test]
fn close_reset_and_late_frames_are_pipe_local() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let closed = open_pipe(&mut state, connector, listener, 1)?;
    let close = state.handle(connector, Frame::Close { pipe_id: closed })?;
    assert!(matches!(
        first_sdk_delivery(&close).map(|delivery| (&delivery.target, &delivery.frame)),
        Some((target, Frame::Close { pipe_id })) if *target == listener && *pipe_id == closed
    ));
    assert_eq!(state.pipe_count(), 0);
    assert!(
        state
            .handle(listener, Frame::Close { pipe_id: closed })?
            .is_empty()
    );

    let reset = open_pipe(&mut state, connector, listener, 2)?;
    let resets = state.handle(
        listener,
        Frame::Reset {
            pipe_id: reset,
            code: ErrorCode::Cancelled,
            message: "listener stopped".to_owned(),
        },
    )?;
    assert!(matches!(
        first_sdk_delivery(&resets).map(|delivery| (&delivery.target, &delivery.frame)),
        Some((target, Frame::Reset { pipe_id, code: ErrorCode::Cancelled, .. }))
            if *target == connector && *pipe_id == reset
    ));
    assert_eq!(state.pipe_count(), 0);
    assert!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id: reset,
                    payload: Bytes::from_static(b"late"),
                },
            )?
            .is_empty()
    );
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn data_after_fin_resets_only_that_pipe() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;
    let violating = open_pipe(&mut state, connector, listener, 1)?;
    let healthy = open_pipe(&mut state, connector, listener, 2)?;

    assert_eq!(
        state
            .handle(connector, Frame::Fin { pipe_id: violating })?
            .len(),
        1
    );
    assert!(
        state
            .handle(connector, Frame::Fin { pipe_id: violating })?
            .is_empty()
    );
    let resets = state.handle(
        connector,
        Frame::Data {
            pipe_id: violating,
            payload: Bytes::from_static(b"invalid"),
        },
    )?;

    assert_eq!(resets.len(), 2);
    assert!(sdk_deliveries(&resets).all(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::ProtocolError,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id: healthy,
                    payload: Bytes::from_static(b"healthy"),
                },
            )?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn unregister_cancels_only_pending_binding_incarnation() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    let registered = state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    let old_binding = sdk_deliveries(&registered)
        .find_map(|delivery| match delivery.frame {
            Frame::Registered { binding_id, .. } => Some(binding_id),
            _ => None,
        })
        .ok_or("missing old binding")?;
    let offers = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let old_pipe = sdk_deliveries(&offers)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer {
                pipe_id,
                binding_id,
                ..
            } if binding_id == old_binding => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing old offer")?;

    let removed = state.handle(
        listener,
        Frame::Unregister {
            request_id: 2,
            binding_id: old_binding,
        },
    )?;
    assert!(sdk_deliveries(&removed).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            connection_id: 1,
            code: ErrorCode::Unavailable,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);

    let renewed = state.handle(
        listener,
        Frame::Register {
            request_id: 3,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    let new_binding = sdk_deliveries(&renewed)
        .find_map(|delivery| match delivery.frame {
            Frame::Registered { binding_id, .. } => Some(binding_id),
            _ => None,
        })
        .ok_or("missing renewed binding")?;
    assert_ne!(old_binding, new_binding);

    assert!(
        state
            .handle(listener, Frame::OfferAccepted { pipe_id: old_pipe })?
            .is_empty()
    );
    assert_eq!(state.pipe_count(), 0);

    let new_offer = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let new_pipe = sdk_deliveries(&new_offer)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer {
                pipe_id,
                binding_id,
                ..
            } if binding_id == new_binding => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing renewed binding offer")?;
    assert_eq!(state.pipe_count(), 1);

    assert_eq!(
        state
            .handle(listener, Frame::OfferAccepted { pipe_id: new_pipe })?
            .len(),
        1
    );
    let removed_after_open = state.handle(
        listener,
        Frame::Unregister {
            request_id: 4,
            binding_id: new_binding,
        },
    )?;
    assert!(
        sdk_deliveries(&removed_after_open)
            .any(|delivery| matches!(delivery.frame, Frame::Unregistered { request_id: 4 }))
    );
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(
        state
            .handle(
                connector,
                Frame::Data {
                    pipe_id: new_pipe,
                    payload: Bytes::from_static(b"already admitted"),
                },
            )?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn terminal_interleavings_emit_once_and_never_resurrect_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let offered = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let pending = sdk_deliveries(&offered)
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing pending offer")?;
    let cancelled = state.handle(connector, Frame::Cancel { pipe_id: pending })?;
    assert_eq!(cancelled.len(), 1);
    assert!(matches!(
        first_sdk_delivery(&cancelled).map(|delivery| &delivery.frame),
        Some(Frame::Reset {
            code: ErrorCode::Cancelled,
            ..
        })
    ));
    assert_eq!(state.pipe_count(), 0);
    assert!(
        state
            .handle(listener, Frame::OfferAccepted { pipe_id: pending })?
            .is_empty()
    );
    assert!(
        state
            .handle(listener, Frame::Close { pipe_id: pending })?
            .is_empty()
    );
    assert!(
        state
            .handle(connector, Frame::Cancel { pipe_id: pending })?
            .is_empty()
    );
    assert_eq!(state.pipe_count(), 0);

    let opened = open_pipe(&mut state, connector, listener, 2)?;
    let closed = state.handle(connector, Frame::Close { pipe_id: opened })?;
    assert_eq!(closed.len(), 1);
    assert!(matches!(
        first_sdk_delivery(&closed).map(|delivery| &delivery.frame),
        Some(Frame::Close { .. })
    ));
    for late in [
        Frame::OfferAccepted { pipe_id: opened },
        Frame::Cancel { pipe_id: opened },
        Frame::Close { pipe_id: opened },
    ] {
        let sender = if matches!(late, Frame::Cancel { .. }) {
            connector
        } else {
            listener
        };
        assert!(state.handle(sender, late)?.is_empty());
    }
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}
