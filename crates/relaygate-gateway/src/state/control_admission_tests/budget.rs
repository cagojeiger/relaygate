use super::*;

fn request(operation: &str, id: u64) -> Frame {
    if operation == "publish" {
        publish(id, &unique_address())
    } else {
        dial(id, &unique_address())
    }
}

fn exhausted(actions: &[GatewayAction]) -> bool {
    frames(actions).any(|frame| {
        matches!(
            frame,
            Frame::PublishFailed {
                code: ErrorCode::ResourceExhausted,
                ..
            } | Frame::DialFailed {
                code: ErrorCode::ResourceExhausted,
                observation: PeerObservation::NotObserved,
                ..
            }
        )
    })
}

fn admitted(actions: &[GatewayAction], operation: &str) -> bool {
    frames(actions).any(|frame| match operation {
        "publish" => matches!(frame, Frame::Published { .. }),
        _ => matches!(
            frame,
            Frame::DialFailed {
                code: ErrorCode::NotFound,
                observation: PeerObservation::NotObserved,
                ..
            }
        ),
    })
}

#[test]
fn session_rejection_does_not_spend_gateway_credit() -> TestResult {
    for operation in ["publish", "dial"] {
        let mut state = GatewayState::new(GatewayLimits {
            control_burst: 2,
            session_control_burst: 1,
            ..GatewayLimits::default()
        });
        let first = session(&mut state)?;
        let sibling = session(&mut state)?;
        let now = Instant::now();
        assert!(admitted(
            &state.handle_at(first, request(operation, 1), now)?,
            operation
        ));
        for id in 2..20 {
            assert!(exhausted(&state.handle_at(
                first,
                request(operation, id),
                now
            )?));
        }
        assert!(admitted(
            &state.handle_at(sibling, request(operation, 1), now)?,
            operation
        ));
        assert!(!state.control_rate.has_capacity(now));
    }
    Ok(())
}

#[test]
fn gateway_rejection_does_not_refund_session_credit() -> TestResult {
    for operation in ["publish", "dial"] {
        let mut state = GatewayState::new(GatewayLimits {
            control_burst: 1,
            control_rate_per_second: 10,
            session_control_burst: 1,
            session_control_rate_per_second: 1,
            ..GatewayLimits::default()
        });
        let first = session(&mut state)?;
        let rejected = session(&mut state)?;
        let now = Instant::now();
        state.handle_at(first, request(operation, 1), now)?;
        assert!(exhausted(&state.handle_at(
            rejected,
            request(operation, 1),
            now
        )?));
        let global_refilled = now + Duration::from_millis(100);
        assert!(state.control_rate.has_capacity(global_refilled));
        assert!(exhausted(&state.handle_at(
            rejected,
            request(operation, 2),
            global_refilled
        )?));
        assert!(state.control_rate.has_capacity(global_refilled));
        assert!(admitted(
            &state.handle_at(
                rejected,
                request(operation, 3),
                now + Duration::from_secs(1)
            )?,
            operation
        ));
    }
    Ok(())
}

#[test]
fn duplicate_publish_is_charged_without_replacing_binding() -> TestResult {
    let mut state = GatewayState::new(GatewayLimits {
        control_burst: 2,
        ..GatewayLimits::default()
    });
    let owner = session(&mut state)?;
    let now = Instant::now();
    let destination = unique_address();
    let mut ids = Vec::new();
    for id in 1..=2 {
        let actions = state.handle_at(owner, publish(id, &destination), now)?;
        ids.push(
            frames(&actions)
                .find_map(|frame| match frame {
                    Frame::Published { binding_id, .. } => Some(*binding_id),
                    _ => None,
                })
                .ok_or("missing published binding")?,
        );
    }
    assert_eq!(ids[0], ids[1]);
    assert!(exhausted(&state.handle_at(
        owner,
        publish(3, &destination),
        now
    )?));
    assert_eq!(state.snapshot().bindings, 1);
    Ok(())
}

#[test]
fn failed_operations_do_not_refund_credit() -> TestResult {
    for publish_failure in [false, true] {
        let mut state = GatewayState::new(GatewayLimits {
            control_burst: 2,
            max_bindings: 1,
            ..GatewayLimits::default()
        });
        let owner = session(&mut state)?;
        let now = Instant::now();
        state.handle_at(owner, publish(1, &unique_address()), now)?;
        let operation = if publish_failure { "publish" } else { "dial" };
        let failed = state.handle_at(owner, request(operation, 2), now)?;
        assert!(frames(&failed).any(|frame| match publish_failure {
            true => matches!(
                frame,
                Frame::PublishFailed {
                    code: ErrorCode::ResourceExhausted,
                    ..
                }
            ),
            false => matches!(
                frame,
                Frame::DialFailed {
                    code: ErrorCode::NotFound,
                    observation: PeerObservation::NotObserved,
                    ..
                }
            ),
        }));
        assert!(!state.control_rate.has_capacity(now));
        assert!(exhausted(&state.handle_at(
            owner,
            request(operation, 3),
            now
        )?));
    }
    Ok(())
}

#[test]
fn missing_session_drain_and_replayed_dial_do_not_spend_credit() -> TestResult {
    for operation in ["publish", "dial"] {
        let mut state = GatewayState::new(GatewayLimits {
            control_burst: 1,
            session_control_burst: 1,
            ..GatewayLimits::default()
        });
        let owner = session(&mut state)?;
        let now = Instant::now();
        assert!(
            state
                .handle_at(SessionId::new(), request(operation, 1), now)?
                .is_empty()
        );
        assert!(state.control_rate.has_capacity(now));
        assert!(
            state
                .sessions
                .get_mut(&owner)
                .ok_or("missing session")?
                .control_rate
                .has_capacity(now)
        );
        state.begin_draining();
        let failed = state.handle_at(owner, request(operation, 1), now)?;
        assert!(frames(&failed).any(|frame| matches!(
            frame,
            Frame::PublishFailed {
                code: ErrorCode::Unavailable,
                ..
            } | Frame::DialFailed {
                code: ErrorCode::Unavailable,
                ..
            }
        )));
        assert!(state.control_rate.has_capacity(now));
        assert!(
            state
                .sessions
                .get_mut(&owner)
                .ok_or("missing session")?
                .control_rate
                .has_capacity(now)
        );
    }
    let mut state = GatewayState::new(GatewayLimits {
        control_burst: 2,
        session_control_burst: 2,
        ..GatewayLimits::default()
    });
    let owner = session(&mut state)?;
    let now = Instant::now();
    let dial = request("dial", 2);
    state.handle_at(owner, dial.clone(), now)?;
    for replayed in [dial, request("dial", 1)] {
        let actions = state.handle_at(owner, replayed, now)?;
        assert!(frames(&actions).any(|frame| matches!(
            frame,
            Frame::DialFailed {
                code: ErrorCode::ProtocolError,
                observation: PeerObservation::NotObserved,
                ..
            }
        )));
    }
    assert!(state.control_rate.try_take(now));
    assert!(!state.control_rate.try_take(now));
    let session_rate = &mut state
        .sessions
        .get_mut(&owner)
        .ok_or("missing session")?
        .control_rate;
    assert!(session_rate.try_take(now));
    assert!(!session_rate.try_take(now));
    Ok(())
}

#[test]
fn direct_unauthenticated_dial_is_fenced_before_authorization() -> TestResult {
    let mut state = GatewayState::new(GatewayLimits {
        control_rate_per_second: 1,
        control_burst: 2,
        session_control_rate_per_second: 1,
        session_control_burst: 2,
        ..GatewayLimits::default()
    });
    let owner = session(&mut state)?;
    let address = unique_address();
    let denied = state.handle(owner, dial(2, &address))?;
    assert!(frames(&denied).any(|frame| matches!(
        frame,
        Frame::DialFailed {
            connection_id: 2,
            code: ErrorCode::Unauthenticated,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    for connection_id in [2, 1] {
        let replayed = state.handle(owner, dial(connection_id, &address))?;
        assert!(frames(&replayed).any(|frame| matches!(
            frame,
            Frame::DialFailed {
                code: ErrorCode::ProtocolError,
                observation: PeerObservation::NotObserved,
                ..
            }
        )));
    }

    let now = Instant::now();
    assert!(state.control_rate.try_take(now));
    assert!(!state.control_rate.try_take(now));
    let session_rate = &mut state
        .sessions
        .get_mut(&owner)
        .ok_or("missing session")?
        .control_rate;
    assert!(session_rate.try_take(now));
    assert!(!session_rate.try_take(now));
    Ok(())
}
