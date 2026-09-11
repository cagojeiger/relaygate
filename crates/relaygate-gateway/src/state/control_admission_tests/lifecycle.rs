use super::*;
use relaygate_protocol::PipeId;

fn offer(
    state: &mut GatewayState,
    caller: SessionId,
    destination: &RouteAddress,
    now: Instant,
) -> Result<PipeId, Box<dyn Error>> {
    let actions = state.handle_at(caller, dial(1, destination), now)?;
    frames(&actions)
        .find_map(|frame| match frame {
            Frame::Offer { pipe_id, .. } => Some(*pipe_id),
            _ => None,
        })
        .ok_or_else(|| "missing offer".into())
}

#[test]
fn rate_rejection_cancel_disconnect_and_late_accept_orders_preserve_siblings() -> TestResult {
    // GatewayState serializes events; enumerate all 24 orders, for both gated operations.
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if (0..4).any(|i| (i + 1..4).any(|j| order[i] == order[j])) {
                        continue;
                    }
                    for publish_rejection in [false, true] {
                        let mut state = GatewayState::new(GatewayLimits {
                            control_burst: 4,
                            ..GatewayLimits::default()
                        });
                        let owner = session(&mut state)?;
                        let healthy = session(&mut state)?;
                        let victim = session(&mut state)?;
                        let now = Instant::now();
                        let destination = unique_address();
                        state.handle_at(owner, publish(1, &destination), now)?;
                        let healthy_pipe = offer(&mut state, healthy, &destination, now)?;
                        state.handle_at(
                            owner,
                            Frame::OfferAccepted {
                                pipe_id: healthy_pipe,
                            },
                            now,
                        )?;
                        state.handle_at(victim, publish(1, &unique_address()), now)?;
                        let pending = offer(&mut state, victim, &destination, now)?;
                        assert!(!state.control_rate.has_capacity(now));
                        let mut removed = false;
                        for event in order {
                            match event {
                                0 => {
                                    let frame = if publish_rejection {
                                        publish(2, &unique_address())
                                    } else {
                                        dial(2, &destination)
                                    };
                                    let actions = state.handle_at(victim, frame, now)?;
                                    if removed {
                                        assert!(actions.is_empty());
                                    } else {
                                        assert_eq!(actions.len(), 1);
                                        assert!(
                                            frames(&actions).any(|frame| matches!(
                                                frame,
                                                Frame::PublishFailed {
                                                    code: ErrorCode::ResourceExhausted,
                                                    ..
                                                } | Frame::DialFailed {
                                                    code: ErrorCode::ResourceExhausted,
                                                    observation: PeerObservation::NotObserved,
                                                    ..
                                                }
                                            )),
                                            "order={order:?}"
                                        );
                                    }
                                }
                                1 => {
                                    state.handle_at(
                                        victim,
                                        Frame::Cancel { pipe_id: pending },
                                        now,
                                    )?;
                                }
                                2 => {
                                    state.remove_session(victim);
                                    removed = true;
                                }
                                _ => {
                                    state.handle_at(
                                        owner,
                                        Frame::OfferAccepted { pipe_id: pending },
                                        now,
                                    )?;
                                }
                            }
                        }
                        // Repeated teardown and late results must not resurrect or double-release state.
                        assert!(state.remove_session(victim).is_empty());
                        state.handle_at(owner, Frame::OfferAccepted { pipe_id: pending }, now)?;
                        state.handle_at(
                            victim,
                            Frame::Cancel {
                                pipe_id: PipeId::new(victim, 2),
                            },
                            now,
                        )?;
                        let snapshot = state.snapshot();
                        assert_eq!(snapshot.sessions, 2, "order={order:?}");
                        assert_eq!(snapshot.bindings, 1, "order={order:?}");
                        assert_eq!(snapshot.pending_offers, 0, "order={order:?}");
                        assert_eq!(snapshot.live_pipes, 1, "order={order:?}");
                        assert_eq!(snapshot.remote_open_attempts, 0);
                        for sender in [owner, healthy] {
                            let actions = state.handle_at(
                                sender,
                                Frame::Data {
                                    pipe_id: healthy_pipe,
                                    payload: Bytes::from_static(b"alive"),
                                },
                                now,
                            )?;
                            assert!(frames(&actions).any(|frame| matches!(frame, Frame::Data { payload, .. } if payload.as_ref() == b"alive")));
                        }
                        state.remove_session(healthy);
                        state.remove_session(owner);
                        let clean = state.snapshot();
                        assert_eq!(
                            (
                                clean.sessions,
                                clean.bindings,
                                clean.pending_offers,
                                clean.live_pipes
                            ),
                            (0, 0, 0, 0)
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn cancelling_a_rate_rejected_dial_does_not_cancel_another_pipe() -> TestResult {
    let mut state = GatewayState::new(GatewayLimits {
        control_burst: 2,
        ..GatewayLimits::default()
    });
    let owner = session(&mut state)?;
    let caller = session(&mut state)?;
    let now = Instant::now();
    let destination = unique_address();
    state.handle_at(owner, publish(1, &destination), now)?;
    let active = offer(&mut state, caller, &destination, now)?;
    state.handle_at(owner, Frame::OfferAccepted { pipe_id: active }, now)?;
    let failed = state.handle_at(caller, dial(2, &destination), now)?;
    assert!(frames(&failed).any(|frame| matches!(
        frame,
        Frame::DialFailed {
            code: ErrorCode::ResourceExhausted,
            ..
        }
    )));
    for _ in 0..3 {
        assert!(
            state
                .handle_at(
                    caller,
                    Frame::Cancel {
                        pipe_id: PipeId::new(caller, 2)
                    },
                    now
                )?
                .is_empty()
        );
    }
    assert_eq!(state.snapshot().live_pipes, 1);
    let data = state.handle_at(
        caller,
        Frame::Data {
            pipe_id: active,
            payload: Bytes::from_static(b"alive"),
        },
        now,
    )?;
    assert!(frames(&data).any(|frame| matches!(frame, Frame::Data { .. })));
    Ok(())
}
