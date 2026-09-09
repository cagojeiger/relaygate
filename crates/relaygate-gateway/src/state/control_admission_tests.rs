use std::{
    error::Error,
    time::{Duration, Instant},
};

use bytes::Bytes;
use relaygate_protocol::{DestinationId, ErrorCode, Frame, PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{GatewayAction, GatewayLimits, GatewayState};

type TestResult = Result<(), Box<dyn Error>>;

mod budget;
mod lifecycle;

fn session(state: &mut GatewayState) -> Result<SessionId, Box<dyn Error>> {
    let (sender, _receiver) = mpsc::channel(16);
    state
        .add_session(sender, CancellationToken::new())
        .ok_or_else(|| "session admission failed".into())
}

fn frames(actions: &[GatewayAction]) -> impl Iterator<Item = &Frame> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::SendSdkFrame(delivery) => Some(&delivery.frame),
        _ => None,
    })
}

fn publish(id: u64, destination_id: DestinationId) -> Frame {
    Frame::Publish {
        request_id: id,
        destination_id,
    }
}

#[test]
fn session_budget_is_shared_by_publish_and_dial_and_preserves_siblings() -> TestResult {
    let mut state = GatewayState::new(GatewayLimits {
        session_control_rate_per_second: 1,
        session_control_burst: 2,
        ..GatewayLimits::default()
    });
    let caller = session(&mut state)?;
    let sibling = session(&mut state)?;
    let now = Instant::now();
    let first = state.handle_at(caller, publish(1, DestinationId::new()), now)?;
    assert!(frames(&first).any(|f| matches!(f, Frame::Published { .. })));
    let second = state.handle_at(
        caller,
        Frame::Dial {
            connection_id: 1,
            destination_id: DestinationId::new(),
        },
        now,
    )?;
    assert!(frames(&second).any(|f| matches!(
        f,
        Frame::DialFailed {
            code: ErrorCode::NotFound,
            ..
        }
    )));
    let rejected = state.handle_at(caller, publish(2, DestinationId::new()), now)?;
    assert!(frames(&rejected).any(|f| matches!(
        f,
        Frame::PublishFailed {
            request_id: 2,
            code: ErrorCode::ResourceExhausted,
            ..
        }
    )));
    assert_eq!(state.snapshot().bindings, 1);
    let allowed = state.handle_at(sibling, publish(1, DestinationId::new()), now)?;
    assert!(frames(&allowed).any(|f| matches!(f, Frame::Published { .. })));
    let recovered = state.handle_at(
        caller,
        publish(3, DestinationId::new()),
        now + Duration::from_secs(1),
    )?;
    assert!(frames(&recovered).any(|f| matches!(f, Frame::Published { .. })));
    assert_eq!(state.snapshot().sessions, 2);
    Ok(())
}

#[test]
fn gateway_rate_rejection_precedes_resolve_and_fences_replayed_dial() -> TestResult {
    let mut state = GatewayState::new_distributed(
        GatewayLimits {
            control_rate_per_second: 1,
            control_burst: 1,
            ..GatewayLimits::default()
        },
        GatewayId::new(),
    );
    let listener = session(&mut state)?;
    let caller = session(&mut state)?;
    let now = Instant::now();
    let destination = DestinationId::new();
    state.handle_at(listener, publish(1, destination), now)?;
    let dial = Frame::Dial {
        connection_id: 1,
        destination_id: DestinationId::new(),
    };
    let rejected = state.handle_at(caller, dial.clone(), now)?;
    assert_eq!(rejected.len(), 1);
    assert!(frames(&rejected).any(|f| matches!(
        f,
        Frame::DialFailed {
            connection_id: 1,
            code: ErrorCode::ResourceExhausted,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    assert_eq!(state.snapshot().remote_open_attempts, 0);
    assert_eq!(state.snapshot().pending_offers, 0);
    let later = now + Duration::from_secs(1);
    assert!(state.handle_at(caller, dial, later)?.is_empty());
    let allowed = state.handle_at(
        caller,
        Frame::Dial {
            connection_id: 2,
            destination_id: destination,
        },
        later,
    )?;
    assert!(frames(&allowed).any(|f| matches!(f, Frame::Offer { .. })));
    Ok(())
}

#[test]
fn exhausted_control_budget_preserves_pipe_data_and_cleanup() -> TestResult {
    for terminal in ["fin", "close", "reset", "cancel", "reject"] {
        let mut state = GatewayState::new(GatewayLimits {
            control_rate_per_second: 1,
            control_burst: 2,
            ..GatewayLimits::default()
        });
        let caller = session(&mut state)?;
        let listener = session(&mut state)?;
        let now = Instant::now();
        let destination = DestinationId::new();
        let published = state.handle_at(listener, publish(1, destination), now)?;
        let binding_id = frames(&published)
            .find_map(|f| match f {
                Frame::Published { binding_id, .. } => Some(*binding_id),
                _ => None,
            })
            .ok_or("no binding")?;
        let offered = state.handle_at(
            caller,
            Frame::Dial {
                connection_id: 1,
                destination_id: destination,
            },
            now,
        )?;
        let pipe_id = frames(&offered)
            .find_map(|f| match f {
                Frame::Offer { pipe_id, .. } => Some(*pipe_id),
                _ => None,
            })
            .ok_or("no offer")?;
        if terminal == "cancel" {
            state.handle_at(caller, Frame::Cancel { pipe_id }, now)?;
        } else if terminal == "reject" {
            state.handle_at(
                listener,
                Frame::OfferRejected {
                    pipe_id,
                    code: ErrorCode::Unavailable,
                    message: "test".into(),
                },
                now,
            )?;
        } else {
            let opened = state.handle_at(listener, Frame::OfferAccepted { pipe_id }, now)?;
            assert!(frames(&opened).any(|f| matches!(f, Frame::Opened { .. })));
            let rejected = state.handle_at(caller, publish(2, DestinationId::new()), now)?;
            assert!(frames(&rejected).any(|f| matches!(
                f,
                Frame::PublishFailed {
                    code: ErrorCode::ResourceExhausted,
                    ..
                }
            )));
            for sender in [caller, listener] {
                let data = state.handle_at(
                    sender,
                    Frame::Data {
                        pipe_id,
                        payload: Bytes::from_static(b"data"),
                    },
                    now,
                )?;
                assert!(frames(&data).any(
                    |f| matches!(f, Frame::Data { payload, .. } if payload.as_ref() == b"data")
                ));
            }
            match terminal {
                "fin" => {
                    state.handle_at(caller, Frame::Fin { pipe_id }, now)?;
                    state.handle_at(listener, Frame::Fin { pipe_id }, now)?;
                }
                "close" => {
                    state.handle_at(caller, Frame::Close { pipe_id }, now)?;
                }
                _ => {
                    state.handle_at(
                        caller,
                        Frame::Reset {
                            pipe_id,
                            code: ErrorCode::Cancelled,
                            message: "test".into(),
                        },
                        now,
                    )?;
                }
            }
        }
        let unpublish = state.handle_at(
            listener,
            Frame::Unpublish {
                request_id: 3,
                binding_id,
            },
            now,
        )?;
        assert!(frames(&unpublish).any(|f| matches!(f, Frame::Unpublished { request_id: 3 })));
        let pong = state.handle_at(caller, Frame::Ping { nonce: 99 }, now)?;
        assert!(frames(&pong).any(|f| matches!(f, Frame::Pong { nonce: 99 })));
        assert_eq!(state.snapshot().bindings, 0);
        assert_eq!(state.snapshot().pending_offers, 0);
        assert_eq!(state.snapshot().live_pipes, 0);
        assert_eq!(state.snapshot().sessions, 2);
    }
    Ok(())
}

#[test]
fn session_recreation_does_not_reset_gateway_budget() -> TestResult {
    let mut state = GatewayState::new(GatewayLimits {
        control_rate_per_second: 1,
        control_burst: 1,
        ..GatewayLimits::default()
    });
    let first = session(&mut state)?;
    let now = Instant::now();
    state.handle_at(first, publish(1, DestinationId::new()), now)?;
    state.remove_session(first);
    let second = session(&mut state)?;
    let rejected = state.handle_at(second, publish(1, DestinationId::new()), now)?;
    assert!(frames(&rejected).any(|f| matches!(
        f,
        Frame::PublishFailed {
            code: ErrorCode::ResourceExhausted,
            ..
        }
    )));
    assert_eq!(state.snapshot().bindings, 0);
    Ok(())
}

#[test]
fn control_rejection_metrics_separate_gateway_and_session_scopes() -> TestResult {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || -> TestResult {
        for operation in ["publish", "dial"] {
            for limits in [
                GatewayLimits {
                    control_burst: 1,
                    control_rate_per_second: 1,
                    ..GatewayLimits::default()
                },
                GatewayLimits {
                    session_control_burst: 1,
                    session_control_rate_per_second: 1,
                    ..GatewayLimits::default()
                },
            ] {
                let mut state = GatewayState::new(limits);
                let caller = session(&mut state)?;
                let now = Instant::now();
                state.handle_at(caller, publish(1, DestinationId::new()), now)?;
                let rejected = if operation == "publish" {
                    publish(2, DestinationId::new())
                } else {
                    Frame::Dial {
                        connection_id: 1,
                        destination_id: DestinationId::new(),
                    }
                };
                state.handle_at(caller, rejected, now)?;
            }
        }
        Ok(())
    })?;
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        snapshot
            .iter()
            .filter(
                |(key, _, _, _)| key.key().name() == "relaygate_gateway_control_rejections_total"
            )
            .count(),
        4
    );
    for operation in ["publish", "dial"] {
        for scope in ["session", "gateway"] {
            assert!(snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == "relaygate_gateway_control_rejections_total"
                    && key.key().labels().count() == 2
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "scope" && label.value() == scope)
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "operation" && label.value() == operation)
                    && matches!(value, DebugValue::Counter(1))
            }));
        }
    }
    for name in [
        "relaygate_gateway_publish_results_total",
        "relaygate_gateway_dial_results_total",
    ] {
        assert!(
            snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == name
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "code" && label.value() == "resource_exhausted")
                    && matches!(value, DebugValue::Counter(2))
            }),
            "missing RED failure accounting for {name}"
        );
    }
    Ok(())
}
