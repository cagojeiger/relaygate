use std::{
    error::Error,
    str::FromStr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use relaygate_protocol::{BindingId, DestinationId, ErrorCode, Frame, PipeId, SessionId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Delivery, GatewayAction, GatewayLimits, GatewayState, ProtocolViolation};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const DESTINATION_A: &str = "11111111-1111-4111-8111-111111111111";
const DESTINATION_B: &str = "22222222-2222-4222-8222-222222222222";

fn destination(raw: &str) -> Result<DestinationId, &'static str> {
    DestinationId::from_str(raw)
}

fn state() -> GatewayState {
    GatewayState::new(GatewayLimits::default())
}

fn limited_state(limits: GatewayLimits) -> GatewayState {
    GatewayState::new(limits)
}

fn add_session(state: &mut GatewayState) -> SessionId {
    let (sender, _receiver) = mpsc::channel(16);
    state
        .add_session(sender, CancellationToken::new())
        .unwrap_or_default()
}

fn sdk_frames(actions: &[GatewayAction]) -> impl Iterator<Item = (SessionId, &Frame)> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::SendSdkFrame(Delivery { target, frame, .. }) => Some((*target, frame)),
        _ => None,
    })
}

fn published_binding(actions: &[GatewayAction]) -> Option<BindingId> {
    sdk_frames(actions).find_map(|(_, frame)| match frame {
        Frame::Published { binding_id, .. } => Some(*binding_id),
        _ => None,
    })
}

fn publish(
    state: &mut GatewayState,
    session: SessionId,
    destination_id: DestinationId,
) -> TestResult<BindingId> {
    let actions = state.handle(
        session,
        Frame::Publish {
            request_id: 1,
            destination_id,
        },
    )?;
    published_binding(&actions).ok_or_else(|| "missing PUBLISHED response".into())
}

fn offered_pipe(actions: &[GatewayAction]) -> Option<PipeId> {
    sdk_frames(actions).find_map(|(_, frame)| match frame {
        Frame::Offer { pipe_id, .. } => Some(*pipe_id),
        _ => None,
    })
}

#[test]
fn one_session_can_publish_and_dial_while_self_binding_is_excluded() -> TestResult {
    let mut state = state();
    let relay_a = add_session(&mut state);
    let relay_b = add_session(&mut state);
    let destination_a = destination(DESTINATION_A)?;
    let destination_b = destination(DESTINATION_B)?;
    publish(&mut state, relay_a, destination_a)?;
    publish(&mut state, relay_b, destination_a)?;
    publish(&mut state, relay_b, destination_b)?;

    let actions = state.handle(
        relay_a,
        Frame::Dial {
            connection_id: 1,
            destination_id: destination_a,
        },
    )?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == relay_b
            && matches!(frame, Frame::Offer { destination_id, .. } if *destination_id == destination_a)
    }));

    let actions = state.handle(
        relay_a,
        Frame::Dial {
            connection_id: 2,
            destination_id: destination_b,
        },
    )?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == relay_b
            && matches!(frame, Frame::Offer { destination_id, .. } if *destination_id == destination_b)
    }));
    Ok(())
}

#[test]
fn publish_is_idempotent_but_unpublish_then_publish_creates_a_new_binding() -> TestResult {
    let mut state = state();
    let relay = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    let first = publish(&mut state, relay, destination_id)?;
    let repeated = publish(&mut state, relay, destination_id)?;
    assert_eq!(first, repeated);
    assert_eq!(state.snapshot().bindings, 1);

    state.handle(
        relay,
        Frame::Unpublish {
            request_id: 2,
            binding_id: first,
        },
    )?;
    assert_eq!(state.snapshot().bindings, 0);
    let replacement = publish(&mut state, relay, destination_id)?;
    assert_ne!(first, replacement);
    Ok(())
}

#[test]
fn accepted_pipe_relays_data_and_closes_without_residue() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    publish(&mut state, receiver, destination_id)?;

    let actions = state.handle(
        caller,
        Frame::Dial {
            connection_id: 7,
            destination_id,
        },
    )?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    let actions = state.handle(receiver, Frame::OfferAccepted { pipe_id })?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == caller && matches!(frame, Frame::Opened { pipe_id: opened } if *opened == pipe_id)
    }));
    assert_eq!(state.snapshot().live_pipes, 1);

    let actions = state.handle(
        caller,
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"hello"),
        },
    )?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == receiver
            && matches!(frame, Frame::Data { pipe_id: data_pipe, payload } if *data_pipe == pipe_id && payload.as_ref() == b"hello")
    }));

    state.handle(caller, Frame::Fin { pipe_id })?;
    state.handle(receiver, Frame::Fin { pipe_id })?;
    assert_eq!(state.snapshot().live_pipes, 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn offer_timeout_closes_the_unresponsive_relay_and_preserves_sibling_binding() -> TestResult {
    let mut state = limited_state(GatewayLimits {
        offer_timeout: Duration::from_millis(10),
        ..GatewayLimits::default()
    });
    let caller = add_session(&mut state);
    let stalled = add_session(&mut state);
    let sibling = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    publish(&mut state, stalled, destination_id)?;
    publish(&mut state, sibling, destination_id)?;
    let started = Instant::now();
    let actions = state.handle_at(
        caller,
        Frame::Dial {
            connection_id: 1,
            destination_id,
        },
        started,
    )?;
    let offered_to = sdk_frames(&actions)
        .find_map(|(target, frame)| matches!(frame, Frame::Offer { .. }).then_some(target))
        .ok_or("missing OFFER")?;

    let actions = state.expire_offers(started + Duration::from_millis(10));
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == caller
            && matches!(
                frame,
                Frame::DialFailed {
                    code: ErrorCode::DeadlineExceeded,
                    ..
                }
            )
    }));
    assert_eq!(state.snapshot().sessions, 2);
    assert_eq!(state.snapshot().bindings, 1);
    assert!(offered_to == stalled || offered_to == sibling);
    Ok(())
}

#[test]
fn session_removal_cleans_its_bindings_and_pipes_only() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let removed = add_session(&mut state);
    let sibling = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    publish(&mut state, removed, destination_id)?;
    publish(&mut state, sibling, destination_id)?;
    let actions = state.handle(
        caller,
        Frame::Dial {
            connection_id: 1,
            destination_id,
        },
    )?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    let offered_to = sdk_frames(&actions)
        .find_map(|(target, frame)| matches!(frame, Frame::Offer { .. }).then_some(target))
        .ok_or("missing OFFER target")?;
    state.handle(offered_to, Frame::OfferAccepted { pipe_id })?;

    let actions = state.remove_session(offered_to);
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == caller
            && matches!(
                frame,
                Frame::Reset {
                    code: ErrorCode::Unavailable,
                    ..
                }
            )
    }));
    assert_eq!(state.snapshot().sessions, 2);
    assert_eq!(state.snapshot().bindings, 1);
    assert_eq!(state.snapshot().live_pipes, 0);
    Ok(())
}

#[test]
fn duplicate_or_out_of_order_dial_identifiers_do_not_create_more_state() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    publish(&mut state, receiver, destination_id)?;
    let first = state.handle(
        caller,
        Frame::Dial {
            connection_id: 2,
            destination_id,
        },
    )?;
    assert!(offered_pipe(&first).is_some());
    assert!(
        state
            .handle(
                caller,
                Frame::Dial {
                    connection_id: 2,
                    destination_id,
                },
            )?
            .is_empty()
    );
    assert!(
        state
            .handle(
                caller,
                Frame::Dial {
                    connection_id: 1,
                    destination_id,
                },
            )?
            .is_empty()
    );
    assert_eq!(state.snapshot().pending_offers, 1);
    Ok(())
}

#[test]
fn foreign_session_cannot_control_an_existing_pipe() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let stranger = add_session(&mut state);
    let destination_id = destination(DESTINATION_A)?;
    publish(&mut state, receiver, destination_id)?;
    let actions = state.handle(
        caller,
        Frame::Dial {
            connection_id: 1,
            destination_id,
        },
    )?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    state.handle(receiver, Frame::OfferAccepted { pipe_id })?;

    let error = state
        .handle(
            stranger,
            Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"intrusion"),
            },
        )
        .err()
        .ok_or("foreign session controlled a Pipe")?;
    assert!(matches!(error, ProtocolViolation::PipeOwnership { .. }));
    assert_eq!(state.snapshot().live_pipes, 1);
    Ok(())
}

#[test]
fn resource_limits_reject_without_leaking_state() -> TestResult {
    let mut state = limited_state(GatewayLimits {
        max_sessions: 2,
        max_bindings: 1,
        max_pending_offers: 1,
        max_live_pipes: 1,
        ..GatewayLimits::default()
    });
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let (sender, _receiver) = mpsc::channel(1);
    assert!(
        state
            .add_session(sender, CancellationToken::new())
            .is_none()
    );
    let destination_a = destination(DESTINATION_A)?;
    let destination_b = destination(DESTINATION_B)?;
    publish(&mut state, receiver, destination_a)?;
    let actions = state.handle(
        receiver,
        Frame::Publish {
            request_id: 2,
            destination_id: destination_b,
        },
    )?;
    assert!(sdk_frames(&actions).any(|(_, frame)| {
        matches!(
            frame,
            Frame::PublishFailed {
                code: ErrorCode::ResourceExhausted,
                ..
            }
        )
    }));

    state.handle(
        caller,
        Frame::Dial {
            connection_id: 1,
            destination_id: destination_a,
        },
    )?;
    let actions = state.handle(
        caller,
        Frame::Dial {
            connection_id: 2,
            destination_id: destination_a,
        },
    )?;
    assert!(sdk_frames(&actions).any(|(_, frame)| {
        matches!(
            frame,
            Frame::DialFailed {
                code: ErrorCode::ResourceExhausted,
                ..
            }
        )
    }));
    assert_eq!(state.snapshot().bindings, 1);
    assert_eq!(state.snapshot().pending_offers, 1);
    Ok(())
}
