use std::{
    error::Error,
    str::FromStr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use relaygate_protocol::{
    BearerToken, BindingId, Destination, ErrorCode, Frame, PeerObservation, PipeId, SessionId,
};
use relaygate_route_table::GatewayId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Delivery, GatewayAction, GatewayLimits, GatewayState, ProtocolViolation};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const DESTINATION_A: &str = "11111111-1111-4111-8111-111111111111";
const DESTINATION_B: &str = "22222222-2222-4222-8222-222222222222";

fn destination(raw: &str) -> Result<Destination, relaygate_destination::DestinationError> {
    Destination::from_str(&format!("test/{raw}"))
}

#[allow(clippy::expect_used)]
fn access_token() -> BearerToken {
    BearerToken::new("state-test-token").expect("bounded test token")
}

trait GatewayStateTestExt {
    fn test_handle(
        &mut self,
        session_id: SessionId,
        frame: Frame,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation>;
}

impl GatewayStateTestExt for GatewayState {
    fn test_handle(
        &mut self,
        session_id: SessionId,
        frame: Frame,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        self.handle_at(session_id, frame, Instant::now())
    }
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
    destination: &Destination,
) -> TestResult<BindingId> {
    let actions = state.test_handle(
        session,
        Frame::Publish {
            request_id: 1,
            destination: destination.clone(),
            access_token: access_token(),
        },
    )?;
    published_binding(&actions).ok_or_else(|| "missing PUBLISHED response".into())
}

fn dial(connection_id: u64, destination: &Destination) -> Frame {
    Frame::Dial {
        connection_id,
        destination: destination.clone(),
        access_token: access_token(),
    }
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
    publish(&mut state, relay_a, &destination_a)?;
    publish(&mut state, relay_b, &destination_a)?;
    publish(&mut state, relay_b, &destination_b)?;

    let actions = state.test_handle(relay_a, dial(1, &destination_a))?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == relay_b
            && matches!(frame, Frame::Offer { destination, .. } if destination == &destination_a)
    }));

    let actions = state.test_handle(relay_a, dial(2, &destination_b))?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == relay_b
            && matches!(frame, Frame::Offer { destination, .. } if destination == &destination_b)
    }));
    Ok(())
}

#[test]
fn publish_is_idempotent_but_unpublish_then_publish_creates_a_new_binding() -> TestResult {
    let mut state = state();
    let relay = add_session(&mut state);
    let destination = destination(DESTINATION_A)?;
    let first = publish(&mut state, relay, &destination)?;
    let repeated = publish(&mut state, relay, &destination)?;
    assert_eq!(first, repeated);
    assert_eq!(state.snapshot().bindings, 1);

    state.test_handle(
        relay,
        Frame::Unpublish {
            request_id: 2,
            binding_id: first,
        },
    )?;
    assert_eq!(state.snapshot().bindings, 0);
    let replacement = publish(&mut state, relay, &destination)?;
    assert_ne!(first, replacement);
    Ok(())
}

#[test]
fn accepted_pipe_relays_data_and_closes_without_residue() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let destination = destination(DESTINATION_A)?;
    publish(&mut state, receiver, &destination)?;

    let actions = state.test_handle(caller, dial(7, &destination))?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    let actions = state.test_handle(receiver, Frame::OfferAccepted { pipe_id })?;
    assert!(sdk_frames(&actions).any(|(target, frame)| {
        target == caller && matches!(frame, Frame::Opened { pipe_id: opened } if *opened == pipe_id)
    }));
    assert_eq!(state.snapshot().live_pipes, 1);
    assert_eq!(state.snapshot().originated_pipes, 1);

    let actions = state.test_handle(
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

    state.test_handle(caller, Frame::Fin { pipe_id })?;
    state.test_handle(receiver, Frame::Fin { pipe_id })?;
    assert_eq!(state.snapshot().live_pipes, 0);
    assert_eq!(state.snapshot().originated_pipes, 0);
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
    let destination = destination(DESTINATION_A)?;
    publish(&mut state, stalled, &destination)?;
    publish(&mut state, sibling, &destination)?;
    let started = Instant::now();
    let actions = state.handle_at(caller, dial(1, &destination), started)?;
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
    let destination = destination(DESTINATION_A)?;
    publish(&mut state, removed, &destination)?;
    publish(&mut state, sibling, &destination)?;
    let actions = state.test_handle(caller, dial(1, &destination))?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    let offered_to = sdk_frames(&actions)
        .find_map(|(target, frame)| matches!(frame, Frame::Offer { .. }).then_some(target))
        .ok_or("missing OFFER target")?;
    state.test_handle(offered_to, Frame::OfferAccepted { pipe_id })?;

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
fn duplicate_or_out_of_order_dial_identifiers_fail_without_creating_more_state() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let destination = destination(DESTINATION_A)?;
    publish(&mut state, receiver, &destination)?;
    let first = state.test_handle(caller, dial(2, &destination))?;
    assert!(offered_pipe(&first).is_some());
    for connection_id in [2, 1] {
        let rejected = state.test_handle(caller, dial(connection_id, &destination))?;
        assert!(sdk_frames(&rejected).any(|(target, frame)| {
            target == caller
                && matches!(
                    frame,
                    Frame::DialFailed {
                        connection_id: rejected_id,
                        code: ErrorCode::ProtocolError,
                        observation: PeerObservation::NotObserved,
                        ..
                    } if *rejected_id == connection_id
                )
        }));
    }
    assert_eq!(state.snapshot().pending_offers, 1);
    Ok(())
}

#[test]
fn foreign_session_cannot_control_an_existing_pipe() -> TestResult {
    let mut state = state();
    let caller = add_session(&mut state);
    let receiver = add_session(&mut state);
    let stranger = add_session(&mut state);
    let destination = destination(DESTINATION_A)?;
    publish(&mut state, receiver, &destination)?;
    let actions = state.test_handle(caller, dial(1, &destination))?;
    let pipe_id = offered_pipe(&actions).ok_or("missing OFFER")?;
    state.test_handle(receiver, Frame::OfferAccepted { pipe_id })?;

    let error = state
        .test_handle(
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
    publish(&mut state, receiver, &destination_a)?;
    let actions = state.test_handle(
        receiver,
        Frame::Publish {
            request_id: 2,
            destination: destination_b,
            access_token: access_token(),
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

    state.test_handle(caller, dial(1, &destination_a))?;
    let actions = state.test_handle(caller, dial(2, &destination_a))?;
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

#[test]
fn remote_dial_admission_rejects_before_resolve_and_releases_after_terminal_result() -> TestResult {
    let mut state = GatewayState::new_distributed(
        GatewayLimits {
            max_remote_dial_attempts: 1,
            ..GatewayLimits::default()
        },
        GatewayId::new(),
    );
    let caller = add_session(&mut state);
    let destination_a = destination(DESTINATION_A)?;
    let destination_b = destination(DESTINATION_B)?;

    let first = state.test_handle(caller, dial(1, &destination_a))?;
    let open_identity = first
        .iter()
        .find_map(|action| match action {
            GatewayAction::ResolveRoute { open_identity, .. } => Some(*open_identity),
            _ => None,
        })
        .ok_or("first remote DIAL did not start route resolution")?;
    assert_eq!(state.snapshot().remote_open_attempts, 1);

    let rejected = state.test_handle(caller, dial(2, &destination_b))?;
    assert!(sdk_frames(&rejected).any(|(target, frame)| {
        target == caller
            && matches!(
                frame,
                Frame::DialFailed {
                    code: ErrorCode::ResourceExhausted,
                    observation: PeerObservation::NotObserved,
                    ..
                }
            )
    }));
    assert_eq!(state.snapshot().sessions, 1);
    assert_eq!(state.snapshot().remote_open_attempts, 1);

    state.route_failed(open_identity, ErrorCode::Unavailable, "test route failure");
    assert_eq!(state.snapshot().remote_open_attempts, 0);

    let admitted = state.test_handle(caller, dial(3, &destination_b))?;
    assert!(
        admitted
            .iter()
            .any(|action| matches!(action, GatewayAction::ResolveRoute { .. }))
    );
    assert_eq!(state.snapshot().remote_open_attempts, 1);
    Ok(())
}

#[test]
fn remote_dial_admission_releases_all_slots_when_caller_session_ends() -> TestResult {
    let mut state = GatewayState::new_distributed(
        GatewayLimits {
            max_remote_dial_attempts: 2,
            ..GatewayLimits::default()
        },
        GatewayId::new(),
    );
    let caller = add_session(&mut state);

    for (connection_id, destination) in [
        (1, destination(DESTINATION_A)?),
        (2, destination(DESTINATION_B)?),
    ] {
        let actions = state.test_handle(caller, dial(connection_id, &destination))?;
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, GatewayAction::ResolveRoute { .. }))
        );
    }
    assert_eq!(state.snapshot().remote_open_attempts, 2);

    state.remove_session(caller);
    assert_eq!(state.snapshot().sessions, 0);
    assert_eq!(state.snapshot().remote_open_attempts, 0);
    assert!(state.is_drained());
    Ok(())
}
