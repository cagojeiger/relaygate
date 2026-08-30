use super::{Delivery, GatewayLimits, GatewayState, ProtocolViolation};
use crate::auth::ClientKeyStore;
use bytes::Bytes;
use relaygate_protocol::{ClientKey, ErrorCode, Frame, PipeId, SessionRole};
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct CapturedWriter {
    output: Arc<Mutex<Vec<u8>>>,
}

impl Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self.output.lock() {
            Ok(mut output) => output.extend_from_slice(buffer),
            Err(poisoned) => poisoned.into_inner().extend_from_slice(buffer),
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn state() -> GatewayState {
    GatewayState::new(
        ClientKeyStore::new([("echo.shared".to_owned(), "secret".to_owned())].into()),
        GatewayLimits::default(),
    )
}

fn limited_state(limits: GatewayLimits) -> GatewayState {
    GatewayState::new(
        ClientKeyStore::new(
            [
                ("echo.shared".to_owned(), "secret".to_owned()),
                ("echo.other".to_owned(), "other-secret".to_owned()),
            ]
            .into(),
        ),
        limits,
    )
}

fn add_session(state: &mut GatewayState, role: SessionRole) -> relaygate_protocol::SessionId {
    add_session_with_cancellation(state, role, CancellationToken::new())
}

fn add_session_with_cancellation(
    state: &mut GatewayState,
    role: SessionRole,
    cancellation: CancellationToken,
) -> relaygate_protocol::SessionId {
    let (sender, _receiver) = mpsc::channel(8);
    let session_id = state.add_session(role, sender, cancellation);
    assert!(session_id.is_some(), "test session limit was reached");
    session_id.unwrap_or_default()
}

fn register_listener(
    state: &mut GatewayState,
    listener: relaygate_protocol::SessionId,
) -> Result<(), Box<dyn std::error::Error>> {
    state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.shared".to_owned(),
            client_key: ClientKey::new("secret"),
        },
    )?;
    Ok(())
}

fn open_pipe(
    state: &mut GatewayState,
    connector: relaygate_protocol::SessionId,
    listener: relaygate_protocol::SessionId,
    connection_id: u64,
) -> Result<PipeId, Box<dyn std::error::Error>> {
    let pipe_id = offer_pipe(state, connector, listener, connection_id)?;
    state.handle(listener, Frame::OfferAccepted { pipe_id })?;
    Ok(pipe_id)
}

fn offer_pipe(
    state: &mut GatewayState,
    connector: relaygate_protocol::SessionId,
    listener: relaygate_protocol::SessionId,
    connection_id: u64,
) -> Result<PipeId, Box<dyn std::error::Error>> {
    let deliveries = state.handle(
        connector,
        Frame::Open {
            connection_id,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let pipe_id = deliveries
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } if delivery.target == listener => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing offer")?;
    Ok(pipe_id)
}

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
    assert!(matches!(
        deliveries.first().map(|delivery| &delivery.frame),
        Some(Frame::RegisterFailed {
            code: ErrorCode::Unauthenticated,
            ..
        })
    ));
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
    assert_ne!(first[0].target, second[0].target);
    assert!(matches!(first[0].frame, Frame::Offer { .. }));
    assert!(matches!(second[0].frame, Frame::Offer { .. }));
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
    assert!(failures.iter().any(|delivery| {
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
        opened.first().map(|delivery| (&delivery.target, &delivery.frame)),
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

        assert!(state.remove_session(offender).is_empty());
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
        assert!(resets.iter().all(|delivery| matches!(
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
        assert!(resets.iter().all(|delivery| matches!(
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
        resets.first().map(|delivery| (&delivery.target, &delivery.frame)),
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
            deliveries.first().map(|delivery| &delivery.frame),
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
fn close_reset_and_late_frames_are_pipe_local() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state();
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let closed = open_pipe(&mut state, connector, listener, 1)?;
    let close = state.handle(connector, Frame::Close { pipe_id: closed })?;
    assert!(matches!(
        close.first().map(|delivery| (&delivery.target, &delivery.frame)),
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
        resets.first().map(|delivery| (&delivery.target, &delivery.frame)),
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
    assert!(resets.iter().all(|delivery| matches!(
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
    let old_binding = registered
        .iter()
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
    let old_pipe = offers
        .iter()
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
    assert!(removed.iter().any(|delivery| matches!(
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
    let new_binding = renewed
        .iter()
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
    let new_pipe = new_offer
        .iter()
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
        removed_after_open
            .iter()
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
    let pending = offered
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing pending offer")?;
    let cancelled = state.handle(connector, Frame::Cancel { pipe_id: pending })?;
    assert_eq!(cancelled.len(), 1);
    assert!(matches!(
        cancelled[0].frame,
        Frame::Reset {
            code: ErrorCode::Cancelled,
            ..
        }
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
    assert!(matches!(closed[0].frame, Frame::Close { .. }));
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
    let binding_id = registered
        .iter()
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
        repeated.first().map(|delivery| &delivery.frame),
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
        exhausted.first().map(|delivery| &delivery.frame),
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
        first.first().map(|delivery| &delivery.frame),
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
        second.first().map(|delivery| &delivery.frame),
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
    let first_pipe = first_offer
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing first offer")?;
    let second_pipe = second_offer
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing second offer")?;

    assert!(matches!(
        state
            .handle(
                listener,
                Frame::OfferAccepted {
                    pipe_id: first_pipe
                }
            )?
            .first()
            .map(|delivery| &delivery.frame),
        Some(Frame::Opened { .. })
    ));
    let exhausted = state.handle(
        listener,
        Frame::OfferAccepted {
            pipe_id: second_pipe,
        },
    )?;
    assert!(exhausted.iter().any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::ResourceExhausted,
            observation: relaygate_protocol::PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert!(exhausted.iter().any(|delivery| matches!(
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
    let live_pipe = live_offer
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } if delivery.target == listener => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing live offer")?;
    state.handle(listener, Frame::OfferAccepted { pipe_id: live_pipe })?;
    let before_open = Instant::now();
    let offered = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let expired_pipe = offered
        .iter()
        .find_map(|delivery| match delivery.frame {
            Frame::Offer { pipe_id, .. } => Some(pipe_id),
            _ => None,
        })
        .ok_or("missing offer")?;

    let expired = state.expire_offers(before_open + Duration::from_millis(20));
    assert!(expired.iter().any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            connection_id: 2,
            code: ErrorCode::DeadlineExceeded,
            observation: relaygate_protocol::PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert!(expired.iter().any(|delivery| {
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
    assert!(next.iter().any(|delivery| {
        delivery.target == sibling && matches!(delivery.frame, Frame::Offer { .. })
    }));
    Ok(())
}
