use super::{
    Delivery, GatewayAction, GatewayLimits, GatewayState, PeerDelivery, ProtocolViolation,
};
use crate::{
    auth::ClientKeyStore,
    peer::{OpenIdentity, PeerStreamKey, PeerTransportId},
    registry::Binding,
};
use bytes::Bytes;
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode, Frame, PeerObservation, PipeId, SessionId, SessionRole,
};
use relaygate_route_table::{
    BindingId as RouteBindingId, BindingSet, ClientId as RouteClientId, GatewayId, GatewayLocator,
    ListenerSessionId, MappingEntry,
};
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

fn routed_state(gateway_id: GatewayId) -> GatewayState {
    GatewayState::new_distributed(
        ClientKeyStore::new([("echo.shared".to_owned(), "secret".to_owned())].into()),
        GatewayLimits::default(),
        gateway_id,
    )
}

fn peer_key(peer_gateway_id: GatewayId, raw_stream_id: u64) -> PeerStreamKey {
    PeerStreamKey::for_test(peer_gateway_id, PeerTransportId::new(), raw_stream_id)
}

fn binding_set(
    client_id: &str,
    gateway_id: GatewayId,
    listener_session_id: SessionId,
    binding_id: BindingId,
    locator: &str,
) -> Result<BindingSet, Box<dyn std::error::Error>> {
    Ok(BindingSet::from_entries(vec![MappingEntry::new(
        RouteClientId::new(client_id)?,
        gateway_id,
        ListenerSessionId::from_uuid(listener_session_id.as_uuid()),
        RouteBindingId::from_uuid(binding_id.as_uuid()),
        GatewayLocator::new(locator)?,
    )])?)
}

fn resolve_identity(actions: &[GatewayAction]) -> Option<OpenIdentity> {
    actions.iter().find_map(|action| match action {
        GatewayAction::ResolveRoute { open_identity, .. } => Some(*open_identity),
        _ => None,
    })
}

fn peer_deliveries(actions: &[GatewayAction]) -> impl Iterator<Item = &PeerDelivery> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::SendPeerFrame(delivery) => Some(delivery),
        _ => None,
    })
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

fn sdk_deliveries(actions: &[GatewayAction]) -> impl Iterator<Item = &Delivery> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::SendSdkFrame(delivery) => Some(delivery),
        GatewayAction::PublishRegistration { .. }
        | GatewayAction::ResolveRoute { .. }
        | GatewayAction::OpenPeer { .. }
        | GatewayAction::CancelPeerOpen { .. }
        | GatewayAction::SendPeerFrame(_) => None,
    })
}

fn first_sdk_delivery(actions: &[GatewayAction]) -> Option<&Delivery> {
    sdk_deliveries(actions).next()
}

fn publications(
    actions: &[GatewayAction],
) -> impl Iterator<Item = (relaygate_protocol::SessionId, &[Binding])> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::PublishRegistration {
            session_id,
            bindings,
        } => Some((*session_id, bindings.as_slice())),
        GatewayAction::SendSdkFrame(_)
        | GatewayAction::ResolveRoute { .. }
        | GatewayAction::OpenPeer { .. }
        | GatewayAction::CancelPeerOpen { .. }
        | GatewayAction::SendPeerFrame(_) => None,
    })
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
    let pipe_id = sdk_deliveries(&deliveries)
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

#[test]
fn routed_local_hit_never_resolves_or_opens_a_peer() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    let connector = add_session(&mut state, SessionRole::Connector);
    register_listener(&mut state, listener)?;

    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;

    assert!(actions.iter().all(|action| {
        !matches!(
            action,
            GatewayAction::ResolveRoute { .. } | GatewayAction::OpenPeer { .. }
        )
    }));
    assert!(sdk_deliveries(&actions).any(|delivery| {
        delivery.target == listener && matches!(delivery.frame, Frame::Offer { .. })
    }));
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn routed_miss_creates_one_request_local_resolve() -> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);

    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 7,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&actions).ok_or("missing ResolveRoute action")?;

    assert_eq!(identity.entry_gateway(), entry_gateway);
    assert_eq!(identity.connector_session(), connector);
    assert_eq!(identity.connection_id(), 7);
    assert_eq!(state.remote_open_attempt_count(), 1);
    assert!(
        state
            .handle(
                connector,
                Frame::Open {
                    connection_id: 7,
                    client_id: "echo.shared".to_owned(),
                },
            )?
            .is_empty()
    );
    assert_eq!(state.remote_open_attempt_count(), 1);
    Ok(())
}

#[test]
fn late_resolve_cannot_resurrect_a_closed_connector_session()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let actions = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&actions).ok_or("missing ResolveRoute action")?;
    state.remove_session(connector);

    let late = state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            GatewayId::new(),
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );

    assert!(late.is_empty());
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn selected_stale_local_mapping_fails_without_fallback() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_id = GatewayId::new();
    let mut state = routed_state(gateway_id);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;

    let actions = state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            gateway_id,
            SessionId::new(),
            BindingId::new(),
            "self.internal:27421",
        )?,
    );

    assert!(sdk_deliveries(&actions).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::Unavailable,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    assert!(
        actions
            .iter()
            .all(|action| !matches!(action, GatewayAction::OpenPeer { .. }))
    );
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn owner_rejects_only_active_duplicate_open_identity() -> Result<(), Box<dyn std::error::Error>> {
    let owner_gateway = GatewayId::new();
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(owner_gateway);
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let identity = OpenIdentity::new(entry_gateway, SessionId::new(), 1);
    let first_key = peer_key(entry_gateway, 0);
    let duplicate_key = peer_key(entry_gateway, 2);

    let first = state.receive_peer_open(
        first_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(sdk_deliveries(&first).any(|delivery| matches!(delivery.frame, Frame::Offer { .. })));
    assert_eq!(state.remote_open_attempt_count(), 0);

    let duplicate = state.receive_peer_open(
        duplicate_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(peer_deliveries(&duplicate).any(|delivery| matches!(
        delivery,
        PeerDelivery::Failed {
            code: ErrorCode::AlreadyExists,
            ..
        }
    )));
    let pipe_id = PipeId::new(identity.connector_session(), identity.connection_id());
    state.handle(
        listener,
        Frame::OfferRejected {
            pipe_id,
            code: ErrorCode::Unavailable,
            message: "not ready".to_owned(),
        },
    )?;
    assert_eq!(state.pipe_count(), 0);

    let reused_after_terminal = state.receive_peer_open(
        duplicate_key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );
    assert!(
        sdk_deliveries(&reused_after_terminal)
            .any(|delivery| matches!(delivery.frame, Frame::Offer { .. }))
    );
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn peer_open_identity_must_match_authenticated_peer_gateway()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let claimed_entry = GatewayId::new();
    let authenticated_peer = GatewayId::new();

    let actions = state.receive_peer_open(
        peer_key(authenticated_peer, 0),
        OpenIdentity::new(claimed_entry, SessionId::new(), 1),
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );

    assert!(peer_deliveries(&actions).any(|delivery| matches!(
        delivery,
        PeerDelivery::Failed {
            code: ErrorCode::PermissionDenied,
            observation: PeerObservation::NotObserved,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn early_opened_then_late_commit_callback_is_idempotent() -> Result<(), Box<dyn std::error::Error>>
{
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    let target_binding = BindingId::new();
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            target_binding,
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let opened = state.peer_opened(key, identity);
    assert!(sdk_deliveries(&opened).any(|delivery| matches!(delivery.frame, Frame::Opened { .. })));
    assert_eq!(state.pipe_count(), 1);
    assert_eq!(state.remote_open_attempt_count(), 0);

    assert!(state.peer_open_committed(identity, key).is_empty());
    assert_eq!(state.pipe_count(), 1);
    Ok(())
}

#[test]
fn early_failed_then_late_commit_callback_never_resurrects_state()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let failed = state.peer_open_failed(
        key,
        identity,
        ErrorCode::Unavailable,
        PeerObservation::NotObserved,
        "owner unavailable",
    );
    assert!(
        sdk_deliveries(&failed).any(|delivery| matches!(delivery.frame, Frame::OpenFailed { .. }))
    );
    assert_eq!(state.remote_open_attempt_count(), 0);

    let late = state.peer_open_committed(identity, key);
    assert!(peer_deliveries(&late).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            code: ErrorCode::Cancelled,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn early_transport_loss_then_late_commit_callback_never_leaves_an_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);

    let lost = state.peer_transport_lost_stream(key, identity, PeerObservation::MaybeObserved);
    assert!(sdk_deliveries(&lost).any(|delivery| matches!(
        delivery.frame,
        Frame::OpenFailed {
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            ..
        }
    )));
    assert_eq!(state.remote_open_attempt_count(), 0);

    let _late = state.peer_open_committed(identity, key);
    assert_eq!(state.remote_open_attempt_count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn connector_session_cleanup_resets_committed_peer_stream_as_cancelled()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);
    state.peer_open_committed(identity, key);
    state.peer_opened(key, identity);

    let cleanup = state.remove_session(connector);

    assert!(peer_deliveries(&cleanup).any(|delivery| matches!(
        delivery,
        PeerDelivery::Reset {
            key: reset_key,
            code: ErrorCode::Cancelled,
            ..
        } if *reset_key == key
    )));
    assert_eq!(state.pipe_count(), 0);
    assert_eq!(state.remote_open_attempt_count(), 0);
    Ok(())
}

#[test]
fn peer_transport_loss_is_scoped_to_its_current_streams() -> Result<(), Box<dyn std::error::Error>>
{
    let entry_gateway = GatewayId::new();
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(entry_gateway);
    let first_connector = add_session(&mut state, SessionRole::Connector);
    let second_connector = add_session(&mut state, SessionRole::Connector);
    let mut identities = Vec::new();
    let mut keys = Vec::new();
    for (connector, connection_id, raw_stream_id) in
        [(first_connector, 1, 0), (second_connector, 1, 2)]
    {
        let resolve = state.handle(
            connector,
            Frame::Open {
                connection_id,
                client_id: "echo.shared".to_owned(),
            },
        )?;
        let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
        state.route_resolved(
            identity,
            binding_set(
                "echo.shared",
                owner_gateway,
                SessionId::new(),
                BindingId::new(),
                "owner.internal:27421",
            )?,
        );
        let key = peer_key(owner_gateway, raw_stream_id);
        state.peer_open_committed(identity, key);
        state.peer_opened(key, identity);
        identities.push(identity);
        keys.push(key);
    }
    assert_eq!(state.pipe_count(), 2);

    let lost =
        state.peer_transport_lost_stream(keys[0], identities[0], PeerObservation::MaybeObserved);
    assert!(sdk_deliveries(&lost).any(|delivery| delivery.target == first_connector));
    assert_eq!(state.pipe_count(), 1);

    let surviving_pipe = PipeId::new(second_connector, 1);
    let relay = state.handle(
        second_connector,
        Frame::Data {
            pipe_id: surviving_pipe,
            payload: Bytes::from_static(b"still-live"),
        },
    )?;
    assert!(peer_deliveries(&relay).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { key, payload }
            if *key == keys[1] && payload.as_ref() == b"still-live"
    )));
    Ok(())
}

#[test]
fn owner_peer_reset_cancels_an_offered_pipe_without_bouncing_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let entry_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let listener = add_session(&mut state, SessionRole::Listener);
    register_listener(&mut state, listener)?;
    let binding = state
        .registry
        .bindings_for_session(listener)
        .into_iter()
        .next()
        .ok_or("missing binding")?;
    let identity = OpenIdentity::new(entry_gateway, SessionId::new(), 1);
    let key = peer_key(entry_gateway, 0);
    state.receive_peer_open(
        key,
        identity,
        "echo.shared".to_owned(),
        listener,
        binding.id,
    );

    let actions = state.peer_reset(key, ErrorCode::Cancelled, "entry cancelled".to_owned());

    assert!(sdk_deliveries(&actions).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::Cancelled,
            ..
        }
    )));
    assert_eq!(peer_deliveries(&actions).count(), 0);
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}

#[test]
fn remote_pipe_data_fin_close_and_reset_are_symmetric() -> Result<(), Box<dyn std::error::Error>> {
    let owner_gateway = GatewayId::new();
    let mut state = routed_state(GatewayId::new());
    let connector = add_session(&mut state, SessionRole::Connector);
    let resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let identity = resolve_identity(&resolve).ok_or("missing resolve")?;
    state.route_resolved(
        identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let key = peer_key(owner_gateway, 0);
    state.peer_open_committed(identity, key);
    state.peer_opened(key, identity);
    let pipe_id = PipeId::new(connector, 1);

    let sdk_data = state.handle(
        connector,
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"sdk-to-peer"),
        },
    )?;
    assert!(peer_deliveries(&sdk_data).any(|delivery| matches!(
        delivery,
        PeerDelivery::Data { payload, .. } if payload.as_ref() == b"sdk-to-peer"
    )));
    let peer_data = state.peer_data(key, Bytes::from_static(b"peer-to-sdk"));
    assert!(sdk_deliveries(&peer_data).any(|delivery| matches!(
        &delivery.frame,
        Frame::Data { payload, .. } if payload.as_ref() == b"peer-to-sdk"
    )));

    let sdk_fin = state.handle(connector, Frame::Fin { pipe_id })?;
    assert!(peer_deliveries(&sdk_fin).any(|delivery| matches!(delivery, PeerDelivery::Fin { .. })));
    let peer_fin = state.peer_fin(key);
    assert!(sdk_deliveries(&peer_fin).any(|delivery| matches!(delivery.frame, Frame::Fin { .. })));
    assert_eq!(state.pipe_count(), 0);

    let second_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 2,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let second_identity = resolve_identity(&second_resolve).ok_or("missing second resolve")?;
    state.route_resolved(
        second_identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let second_key = peer_key(owner_gateway, 2);
    state.peer_open_committed(second_identity, second_key);
    state.peer_opened(second_key, second_identity);
    let close = state.peer_close(second_key);
    assert!(sdk_deliveries(&close).any(|delivery| matches!(delivery.frame, Frame::Close { .. })));

    let third_resolve = state.handle(
        connector,
        Frame::Open {
            connection_id: 3,
            client_id: "echo.shared".to_owned(),
        },
    )?;
    let third_identity = resolve_identity(&third_resolve).ok_or("missing third resolve")?;
    state.route_resolved(
        third_identity,
        binding_set(
            "echo.shared",
            owner_gateway,
            SessionId::new(),
            BindingId::new(),
            "owner.internal:27421",
        )?,
    );
    let third_key = peer_key(owner_gateway, 4);
    state.peer_open_committed(third_identity, third_key);
    state.peer_opened(third_key, third_identity);
    let reset = state.peer_reset(third_key, ErrorCode::Unavailable, "peer failed".to_owned());
    assert!(sdk_deliveries(&reset).any(|delivery| matches!(
        delivery.frame,
        Frame::Reset {
            code: ErrorCode::Unavailable,
            ..
        }
    )));
    assert_eq!(state.pipe_count(), 0);
    Ok(())
}
