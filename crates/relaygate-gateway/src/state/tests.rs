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

include!("tests/registration.rs");
include!("tests/pipe_protocol.rs");
include!("tests/limits.rs");
include!("tests/routing.rs");
include!("tests/peer.rs");
