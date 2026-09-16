//! Synchronous coverage of the remote-OPEN phase machine in `remote.rs`:
//! `Resolving -> StartingPeer -> AwaitingPeer` and the concurrent peer/route
//! events that may arrive in each phase.

use std::{error::Error, str::FromStr, time::Instant};

use relaygate_protocol::{
    BearerToken, BindingId, Destination, ErrorCode, Frame, PeerObservation, PipeId, SessionId,
};
use relaygate_route_table::{
    BindingId as RouteBindingId, BindingProjection, BindingSet, GatewayId, GatewayLocator,
    RelaySessionId,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Delivery, GatewayAction, GatewayLimits, GatewayState};
use crate::peer::{OpenIdentity, PeerStreamKey, PeerTransportId, StreamId};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const DESTINATION: &str = "test/11111111-1111-4111-8111-111111111111";

struct Fixture {
    state: GatewayState,
    caller: SessionId,
    destination: Destination,
    peer_gateway: GatewayId,
    peer_transport: PeerTransportId,
}

impl Fixture {
    fn new() -> TestResult<Self> {
        let mut state = GatewayState::new_distributed(GatewayLimits::default(), GatewayId::new());
        let caller = add_session(&mut state);
        Ok(Self {
            state,
            caller,
            destination: Destination::from_str(DESTINATION)?,
            peer_gateway: GatewayId::new(),
            peer_transport: PeerTransportId::new(),
        })
    }

    fn key(&self, stream: u64) -> PeerStreamKey {
        PeerStreamKey::new(
            self.peer_gateway,
            self.peer_transport,
            StreamId::from_raw(stream),
        )
    }

    /// Local DIAL with no local Binding: the attempt enters `Resolving`.
    fn dial(&mut self, connection_id: u64) -> TestResult<(OpenIdentity, PipeId)> {
        let actions = self.state.handle_at(
            self.caller,
            Frame::Dial {
                connection_id,
                destination: self.destination.clone(),
                access_token: access_token(),
            },
            Instant::now(),
        )?;
        let open_identity = actions
            .iter()
            .find_map(|action| match action {
                GatewayAction::ResolveRoute { open_identity, .. } => Some(*open_identity),
                _ => None,
            })
            .ok_or("DIAL did not start route resolution")?;
        Ok((open_identity, PipeId::new(self.caller, connection_id)))
    }

    /// Route resolution selects a Binding on the peer Gateway: `StartingPeer`.
    fn resolve_to_peer(&mut self, open_identity: OpenIdentity) -> TestResult<BindingId> {
        let projection = BindingProjection::new(
            self.destination.clone(),
            self.peer_gateway,
            RelaySessionId::new(),
            RouteBindingId::new(),
            GatewayLocator::new("gw-b.internal:27431")?,
        );
        let actions = self
            .state
            .route_resolved(open_identity, BindingSet::from_entries(vec![projection])?);
        actions
            .iter()
            .find_map(|action| match action {
                GatewayAction::OpenPeer {
                    open_identity: started,
                    gateway_id,
                    binding_id,
                    ..
                } if *started == open_identity && *gateway_id == self.peer_gateway => {
                    Some(*binding_id)
                }
                _ => None,
            })
            .ok_or_else(|| "route resolution did not start the peer OPEN".into())
    }

    /// `Resolving -> StartingPeer -> AwaitingPeer` on `key`.
    fn dial_until_awaiting_peer(
        &mut self,
        connection_id: u64,
        key: PeerStreamKey,
    ) -> TestResult<(OpenIdentity, PipeId)> {
        let (open_identity, pipe_id) = self.dial(connection_id)?;
        self.resolve_to_peer(open_identity)?;
        let committed = self.state.peer_open_committed(open_identity, key);
        assert!(
            committed.is_empty(),
            "commit on the started key must be silent"
        );
        Ok((open_identity, pipe_id))
    }

    fn attempts(&self) -> usize {
        self.state.snapshot().remote_open_attempts
    }

    fn live_pipes(&self) -> usize {
        self.state.snapshot().live_pipes
    }
}

fn add_session(state: &mut GatewayState) -> SessionId {
    let (sender, _receiver) = mpsc::channel(16);
    state
        .add_session(sender, CancellationToken::new())
        .unwrap_or_default()
}

#[allow(clippy::expect_used)]
fn access_token() -> BearerToken {
    BearerToken::new("remote-test-token").expect("bounded test token")
}

fn sdk_frames(actions: &[GatewayAction]) -> impl Iterator<Item = (SessionId, &Frame)> {
    actions.iter().filter_map(|action| match action {
        GatewayAction::SendSdkFrame(Delivery { target, frame, .. }) => Some((*target, frame)),
        _ => None,
    })
}

fn dial_failed(
    actions: &[GatewayAction],
    target: SessionId,
) -> Option<(ErrorCode, PeerObservation)> {
    sdk_frames(actions).find_map(|(to, frame)| match frame {
        Frame::DialFailed {
            code, observation, ..
        } if to == target => Some((*code, *observation)),
        _ => None,
    })
}

fn opened(actions: &[GatewayAction], target: SessionId) -> Option<PipeId> {
    sdk_frames(actions).find_map(|(to, frame)| match frame {
        Frame::Opened { pipe_id } if to == target => Some(*pipe_id),
        _ => None,
    })
}

#[test]
fn resolve_commit_and_opened_create_the_relayed_pipe() -> TestResult {
    let mut fx = Fixture::new()?;
    let key = fx.key(0);
    let (open_identity, pipe_id) = fx.dial_until_awaiting_peer(1, key)?;
    assert_eq!(fx.attempts(), 1);

    let actions = fx.state.peer_opened_at(key, open_identity, Instant::now());
    assert_eq!(opened(&actions, fx.caller), Some(pipe_id));
    assert_eq!(fx.attempts(), 0);
    assert_eq!(fx.live_pipes(), 1);
    Ok(())
}

#[test]
fn route_failure_while_resolving_fails_the_dial_once() -> TestResult {
    let mut fx = Fixture::new()?;
    let (open_identity, _) = fx.dial(1)?;

    let actions = fx
        .state
        .route_failed(open_identity, ErrorCode::Unavailable, "shard down");
    assert_eq!(
        dial_failed(&actions, fx.caller),
        Some((ErrorCode::Unavailable, PeerObservation::NotObserved))
    );
    assert_eq!(fx.attempts(), 0);

    // A second report for the ended attempt is inert.
    assert!(
        fx.state
            .route_failed(open_identity, ErrorCode::Unavailable, "again")
            .is_empty()
    );
    Ok(())
}

#[test]
fn route_resolved_with_a_foreign_destination_is_a_precondition_failure() -> TestResult {
    let mut fx = Fixture::new()?;
    let (open_identity, _) = fx.dial(1)?;
    let projection = BindingProjection::new(
        Destination::from_str("test/22222222-2222-4222-8222-222222222222")?,
        fx.peer_gateway,
        RelaySessionId::new(),
        RouteBindingId::new(),
        GatewayLocator::new("gw-b.internal:27431")?,
    );

    let actions = fx
        .state
        .route_resolved(open_identity, BindingSet::from_entries(vec![projection])?);
    assert_eq!(
        dial_failed(&actions, fx.caller),
        Some((ErrorCode::FailedPrecondition, PeerObservation::NotObserved))
    );
    assert_eq!(fx.attempts(), 0);
    Ok(())
}

#[test]
fn route_resolved_to_this_gateway_offers_locally_or_fails_when_stale() -> TestResult {
    let mut fx = Fixture::new()?;
    // Both DIALs enter `Resolving` before any local Binding exists.
    let (fresh, _) = fx.dial(1)?;
    let (stale, _) = fx.dial(2)?;
    let listener = add_session(&mut fx.state);
    let published = fx.state.handle_at(
        listener,
        Frame::Publish {
            request_id: 1,
            destination: fx.destination.clone(),
            access_token: access_token(),
        },
        Instant::now(),
    )?;
    let binding_id = sdk_frames(&published)
        .find_map(|(_, frame)| match frame {
            Frame::Published { binding_id, .. } => Some(*binding_id),
            _ => None,
        })
        .ok_or("listener did not publish")?;
    let own_gateway = fx
        .state
        .gateway_id
        .ok_or("distributed state has a gateway id")?;
    let locator = GatewayLocator::new("gw-a.internal:27431")?;

    let current = BindingProjection::new(
        fx.destination.clone(),
        own_gateway,
        RelaySessionId::from_uuid(listener.as_uuid()),
        RouteBindingId::from_uuid(binding_id.as_uuid()),
        locator.clone(),
    );
    let actions = fx
        .state
        .route_resolved(fresh, BindingSet::from_entries(vec![current])?);
    assert!(
        sdk_frames(&actions)
            .any(|(to, frame)| to == listener && matches!(frame, Frame::Offer { .. })),
        "own-gateway projection must fall back to a local OFFER"
    );
    assert_eq!(fx.attempts(), 1);

    let outdated = BindingProjection::new(
        fx.destination.clone(),
        own_gateway,
        RelaySessionId::from_uuid(listener.as_uuid()),
        RouteBindingId::from_uuid(Uuid::new_v4()),
        locator,
    );
    let actions = fx
        .state
        .route_resolved(stale, BindingSet::from_entries(vec![outdated])?);
    assert_eq!(
        dial_failed(&actions, fx.caller),
        Some((ErrorCode::Unavailable, PeerObservation::NotObserved))
    );
    assert_eq!(fx.attempts(), 0);
    Ok(())
}

#[test]
fn late_peer_opened_after_cancel_does_not_recreate_the_pipe() -> TestResult {
    let mut fx = Fixture::new()?;
    let key = fx.key(0);
    let (open_identity, pipe_id) = fx.dial_until_awaiting_peer(1, key)?;

    let cancelled = fx.state.cancel_remote_attempt(fx.caller, pipe_id);
    assert!(!cancelled.is_empty(), "cancel must reset the peer stream");
    assert_eq!(fx.attempts(), 0);

    let late = fx.state.peer_opened_at(key, open_identity, Instant::now());
    assert!(opened(&late, fx.caller).is_none());
    assert!(!late.is_empty(), "late OPENED must reset the peer stream");
    assert_eq!(fx.live_pipes(), 0);
    Ok(())
}

#[test]
fn cancel_while_starting_peer_cancels_the_peer_open() -> TestResult {
    let mut fx = Fixture::new()?;
    let (open_identity, pipe_id) = fx.dial(1)?;
    fx.resolve_to_peer(open_identity)?;

    let actions = fx.state.cancel_remote_attempt(fx.caller, pipe_id);
    assert!(actions.iter().any(|action| matches!(
        action,
        GatewayAction::CancelPeerOpen { open_identity: cancelled } if *cancelled == open_identity
    )));
    assert_eq!(fx.attempts(), 0);

    // The peer commit that races the cancel is answered with a reset, not a pipe.
    let raced = fx.state.peer_open_committed(open_identity, fx.key(0));
    assert!(!raced.is_empty());
    assert_eq!(fx.live_pipes(), 0);
    Ok(())
}

#[test]
fn a_second_commit_with_another_key_is_rejected_without_disturbing_the_first() -> TestResult {
    let mut fx = Fixture::new()?;
    let first = fx.key(0);
    let second = fx.key(2);
    let (open_identity, pipe_id) = fx.dial_until_awaiting_peer(1, first)?;

    let rejected = fx.state.peer_open_committed(open_identity, second);
    assert!(!rejected.is_empty(), "second key must be reset");
    assert_eq!(fx.attempts(), 1);

    let actions = fx
        .state
        .peer_opened_at(first, open_identity, Instant::now());
    assert_eq!(opened(&actions, fx.caller), Some(pipe_id));
    assert_eq!(fx.live_pipes(), 1);
    Ok(())
}

#[test]
fn peer_failures_on_a_stale_key_are_ignored_until_the_current_key_fails() -> TestResult {
    let mut fx = Fixture::new()?;
    let current = fx.key(0);
    let stale = fx.key(2);
    let (open_identity, _) = fx.dial_until_awaiting_peer(1, current)?;

    assert!(
        fx.state
            .peer_open_failed(
                stale,
                open_identity,
                ErrorCode::Unavailable,
                PeerObservation::NotObserved,
                "stale"
            )
            .is_empty()
    );
    assert!(
        fx.state
            .peer_transport_lost_stream(stale, open_identity, PeerObservation::NotObserved)
            .is_empty()
    );
    assert_eq!(fx.attempts(), 1);

    let actions =
        fx.state
            .peer_transport_lost_stream(current, open_identity, PeerObservation::MaybeObserved);
    assert_eq!(
        dial_failed(&actions, fx.caller),
        Some((ErrorCode::Unavailable, PeerObservation::MaybeObserved))
    );
    assert_eq!(fx.attempts(), 0);
    Ok(())
}

#[test]
fn peer_opened_after_the_dialer_left_resets_the_stream() -> TestResult {
    let mut fx = Fixture::new()?;
    let key = fx.key(0);
    let (open_identity, _) = fx.dial_until_awaiting_peer(1, key)?;

    fx.state.remove_session(fx.caller);
    assert_eq!(fx.attempts(), 0, "session removal ends its remote attempts");

    let actions = fx.state.peer_opened_at(key, open_identity, Instant::now());
    assert!(opened(&actions, fx.caller).is_none());
    assert!(!actions.is_empty(), "the peer stream must be reset");
    assert_eq!(fx.live_pipes(), 0);
    Ok(())
}
