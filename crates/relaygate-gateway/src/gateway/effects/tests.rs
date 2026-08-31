use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::future::BoxFuture;
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode, Frame, PeerObservation, SessionId, SessionRole,
};
use relaygate_route_table::{
    BindingId as RouteBindingId, BindingSet, ClientId, GatewayId, GatewayLocator,
    ListenerSessionId, MappingEntry,
};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    auth::ClientKeyStore,
    peer::{PeerStreamKey, PeerTransportId},
    state::{GatewayAction, GatewayLimits, GatewayState},
};

use super::{ControlAction, ControlEffects};
use crate::gateway::{
    Inner,
    route_resolver::{RouteResolveFailure, RouteResolver},
};

type ResolveResult = Result<BindingSet, RouteResolveFailure>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

mod peer_cleanup;
mod peer_liveness;

#[derive(Clone)]
struct ScriptedResolver {
    results: Arc<Mutex<VecDeque<ResolveResult>>>,
    calls: Arc<AtomicUsize>,
}

impl ScriptedResolver {
    fn new(results: impl IntoIterator<Item = ResolveResult>) -> Self {
        Self {
            results: Arc::new(Mutex::new(results.into_iter().collect())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn push(&self, result: ResolveResult) {
        match self.results.lock() {
            Ok(mut results) => results.push_back(result),
            Err(poisoned) => poisoned.into_inner().push_back(result),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl RouteResolver for ScriptedResolver {
    fn resolve(&self, _client_id: ClientId) -> BoxFuture<'_, ResolveResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = match self.results.lock() {
            Ok(mut results) => results.pop_front(),
            Err(poisoned) => poisoned.into_inner().pop_front(),
        }
        .unwrap_or_else(|| {
            Err(RouteResolveFailure::new(
                ErrorCode::Internal,
                "scripted resolver has no result",
            ))
        });
        Box::pin(async move { result })
    }
}

#[tokio::test]
async fn generation_mismatch_resolve_fails_once_without_remote_state() -> TestResult {
    let resolver = ScriptedResolver::new([Err(RouteResolveFailure::new(
        ErrorCode::FailedPrecondition,
        "ShardDirectory generation mismatch",
    ))]);
    let inner = test_inner(Arc::new(resolver.clone()));
    let connector = add_session(&inner, SessionRole::Connector)?;
    let (open_identity, client_id) = begin_remote_open(&inner, connector, 1, "echo.remote")?;

    let actions = inner
        .run_control_effect(ControlAction::ResolveRoute {
            open_identity,
            client_id,
        })
        .await;

    assert_open_failed(&actions, 1, ErrorCode::FailedPrecondition);
    assert_terminal_remote_failure(&inner);
    assert_eq!(resolver.call_count(), 1);
    Ok(())
}

#[tokio::test]
async fn component_identity_resolve_failures_never_create_peer_or_pipe_state() -> TestResult {
    for code in [ErrorCode::Unauthenticated, ErrorCode::PermissionDenied] {
        let resolver = ScriptedResolver::new([Err(RouteResolveFailure::new(
            code,
            "RouteTable component identity rejected",
        ))]);
        let inner = test_inner(Arc::new(resolver.clone()));
        let connector = add_session(&inner, SessionRole::Connector)?;
        let (open_identity, client_id) = begin_remote_open(&inner, connector, 1, "echo.remote")?;

        let actions = inner
            .run_control_effect(ControlAction::ResolveRoute {
                open_identity,
                client_id,
            })
            .await;

        assert_open_failed(&actions, 1, code);
        assert_terminal_remote_failure(&inner);
        assert_eq!(resolver.call_count(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn identity_failure_preserves_local_state_and_only_new_open_resolves_again() -> TestResult {
    let resolver = ScriptedResolver::new([Err(RouteResolveFailure::new(
        ErrorCode::Unauthenticated,
        "RouteTable component identity rejected",
    ))]);
    let inner = test_inner(Arc::new(resolver.clone()));
    let listener = add_session(&inner, SessionRole::Listener)?;
    let connector = add_session(&inner, SessionRole::Connector)?;
    establish_local_pipe(&inner, listener, connector)?;

    let (first_identity, first_client) = begin_remote_open(&inner, connector, 2, "echo.remote")?;
    let first = inner
        .run_control_effect(ControlAction::ResolveRoute {
            open_identity: first_identity,
            client_id: first_client,
        })
        .await;

    assert_open_failed(&first, 2, ErrorCode::Unauthenticated);
    assert_eq!(resolver.call_count(), 1);
    assert_local_state_is_unchanged(&inner);

    // A replacement trust configuration is represented by the next scripted
    // authenticated response. Only a caller-created new OPEN may consume it.
    resolver.push(Err(RouteResolveFailure::new(
        ErrorCode::NotFound,
        "no current remote binding",
    )));
    assert_eq!(resolver.call_count(), 1);

    let (second_identity, second_client) = begin_remote_open(&inner, connector, 3, "echo.remote")?;
    let second = inner
        .run_control_effect(ControlAction::ResolveRoute {
            open_identity: second_identity,
            client_id: second_client,
        })
        .await;

    assert_open_failed(&second, 3, ErrorCode::NotFound);
    assert_eq!(resolver.call_count(), 2);
    assert_local_state_is_unchanged(&inner);
    Ok(())
}

#[tokio::test]
async fn entry_opened_delivery_failure_closes_connector_and_remote_pipe() -> TestResult {
    let inner = test_inner(Arc::new(ScriptedResolver::new([])));
    let (sender, mut receiver) = mpsc::channel(1);
    sender.try_send(Frame::Ping { nonce: 1 })?;
    let cancellation = CancellationToken::new();
    let connector = inner
        .lock_state()
        .add_session(SessionRole::Connector, sender, cancellation.clone())
        .ok_or("test Gateway session limit reached")?;
    let (open_identity, _) = begin_remote_open(&inner, connector, 1, "echo.remote")?;
    let owner_gateway = GatewayId::new();
    let binding_id = BindingId::new();
    let key = PeerStreamKey::for_test(owner_gateway, PeerTransportId::new(), 0);
    let opened = {
        let mut state = inner.lock_state();
        state.route_resolved(
            open_identity,
            remote_binding_set("echo.remote", owner_gateway, binding_id)?,
        );
        state.peer_open_committed(open_identity, key);
        state.peer_opened(key, open_identity)
    };
    assert!(opened.iter().any(|action| matches!(
        action,
        GatewayAction::SendSdkFrame(delivery)
            if matches!(delivery.frame, Frame::Opened { .. })
    )));

    inner.execute_all(opened).await;

    assert!(cancellation.is_cancelled());
    let snapshot = inner.lock_state().snapshot();
    assert_eq!(snapshot.sessions, 0);
    assert_eq!(snapshot.connector_sessions, 0);
    assert_eq!(snapshot.remote_open_attempts, 0);
    assert_eq!(snapshot.live_pipes, 0);
    assert!(matches!(receiver.try_recv(), Ok(Frame::Ping { nonce: 1 })));
    assert!(receiver.try_recv().is_err());
    Ok(())
}

fn test_inner(route_resolver: Arc<dyn RouteResolver>) -> Arc<Inner> {
    let (results, _result_receiver) = mpsc::channel(8);
    Arc::new(Inner {
        state: Mutex::new(GatewayState::new_distributed(
            ClientKeyStore::new(
                [
                    ("echo.local".to_owned(), "local-key".to_owned()),
                    ("echo.remote".to_owned(), "remote-key".to_owned()),
                ]
                .into(),
            ),
            GatewayLimits::default(),
            GatewayId::new(),
        )),
        writer_queue_capacity: 8,
        max_frame_len: 64 * 1024,
        offer_timeout: Duration::from_secs(1),
        heartbeat_idle_interval: Duration::from_secs(60),
        heartbeat_response_timeout: Duration::from_secs(20),
        session_slots: Arc::new(Semaphore::new(8)),
        routing: None,
        peer: None,
        control_effects: Some(ControlEffects::new(
            8,
            route_resolver,
            results,
            CancellationToken::new(),
        )),
        distributed_runtime: Mutex::new(None),
    })
}

fn add_session(inner: &Inner, role: SessionRole) -> TestResult<SessionId> {
    let (sender, _receiver) = mpsc::channel(8);
    inner
        .lock_state()
        .add_session(role, sender, CancellationToken::new())
        .ok_or_else(|| "test Gateway session limit reached".into())
}

fn begin_remote_open(
    inner: &Inner,
    connector: SessionId,
    connection_id: u64,
    client_id: &str,
) -> TestResult<(crate::peer::OpenIdentity, ClientId)> {
    let actions = inner.lock_state().handle(
        connector,
        Frame::Open {
            connection_id,
            client_id: client_id.to_owned(),
        },
    )?;
    actions
        .into_iter()
        .find_map(|action| match action {
            GatewayAction::ResolveRoute {
                open_identity,
                client_id,
            } => Some((open_identity, client_id)),
            _ => None,
        })
        .ok_or_else(|| "missing ResolveRoute action".into())
}

fn remote_binding_set(
    client_id: &str,
    gateway_id: GatewayId,
    binding_id: BindingId,
) -> TestResult<BindingSet> {
    Ok(BindingSet::from_entries(vec![MappingEntry::new(
        ClientId::new(client_id)?,
        gateway_id,
        ListenerSessionId::from_uuid(SessionId::new().as_uuid()),
        RouteBindingId::from_uuid(binding_id.as_uuid()),
        GatewayLocator::new("owner.internal:27421")?,
    )])?)
}

fn establish_local_pipe(inner: &Inner, listener: SessionId, connector: SessionId) -> TestResult {
    let mut state = inner.lock_state();
    state.handle(
        listener,
        Frame::Register {
            request_id: 1,
            client_id: "echo.local".to_owned(),
            client_key: ClientKey::new("local-key"),
        },
    )?;
    let offered = state.handle(
        connector,
        Frame::Open {
            connection_id: 1,
            client_id: "echo.local".to_owned(),
        },
    )?;
    let pipe_id = offered
        .into_iter()
        .find_map(|action| match action {
            GatewayAction::SendSdkFrame(delivery) => match delivery.frame {
                Frame::Offer { pipe_id, .. } => Some(pipe_id),
                _ => None,
            },
            _ => None,
        })
        .ok_or("missing local OFFER")?;
    state.handle(listener, Frame::OfferAccepted { pipe_id })?;
    Ok(())
}

fn assert_open_failed(actions: &[GatewayAction], connection_id: u64, code: ErrorCode) {
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        actions.first(),
        Some(GatewayAction::SendSdkFrame(delivery))
            if matches!(
                delivery.frame,
                Frame::OpenFailed {
                    connection_id: actual,
                    code: actual_code,
                    observation: PeerObservation::NotObserved,
                    ..
                } if actual == connection_id && actual_code == code
            )
    ));
}

fn assert_terminal_remote_failure(inner: &Inner) {
    let snapshot = inner.lock_state().snapshot();
    assert_eq!(snapshot.remote_open_attempts, 0);
    assert_eq!(snapshot.pending_offers, 0);
    assert_eq!(snapshot.live_pipes, 0);
    assert_eq!(snapshot.listener_bindings, 0);
}

fn assert_local_state_is_unchanged(inner: &Inner) {
    let snapshot = inner.lock_state().snapshot();
    assert_eq!(snapshot.remote_open_attempts, 0);
    assert_eq!(snapshot.pending_offers, 0);
    assert_eq!(snapshot.live_pipes, 1);
    assert_eq!(snapshot.listener_bindings, 1);
}
