use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use futures_util::StreamExt;
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode as WireErrorCode, Frame, PipeId, SessionRole,
};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;

use super::{ListenerRuntimeInner, ListenerState, ListenerStatus};
use crate::{
    Error, ErrorCode, PeerObservation,
    pipe::{PipeState, to_wire_code},
    session::{
        EstablishedSession, SessionHeartbeat, SessionOutbound, establish, next_backoff,
        send_bounded, session_outbound_channel, wait_for_heartbeat,
    },
};

pub(super) async fn listener_supervisor(
    inner: Arc<ListenerRuntimeInner>,
    initial: EstablishedSession,
) {
    let mut established = Some(initial);
    let mut backoff = inner.config.reconnect_initial;
    loop {
        if inner.cancel.is_cancelled() {
            inner.close_all();
            return;
        }
        let next = match established.take() {
            Some(session) => session,
            None => match establish(&inner.config, SessionRole::Listener).await {
                Ok(session) => session,
                Err(error) => {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.reconnect_failed",
                        role = "listener",
                        error_code = ?error.code(),
                        observation = ?error.observation(),
                        "Listener session reconnect failed"
                    );
                    tokio::select! {
                        _ = inner.cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = next_backoff(backoff, inner.config.reconnect_maximum);
                    continue;
                }
            },
        };
        let session_cancel = inner.cancel.child_token();
        let registration_succeeded = run_listener_session(next, &inner, session_cancel).await;
        if inner.cancel.is_cancelled() {
            inner.close_all();
            return;
        }
        if registration_succeeded {
            backoff = inner.config.reconnect_initial;
        }
        tokio::select! {
            _ = inner.cancel.cancelled() => {
                inner.close_all();
                return;
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = next_backoff(backoff, inner.config.reconnect_maximum);
    }
}

struct PendingRegistration {
    state: Arc<ListenerState>,
    committed: bool,
    deadline: Instant,
}

struct Registration {
    state: Arc<ListenerState>,
    binding_id: BindingId,
}

struct LivePipe {
    state: Arc<PipeState>,
    listener: Weak<ListenerState>,
}

impl LivePipe {
    fn compact_listener_queue(&self) -> bool {
        self.listener
            .upgrade()
            .is_none_or(|listener| listener.try_compact_terminal_queue())
    }
}

struct ListenerSessionState {
    next_request_id: u64,
    pending: HashMap<u64, PendingRegistration>,
    pending_by_client: HashMap<String, u64>,
    registrations: HashMap<String, Registration>,
    pipes: HashMap<PipeId, LivePipe>,
}

impl ListenerSessionState {
    fn new() -> Self {
        Self {
            next_request_id: 1,
            pending: HashMap::new(),
            pending_by_client: HashMap::new(),
            registrations: HashMap::new(),
            pipes: HashMap::new(),
        }
    }

    fn next_request_id(&mut self) -> Option<u64> {
        let current = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1)?;
        Some(current)
    }
}

async fn run_listener_session(
    mut established: EstablishedSession,
    inner: &ListenerRuntimeInner,
    session_cancel: CancellationToken,
) -> bool {
    let (outbound_tx, mut outbound_rx) = session_outbound_channel(inner.config.outbound_capacity);
    // One unique Pipe value can abandon one current PipeId, so this lane is
    // logically bounded by the session's live Pipe map rather than history.
    let (abandoned_tx, mut abandoned_rx) = mpsc::unbounded_channel();
    let mut state = ListenerSessionState::new();
    let mut needs_reconcile = true;
    let mut timed_out_request = None;
    let mut registration_succeeded = false;
    let mut heartbeat = SessionHeartbeat::new(&inner.config, established.id, 0x4c);

    loop {
        if needs_reconcile
            && !reconcile_registrations(inner, &mut established, &mut state, &session_cancel).await
        {
            break;
        }
        needs_reconcile = false;
        let registration_deadline = state
            .pending
            .iter()
            .filter(|(_, pending)| pending.committed)
            .min_by_key(|(_, pending)| pending.deadline)
            .map(|(request_id, pending)| (*request_id, pending.deadline));
        tokio::select! {
            biased;
            _ = session_cancel.cancelled() => break,
            incoming = established.transport.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(frame) = incoming else { break; };
                heartbeat.observe_inbound(&frame);
                if heartbeat.response_timed_out() {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.heartbeat_timeout",
                        role = "listener",
                        session_id = %established.id.as_uuid(),
                        "Listener session heartbeat response timed out"
                    );
                    break;
                }
                match handle_listener_frame(
                    frame,
                    established.id,
                    &mut state,
                    &outbound_tx,
                    &abandoned_tx,
                    inner,
                    &mut established.transport,
                    &session_cancel,
                ).await {
                    ListenerFrameAction::Continue => {}
                    ListenerFrameAction::RegistrationSucceeded => {
                        registration_succeeded = true;
                    }
                    ListenerFrameAction::Reconcile => needs_reconcile = true,
                    ListenerFrameAction::Stop => break,
                }
            }
            () = wait_for_heartbeat(heartbeat.next_deadline()) => {
                let Some(frame) = heartbeat.on_deadline() else {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.heartbeat_timeout",
                        role = "listener",
                        session_id = %established.id.as_uuid(),
                        "Listener session heartbeat response timed out"
                    );
                    break;
                };
                if send_bounded(
                    &mut established.transport,
                    frame,
                    inner.config.operation_timeout,
                    &session_cancel,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            _ = wait_for_registration_deadline(registration_deadline), if registration_deadline.is_some() => {
                timed_out_request = registration_deadline.map(|(request_id, _)| request_id);
                session_cancel.cancel();
                break;
            }
            _ = inner.reconcile.notified() => {
                needs_reconcile = true;
            }
            frame = outbound_rx.recv() => {
                let Some(frame) = frame else { break; };
                let terminal_pipe = match &frame {
                    Frame::Close { pipe_id } | Frame::Reset { pipe_id, .. } => Some(*pipe_id),
                    Frame::Fin { pipe_id } if state.pipes.get(pipe_id).is_some_and(|pipe| pipe.state.is_finished()) => Some(*pipe_id),
                    _ => None,
                };
                if send_bounded(
                    &mut established.transport,
                    frame,
                    inner.config.operation_timeout,
                    &session_cancel,
                )
                .await
                .is_err()
                {
                    break;
                }
                if let Some(pipe_id) = terminal_pipe {
                    state.pipes.remove(&pipe_id);
                }
            }
            abandoned = abandoned_rx.recv() => {
                let Some(pipe_id) = abandoned else { continue; };
                if state.pipes.remove(&pipe_id).is_some()
                    && send_bounded(
                        &mut established.transport,
                        Frame::Close { pipe_id },
                        inner.config.operation_timeout,
                        &session_cancel,
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ended",
        role = "listener",
        session_id = %established.id.as_uuid(),
        pending_registrations = state.pending.len(),
        active_registrations = state.registrations.len(),
        live_pipes = state.pipes.len(),
        registration_timed_out = timed_out_request.is_some(),
        "Listener session ended"
    );
    // Fail every Pipe first. Once the Listener status leaves ACTIVE, pending
    // accept calls release the receiver lane and the old session queue can be
    // drained before this function permits a replacement session to start.
    for pipe in state.pipes.values() {
        pipe.state
            .fail(Error::unavailable("ListenerSession transport ended"));
    }
    let mut queues_to_drain = Vec::new();
    for (request_id, pending) in state.pending {
        if *pending.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((pending.state, true));
            continue;
        }
        if pending.committed {
            let recovery_error = if timed_out_request == Some(request_id) {
                Error::deadline(PeerObservation::MaybeObserved)
            } else {
                Error::maybe_observed("ListenerSession ended during managed REGISTER")
            };
            let initial_error = if timed_out_request == Some(request_id) {
                Error::deadline(PeerObservation::MaybeObserved)
            } else {
                Error::maybe_observed("ListenerSession ended after REGISTER commit")
            };
            if pending
                .state
                .suspend_or_fail_initial(recovery_error, initial_error)
            {
                inner.remove_terminal_listener(&pending.state);
            }
        } else if is_current_desired(inner, &pending.state) {
            pending
                .state
                .handle_precommit_session_end(Error::unavailable(
                    "ListenerSession ended before managed REGISTER commit",
                ));
        }
        let close_queue = matches!(
            *pending.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((pending.state, close_queue));
    }
    for (_, registration) in state.registrations {
        if *registration.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((registration.state, true));
            continue;
        }
        if registration.state.suspend_or_fail_initial(
            Error::unavailable("ListenerSession transport ended"),
            Error::new(
                ErrorCode::Unavailable,
                PeerObservation::Observed,
                "ListenerSession ended after REGISTERED before listen returned",
            ),
        ) {
            inner.remove_terminal_listener(&registration.state);
        }
        let close_queue = matches!(
            *registration.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((registration.state, close_queue));
    }
    for (listener, close_queue) in queues_to_drain {
        listener.drain_unaccepted(close_queue).await;
    }
    registration_succeeded
}

async fn reconcile_registrations(
    inner: &ListenerRuntimeInner,
    established: &mut EstablishedSession,
    session: &mut ListenerSessionState,
    session_cancel: &CancellationToken,
) -> bool {
    let Some(desired) = snapshot_desired_by_client(inner) else {
        return false;
    };
    let abandoned_committed_registration = session.pending.values().any(|pending| {
        pending.committed
            && (!desired
                .get(&pending.state.client_id)
                .is_some_and(|current| Arc::ptr_eq(current, &pending.state))
                || *pending.state.status.borrow() == ListenerStatus::Closed)
    });
    if abandoned_committed_registration {
        return false;
    }
    let registered_clients = session.registrations.keys().cloned().collect::<Vec<_>>();
    for client_id in registered_clients {
        let stale = session
            .registrations
            .get(&client_id)
            .is_some_and(|registration| {
                !desired
                    .get(&client_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() == ListenerStatus::Closed
            });
        if !stale {
            continue;
        }
        let Some(registration) = session.registrations.remove(&client_id) else {
            continue;
        };
        let Some(request_id) = session.next_request_id() else {
            return false;
        };
        if send_bounded(
            &mut established.transport,
            Frame::Unregister {
                request_id,
                binding_id: registration.binding_id,
            },
            inner.config.operation_timeout,
            session_cancel,
        )
        .await
        .is_err()
        {
            return false;
        }
    }

    for state in desired.values() {
        if !is_current_desired(inner, state) {
            continue;
        }
        if matches!(
            *state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        ) || session.registrations.contains_key(&state.client_id)
            || session.pending_by_client.contains_key(&state.client_id)
        {
            continue;
        }
        let deadline = if state.was_returned() {
            Instant::now() + inner.config.operation_timeout
        } else {
            state.initial_deadline
        };
        if deadline <= Instant::now() {
            if state.was_returned() {
                state.set_status(
                    ListenerStatus::Suspended,
                    Some(Error::deadline(PeerObservation::NotObserved)),
                );
            } else {
                inner.fail_initial_listener(state, Error::deadline(PeerObservation::NotObserved));
            }
            continue;
        }
        let Some(request_id) = session.next_request_id() else {
            let error = Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "ListenerSession exhausted request IDs",
            );
            if state.was_returned() {
                state.block(error);
                state.drain_unaccepted(true).await;
            } else {
                inner.fail_initial_listener(state, error);
            }
            continue;
        };
        session.pending.insert(
            request_id,
            PendingRegistration {
                state: Arc::clone(state),
                committed: false,
                deadline,
            },
        );
        session
            .pending_by_client
            .insert(state.client_id.clone(), request_id);
        if !state.begin_registration_commit() {
            session.pending.remove(&request_id);
            session.pending_by_client.remove(&state.client_id);
            continue;
        }
        if let Some(pending) = session.pending.get_mut(&request_id) {
            pending.committed = true;
        }
        if send_bounded(
            &mut established.transport,
            Frame::Register {
                request_id,
                client_id: state.client_id.clone(),
                client_key: ClientKey::new(state.client_key.clone()),
            },
            deadline
                .saturating_duration_since(Instant::now())
                .min(inner.config.operation_timeout),
            session_cancel,
        )
        .await
        .is_err()
        {
            return false;
        }
    }

    !inner.cancel.is_cancelled()
}

enum ListenerFrameAction {
    Continue,
    RegistrationSucceeded,
    Reconcile,
    Stop,
}

#[allow(clippy::too_many_arguments)]
async fn handle_listener_frame(
    frame: Frame,
    session_id: relaygate_protocol::SessionId,
    session: &mut ListenerSessionState,
    outbound: &SessionOutbound,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    inner: &ListenerRuntimeInner,
    transport: &mut crate::session::WireTransport,
    session_cancel: &CancellationToken,
) -> ListenerFrameAction {
    match frame {
        Frame::Registered {
            request_id,
            binding_id,
        } => {
            let Some(pending) = session.pending.remove(&request_id) else {
                return ListenerFrameAction::Continue;
            };
            session.pending_by_client.remove(&pending.state.client_id);
            pending.state.finish_registration_attempt();
            if is_current_desired(inner, &pending.state) && pending.state.activate() {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener_registration.active",
                    session_id = %session_id.as_uuid(),
                    request_id,
                    client_id = %pending.state.client_id,
                    binding_id = %binding_id.as_uuid(),
                    "Listener registration is active"
                );
                session.registrations.insert(
                    pending.state.client_id.clone(),
                    Registration {
                        state: pending.state,
                        binding_id,
                    },
                );
                return ListenerFrameAction::RegistrationSucceeded;
            } else {
                let Some(request_id) = session.next_request_id() else {
                    return ListenerFrameAction::Stop;
                };
                if send_bounded(
                    transport,
                    Frame::Unregister {
                        request_id,
                        binding_id,
                    },
                    inner.config.operation_timeout,
                    session_cancel,
                )
                .await
                .is_err()
                {
                    return ListenerFrameAction::Stop;
                }
                return ListenerFrameAction::Reconcile;
            }
        }
        Frame::RegisterFailed {
            request_id,
            code,
            message,
        } => {
            let Some(pending) = session.pending.remove(&request_id) else {
                return ListenerFrameAction::Continue;
            };
            session.pending_by_client.remove(&pending.state.client_id);
            pending.state.finish_registration_attempt();
            if !is_current_desired(inner, &pending.state)
                || *pending.state.status.borrow() == ListenerStatus::Closed
            {
                return ListenerFrameAction::Reconcile;
            }
            let error = Error::new(
                ErrorCode::from_wire(code),
                PeerObservation::NotObserved,
                message,
            );
            if permanent_registration_failure(code) {
                if pending.state.was_returned() {
                    pending.state.block(error);
                    pending.state.drain_unaccepted(true).await;
                } else {
                    inner.fail_initial_listener(&pending.state, error);
                    return ListenerFrameAction::Reconcile;
                }
            } else if pending.state.was_returned() {
                pending
                    .state
                    .set_status(ListenerStatus::Suspended, Some(error));
                pending.state.drain_unaccepted(false).await;
                let delay = inner.config.reconnect_initial;
                let cancel = inner.cancel.clone();
                let inner_reconcile = inner.reconcile.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::time::sleep(delay) => inner_reconcile.notify_one(),
                    }
                });
            } else {
                inner.fail_initial_listener(&pending.state, error);
                return ListenerFrameAction::Reconcile;
            }
        }
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id,
        } => {
            if let Some(existing) = session.pipes.get(&pipe_id) {
                if !existing.state.is_finished()
                    && send_bounded(
                        transport,
                        Frame::OfferAccepted { pipe_id },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await
                    .is_err()
                {
                    return ListenerFrameAction::Stop;
                }
                return ListenerFrameAction::Continue;
            }
            let Some(registration) = session.registrations.get(&client_id) else {
                return listener_frame_action(
                    send_bounded(
                        transport,
                        Frame::OfferRejected {
                            pipe_id,
                            code: WireErrorCode::NotFound,
                            message: "Listener is not active".to_owned(),
                        },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await,
                );
            };
            if registration.binding_id != binding_id {
                return listener_frame_action(
                    send_bounded(
                        transport,
                        Frame::OfferRejected {
                            pipe_id,
                            code: WireErrorCode::FailedPrecondition,
                            message: "Listener binding incarnation is stale".to_owned(),
                        },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await,
                );
            }
            if !is_current_desired(inner, &registration.state)
                || *registration.state.status.borrow() != ListenerStatus::Active
            {
                return listener_frame_action(
                    send_bounded(
                        transport,
                        Frame::OfferRejected {
                            pipe_id,
                            code: WireErrorCode::Unavailable,
                            message: "Listener is not active".to_owned(),
                        },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await,
                );
            }
            if !registration.state.try_compact_terminal_queue() {
                tracing::error!(
                    component = "sdk",
                    event = "sdk.listener_queue.invariant_failed",
                    client_id = %client_id,
                    "Listener incoming queue compaction could not preserve live Pipes"
                );
                return ListenerFrameAction::Stop;
            }
            let permit = timeout(
                inner.config.offer_timeout,
                registration.state.incoming_tx.reserve(),
            )
            .await;
            let Ok(Ok(permit)) = permit else {
                return listener_frame_action(
                    send_bounded(
                        transport,
                        Frame::OfferRejected {
                            pipe_id,
                            code: WireErrorCode::ResourceExhausted,
                            message: "Listener incoming queue is full".to_owned(),
                        },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await,
                );
            };
            let admitted = {
                let desired = match inner.desired.lock() {
                    Ok(desired) => desired,
                    Err(_) => {
                        tracing::error!(
                            component = "sdk",
                            event = "sdk.listener_registry.lock_poisoned",
                            "Listener desired registry lock is poisoned during Pipe admission"
                        );
                        inner.cancel.cancel();
                        return ListenerFrameAction::Stop;
                    }
                };
                if !desired
                    .get(&client_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() != ListenerStatus::Active
                {
                    false
                } else {
                    let Some(lifetime) = inner.lifetime.upgrade() else {
                        inner.cancel.cancel();
                        return ListenerFrameAction::Stop;
                    };
                    let (pipe, state) = PipeState::pair_with_lifetime(
                        pipe_id,
                        outbound.clone(),
                        inner.config.pipe_inbound_capacity,
                        abandoned.clone(),
                        lifetime,
                    );
                    session.pipes.insert(
                        pipe_id,
                        LivePipe {
                            state,
                            listener: Arc::downgrade(&registration.state),
                        },
                    );
                    permit.send(pipe);
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.pipe.admitted",
                        client_id = %client_id,
                        binding_id = %binding_id.as_uuid(),
                        connector_session_id = %pipe_id.connector_session_id().as_uuid(),
                        connection_id = pipe_id.connection_id(),
                        "Listener admitted a Pipe"
                    );
                    true
                }
            };
            if !admitted {
                return listener_frame_action(
                    send_bounded(
                        transport,
                        Frame::OfferRejected {
                            pipe_id,
                            code: WireErrorCode::Unavailable,
                            message: "Listener closed during Pipe admission".to_owned(),
                        },
                        inner.config.operation_timeout,
                        session_cancel,
                    )
                    .await,
                );
            }
            if send_bounded(
                transport,
                Frame::OfferAccepted { pipe_id },
                inner.config.operation_timeout,
                session_cancel,
            )
            .await
            .is_err()
            {
                return ListenerFrameAction::Stop;
            }
        }
        Frame::Data { pipe_id, payload } => {
            let error = session
                .pipes
                .get(&pipe_id)
                .and_then(|pipe| pipe.state.push_data(payload).err());
            if let Some(error) = error {
                if let Some(pipe) = session.pipes.remove(&pipe_id) {
                    pipe.state.fail(error.clone());
                    if !pipe.compact_listener_queue() {
                        return ListenerFrameAction::Stop;
                    }
                }
                let _ = send_bounded(
                    transport,
                    Frame::Reset {
                        pipe_id,
                        code: to_wire_code(error.code()),
                        message: error.message().to_owned(),
                    },
                    inner.config.operation_timeout,
                    session_cancel,
                )
                .await;
            }
        }
        Frame::Fin { pipe_id } => {
            if let Some(pipe) = session.pipes.get(&pipe_id) {
                pipe.state.remote_fin();
            }
            let finished = session
                .pipes
                .get(&pipe_id)
                .is_some_and(|pipe| pipe.state.is_finished());
            if finished
                && let Some(pipe) = session.pipes.remove(&pipe_id)
                && !pipe.compact_listener_queue()
            {
                return ListenerFrameAction::Stop;
            }
        }
        Frame::Close { pipe_id } => {
            if let Some(pipe) = session.pipes.remove(&pipe_id) {
                pipe.state.close_normal();
                if !pipe.compact_listener_queue() {
                    return ListenerFrameAction::Stop;
                }
            }
        }
        Frame::Reset {
            pipe_id,
            code,
            message,
        } => {
            if let Some(pipe) = session.pipes.remove(&pipe_id) {
                pipe.state.fail(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::Observed,
                    message,
                ));
                if !pipe.compact_listener_queue() {
                    return ListenerFrameAction::Stop;
                }
            }
        }
        Frame::Ping { nonce } => {
            if send_bounded(
                transport,
                Frame::Pong { nonce },
                inner.config.operation_timeout,
                session_cancel,
            )
            .await
            .is_err()
            {
                return ListenerFrameAction::Stop;
            }
        }
        Frame::Pong { .. } | Frame::Unregistered { .. } => {}
        _ => return ListenerFrameAction::Stop,
    }
    if inner.cancel.is_cancelled() {
        ListenerFrameAction::Stop
    } else {
        ListenerFrameAction::Continue
    }
}

fn listener_frame_action<T, E>(result: Result<T, E>) -> ListenerFrameAction {
    if result.is_ok() {
        ListenerFrameAction::Continue
    } else {
        ListenerFrameAction::Stop
    }
}

fn snapshot_desired_by_client(
    inner: &ListenerRuntimeInner,
) -> Option<HashMap<String, Arc<ListenerState>>> {
    match inner.desired.lock() {
        Ok(desired) => Some(
            desired
                .iter()
                .map(|(client_id, state)| (client_id.clone(), Arc::clone(state)))
                .collect(),
        ),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            None
        }
    }
}

fn is_current_desired(inner: &ListenerRuntimeInner, state: &Arc<ListenerState>) -> bool {
    match inner.desired.lock() {
        Ok(desired) => desired
            .get(&state.client_id)
            .is_some_and(|current| Arc::ptr_eq(current, state)),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            false
        }
    }
}

async fn wait_for_registration_deadline(deadline: Option<(u64, Instant)>) {
    if let Some((_, deadline)) = deadline {
        sleep_until(deadline).await;
    }
}

const fn permanent_registration_failure(code: WireErrorCode) -> bool {
    matches!(
        code,
        WireErrorCode::InvalidArgument
            | WireErrorCode::Unauthenticated
            | WireErrorCode::PermissionDenied
            | WireErrorCode::FailedPrecondition
            | WireErrorCode::AlreadyExists
    )
}
