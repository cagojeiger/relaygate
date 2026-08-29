use std::{collections::HashMap, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode as WireErrorCode, Frame, PipeId, SessionRole,
};
use tokio::{sync::mpsc, time::timeout};

use super::{ListenerRuntimeInner, ListenerState, ListenerStatus};
use crate::{
    Error, ErrorCode, PeerObservation,
    pipe::{PipeState, to_wire_code},
    session::{EstablishedSession, establish, next_backoff},
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
                    tracing::debug!(%error, "Listener reconnect failed");
                    tokio::select! {
                        _ = inner.cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = next_backoff(backoff, inner.config.reconnect_maximum);
                    continue;
                }
            },
        };
        backoff = inner.config.reconnect_initial;
        run_listener_session(next, &inner).await;
    }
}

struct PendingRegistration {
    state: Arc<ListenerState>,
}

struct Registration {
    state: Arc<ListenerState>,
    binding_id: BindingId,
}

struct ListenerSessionState {
    next_request_id: u64,
    pending: HashMap<u64, PendingRegistration>,
    pending_by_client: HashMap<String, u64>,
    registrations: HashMap<String, Registration>,
    pipes: HashMap<PipeId, Arc<PipeState>>,
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

async fn run_listener_session(mut established: EstablishedSession, inner: &ListenerRuntimeInner) {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(inner.config.outbound_capacity);
    // One unique Pipe value can abandon one current PipeId, so this lane is
    // logically bounded by the session's live Pipe map rather than history.
    let (abandoned_tx, mut abandoned_rx) = mpsc::unbounded_channel();
    let mut state = ListenerSessionState::new();
    let mut needs_reconcile = true;

    loop {
        if needs_reconcile && !reconcile_registrations(inner, &mut established, &mut state).await {
            break;
        }
        needs_reconcile = false;
        tokio::select! {
            biased;
            _ = inner.cancel.cancelled() => break,
            _ = inner.reconcile.notified() => {
                needs_reconcile = true;
            }
            abandoned = abandoned_rx.recv() => {
                let Some(pipe_id) = abandoned else { continue; };
                if state.pipes.remove(&pipe_id).is_some()
                    && established.transport.send(Frame::Close { pipe_id }).await.is_err()
                {
                    break;
                }
            }
            frame = outbound_rx.recv() => {
                let Some(frame) = frame else { break; };
                let terminal_pipe = match &frame {
                    Frame::Close { pipe_id } | Frame::Reset { pipe_id, .. } => Some(*pipe_id),
                    Frame::Fin { pipe_id } if state.pipes.get(pipe_id).is_some_and(|pipe| pipe.is_finished()) => Some(*pipe_id),
                    _ => None,
                };
                if established.transport.send(frame).await.is_err() {
                    break;
                }
                if let Some(pipe_id) = terminal_pipe {
                    state.pipes.remove(&pipe_id);
                }
            }
            incoming = established.transport.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(frame) = incoming else { break; };
                match handle_listener_frame(
                    frame,
                    &mut state,
                    &outbound_tx,
                    &abandoned_tx,
                    inner,
                    &mut established.transport,
                ).await {
                    ListenerFrameAction::Continue => {}
                    ListenerFrameAction::Reconcile => needs_reconcile = true,
                    ListenerFrameAction::Stop => break,
                }
            }
        }
    }

    for (_, pending) in state.pending {
        suspend_if_live(&pending.state);
    }
    for (_, registration) in state.registrations {
        suspend_if_live(&registration.state);
    }
    for (_, pipe) in state.pipes {
        pipe.fail(Error::unavailable("ListenerSession transport ended"));
    }
}

async fn reconcile_registrations(
    inner: &ListenerRuntimeInner,
    established: &mut EstablishedSession,
    session: &mut ListenerSessionState,
) -> bool {
    let Some(desired) = snapshot_desired_by_client(inner) else {
        return false;
    };
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
        if established
            .transport
            .send(Frame::Unregister {
                request_id,
                binding_id: registration.binding_id,
            })
            .await
            .is_err()
        {
            return false;
        }
    }

    for state in desired.values() {
        if matches!(
            *state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        ) || session.registrations.contains_key(&state.client_id)
            || session.pending_by_client.contains_key(&state.client_id)
        {
            continue;
        }
        let Some(request_id) = session.next_request_id() else {
            state.set_status(
                ListenerStatus::Blocked,
                Some(Error::new(
                    ErrorCode::ResourceExhausted,
                    PeerObservation::NotObserved,
                    "ListenerSession exhausted request IDs",
                )),
            );
            continue;
        };
        state.set_status(ListenerStatus::Registering, None);
        session.pending.insert(
            request_id,
            PendingRegistration {
                state: Arc::clone(state),
            },
        );
        session
            .pending_by_client
            .insert(state.client_id.clone(), request_id);
        if established
            .transport
            .send(Frame::Register {
                request_id,
                client_id: state.client_id.clone(),
                client_key: ClientKey::new(state.client_key.clone()),
            })
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
    Reconcile,
    Stop,
}

#[allow(clippy::too_many_arguments)]
async fn handle_listener_frame(
    frame: Frame,
    session: &mut ListenerSessionState,
    outbound: &mpsc::Sender<Frame>,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    inner: &ListenerRuntimeInner,
    transport: &mut crate::session::WireTransport,
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
            if is_current_desired(inner, &pending.state)
                && *pending.state.status.borrow() != ListenerStatus::Closed
            {
                pending.state.set_status(ListenerStatus::Active, None);
                session.registrations.insert(
                    pending.state.client_id.clone(),
                    Registration {
                        state: pending.state,
                        binding_id,
                    },
                );
            } else {
                let Some(request_id) = session.next_request_id() else {
                    return ListenerFrameAction::Stop;
                };
                if transport
                    .send(Frame::Unregister {
                        request_id,
                        binding_id,
                    })
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
                pending
                    .state
                    .set_status(ListenerStatus::Blocked, Some(error));
            } else {
                pending
                    .state
                    .set_status(ListenerStatus::Suspended, Some(error));
                let delay = inner.config.reconnect_initial;
                let cancel = inner.cancel.clone();
                let inner_reconcile = inner.reconcile.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::time::sleep(delay) => inner_reconcile.notify_one(),
                    }
                });
            }
        }
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id,
        } => {
            if let Some(existing) = session.pipes.get(&pipe_id) {
                if !existing.is_finished()
                    && transport
                        .send(Frame::OfferAccepted { pipe_id })
                        .await
                        .is_err()
                {
                    return ListenerFrameAction::Stop;
                }
                return ListenerFrameAction::Continue;
            }
            let Some(registration) = session.registrations.get(&client_id) else {
                return transport
                    .send(Frame::OfferRejected {
                        pipe_id,
                        code: WireErrorCode::NotFound,
                        message: "Listener is not active".to_owned(),
                    })
                    .await
                    .map_send_action();
            };
            if registration.binding_id != binding_id {
                return transport
                    .send(Frame::OfferRejected {
                        pipe_id,
                        code: WireErrorCode::FailedPrecondition,
                        message: "Listener binding incarnation is stale".to_owned(),
                    })
                    .await
                    .map_send_action();
            }
            if !is_current_desired(inner, &registration.state)
                || *registration.state.status.borrow() != ListenerStatus::Active
            {
                return transport
                    .send(Frame::OfferRejected {
                        pipe_id,
                        code: WireErrorCode::Unavailable,
                        message: "Listener is not active".to_owned(),
                    })
                    .await
                    .map_send_action();
            }
            let permit = timeout(
                inner.config.offer_timeout,
                registration.state.incoming_tx.reserve(),
            )
            .await;
            let Ok(Ok(permit)) = permit else {
                return transport
                    .send(Frame::OfferRejected {
                        pipe_id,
                        code: WireErrorCode::ResourceExhausted,
                        message: "Listener incoming queue is full".to_owned(),
                    })
                    .await
                    .map_send_action();
            };
            let admitted = {
                let desired = match inner.desired.lock() {
                    Ok(desired) => desired,
                    Err(_) => {
                        tracing::error!(
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
                    let (pipe, state) = PipeState::pair(
                        pipe_id,
                        outbound.clone(),
                        inner.config.pipe_inbound_capacity,
                        abandoned.clone(),
                    );
                    session.pipes.insert(pipe_id, state);
                    permit.send(pipe);
                    true
                }
            };
            if !admitted {
                return transport
                    .send(Frame::OfferRejected {
                        pipe_id,
                        code: WireErrorCode::Unavailable,
                        message: "Listener closed during Pipe admission".to_owned(),
                    })
                    .await
                    .map_send_action();
            }
            if transport
                .send(Frame::OfferAccepted { pipe_id })
                .await
                .is_err()
            {
                return ListenerFrameAction::Stop;
            }
        }
        Frame::Data { pipe_id, payload } => {
            if let Some(pipe) = session.pipes.get(&pipe_id)
                && let Err(error) = pipe.push_data(payload)
            {
                pipe.fail(error.clone());
                session.pipes.remove(&pipe_id);
                let _ = transport
                    .send(Frame::Reset {
                        pipe_id,
                        code: to_wire_code(error.code()),
                        message: error.message().to_owned(),
                    })
                    .await;
            }
        }
        Frame::Fin { pipe_id } => {
            if let Some(pipe) = session.pipes.get(&pipe_id) {
                pipe.remote_fin();
                if pipe.is_finished() {
                    session.pipes.remove(&pipe_id);
                }
            }
        }
        Frame::Close { pipe_id } => {
            if let Some(pipe) = session.pipes.remove(&pipe_id) {
                pipe.close_normal();
            }
        }
        Frame::Reset {
            pipe_id,
            code,
            message,
        } => {
            if let Some(pipe) = session.pipes.remove(&pipe_id) {
                pipe.fail(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::Observed,
                    message,
                ));
            }
        }
        Frame::Ping { nonce } => {
            if transport.send(Frame::Pong { nonce }).await.is_err() {
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

trait SendAction {
    fn map_send_action(self) -> ListenerFrameAction;
}

impl<T, E> SendAction for Result<T, E> {
    fn map_send_action(self) -> ListenerFrameAction {
        if self.is_ok() {
            ListenerFrameAction::Continue
        } else {
            ListenerFrameAction::Stop
        }
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
            tracing::error!("Listener desired registry lock is poisoned; stopping runtime");
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
            tracing::error!("Listener desired registry lock is poisoned; stopping runtime");
            inner.cancel.cancel();
            false
        }
    }
}

fn suspend_if_live(state: &Arc<ListenerState>) {
    if !matches!(
        *state.status.borrow(),
        ListenerStatus::Blocked | ListenerStatus::Closed
    ) {
        state.set_status(ListenerStatus::Suspended, None);
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
