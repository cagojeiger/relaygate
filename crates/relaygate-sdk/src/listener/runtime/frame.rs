use std::sync::Arc;

use relaygate_protocol::{ErrorCode as WireErrorCode, Frame, PipeId, SessionId};
use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

use super::{ListenerFrameAction, ListenerSessionState, LivePipe, Registration};
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerRuntimeInner, ListenerStatus, is_current_desired},
    pipe::{PipeState, to_wire_code},
    session::{SessionOutbound, WireTransport, send_bounded},
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_listener_frame(
    frame: Frame,
    session_id: SessionId,
    session: &mut ListenerSessionState,
    outbound: &SessionOutbound,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    inner: &ListenerRuntimeInner,
    transport: &mut WireTransport,
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
