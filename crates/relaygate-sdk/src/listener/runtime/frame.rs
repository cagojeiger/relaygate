use std::sync::Arc;

use relaygate_protocol::{ErrorCode as WireErrorCode, Frame, PipeId, SessionId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{LivePipe, Registration, RelayFrameAction, RelaySessionState};
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerStatus, RelayInner, is_current_desired},
    pipe::{PipeState, to_wire_code},
    resource::{ResourceLimitKind, resource_exhausted},
    session::{SessionOutbound, WireTransport, send_bounded},
};

#[cfg(test)]
mod tests;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_relay_frame(
    frame: Frame,
    session_id: SessionId,
    session: &mut RelaySessionState,
    outbound: &SessionOutbound,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    inner: &RelayInner,
    transport: &mut WireTransport,
    session_cancel: &CancellationToken,
) -> RelayFrameAction {
    match frame {
        Frame::Published {
            request_id,
            binding_id,
        } => {
            let Some(pending) = session.pending.remove(&request_id) else {
                return RelayFrameAction::Continue;
            };
            session
                .pending_by_destination
                .remove(&pending.state.destination);
            pending.state.finish_registration_attempt();
            if is_current_desired(inner, &pending.state) && pending.state.activate() {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener.active",
                    session_id = %session_id.as_uuid(),
                    request_id,
                    destination = %pending.state.destination,
                    binding_id = %binding_id.as_uuid(),
                    "Listener registration is active"
                );
                session.registrations.insert(
                    pending.state.destination.clone(),
                    Registration {
                        state: pending.state,
                        binding_id,
                    },
                );
                return RelayFrameAction::RegistrationSucceeded;
            } else {
                let Some(request_id) = session.next_request_id() else {
                    return RelayFrameAction::Stop;
                };
                if send_bounded(
                    transport,
                    Frame::Unpublish {
                        request_id,
                        binding_id,
                    },
                    inner.config.operation_timeout,
                    session_cancel,
                )
                .await
                .is_err()
                {
                    return RelayFrameAction::Stop;
                }
                return RelayFrameAction::Reconcile;
            }
        }
        Frame::PublishFailed {
            request_id,
            code,
            message,
        } => {
            let Some(pending) = session.pending.remove(&request_id) else {
                return RelayFrameAction::Continue;
            };
            session
                .pending_by_destination
                .remove(&pending.state.destination);
            pending.state.finish_registration_attempt();
            if !is_current_desired(inner, &pending.state)
                || *pending.state.status.borrow() == ListenerStatus::Closed
            {
                return RelayFrameAction::Reconcile;
            }
            let error = Error::new(
                ErrorCode::from_wire(code),
                PeerObservation::NotObserved,
                message,
            );
            if permanent_registration_failure(code) {
                if pending.state.was_returned() {
                    pending.state.block(error);
                    inner.mark_reconnect_degraded();
                    pending.state.drain_unaccepted(true).await;
                } else {
                    inner.fail_initial_listener(&pending.state, error);
                    return RelayFrameAction::Reconcile;
                }
            } else if pending.state.was_returned() {
                pending
                    .state
                    .set_status(ListenerStatus::Suspended, Some(error));
                pending.state.drain_unaccepted(false).await;
                inner.schedule_reconcile();
            } else {
                inner.fail_initial_listener(&pending.state, error);
                return RelayFrameAction::Reconcile;
            }
        }
        Frame::Offer {
            pipe_id,
            binding_id,
            destination,
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
                    return RelayFrameAction::Stop;
                }
                return RelayFrameAction::Continue;
            }
            let Some(registration) = session.registrations.get(&destination) else {
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
                            message: "Binding incarnation is stale".to_owned(),
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
                    destination = %destination,
                    "Listener incoming queue compaction could not preserve live Pipes"
                );
                return RelayFrameAction::Stop;
            }
            let permit = match registration.state.incoming_tx.try_reserve() {
                Ok(permit) => permit,
                Err(error) => {
                    let error = match error {
                        mpsc::error::TrySendError::Full(()) => resource_exhausted(
                            ResourceLimitKind::ListenerPendingPipes,
                            PeerObservation::Observed,
                        ),
                        mpsc::error::TrySendError::Closed(()) => Error::new(
                            ErrorCode::Unavailable,
                            PeerObservation::Observed,
                            "Listener incoming queue is closed",
                        ),
                    };
                    return listener_frame_action(
                        send_bounded(
                            transport,
                            Frame::OfferRejected {
                                pipe_id,
                                code: to_wire_code(error.code()),
                                message: error.message().to_owned(),
                            },
                            inner.config.operation_timeout,
                            session_cancel,
                        )
                        .await,
                    );
                }
            };
            let live = match inner
                .resources
                .try_reserve_incoming(&registration.state.live_pipe_slots)
            {
                Ok(live) => live,
                Err(error) => {
                    return listener_frame_action(
                        send_bounded(
                            transport,
                            Frame::OfferRejected {
                                pipe_id,
                                code: to_wire_code(error.code()),
                                message: error.message().to_owned(),
                            },
                            inner.config.operation_timeout,
                            session_cancel,
                        )
                        .await,
                    );
                }
            };
            let pipe_resources = inner.resources.pipe_resources(
                live,
                inner.config.resource_limits.max_buffered_bytes_per_pipe,
            );
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
                        return RelayFrameAction::Stop;
                    }
                };
                if !desired
                    .get(&destination)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() != ListenerStatus::Active
                {
                    false
                } else {
                    let Some(lifetime) = inner.lifetime.upgrade() else {
                        inner.cancel.cancel();
                        return RelayFrameAction::Stop;
                    };
                    let (pipe, state) = PipeState::pair_with_lifetime(
                        pipe_id,
                        outbound.clone(),
                        inner.config.resource_limits.max_buffered_frames_per_pipe,
                        abandoned.clone(),
                        lifetime,
                        pipe_resources,
                    );
                    session.pipes.insert(
                        pipe_id,
                        LivePipe {
                            state,
                            listener: Some(Arc::downgrade(&registration.state)),
                        },
                    );
                    permit.send(pipe);
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.pipe.admitted",
                        destination = %destination,
                        binding_id = %binding_id.as_uuid(),
                        dialer_session_id = %pipe_id.origin_session_id().as_uuid(),
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
                return RelayFrameAction::Stop;
            }
        }
        Frame::Opened { pipe_id } if pipe_id.origin_session_id() == session_id => {
            let Some(pending) = session.pending_dials.remove(&pipe_id.connection_id()) else {
                return RelayFrameAction::Continue;
            };
            let Some(lifetime) = inner.lifetime.upgrade() else {
                return RelayFrameAction::Stop;
            };
            let (pipe, state) = PipeState::pair_with_lifetime(
                pipe_id,
                outbound.clone(),
                inner.config.resource_limits.max_buffered_frames_per_pipe,
                abandoned.clone(),
                lifetime,
                inner.resources.pipe_resources(
                    pending.resources,
                    inner.config.resource_limits.max_buffered_bytes_per_pipe,
                ),
            );
            if pending.response.send(Ok(pipe)).is_ok() {
                session.pipes.insert(
                    pipe_id,
                    LivePipe {
                        state,
                        listener: None,
                    },
                );
            } else if send_bounded(
                transport,
                Frame::Cancel { pipe_id },
                inner.config.operation_timeout,
                session_cancel,
            )
            .await
            .is_err()
            {
                return RelayFrameAction::Stop;
            }
        }
        Frame::DialFailed {
            connection_id,
            code,
            observation,
            message,
        } => {
            if let Some(pending) = session.pending_dials.remove(&connection_id) {
                let _ = pending.response.send(Err(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::from_wire(observation),
                    message,
                )));
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
                        return RelayFrameAction::Stop;
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
                return RelayFrameAction::Stop;
            }
        }
        Frame::Close { pipe_id } => {
            if let Some(pipe) = session.pipes.remove(&pipe_id) {
                pipe.state.close_normal();
                if !pipe.compact_listener_queue() {
                    return RelayFrameAction::Stop;
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
                    return RelayFrameAction::Stop;
                }
            } else if pipe_id.origin_session_id() == session_id
                && let Some(pending) = session.pending_dials.remove(&pipe_id.connection_id())
            {
                let _ = pending.response.send(Err(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::Observed,
                    message,
                )));
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
                return RelayFrameAction::Stop;
            }
        }
        Frame::Pong { .. } | Frame::Unpublished { .. } => {}
        _ => return RelayFrameAction::Stop,
    }
    if inner.cancel.is_cancelled() {
        RelayFrameAction::Stop
    } else {
        RelayFrameAction::Continue
    }
}

fn listener_frame_action<T, E>(result: Result<T, E>) -> RelayFrameAction {
    if result.is_ok() {
        RelayFrameAction::Continue
    } else {
        RelayFrameAction::Stop
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
