use std::{collections::HashMap, sync::Arc};

use futures_util::StreamExt;
use relaygate_protocol::{Frame, PipeId, SessionId, SessionRole};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{ConnectorCommand, ConnectorInner, ConnectorSession};
use crate::{
    Error, ErrorCode, PeerObservation, Pipe, Result,
    pipe::{PipeState, to_wire_code},
    session::{
        EstablishedSession, ReconnectBackoff, SessionHeartbeat, SessionOutbound,
        SessionOutboundReceiver, establish, send_bounded, session_outbound_channel,
        wait_for_heartbeat,
    },
};

pub(super) async fn connector_supervisor(inner: Arc<ConnectorInner>, initial: EstablishedSession) {
    let mut established = Some(initial);
    let mut backoff = ReconnectBackoff::new(
        inner.config.reconnect_initial,
        inner.config.reconnect_maximum,
    );
    loop {
        if inner.cancel.is_cancelled() {
            inner.current.send_replace(None);
            return;
        }
        let next = match established.take() {
            Some(session) => session,
            None => match establish(&inner.config, SessionRole::Connector).await {
                Ok(session) => session,
                Err(error) => {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.reconnect_failed",
                        role = "connector",
                        error_code = ?error.code(),
                        observation = ?error.observation(),
                        "Connector session reconnect failed"
                    );
                    tokio::select! {
                        _ = inner.cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff.next_delay()) => {}
                    }
                    continue;
                }
            },
        };
        let started_at = Instant::now();
        let (control_tx, control_rx) = mpsc::channel(inner.config.outbound_capacity);
        let (cancellation_tx, cancellation_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = session_outbound_channel(inner.config.outbound_capacity);
        let session_cancel = inner.cancel.child_token();
        let heartbeat = SessionHeartbeat::new(&inner.config, next.id, 0x43);
        let session = Arc::new(ConnectorSession {
            id: next.id,
            next_connection_id: Mutex::new(1),
            control: control_tx,
            cancellations: cancellation_tx,
            cancel: session_cancel.clone(),
        });
        inner.current.send_replace(Some(Arc::clone(&session)));
        run_connector_session(
            next,
            control_rx,
            cancellation_rx,
            outbound_tx,
            outbound_rx,
            inner.config.pipe_inbound_capacity,
            inner.config.operation_timeout,
            heartbeat,
            inner.lifetime.clone(),
            session_cancel,
        )
        .await;
        if inner
            .current
            .borrow()
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &session))
        {
            inner.current.send_replace(None);
        }
        if inner.cancel.is_cancelled() {
            return;
        }
        if started_at.elapsed() >= inner.config.reconnect_maximum {
            backoff.reset();
        }
        tokio::select! {
            _ = inner.cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_connector_session(
    mut established: EstablishedSession,
    mut control: mpsc::Receiver<ConnectorCommand>,
    mut cancellations: mpsc::UnboundedReceiver<PipeId>,
    outbound_tx: SessionOutbound,
    mut outbound_rx: SessionOutboundReceiver,
    pipe_inbound_capacity: usize,
    operation_timeout: std::time::Duration,
    mut heartbeat: SessionHeartbeat,
    lifetime: std::sync::Weak<crate::lifetime::RuntimeLifetime>,
    cancel: CancellationToken,
) {
    let mut pending = HashMap::<u64, oneshot::Sender<Result<Pipe>>>::new();
    let mut pipes = HashMap::<PipeId, Arc<PipeState>>::new();
    let (abandoned_tx, mut abandoned_rx) = mpsc::unbounded_channel();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            incoming = established.transport.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(frame) = incoming else { break; };
                heartbeat.observe_inbound(&frame);
                if heartbeat.response_timed_out() {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.heartbeat_timeout",
                        role = "connector",
                        session_id = %established.id.as_uuid(),
                        "Connector session heartbeat response timed out"
                    );
                    break;
                }
                if !handle_connector_frame(
                    frame,
                    established.id,
                    &mut pending,
                    &mut pipes,
                    &outbound_tx,
                    pipe_inbound_capacity,
                    &abandoned_tx,
                    &mut established.transport,
                    operation_timeout,
                    &lifetime,
                    &cancel,
                ).await {
                    break;
                }
            }
            () = wait_for_heartbeat(heartbeat.next_deadline()) => {
                let Some(frame) = heartbeat.on_deadline() else {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.heartbeat_timeout",
                        role = "connector",
                        session_id = %established.id.as_uuid(),
                        "Connector session heartbeat response timed out"
                    );
                    break;
                };
                if send_bounded(&mut established.transport, frame, operation_timeout, &cancel)
                    .await
                    .is_err()
                {
                    break;
                }
                heartbeat.mark_probe_committed();
            }
            command = control.recv() => {
                let Some(command) = command else { break; };
                let frame = match command {
                    ConnectorCommand::Open { connection_id, client_id, response } => {
                        if pending.insert(connection_id, response).is_some() {
                            if let Some(response) = pending.remove(&connection_id) {
                                let _ = response.send(Err(Error::new(
                                    ErrorCode::AlreadyExists,
                                    PeerObservation::NotObserved,
                                    "ConnectionId is already in flight",
                                )));
                            }
                            continue;
                        }
                        Frame::Open { connection_id, client_id }
                    }
                };
                if send_bounded(&mut established.transport, frame, operation_timeout, &cancel)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            cancelled = cancellations.recv() => {
                let Some(pipe_id) = cancelled else { continue; };
                let mut removed_current_state = false;
                if let Some(response) = pending.remove(&pipe_id.connection_id()) {
                    removed_current_state = true;
                    let _ = response.send(Err(Error::new(
                        ErrorCode::Cancelled,
                        PeerObservation::MaybeObserved,
                        "committed OPEN was cancelled",
                    )));
                }
                if let Some(pipe) = pipes.remove(&pipe_id) {
                    removed_current_state = true;
                    pipe.close_normal();
                }
                if !removed_current_state {
                    continue;
                }
                if send_bounded(
                    &mut established.transport,
                    Frame::Cancel { pipe_id },
                    operation_timeout,
                    &cancel,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            frame = outbound_rx.recv() => {
                let Some(frame) = frame else { break; };
                let terminal_pipe = match &frame {
                    Frame::Close { pipe_id } | Frame::Reset { pipe_id, .. } => Some(*pipe_id),
                    Frame::Fin { pipe_id } if pipes.get(pipe_id).is_some_and(|pipe| pipe.is_finished()) => Some(*pipe_id),
                    _ => None,
                };
                if send_bounded(&mut established.transport, frame, operation_timeout, &cancel)
                    .await
                    .is_err()
                {
                    break;
                }
                if let Some(pipe_id) = terminal_pipe {
                    pipes.remove(&pipe_id);
                }
            }
            abandoned = abandoned_rx.recv() => {
                let Some(pipe_id) = abandoned else { continue; };
                if pipes.remove(&pipe_id).is_some()
                    && send_bounded(
                        &mut established.transport,
                        Frame::Close { pipe_id },
                        operation_timeout,
                        &cancel,
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
        role = "connector",
        session_id = %established.id.as_uuid(),
        pending_opens = pending.len(),
        live_pipes = pipes.len(),
        "Connector session ended"
    );
    for (_, response) in pending {
        let _ = response.send(Err(Error::maybe_observed(
            "ConnectorSession transport ended after OPEN commit",
        )));
    }
    for (_, pipe) in pipes {
        pipe.fail(Error::unavailable("ConnectorSession transport ended"));
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connector_frame(
    frame: Frame,
    session_id: SessionId,
    pending: &mut HashMap<u64, oneshot::Sender<Result<Pipe>>>,
    pipes: &mut HashMap<PipeId, Arc<PipeState>>,
    outbound: &SessionOutbound,
    pipe_inbound_capacity: usize,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    transport: &mut crate::session::WireTransport,
    operation_timeout: std::time::Duration,
    lifetime: &std::sync::Weak<crate::lifetime::RuntimeLifetime>,
    cancel: &CancellationToken,
) -> bool {
    match frame {
        Frame::Opened { pipe_id } if pipe_id.connector_session_id() == session_id => {
            let Some(response) = pending.remove(&pipe_id.connection_id()) else {
                return true;
            };
            let Some(lifetime) = lifetime.upgrade() else {
                return false;
            };
            let (pipe, state) = PipeState::pair_with_lifetime(
                pipe_id,
                outbound.clone(),
                pipe_inbound_capacity,
                abandoned.clone(),
                lifetime,
            );
            if response.send(Ok(pipe)).is_ok() {
                pipes.insert(pipe_id, state);
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.open.succeeded",
                    connector_session_id = %pipe_id.connector_session_id().as_uuid(),
                    connection_id = pipe_id.connection_id(),
                    "Connector OPEN succeeded"
                );
            } else if send_bounded(
                transport,
                Frame::Cancel { pipe_id },
                operation_timeout,
                cancel,
            )
            .await
            .is_err()
            {
                return false;
            }
        }
        Frame::OpenFailed {
            connection_id,
            code,
            observation,
            message,
        } => {
            if let Some(response) = pending.remove(&connection_id) {
                let error = Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::from_wire(observation),
                    message,
                );
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.open.failed",
                    connector_session_id = %session_id.as_uuid(),
                    connection_id,
                    error_code = ?error.code(),
                    observation = ?error.observation(),
                    "Connector OPEN failed"
                );
                let _ = response.send(Err(error));
            }
        }
        Frame::Data { pipe_id, payload } => {
            if let Some(pipe) = pipes.get(&pipe_id)
                && let Err(error) = pipe.push_data(payload)
            {
                pipe.fail(error.clone());
                pipes.remove(&pipe_id);
                let _ = send_bounded(
                    transport,
                    Frame::Reset {
                        pipe_id,
                        code: to_wire_code(error.code()),
                        message: error.message().to_owned(),
                    },
                    operation_timeout,
                    cancel,
                )
                .await;
            }
        }
        Frame::Fin { pipe_id } => {
            if let Some(pipe) = pipes.get(&pipe_id) {
                pipe.remote_fin();
                if pipe.is_finished() {
                    pipes.remove(&pipe_id);
                }
            }
        }
        Frame::Close { pipe_id } => {
            if let Some(pipe) = pipes.remove(&pipe_id) {
                pipe.close_normal();
            }
        }
        Frame::Reset {
            pipe_id,
            code,
            message,
        } => {
            if let Some(pipe) = pipes.remove(&pipe_id) {
                pipe.fail(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::Observed,
                    message,
                ));
            } else if pipe_id.connector_session_id() == session_id
                && let Some(response) = pending.remove(&pipe_id.connection_id())
            {
                let error = Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::MaybeObserved,
                    message,
                );
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.open.failed",
                    connector_session_id = %pipe_id.connector_session_id().as_uuid(),
                    connection_id = pipe_id.connection_id(),
                    error_code = ?error.code(),
                    observation = ?error.observation(),
                    "Connector OPEN was reset"
                );
                let _ = response.send(Err(error));
            }
        }
        Frame::Ping { nonce } => {
            if send_bounded(transport, Frame::Pong { nonce }, operation_timeout, cancel)
                .await
                .is_err()
            {
                return false;
            }
        }
        Frame::Pong { .. } | Frame::Opened { .. } => {}
        _ => return false,
    }
    true
}
