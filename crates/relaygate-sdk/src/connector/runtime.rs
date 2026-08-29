use std::{collections::HashMap, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, PipeId, SessionId, SessionRole};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{ConnectorCommand, ConnectorInner, ConnectorSession};
use crate::{
    Error, ErrorCode, PeerObservation, Pipe, Result,
    pipe::{PipeState, to_wire_code},
    session::{EstablishedSession, establish, next_backoff},
};

pub(super) async fn connector_supervisor(inner: Arc<ConnectorInner>, initial: EstablishedSession) {
    let mut established = Some(initial);
    let mut backoff = inner.config.reconnect_initial;
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
                    tracing::debug!(%error, "Connector reconnect failed");
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
        let (control_tx, control_rx) = mpsc::channel(inner.config.outbound_capacity);
        let (cancellation_tx, cancellation_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::channel(inner.config.outbound_capacity);
        let session_cancel = inner.cancel.child_token();
        let session = Arc::new(ConnectorSession {
            id: next.id,
            next_connection_id: Mutex::new(1),
            control: control_tx,
            cancellations: cancellation_tx,
        });
        inner.current.send_replace(Some(Arc::clone(&session)));
        run_connector_session(
            next,
            control_rx,
            cancellation_rx,
            outbound_tx,
            outbound_rx,
            inner.config.pipe_inbound_capacity,
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
    }
}

async fn run_connector_session(
    mut established: EstablishedSession,
    mut control: mpsc::Receiver<ConnectorCommand>,
    mut cancellations: mpsc::UnboundedReceiver<PipeId>,
    outbound_tx: mpsc::Sender<Frame>,
    mut outbound_rx: mpsc::Receiver<Frame>,
    pipe_inbound_capacity: usize,
    cancel: CancellationToken,
) {
    let mut pending = HashMap::<u64, oneshot::Sender<Result<Pipe>>>::new();
    let mut pipes = HashMap::<PipeId, Arc<PipeState>>::new();
    let (abandoned_tx, mut abandoned_rx) = mpsc::unbounded_channel();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
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
                if established.transport.send(frame).await.is_err() {
                    break;
                }
            }
            cancelled = cancellations.recv() => {
                let Some(pipe_id) = cancelled else { continue; };
                if let Some(response) = pending.remove(&pipe_id.connection_id()) {
                    let _ = response.send(Err(Error::new(
                        ErrorCode::Cancelled,
                        PeerObservation::MaybeObserved,
                        "committed OPEN was cancelled",
                    )));
                }
                if let Some(pipe) = pipes.remove(&pipe_id) {
                    pipe.close_normal();
                }
                if established.transport.send(Frame::Cancel { pipe_id }).await.is_err() {
                    break;
                }
            }
            abandoned = abandoned_rx.recv() => {
                let Some(pipe_id) = abandoned else { continue; };
                if pipes.remove(&pipe_id).is_some()
                    && established.transport.send(Frame::Close { pipe_id }).await.is_err()
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
                if established.transport.send(frame).await.is_err() {
                    break;
                }
                if let Some(pipe_id) = terminal_pipe {
                    pipes.remove(&pipe_id);
                }
            }
            incoming = established.transport.next() => {
                let Some(incoming) = incoming else { break; };
                let Ok(frame) = incoming else { break; };
                if !handle_connector_frame(
                    frame,
                    established.id,
                    &mut pending,
                    &mut pipes,
                    &outbound_tx,
                    pipe_inbound_capacity,
                    &abandoned_tx,
                    &mut established.transport,
                ).await {
                    break;
                }
            }
        }
    }

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
    outbound: &mpsc::Sender<Frame>,
    pipe_inbound_capacity: usize,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    transport: &mut crate::session::WireTransport,
) -> bool {
    match frame {
        Frame::Opened { pipe_id } if pipe_id.connector_session_id() == session_id => {
            let Some(response) = pending.remove(&pipe_id.connection_id()) else {
                let _ = transport.send(Frame::Cancel { pipe_id }).await;
                return true;
            };
            let (pipe, state) = PipeState::pair(
                pipe_id,
                outbound.clone(),
                pipe_inbound_capacity,
                abandoned.clone(),
            );
            if response.send(Ok(pipe)).is_ok() {
                pipes.insert(pipe_id, state);
            } else {
                let _ = transport.send(Frame::Cancel { pipe_id }).await;
            }
        }
        Frame::OpenFailed {
            connection_id,
            code,
            observation,
            message,
        } => {
            if let Some(response) = pending.remove(&connection_id) {
                let _ = response.send(Err(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::from_wire(observation),
                    message,
                )));
            }
        }
        Frame::Data { pipe_id, payload } => {
            if let Some(pipe) = pipes.get(&pipe_id)
                && let Err(error) = pipe.push_data(payload)
            {
                pipe.fail(error.clone());
                pipes.remove(&pipe_id);
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
                let _ = response.send(Err(Error::new(
                    ErrorCode::from_wire(code),
                    PeerObservation::MaybeObserved,
                    message,
                )));
            }
        }
        Frame::Ping { nonce } => {
            if transport.send(Frame::Pong { nonce }).await.is_err() {
                return false;
            }
        }
        Frame::Pong { .. } | Frame::Opened { .. } => {}
        _ => return false,
    }
    true
}
