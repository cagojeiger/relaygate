use std::{cmp::min, future::pending, sync::Arc};

use futures_util::StreamExt;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

use super::{
    ActiveOpenSet, TransportCloseReason, TransportCommand, TransportNotice,
    liveness::{LivenessAction, TransportLiveness, staggered_interval},
    state::TransportActor,
    writer::run_writer,
};
use crate::peer::{config::GatewayPeerConfig, handshake::EstablishedPeer};

/// Owns the read-side lifecycle for one authenticated peer transport.
/// Protocol state transitions live in `command`, `inbound`, and `state`;
/// `writer` remains the only task allowed to write frames to the socket.
pub(super) async fn run_transport_actor(
    established: EstablishedPeer,
    config: GatewayPeerConfig,
    mut commands: mpsc::Receiver<TransportCommand>,
    notices: mpsc::Sender<TransportNotice>,
    active_opens: Arc<ActiveOpenSet>,
    stream_count: Arc<std::sync::atomic::AtomicUsize>,
    close: CancellationToken,
) {
    let peer_gateway_id = established.remote_gateway_id;
    let peer_transport_id = established.peer_transport_id;
    let heartbeat_idle_interval = staggered_interval(
        config.heartbeat_idle_interval,
        peer_transport_id,
        established.local_endpoint,
    );
    let heartbeat_response_timeout = config.heartbeat_response_timeout;
    let idle_retirement_timeout = config.idle_retirement_timeout;
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(config.writer_queue_capacity);
    let (writer_wake, mut writer_wakes) = mpsc::channel(1);
    let (writer_failure, mut writer_failures) = mpsc::channel(1);
    let actor_config = config.clone();
    let mut actor = TransportActor::new(
        &established,
        actor_config,
        aggregate_writer,
        notices.clone(),
        active_opens,
        stream_count,
        close.clone(),
    );
    let (sink, mut source) = established.framed.split();
    let mut writer_tasks = JoinSet::new();
    writer_tasks.spawn(run_writer(
        sink,
        aggregate_receiver,
        writer_wake,
        writer_failure,
        close.clone(),
    ));
    let mut liveness = TransportLiveness::new(
        heartbeat_idle_interval,
        heartbeat_response_timeout,
        idle_retirement_timeout,
    );
    liveness.sync_stream_state(actor.streams.is_empty());
    let mut close_reason = TransportCloseReason::LocalClose;

    loop {
        let deadline = earliest_deadline(actor.next_open_deadline(), liveness.next_deadline());
        tokio::select! {
            () = close.cancelled() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                actor.handle_command(command).await;
                actor.flush_stream_queues().await;
                liveness.sync_stream_state(actor.streams.is_empty());
            }
            frame = source.next() => {
                let Some(frame) = frame else {
                    close_reason = TransportCloseReason::RemoteClosed;
                    break;
                };
                match frame {
                    Ok(frame) => {
                        #[cfg(test)]
                        if matches!(
                            established.local_endpoint,
                            crate::peer::identity::StreamEndpoint::Dialer
                        ) && matches!(&frame, crate::peer::frame::PeerFrame::Pong { .. })
                            && config
                                .drop_dialer_heartbeat_pong_gate
                                .as_ref()
                                .is_some_and(crate::peer::config::DropHeartbeatPongGate::trip)
                        {
                            continue;
                        }
                        liveness.observe_inbound(&frame);
                        if liveness.response_timed_out() {
                            tracing::debug!(
                                component = "gateway",
                                event = "gateway.peer.transport.heartbeat_timeout",
                                peer_gateway_id = %actor.peer_gateway_id.as_uuid(),
                                peer_transport_id = %actor.peer_transport_id.as_uuid(),
                                streams = actor.streams.len(),
                                "PeerTransport heartbeat response timed out"
                            );
                            close_reason = TransportCloseReason::HeartbeatTimeout;
                            break;
                        }
                        if !actor.handle_frame(frame).await {
                            close_reason = TransportCloseReason::ProtocolError;
                            break;
                        }
                        actor.flush_stream_queues().await;
                        liveness.sync_stream_state(actor.streams.is_empty());
                    }
                    Err(error) => {
                        close_reason = if error.is_io() {
                            TransportCloseReason::RemoteClosed
                        } else {
                            TransportCloseReason::ProtocolError
                        };
                        break;
                    }
                }
            }
            () = wait_for_deadline(deadline), if deadline.is_some() => {
                let now = Instant::now();
                actor.expire_open_deadlines().await;
                actor.flush_stream_queues().await;
                if let Some(action) = liveness.on_deadline(
                    now,
                    actor.streams.is_empty(),
                    commands.is_empty(),
                ) {
                    match action {
                        LivenessAction::Ping(frame) => {
                            if actor.aggregate_writer.try_send(frame).is_err() {
                                close_reason = TransportCloseReason::WriterFailed;
                                break;
                            }
                            liveness.mark_probe_committed();
                        }
                        LivenessAction::HeartbeatTimeout => {
                            tracing::debug!(
                                component = "gateway",
                                event = "gateway.peer.transport.heartbeat_timeout",
                                peer_gateway_id = %actor.peer_gateway_id.as_uuid(),
                                peer_transport_id = %actor.peer_transport_id.as_uuid(),
                                streams = actor.streams.len(),
                                "PeerTransport heartbeat response timed out"
                            );
                            close_reason = TransportCloseReason::HeartbeatTimeout;
                            break;
                        }
                        LivenessAction::IdleRetired => {
                            tracing::debug!(
                                component = "gateway",
                                event = "gateway.peer.transport.idle_retired",
                                peer_gateway_id = %actor.peer_gateway_id.as_uuid(),
                                peer_transport_id = %actor.peer_transport_id.as_uuid(),
                                streams = actor.streams.len(),
                                "PeerTransport idle retirement timeout expired"
                            );
                            close_reason = TransportCloseReason::IdleRetired;
                            break;
                        }
                    }
                }
                liveness.sync_stream_state(actor.streams.is_empty());
            }
            wake = writer_wakes.recv() => {
                if wake.is_none() {
                    if !close.is_cancelled() {
                        close_reason = TransportCloseReason::WriterFailed;
                    }
                    break;
                }
                actor.flush_stream_queues().await;
                liveness.sync_stream_state(actor.streams.is_empty());
            }
            failure = writer_failures.recv() => {
                if failure.is_some() || !close.is_cancelled() {
                    close_reason = TransportCloseReason::WriterFailed;
                }
                break;
            }
        }
    }

    if let Some(failure_reason) = actor.failure_reason {
        close_reason = failure_reason;
    }
    close.cancel();
    let losses = actor.drain_losses();
    drop(actor.aggregate_writer);
    while writer_tasks.join_next().await.is_some() {}

    // The manager keeps draining notices until transports report loss (or the
    // bounded shutdown deadline aborts all tasks), preserving per-transport
    // FIFO ordering behind all prior stream events.
    let _ = notices
        .send(TransportNotice::TransportLost {
            peer_gateway_id,
            peer_transport_id,
            reason: close_reason,
            streams: losses,
        })
        .await;
}

fn earliest_deadline(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(min(left, right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}
