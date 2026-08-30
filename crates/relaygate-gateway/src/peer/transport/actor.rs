use std::{future::pending, sync::Arc};

use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinSet, time::sleep_until};
use tokio_util::sync::CancellationToken;

use super::{
    ActiveOpenSet, TransportCommand, TransportNotice, state::TransportActor, writer::run_writer,
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
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(config.writer_queue_capacity);
    let (writer_wake, mut writer_wakes) = mpsc::channel(1);
    let mut actor = TransportActor::new(
        &established,
        config,
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
        close.clone(),
    ));

    loop {
        let deadline = actor.next_open_deadline();
        tokio::select! {
            () = close.cancelled() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                actor.handle_command(command).await;
                actor.flush_stream_queues().await;
            }
            frame = source.next() => {
                let Some(frame) = frame else { break };
                match frame {
                    Ok(frame) => {
                        if !actor.handle_frame(frame).await {
                            break;
                        }
                        actor.flush_stream_queues().await;
                    }
                    Err(_) => break,
                }
            }
            () = wait_for_deadline(deadline), if deadline.is_some() => {
                actor.expire_open_deadlines().await;
                actor.flush_stream_queues().await;
            }
            wake = writer_wakes.recv() => {
                if wake.is_none() {
                    break;
                }
                actor.flush_stream_queues().await;
            }
        }
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
            streams: losses,
        })
        .await;
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}
