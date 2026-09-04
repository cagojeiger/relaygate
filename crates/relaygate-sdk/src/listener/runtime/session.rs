use futures_util::StreamExt;
use relaygate_protocol::Frame;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    ListenerFrameAction, ListenerSessionState,
    cleanup::cleanup_listener_session,
    frame::handle_listener_frame,
    registration::{reconcile_registrations, wait_for_registration_deadline},
};
use crate::{
    listener::ListenerRuntimeInner,
    observability::ReconnectEpisode,
    session::{
        EstablishedSession, SessionHeartbeat, send_bounded, session_outbound_channel,
        wait_for_heartbeat,
    },
};

pub(super) async fn run_listener_session(
    mut established: EstablishedSession,
    inner: &ListenerRuntimeInner,
    session_cancel: CancellationToken,
    reconnect_episode: &mut Option<ReconnectEpisode>,
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
        if inner.desired_is_converged()
            && let Some(episode) = reconnect_episode.take()
        {
            episode.recover();
        }
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
                heartbeat.mark_probe_committed();
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

    cleanup_listener_session(
        established.id,
        inner,
        state,
        timed_out_request,
        registration_succeeded,
    )
    .await
}
