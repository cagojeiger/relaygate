#[cfg(test)]
use std::sync::atomic::Ordering;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{sleep_until, timeout},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::state::ProtocolViolation;

use super::{Inner, heartbeat::SessionHeartbeat};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

impl Inner {
    pub(super) async fn run_session(
        self: Arc<Self>,
        stream: TcpStream,
        cancellation: CancellationToken,
    ) -> Result<(), SessionError> {
        let mut framed = Framed::new(stream, FrameCodec::new(self.max_frame_len));
        let first = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = timeout(HANDSHAKE_TIMEOUT, framed.next()) => {
                result
                    .map_err(|_| SessionError::HandshakeTimeout)?
                    .ok_or(SessionError::HandshakeClosed)??
            }
        };
        let Frame::Hello { role } = first else {
            return Err(SessionError::ExpectedHello);
        };

        let (sender, receiver) = mpsc::channel(self.writer_queue_capacity);
        let heartbeat_sender = sender.clone();
        let Some(session_id) = self
            .lock_state()
            .add_session(role, sender, cancellation.clone())
        else {
            return Err(SessionError::ResourceExhausted);
        };
        let run_inner = Arc::clone(&self);
        let run_cancellation = cancellation.clone();
        let read_cancellation = cancellation.clone();
        run_admitted_session(
            async move {
                #[cfg(test)]
                run_inner.panic_after_admission_if_armed();
                framed.send(Frame::Welcome { session_id }).await?;
                let (sink, source) = framed.split();
                let read =
                    run_inner.read_frames(session_id, heartbeat_sender, source, read_cancellation);
                let write = write_frames(receiver, sink);
                tokio::select! {
                    _ = run_cancellation.cancelled() => Ok(()),
                    result = read => result,
                    result = write => result,
                }
            },
            || async {
                cancellation.cancel();
                self.cleanup(session_id).await;
            },
        )
        .await
    }

    async fn read_frames(
        self: Arc<Self>,
        session_id: relaygate_protocol::SessionId,
        sender: mpsc::Sender<Frame>,
        mut source: futures_util::stream::SplitStream<Framed<TcpStream, FrameCodec>>,
        cancellation: CancellationToken,
    ) -> Result<(), SessionError> {
        let mut heartbeat = SessionHeartbeat::new(
            self.heartbeat_idle_interval,
            self.heartbeat_response_timeout,
            session_id,
            0x47,
        );
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                () = sleep_until(heartbeat.next_deadline()) => {
                    let Some(frame) = heartbeat.on_deadline() else {
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.session.heartbeat_timeout",
                            session_id = %session_id.as_uuid(),
                            "SDK session heartbeat response timed out"
                        );
                        break;
                    };
                    if sender.try_send(frame).is_err() {
                        cancellation.cancel();
                        break;
                    }
                    heartbeat.mark_probe_committed();
                }
                frame = source.next() => {
                    let Some(frame) = frame else { break; };
                    let frame = frame?;
                    heartbeat.observe_inbound(&frame);
                    if heartbeat.response_timed_out() {
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.session.heartbeat_timeout",
                            session_id = %session_id.as_uuid(),
                            "SDK session heartbeat response timed out"
                        );
                        break;
                    }
                    let actions = {
                        let mut state = self.lock_state();
                        let actions = state.handle(session_id, frame)?;
                        self.commit_registration_actions(&actions);
                        actions
                    };
                    self.execute_all(actions).await;
                }
            }
        }
        Ok(())
    }

    async fn cleanup(self: &Arc<Self>, session_id: relaygate_protocol::SessionId) {
        let actions = {
            let mut state = self.lock_state();
            let actions = state.remove_session(session_id);
            self.commit_registration_actions(&actions);
            actions
        };
        self.execute_all(actions).await;
    }

    #[cfg(test)]
    fn panic_after_admission_if_armed(&self) {
        if self
            .panic_next_session_after_admission
            .swap(false, Ordering::SeqCst)
        {
            resume_unwind(Box::new("synthetic admitted SDK session panic"));
        }
    }
}

async fn run_admitted_session<Run, Cleanup, CleanupFuture>(
    run: Run,
    cleanup: Cleanup,
) -> Result<(), SessionError>
where
    Run: Future<Output = Result<(), SessionError>>,
    Cleanup: FnOnce() -> CleanupFuture,
    CleanupFuture: Future<Output = ()>,
{
    let result = AssertUnwindSafe(run).catch_unwind().await;
    cleanup().await;
    match result {
        Ok(result) => result,
        Err(payload) => resume_unwind(payload),
    }
}

async fn write_frames(
    mut receiver: mpsc::Receiver<Frame>,
    mut sink: futures_util::stream::SplitSink<Framed<TcpStream, FrameCodec>, Frame>,
) -> Result<(), SessionError> {
    while let Some(frame) = receiver.recv().await {
        sink.send(frame).await?;
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SessionError {
    #[error("SDK session closed before HELLO")]
    HandshakeClosed,
    #[error("SDK session did not send HELLO before the handshake deadline")]
    HandshakeTimeout,
    #[error("first SDK frame was not HELLO")]
    ExpectedHello,
    #[error("Gateway SDK session limit reached")]
    ResourceExhausted,
    #[error(transparent)]
    Protocol(#[from] relaygate_protocol::ProtocolError),
    #[error(transparent)]
    ProtocolViolation(#[from] ProtocolViolation),
}
