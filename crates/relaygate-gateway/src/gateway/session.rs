#[cfg(test)]
use std::sync::atomic::Ordering;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, MAX_HELLO_FRAME_LEN};
use relaygate_transport::BoxedIo;
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc},
    time::{Instant, sleep_until, timeout_at},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::authorization::ControlOperation;
use crate::metrics::{HeartbeatTransport, observe_heartbeat_round_trip, observe_heartbeat_timeout};
use crate::state::ProtocolViolation;

use super::{Inner, heartbeat::SessionHeartbeat};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const SDK_FRAME_INITIAL_CAPACITY: usize = 2 * 1024;
const SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

mod admission;

impl Inner {
    pub(super) async fn run_session(
        self: Arc<Self>,
        stream: BoxedIo,
        cancellation: CancellationToken,
        handshake_slot: OwnedSemaphorePermit,
    ) -> Result<(), SessionError> {
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        let mut framed = Framed::with_capacity(
            stream,
            FrameCodec::new(MAX_HELLO_FRAME_LEN),
            SDK_FRAME_INITIAL_CAPACITY,
        );
        framed.set_backpressure_boundary(SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY);
        let first = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = timeout_at(deadline, framed.next()) => {
                result
                    .map_err(|_| SessionError::HandshakeTimeout)?
                    .ok_or(SessionError::HandshakeClosed)??
            }
        };
        let Frame::Hello = first else {
            return Err(SessionError::ExpectedHello);
        };
        *framed.codec_mut() = FrameCodec::new(self.max_frame_len);

        let (sender, receiver) = mpsc::channel(self.writer_queue_capacity);
        let heartbeat_sender = sender.clone();
        let Some(session_id) = self.lock_state().add_session(sender, cancellation.clone()) else {
            return Err(SessionError::ResourceExhausted);
        };
        let run_inner = Arc::clone(&self);
        let run_cancellation = cancellation.clone();
        let read_cancellation = cancellation.clone();
        run_admitted_session(
            async move {
                #[cfg(test)]
                run_inner.panic_after_admission_if_armed();
                tokio::select! {
                    _ = run_cancellation.cancelled() => return Ok(()),
                    result = timeout_at(deadline, framed.send(Frame::Welcome { session_id })) => {
                        result.map_err(|_| SessionError::HandshakeTimeout)??;
                    }
                }
                drop(handshake_slot);
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
        mut source: futures_util::stream::SplitStream<Framed<BoxedIo, FrameCodec>>,
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
                        observe_heartbeat_timeout(HeartbeatTransport::Sdk);
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
                    if let Some(round_trip) = heartbeat.observe_inbound(&frame) {
                        observe_heartbeat_round_trip(HeartbeatTransport::Sdk, round_trip);
                    }
                    if heartbeat.response_timed_out() {
                        observe_heartbeat_timeout(HeartbeatTransport::Sdk);
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.session.heartbeat_timeout",
                            session_id = %session_id.as_uuid(),
                            "SDK session heartbeat response timed out"
                        );
                        break;
                    }
                    let (operation, access_token) = match ControlOperation::take(frame) {
                        Ok(control) => control,
                        Err(frame) => {
                            let actions = {
                                let mut state = self.lock_state();
                                let actions = state.handle(session_id, frame)?;
                                self.commit_registration_actions(&actions);
                                actions
                            };
                            self.send_session_actions(
                                actions,
                                session_id,
                                &sender,
                                &cancellation,
                                heartbeat.next_deadline(),
                            ).await?;
                            continue;
                        }
                    };
                    let early = self.lock_state().prepare_authorization(
                        session_id,
                        &operation,
                        std::time::Instant::now(),
                    );
                    if let Some(actions) = early {
                        self.send_session_actions(
                            actions,
                            session_id,
                            &sender,
                            &cancellation,
                            heartbeat.next_deadline(),
                        ).await?;
                        continue;
                    }
                    let verification_started = std::time::Instant::now();
                    let verification_deadline = Instant::now()
                        .checked_add(self.authorization_timeout)
                        .ok_or(SessionError::AuthorizationDeadline)?;
                    let verified = match self.authorization.start(access_token, &operation) {
                        Ok(job) => tokio::select! {
                            _ = cancellation.cancelled() => return Ok(()),
                            result = job.finish(verification_deadline) => result,
                        },
                        Err(code) => Err(code),
                    };
                    let (outcome, code) = match &verified {
                        Ok(_) => ("success", "ok"),
                        Err(code) => ("error", crate::state::error_code_name(*code)),
                    };
                    metrics::counter!(
                        "relaygate_gateway_authorization_results_total",
                        "operation" => operation.name(),
                        "outcome" => outcome,
                        "code" => code,
                    ).increment(1);
                    metrics::histogram!(
                        "relaygate_gateway_authorization_duration_seconds",
                        "operation" => operation.name(),
                        "outcome" => outcome,
                    ).record(verification_started.elapsed().as_secs_f64());
                    let actions = {
                        let mut state = self.lock_state();
                        let actions = match verified {
                            Ok(verified) => state.commit_authorized(
                                session_id,
                                operation,
                                verified,
                                std::time::Instant::now(),
                            ),
                            Err(code) => state.authorization_failed(session_id, &operation, code),
                        };
                        self.commit_registration_actions(&actions);
                        actions
                    };
                    self.send_session_actions(
                        actions,
                        session_id,
                        &sender,
                        &cancellation,
                        heartbeat.next_deadline(),
                    ).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_session_actions(
        self: &Arc<Self>,
        mut actions: Vec<crate::state::GatewayAction>,
        session_id: relaygate_protocol::SessionId,
        sender: &mpsc::Sender<Frame>,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SessionError> {
        if admission::is_local_rejection(&actions, session_id)
            && let Some(crate::state::GatewayAction::SendSdkFrame(delivery)) = actions.pop()
        {
            admission::send_rejection(sender, delivery.frame, cancellation, deadline).await?;
        } else {
            self.execute_all(actions).await;
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
    mut sink: futures_util::stream::SplitSink<Framed<BoxedIo, FrameCodec>, Frame>,
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
    #[error("SDK HELLO exchange did not finish before the handshake deadline")]
    HandshakeTimeout,
    #[error("first SDK frame was not HELLO")]
    ExpectedHello,
    #[error("Gateway SDK session limit reached")]
    ResourceExhausted,
    #[error("SDK admission response could not be queued before the liveness deadline")]
    AdmissionResponseUnavailable,
    #[error("authorization timeout is too large to form a deadline")]
    AuthorizationDeadline,
    #[error(transparent)]
    Protocol(#[from] relaygate_protocol::ProtocolError),
    #[error(transparent)]
    ProtocolViolation(#[from] ProtocolViolation),
}
