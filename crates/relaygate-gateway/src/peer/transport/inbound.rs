use std::collections::VecDeque;

use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};

use super::state::{RuntimeStream, StreamOrigin, TransportActor, failed_not_observed};
use crate::peer::{
    event::{PeerEvent, PeerFailure},
    frame::PeerFrame,
    identity::{OpenIdentity, PeerOpenProgress, StreamId},
    stream::RelayStream,
};

/// Validates and applies peer-originated frames. Unknown or already-terminal
/// StreamIds are late no-ops; invalid frames for a current stream receive a
/// stream-scoped protocol RESET without taking down the opposite pair slot.
impl TransportActor {
    pub(super) async fn handle_frame(&mut self, frame: PeerFrame) -> bool {
        match frame {
            PeerFrame::Open {
                stream_id,
                open_identity,
                client_id,
                listener_session_id,
                binding_id,
            } => {
                self.receive_open(
                    stream_id,
                    open_identity,
                    client_id,
                    listener_session_id,
                    binding_id,
                )
                .await;
                true
            }
            PeerFrame::Opened { stream_id } => {
                self.receive_opened(stream_id).await;
                true
            }
            PeerFrame::Failed {
                stream_id,
                code,
                observation,
                message,
            } => {
                self.receive_failed(stream_id, code, observation, message)
                    .await;
                true
            }
            PeerFrame::Data { stream_id, payload } => {
                self.receive_data(stream_id, payload).await;
                true
            }
            PeerFrame::Fin { stream_id } => {
                self.receive_fin(stream_id).await;
                true
            }
            PeerFrame::Close { stream_id } => {
                self.receive_close(stream_id).await;
                true
            }
            PeerFrame::Reset {
                stream_id,
                code,
                message,
            } => {
                self.receive_reset(stream_id, code, message).await;
                true
            }
            PeerFrame::Ping { nonce } => {
                if self
                    .aggregate_writer
                    .try_send(PeerFrame::Pong { nonce })
                    .is_err()
                {
                    self.close.cancel();
                    return false;
                }
                true
            }
            PeerFrame::Pong { .. } => true,
            PeerFrame::Hello(_) | PeerFrame::Welcome(_) | PeerFrame::HandshakeRejected { .. } => {
                false
            }
        }
    }

    async fn receive_open(
        &mut self,
        stream_id: StreamId,
        open_identity: OpenIdentity,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
    ) {
        if self.remote_guard.accept_open(stream_id).is_err() {
            self.reject_remote_open(
                stream_id,
                ErrorCode::ProtocolError,
                "remote StreamId is invalid or not strictly increasing",
            );
            return;
        }
        if open_identity.entry_gateway() != self.peer_gateway_id {
            self.reject_remote_open(
                stream_id,
                ErrorCode::PermissionDenied,
                "peer OPEN entry Gateway identity does not match the authenticated transport",
            );
            return;
        }
        if self.streams.len() >= self.config.max_streams_per_transport {
            self.reject_remote_open(
                stream_id,
                ErrorCode::ResourceExhausted,
                "peer transport stream limit is reached",
            );
            return;
        }
        match self.active_opens.reserve(open_identity) {
            Ok(true) => {}
            Ok(false) => {
                self.reject_remote_open(
                    stream_id,
                    ErrorCode::AlreadyExists,
                    "peer OPEN identity is already active",
                );
                return;
            }
            Err(_) => {
                self.close.cancel();
                return;
            }
        }

        self.streams.insert(
            stream_id,
            RuntimeStream {
                relay: RelayStream::owned_opening(open_identity),
                queued_frames: VecDeque::new(),
                terminal_queued: false,
                progress: PeerOpenProgress::AfterOpenCommit,
                origin: StreamOrigin::Remote,
                open_deadline: None,
            },
        );
        self.stream_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.emit(PeerEvent::IncomingOpen {
            key: self.key(stream_id),
            open_identity,
            client_id,
            listener_session_id,
            binding_id,
        })
        .await;
    }

    async fn receive_opened(&mut self, stream_id: StreamId) {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        if stream.origin != StreamOrigin::Local {
            self.protocol_reset(stream_id, "OPENED is invalid for a peer-originated stream")
                .await;
            return;
        }
        if stream.progress == PeerOpenProgress::Opened {
            return;
        }
        if stream.relay.opened().is_err() {
            self.protocol_reset(stream_id, "OPENED is invalid in the current stream phase")
                .await;
            return;
        }
        stream.progress = PeerOpenProgress::Opened;
        stream.open_deadline = None;
        let Some(open_identity) = stream.relay.owner() else {
            self.close.cancel();
            return;
        };
        self.emit(PeerEvent::Opened {
            key: self.key(stream_id),
            open_identity,
        })
        .await;
    }

    async fn receive_failed(
        &mut self,
        stream_id: StreamId,
        code: ErrorCode,
        observation: PeerObservation,
        message: String,
    ) {
        let Some(stream) = self.streams.get(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        if stream.origin != StreamOrigin::Local || stream.progress == PeerOpenProgress::Opened {
            self.protocol_reset(stream_id, "FAILED is invalid in the current stream phase")
                .await;
            return;
        }
        let Some(open_identity) = stream.relay.owner() else {
            self.close.cancel();
            return;
        };
        self.emit(PeerEvent::Failed {
            key: self.key(stream_id),
            open_identity,
            failure: PeerFailure::new(code, observation, message),
        })
        .await;
        self.finish_stream(stream_id).await;
    }

    async fn receive_data(&mut self, stream_id: StreamId, payload: bytes::Bytes) {
        let Some(stream) = self.streams.get(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        if stream.relay.data(self.remote_endpoint).is_err() {
            self.protocol_reset(stream_id, "DATA is invalid in the current stream phase")
                .await;
            return;
        }
        self.emit(PeerEvent::Data {
            key: self.key(stream_id),
            payload,
        })
        .await;
    }

    async fn receive_fin(&mut self, stream_id: StreamId) {
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        let newly_finished = match stream.relay.fin(self.remote_endpoint) {
            Ok(newly_finished) => newly_finished,
            Err(_) => {
                self.protocol_reset(stream_id, "FIN is invalid in the current stream phase")
                    .await;
                return;
            }
        };
        if !newly_finished {
            return;
        }
        let closed = stream.relay.is_closed();
        let finish_now = closed && stream.queued_frames.is_empty();
        if closed {
            stream.terminal_queued = true;
            stream.open_deadline = None;
        }
        self.emit(PeerEvent::Fin {
            key: self.key(stream_id),
        })
        .await;
        if finish_now {
            self.finish_stream(stream_id).await;
        }
    }

    async fn receive_close(&mut self, stream_id: StreamId) {
        let Some(stream) = self.streams.get(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        if !stream.relay.is_open() {
            self.protocol_reset(stream_id, "CLOSE is invalid before OPENED")
                .await;
            return;
        }
        self.emit(PeerEvent::Close {
            key: self.key(stream_id),
        })
        .await;
        self.finish_stream(stream_id).await;
    }

    async fn receive_reset(&mut self, stream_id: StreamId, code: ErrorCode, message: String) {
        let Some(stream) = self.streams.get(&stream_id) else {
            return;
        };
        if stream.terminal_queued {
            return;
        }
        self.emit_terminal_reset(stream_id, code, message).await;
        self.finish_stream(stream_id).await;
    }

    fn reject_remote_open(&mut self, stream_id: StreamId, code: ErrorCode, message: &str) {
        if self
            .aggregate_writer
            .try_send(failed_not_observed(stream_id, code, message))
            .is_err()
        {
            self.close.cancel();
        }
    }

    async fn protocol_reset(&mut self, stream_id: StreamId, message: &str) {
        if self
            .send_reset(stream_id, ErrorCode::ProtocolError, message.to_owned())
            .await
            .is_ok()
        {
            self.emit_terminal_reset(stream_id, ErrorCode::ProtocolError, message.to_owned())
                .await;
        }
    }

    async fn emit_terminal_reset(&mut self, stream_id: StreamId, code: ErrorCode, message: String) {
        let Some(stream) = self.streams.get(&stream_id) else {
            return;
        };
        let local_opening =
            stream.origin == StreamOrigin::Local && stream.progress != PeerOpenProgress::Opened;
        let open_identity = stream.relay.owner();
        if local_opening {
            let Some(open_identity) = open_identity else {
                self.close.cancel();
                return;
            };
            self.emit(PeerEvent::Failed {
                key: self.key(stream_id),
                open_identity,
                failure: PeerFailure::maybe_observed(code, message),
            })
            .await;
        } else {
            self.emit(PeerEvent::Reset {
                key: self.key(stream_id),
                code,
                message,
            })
            .await;
        }
    }
}
