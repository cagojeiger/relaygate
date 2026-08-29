use bytes::Bytes;
use relaygate_protocol::{ErrorCode, Frame, PipeId, SessionId};

use super::{Delivery, GatewayState, PipeEntry, PipePhase};

impl GatewayState {
    pub(super) fn data(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        payload: Bytes,
    ) -> Vec<Delivery> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, finished)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        if pipe.phase != PipePhase::Open || finished {
            return self.protocol_reset(pipe_id, "DATA is not valid in the current Pipe state");
        }
        self.to(counterpart, Frame::Data { pipe_id, payload })
            .into_iter()
            .collect()
    }

    pub(super) fn fin(&mut self, sender: SessionId, pipe_id: PipeId) -> Vec<Delivery> {
        let (counterpart, remove_after_delivery) = {
            let Some(pipe) = self.pipes.get_mut(&pipe_id) else {
                return Vec::new();
            };
            if pipe.phase != PipePhase::Open {
                return self.protocol_reset(pipe_id, "FIN arrived before the Pipe opened");
            }
            if pipe.connector == sender {
                if pipe.connector_finished {
                    return Vec::new();
                }
                pipe.connector_finished = true;
                (pipe.listener, pipe.listener_finished)
            } else if pipe.listener == sender {
                if pipe.listener_finished {
                    return Vec::new();
                }
                pipe.listener_finished = true;
                (pipe.connector, pipe.connector_finished)
            } else {
                return Vec::new();
            }
        };
        if remove_after_delivery {
            self.remove_pipe(pipe_id);
        }
        self.to(counterpart, Frame::Fin { pipe_id })
            .into_iter()
            .collect()
    }

    pub(super) fn close(&mut self, sender: SessionId, pipe_id: PipeId) -> Vec<Delivery> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, _)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        self.remove_pipe(pipe_id);
        self.to(counterpart, Frame::Close { pipe_id })
            .into_iter()
            .collect()
    }

    pub(super) fn reset(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Vec<Delivery> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, _)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        self.remove_pipe(pipe_id);
        self.to(
            counterpart,
            Frame::Reset {
                pipe_id,
                code,
                message,
            },
        )
        .into_iter()
        .collect()
    }

    fn protocol_reset(&mut self, pipe_id: PipeId, message: &str) -> Vec<Delivery> {
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        [pipe.connector, pipe.listener]
            .into_iter()
            .filter_map(|target| {
                self.to(
                    target,
                    Frame::Reset {
                        pipe_id,
                        code: ErrorCode::ProtocolError,
                        message: message.to_owned(),
                    },
                )
            })
            .collect()
    }
}

fn endpoint(pipe: &PipeEntry, session_id: SessionId) -> Option<(SessionId, bool)> {
    if pipe.connector == session_id {
        Some((pipe.listener, pipe.connector_finished))
    } else if pipe.listener == session_id {
        Some((pipe.connector, pipe.listener_finished))
    } else {
        None
    }
}
