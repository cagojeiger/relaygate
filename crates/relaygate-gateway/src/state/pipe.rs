use bytes::Bytes;
use relaygate_protocol::{ErrorCode, Frame, PipeId, SessionId};

use super::{Delivery, GatewayState, PipeEntry, PipePhase, ProtocolViolation};

impl GatewayState {
    pub(super) fn data(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        payload: Bytes,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        let (counterpart, finished) = endpoint(pipe, sender, pipe_id, "DATA")?;
        if pipe.phase != PipePhase::Open || finished {
            return Ok(self.protocol_reset(pipe_id, "DATA is not valid in the current Pipe state"));
        }
        Ok(self
            .to(counterpart, Frame::Data { pipe_id, payload })
            .into_iter()
            .collect())
    }

    pub(super) fn fin(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let (counterpart, finished, phase) = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Ok(Vec::new());
            };
            let (counterpart, finished) = endpoint(pipe, sender, pipe_id, "FIN")?;
            (counterpart, finished, pipe.phase)
        };
        if phase != PipePhase::Open {
            return Ok(self.protocol_reset(pipe_id, "FIN arrived before the Pipe opened"));
        }
        if finished {
            return Ok(Vec::new());
        }
        let remove_after_delivery = {
            let Some(pipe) = self.pipes.get_mut(&pipe_id) else {
                return Ok(Vec::new());
            };
            if pipe.connector == sender {
                pipe.connector_finished = true;
                pipe.listener_finished
            } else {
                debug_assert_eq!(pipe.listener, sender);
                pipe.listener_finished = true;
                pipe.connector_finished
            }
        };
        if remove_after_delivery && let Some(pipe) = self.remove_pipe(pipe_id) {
            tracing::debug!(
                component = "gateway",
                event = "gateway.pipe.closed",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                sender_session_id = %sender.as_uuid(),
                reason = "both_directions_finished",
                "Pipe closed after both directions finished"
            );
        }
        Ok(self
            .to(counterpart, Frame::Fin { pipe_id })
            .into_iter()
            .collect())
    }

    pub(super) fn close(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        let (counterpart, _) = endpoint(pipe, sender, pipe_id, "CLOSE")?;
        if pipe.phase != PipePhase::Open {
            return Ok(self.protocol_reset(pipe_id, "CLOSE arrived before the Pipe opened"));
        }
        if let Some(pipe) = self.remove_pipe(pipe_id) {
            tracing::debug!(
                component = "gateway",
                event = "gateway.pipe.closed",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                sender_session_id = %sender.as_uuid(),
                reason = "close",
                "Pipe closed"
            );
        }
        Ok(self
            .to(counterpart, Frame::Close { pipe_id })
            .into_iter()
            .collect())
    }

    pub(super) fn reset(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Result<Vec<Delivery>, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(Vec::new());
        };
        let (counterpart, _) = endpoint(pipe, sender, pipe_id, "RESET")?;
        if pipe.phase != PipePhase::Open {
            return Ok(self.protocol_reset(pipe_id, "RESET arrived before the Pipe opened"));
        }
        if let Some(pipe) = self.remove_pipe(pipe_id) {
            tracing::debug!(
                component = "gateway",
                event = "gateway.pipe.reset",
                connector_session_id = %pipe.connector.as_uuid(),
                listener_session_id = %pipe.listener.as_uuid(),
                connection_id = pipe_id.connection_id(),
                binding_id = %pipe.binding_id.as_uuid(),
                sender_session_id = %sender.as_uuid(),
                error_code = ?code,
                "Pipe reset"
            );
        }
        Ok(self
            .to(
                counterpart,
                Frame::Reset {
                    pipe_id,
                    code,
                    message,
                },
            )
            .into_iter()
            .collect())
    }

    pub(super) fn protocol_reset(&mut self, pipe_id: PipeId, message: &str) -> Vec<Delivery> {
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.pipe.protocol_reset",
            connector_session_id = %pipe.connector.as_uuid(),
            listener_session_id = %pipe.listener.as_uuid(),
            connection_id = pipe_id.connection_id(),
            binding_id = %pipe.binding_id.as_uuid(),
            "Pipe protocol reset"
        );
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

fn endpoint(
    pipe: &PipeEntry,
    session_id: SessionId,
    pipe_id: PipeId,
    frame_name: &'static str,
) -> Result<(SessionId, bool), ProtocolViolation> {
    pipe.ensure_owner(session_id, pipe_id, frame_name)?;
    if pipe.connector == session_id {
        Ok((pipe.listener, pipe.connector_finished))
    } else {
        debug_assert_eq!(pipe.listener, session_id);
        Ok((pipe.connector, pipe.listener_finished))
    }
}
