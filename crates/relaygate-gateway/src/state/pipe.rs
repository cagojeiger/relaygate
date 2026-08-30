use bytes::Bytes;
use relaygate_protocol::{ErrorCode, Frame, PipeId, SessionId};

use crate::peer::PeerStreamKey;

use super::{
    GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipeEntry, PipePhase,
    ProtocolViolation,
};

impl GatewayState {
    pub(super) fn data(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        payload: Bytes,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let sender = self.sdk_endpoint(sender, pipe_id, "DATA")?;
        Ok(self.relay_data(sender, pipe_id, payload))
    }

    pub(super) fn fin(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let sender = self.sdk_endpoint(sender, pipe_id, "FIN")?;
        Ok(self.relay_fin(sender, pipe_id))
    }

    pub(super) fn close(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let sender = self.sdk_endpoint(sender, pipe_id, "CLOSE")?;
        Ok(self.relay_close(sender, pipe_id))
    }

    pub(super) fn reset(
        &mut self,
        sender: SessionId,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let sender = self.sdk_endpoint(sender, pipe_id, "RESET")?;
        Ok(self.relay_reset(sender, pipe_id, code, message))
    }

    pub(crate) fn peer_data(&mut self, key: PeerStreamKey, payload: Bytes) -> Vec<GatewayAction> {
        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        self.relay_data(PipeEndpoint::Peer(key), pipe_id, payload)
    }

    pub(crate) fn peer_fin(&mut self, key: PeerStreamKey) -> Vec<GatewayAction> {
        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        self.relay_fin(PipeEndpoint::Peer(key), pipe_id)
    }

    pub(crate) fn peer_close(&mut self, key: PeerStreamKey) -> Vec<GatewayAction> {
        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        self.relay_close(PipeEndpoint::Peer(key), pipe_id)
    }

    pub(crate) fn peer_reset(
        &mut self,
        key: PeerStreamKey,
        code: ErrorCode,
        message: String,
    ) -> Vec<GatewayAction> {
        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        let peer_cancelled_offer = self.pipes.get(&pipe_id).is_some_and(|pipe| {
            pipe.phase == PipePhase::Offered && pipe.connector == PipeEndpoint::Peer(key)
        });
        if peer_cancelled_offer {
            let Some(pipe) = self.remove_pipe(pipe_id) else {
                return Vec::new();
            };
            return self.endpoint_reset(pipe.listener, pipe_id, code, &message);
        }
        self.relay_reset(PipeEndpoint::Peer(key), pipe_id, code, message)
    }

    fn relay_data(
        &mut self,
        sender: PipeEndpoint,
        pipe_id: PipeId,
        payload: Bytes,
    ) -> Vec<GatewayAction> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, finished)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        if pipe.phase != PipePhase::Open || finished {
            return self.protocol_reset(pipe_id, "DATA is not valid in the current Pipe state");
        }
        self.endpoint_data(counterpart, pipe_id, payload)
    }

    fn relay_fin(&mut self, sender: PipeEndpoint, pipe_id: PipeId) -> Vec<GatewayAction> {
        let (counterpart, finished, phase) = {
            let Some(pipe) = self.pipes.get(&pipe_id) else {
                return Vec::new();
            };
            let Some((counterpart, finished)) = endpoint(pipe, sender) else {
                return Vec::new();
            };
            (counterpart, finished, pipe.phase)
        };
        if phase != PipePhase::Open {
            return self.protocol_reset(pipe_id, "FIN arrived before the Pipe opened");
        }
        if finished {
            return Vec::new();
        }
        let remove_after_delivery = {
            let Some(pipe) = self.pipes.get_mut(&pipe_id) else {
                return Vec::new();
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
        if remove_after_delivery {
            let _ = self.remove_pipe(pipe_id);
        }
        self.endpoint_fin(counterpart, pipe_id)
    }

    fn relay_close(&mut self, sender: PipeEndpoint, pipe_id: PipeId) -> Vec<GatewayAction> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, _)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        if pipe.phase != PipePhase::Open {
            return self.protocol_reset(pipe_id, "CLOSE arrived before the Pipe opened");
        }
        let _ = self.remove_pipe(pipe_id);
        self.endpoint_close(counterpart, pipe_id)
    }

    fn relay_reset(
        &mut self,
        sender: PipeEndpoint,
        pipe_id: PipeId,
        code: ErrorCode,
        message: String,
    ) -> Vec<GatewayAction> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Vec::new();
        };
        let Some((counterpart, _)) = endpoint(pipe, sender) else {
            return Vec::new();
        };
        if pipe.phase != PipePhase::Open {
            return self.protocol_reset(pipe_id, "RESET arrived before the Pipe opened");
        }
        let _ = self.remove_pipe(pipe_id);
        self.endpoint_reset(counterpart, pipe_id, code, &message)
    }

    pub(super) fn protocol_reset(&mut self, pipe_id: PipeId, message: &str) -> Vec<GatewayAction> {
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        [pipe.connector, pipe.listener]
            .into_iter()
            .flat_map(|target| {
                self.endpoint_reset(target, pipe_id, ErrorCode::ProtocolError, message)
            })
            .collect()
    }

    pub(super) fn endpoint_reset(
        &self,
        target: PipeEndpoint,
        pipe_id: PipeId,
        code: ErrorCode,
        message: &str,
    ) -> Vec<GatewayAction> {
        match target {
            PipeEndpoint::Sdk(session_id) => self
                .to(
                    session_id,
                    Frame::Reset {
                        pipe_id,
                        code,
                        message: message.to_owned(),
                    },
                )
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![
                PeerDelivery::Reset {
                    key,
                    code,
                    message: message.to_owned(),
                }
                .into(),
            ],
        }
    }

    fn endpoint_data(
        &self,
        target: PipeEndpoint,
        pipe_id: PipeId,
        payload: Bytes,
    ) -> Vec<GatewayAction> {
        match target {
            PipeEndpoint::Sdk(session_id) => self
                .to(session_id, Frame::Data { pipe_id, payload })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![PeerDelivery::Data { key, payload }.into()],
        }
    }

    fn endpoint_fin(&self, target: PipeEndpoint, pipe_id: PipeId) -> Vec<GatewayAction> {
        match target {
            PipeEndpoint::Sdk(session_id) => self
                .to(session_id, Frame::Fin { pipe_id })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![PeerDelivery::Fin { key }.into()],
        }
    }

    fn endpoint_close(&self, target: PipeEndpoint, pipe_id: PipeId) -> Vec<GatewayAction> {
        match target {
            PipeEndpoint::Sdk(session_id) => self
                .to(session_id, Frame::Close { pipe_id })
                .map(GatewayAction::SendSdkFrame)
                .into_iter()
                .collect(),
            PipeEndpoint::Peer(key) => vec![PeerDelivery::Close { key }.into()],
        }
    }

    fn sdk_endpoint(
        &self,
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    ) -> Result<PipeEndpoint, ProtocolViolation> {
        let Some(pipe) = self.pipes.get(&pipe_id) else {
            return Ok(PipeEndpoint::Sdk(sender));
        };
        pipe.ensure_sdk_owner(sender, pipe_id, frame_name)?;
        Ok(PipeEndpoint::Sdk(sender))
    }
}

fn endpoint(pipe: &PipeEntry, sender: PipeEndpoint) -> Option<(PipeEndpoint, bool)> {
    if pipe.connector == sender {
        Some((pipe.listener, pipe.connector_finished))
    } else if pipe.listener == sender {
        Some((pipe.connector, pipe.listener_finished))
    } else {
        None
    }
}
