//! Actions the state core returns instead of performing effects itself. The
//! runtime (`gateway::effects`, `gateway::transition`) executes them outside
//! the state lock; `PublishRegistration` is committed under the lock by
//! `transition` to keep the registration snapshot order.
use bytes::Bytes;
use relaygate_protocol::{BindingId, Destination, ErrorCode, PeerObservation, PipeId, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};

use crate::{
    peer::{OpenIdentity, PeerStreamKey},
    registry::Binding,
};

use super::{Delivery, TerminalBatchDelivery};

#[derive(Debug, Clone)]
pub(crate) enum GatewayAction {
    SendSdkFrame(Delivery),
    SendSdkTerminalBatch(TerminalBatchDelivery),
    PublishRegistration {
        session_id: SessionId,
        bindings: Vec<Binding>,
    },
    ResolveRoute {
        open_identity: OpenIdentity,
        destination: Destination,
    },
    OpenPeer {
        open_identity: OpenIdentity,
        gateway_id: GatewayId,
        gateway_locator: GatewayLocator,
        destination: Destination,
        relay_session_id: SessionId,
        binding_id: BindingId,
    },
    CancelPeerOpen {
        open_identity: OpenIdentity,
    },
    SendPeerFrame(PeerDelivery),
}

impl From<Delivery> for GatewayAction {
    fn from(delivery: Delivery) -> Self {
        Self::SendSdkFrame(delivery)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PeerDelivery {
    Opened {
        key: PeerStreamKey,
    },
    Failed {
        key: PeerStreamKey,
        code: ErrorCode,
        observation: PeerObservation,
        message: String,
    },
    Data {
        key: PeerStreamKey,
        payload: Bytes,
    },
    Fin {
        key: PeerStreamKey,
    },
    Close {
        key: PeerStreamKey,
    },
    Reset {
        key: PeerStreamKey,
        code: ErrorCode,
        message: String,
    },
}

impl From<PeerDelivery> for GatewayAction {
    fn from(delivery: PeerDelivery) -> Self {
        Self::SendPeerFrame(delivery)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolViolation {
    #[error("session {sender:?} does not own existing Pipe {pipe_id:?} for frame {frame_name}")]
    PipeOwnership {
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    },
}
