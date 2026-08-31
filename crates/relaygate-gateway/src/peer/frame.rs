use std::fmt;

use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};

use super::identity::{OpenIdentity, PeerHandshake, StreamId};

/// Private Gateway-to-Gateway wire frames.
///
/// `DATA` keeps its payload as `Bytes`; the codec carries those bytes exactly
/// once without text or base64 expansion.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum PeerFrame {
    Hello(PeerHandshake),
    Welcome(PeerHandshake),
    HandshakeRejected {
        code: ErrorCode,
        message: String,
    },
    Open {
        stream_id: StreamId,
        open_identity: OpenIdentity,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
    },
    Opened {
        stream_id: StreamId,
    },
    Failed {
        stream_id: StreamId,
        code: ErrorCode,
        observation: PeerObservation,
        message: String,
    },
    Data {
        stream_id: StreamId,
        payload: Bytes,
    },
    Fin {
        stream_id: StreamId,
    },
    Close {
        stream_id: StreamId,
    },
    Reset {
        stream_id: StreamId,
        code: ErrorCode,
        message: String,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

impl fmt::Debug for PeerFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hello(handshake) => formatter.debug_tuple("Hello").field(handshake).finish(),
            Self::Welcome(handshake) => formatter.debug_tuple("Welcome").field(handshake).finish(),
            Self::HandshakeRejected { code, message } => formatter
                .debug_struct("HandshakeRejected")
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Open {
                stream_id,
                open_identity,
                client_id,
                listener_session_id,
                binding_id,
            } => formatter
                .debug_struct("Open")
                .field("stream_id", stream_id)
                .field("open_identity", open_identity)
                .field("client_id", client_id)
                .field("listener_session_id", listener_session_id)
                .field("binding_id", binding_id)
                .finish(),
            Self::Opened { stream_id } => formatter
                .debug_struct("Opened")
                .field("stream_id", stream_id)
                .finish(),
            Self::Failed {
                stream_id,
                code,
                observation,
                message,
            } => formatter
                .debug_struct("Failed")
                .field("stream_id", stream_id)
                .field("code", code)
                .field("observation", observation)
                .field("message", message)
                .finish(),
            Self::Data { stream_id, payload } => formatter
                .debug_struct("Data")
                .field("stream_id", stream_id)
                .field("payload_len", &payload.len())
                .finish(),
            Self::Fin { stream_id } => formatter
                .debug_struct("Fin")
                .field("stream_id", stream_id)
                .finish(),
            Self::Close { stream_id } => formatter
                .debug_struct("Close")
                .field("stream_id", stream_id)
                .finish(),
            Self::Reset {
                stream_id,
                code,
                message,
            } => formatter
                .debug_struct("Reset")
                .field("stream_id", stream_id)
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::Ping { nonce } => formatter
                .debug_struct("Ping")
                .field("nonce", nonce)
                .finish(),
            Self::Pong { nonce } => formatter
                .debug_struct("Pong")
                .field("nonce", nonce)
                .finish(),
        }
    }
}
