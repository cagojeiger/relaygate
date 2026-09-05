use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use super::{
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerGatewayKey, PeerGatewayName, PeerHandshake, PeerTransportId, StreamId,
    },
};

const MAGIC: [u8; 2] = *b"GP";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 8;
const MAX_STRING_LEN: usize = u16::MAX as usize;

const HELLO: u8 = 1;
const WELCOME: u8 = 2;
const HANDSHAKE_REJECTED: u8 = 3;
const OPEN: u8 = 4;
const OPENED: u8 = 5;
const FAILED: u8 = 6;
const DATA: u8 = 7;
const FIN: u8 = 8;
const CLOSE: u8 = 9;
const RESET: u8 = 10;
const PING: u8 = 11;
const PONG: u8 = 12;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PeerCodecError {
    #[error("peer frame I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid peer frame magic")]
    InvalidMagic,
    #[error("unsupported peer protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown peer frame kind {0}")]
    UnknownFrameKind(u8),
    #[error("unknown peer {name} value {value}")]
    UnknownEnum { name: &'static str, value: u8 },
    #[error("truncated peer frame field {0}")]
    Truncated(&'static str),
    #[error("invalid UTF-8 in peer frame field {0}")]
    InvalidUtf8(&'static str),
    #[error("invalid peer frame field {0}")]
    InvalidField(&'static str),
    #[error("invalid UUID bytes in peer frame field {0}")]
    InvalidUuid(&'static str),
    #[error("peer frame field {field} is too long: {actual} bytes, maximum {maximum}")]
    FieldTooLong {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("peer frame has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("peer frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("peer frame length cannot be represented on the wire")]
    LengthOverflow,
}

impl PeerCodecError {
    pub(crate) const fn is_io(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PeerFrameCodec {
    max_frame_len: usize,
}

impl PeerFrameCodec {
    pub(crate) const fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    #[cfg(test)]
    pub(crate) fn validate(&self, frame: &PeerFrame) -> Result<(), PeerCodecError> {
        let (_, payload_len) = frame_metadata(frame)?;
        self.validate_payload_len(payload_len)
    }

    fn validate_payload_len(&self, payload_len: usize) -> Result<(), PeerCodecError> {
        if payload_len > self.max_frame_len {
            return Err(PeerCodecError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        let _ = u32::try_from(payload_len).map_err(|_| PeerCodecError::LengthOverflow)?;
        Ok(())
    }
}

impl Encoder<PeerFrame> for PeerFrameCodec {
    type Error = PeerCodecError;

    fn encode(&mut self, frame: PeerFrame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let (kind, payload_len) = frame_metadata(&frame)?;
        self.validate_payload_len(payload_len)?;
        let payload_len_wire =
            u32::try_from(payload_len).map_err(|_| PeerCodecError::LengthOverflow)?;
        let frame_len = checked_add(HEADER_LEN, payload_len)?;

        destination.reserve(frame_len);
        destination.extend_from_slice(&MAGIC);
        destination.put_u8(VERSION);
        destination.put_u8(kind);
        destination.put_u32(payload_len_wire);
        encode_payload(frame, destination)?;
        Ok(())
    }
}

impl Decoder for PeerFrameCodec {
    type Item = PeerFrame;
    type Error = PeerCodecError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }
        if source[..2] != MAGIC {
            return Err(PeerCodecError::InvalidMagic);
        }
        let version = source[2];
        if version != VERSION {
            return Err(PeerCodecError::UnsupportedVersion(version));
        }
        let kind = source[3];
        let payload_len = u32::from_be_bytes([source[4], source[5], source[6], source[7]]) as usize;
        self.validate_payload_len(payload_len)?;
        let frame_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(PeerCodecError::LengthOverflow)?;
        if source.len() < frame_len {
            source.reserve(frame_len - source.len());
            return Ok(None);
        }

        source.advance(HEADER_LEN);
        let payload = source.split_to(payload_len).freeze();
        decode_payload(kind, payload).map(Some)
    }
}

fn frame_metadata(frame: &PeerFrame) -> Result<(u8, usize), PeerCodecError> {
    let metadata = match frame {
        PeerFrame::Hello(handshake) => (HELLO, handshake_wire_len(handshake)?),
        PeerFrame::Welcome(handshake) => (WELCOME, handshake_wire_len(handshake)?),
        PeerFrame::HandshakeRejected { message, .. } => (
            HANDSHAKE_REJECTED,
            checked_add(1, string_wire_len("message", message)?)?,
        ),
        PeerFrame::Open { destination_id, .. } => {
            if destination_id.is_empty() {
                return Err(PeerCodecError::InvalidField("destination_id"));
            }
            (
                OPEN,
                checked_add(80, string_wire_len("destination_id", destination_id)?)?,
            )
        }
        PeerFrame::Opened { .. } => (OPENED, 8),
        PeerFrame::Failed { message, .. } => (
            FAILED,
            checked_add(10, string_wire_len("message", message)?)?,
        ),
        PeerFrame::Data { payload, .. } => (DATA, checked_add(8, payload.len())?),
        PeerFrame::Fin { .. } => (FIN, 8),
        PeerFrame::Close { .. } => (CLOSE, 8),
        PeerFrame::Reset { message, .. } => {
            (RESET, checked_add(9, string_wire_len("message", message)?)?)
        }
        PeerFrame::Ping { .. } => (PING, 8),
        PeerFrame::Pong { .. } => (PONG, 8),
    };
    Ok(metadata)
}

fn handshake_wire_len(handshake: &PeerHandshake) -> Result<usize, PeerCodecError> {
    let name = string_wire_len("gateway_name", handshake.gateway_name.as_str())?;
    let key = string_wire_len(
        "internal_gateway_key",
        handshake.internal_gateway_key.expose_secret(),
    )?;
    checked_add(checked_add(name, key)?, 64)
}

fn string_wire_len(field: &'static str, value: &str) -> Result<usize, PeerCodecError> {
    if value.len() > MAX_STRING_LEN {
        return Err(PeerCodecError::FieldTooLong {
            field,
            actual: value.len(),
            maximum: MAX_STRING_LEN,
        });
    }
    checked_add(2, value.len())
}

fn checked_add(left: usize, right: usize) -> Result<usize, PeerCodecError> {
    left.checked_add(right)
        .ok_or(PeerCodecError::LengthOverflow)
}

fn encode_payload(frame: PeerFrame, destination: &mut BytesMut) -> Result<(), PeerCodecError> {
    match frame {
        PeerFrame::Hello(handshake) | PeerFrame::Welcome(handshake) => {
            put_handshake(destination, handshake)?;
        }
        PeerFrame::HandshakeRejected { code, message } => {
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
        }
        PeerFrame::Open {
            stream_id,
            open_identity,
            destination_id,
            relay_session_id,
            binding_id,
        } => {
            destination.put_u64(stream_id.raw());
            put_gateway_id(destination, open_identity.entry_gateway());
            put_session_id(destination, open_identity.connector_session());
            destination.put_u64(open_identity.connection_id());
            put_string(destination, "destination_id", &destination_id)?;
            put_session_id(destination, relay_session_id);
            put_binding_id(destination, binding_id);
        }
        PeerFrame::Opened { stream_id }
        | PeerFrame::Fin { stream_id }
        | PeerFrame::Close { stream_id } => destination.put_u64(stream_id.raw()),
        PeerFrame::Failed {
            stream_id,
            code,
            observation,
            message,
        } => {
            destination.put_u64(stream_id.raw());
            destination.put_u8(code as u8);
            destination.put_u8(observation as u8);
            put_string(destination, "message", &message)?;
        }
        PeerFrame::Data { stream_id, payload } => {
            destination.put_u64(stream_id.raw());
            destination.extend_from_slice(&payload);
        }
        PeerFrame::Reset {
            stream_id,
            code,
            message,
        } => {
            destination.put_u64(stream_id.raw());
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
        }
        PeerFrame::Ping { nonce } | PeerFrame::Pong { nonce } => {
            destination.put_u64(nonce);
        }
    }
    Ok(())
}

fn put_handshake(
    destination: &mut BytesMut,
    handshake: PeerHandshake,
) -> Result<(), PeerCodecError> {
    put_string(destination, "gateway_name", handshake.gateway_name.as_str())?;
    put_string(
        destination,
        "internal_gateway_key",
        handshake.internal_gateway_key.expose_secret(),
    )?;
    put_gateway_id(destination, handshake.gateway_id);
    put_gateway_id(destination, handshake.expected_peer_gateway_id);
    put_gateway_id(destination, handshake.dialer_gateway_id);
    put_peer_transport_id(destination, handshake.peer_transport_id);
    Ok(())
}

fn put_string(
    destination: &mut BytesMut,
    field: &'static str,
    value: &str,
) -> Result<(), PeerCodecError> {
    let wire_len = u16::try_from(value.len()).map_err(|_| PeerCodecError::FieldTooLong {
        field,
        actual: value.len(),
        maximum: MAX_STRING_LEN,
    })?;
    destination.put_u16(wire_len);
    destination.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_gateway_id(destination: &mut BytesMut, value: GatewayId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_session_id(destination: &mut BytesMut, value: SessionId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_binding_id(destination: &mut BytesMut, value: BindingId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_peer_transport_id(destination: &mut BytesMut, value: PeerTransportId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn decode_payload(kind: u8, payload: Bytes) -> Result<PeerFrame, PeerCodecError> {
    let mut reader = PayloadReader::new(payload);
    let frame = match kind {
        HELLO => PeerFrame::Hello(reader.handshake()?),
        WELCOME => PeerFrame::Welcome(reader.handshake()?),
        HANDSHAKE_REJECTED => PeerFrame::HandshakeRejected {
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        OPEN => {
            let stream_id = reader.stream_id()?;
            let entry_gateway_id = reader.gateway_id("entry_gateway_id")?;
            let connector_session_id = reader.session_id("connector_session_id")?;
            let connection_id = reader.u64("connection_id")?;
            let destination_id = reader.non_empty_string("destination_id")?;
            let relay_session_id = reader.session_id("relay_session_id")?;
            let binding_id = reader.binding_id()?;
            PeerFrame::Open {
                stream_id,
                open_identity: OpenIdentity::new(
                    entry_gateway_id,
                    connector_session_id,
                    connection_id,
                ),
                destination_id,
                relay_session_id,
                binding_id,
            }
        }
        OPENED => PeerFrame::Opened {
            stream_id: reader.stream_id()?,
        },
        FAILED => PeerFrame::Failed {
            stream_id: reader.stream_id()?,
            code: reader.error_code()?,
            observation: reader.observation()?,
            message: reader.string("message")?,
        },
        DATA => PeerFrame::Data {
            stream_id: reader.stream_id()?,
            payload: reader.remaining(),
        },
        FIN => PeerFrame::Fin {
            stream_id: reader.stream_id()?,
        },
        CLOSE => PeerFrame::Close {
            stream_id: reader.stream_id()?,
        },
        RESET => PeerFrame::Reset {
            stream_id: reader.stream_id()?,
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        PING => PeerFrame::Ping {
            nonce: reader.u64("nonce")?,
        },
        PONG => PeerFrame::Pong {
            nonce: reader.u64("nonce")?,
        },
        other => return Err(PeerCodecError::UnknownFrameKind(other)),
    };
    reader.finish()?;
    Ok(frame)
}

struct PayloadReader {
    payload: Bytes,
    position: usize,
}

impl PayloadReader {
    fn new(payload: Bytes) -> Self {
        Self {
            payload,
            position: 0,
        }
    }

    fn take(&mut self, length: usize, field: &'static str) -> Result<&[u8], PeerCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(PeerCodecError::LengthOverflow)?;
        let Some(bytes) = self.payload.get(self.position..end) else {
            return Err(PeerCodecError::Truncated(field));
        };
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, PeerCodecError> {
        Ok(self.take(1, field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, PeerCodecError> {
        let bytes = self.take(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, PeerCodecError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn string(&mut self, field: &'static str) -> Result<String, PeerCodecError> {
        let length = self.u16(field)? as usize;
        let bytes = self.take(length, field)?;
        let value = std::str::from_utf8(bytes).map_err(|_| PeerCodecError::InvalidUtf8(field))?;
        Ok(value.to_owned())
    }

    fn non_empty_string(&mut self, field: &'static str) -> Result<String, PeerCodecError> {
        let value = self.string(field)?;
        if value.is_empty() {
            return Err(PeerCodecError::InvalidField(field));
        }
        Ok(value)
    }

    fn uuid(&mut self, field: &'static str) -> Result<Uuid, PeerCodecError> {
        let bytes = self.take(16, field)?;
        Uuid::from_slice(bytes).map_err(|_| PeerCodecError::InvalidUuid(field))
    }

    fn gateway_id(&mut self, field: &'static str) -> Result<GatewayId, PeerCodecError> {
        self.uuid(field).map(GatewayId::from_uuid)
    }

    fn session_id(&mut self, field: &'static str) -> Result<SessionId, PeerCodecError> {
        self.uuid(field).map(SessionId::from_uuid)
    }

    fn binding_id(&mut self) -> Result<BindingId, PeerCodecError> {
        self.uuid("binding_id").map(BindingId::from_uuid)
    }

    fn peer_transport_id(&mut self) -> Result<PeerTransportId, PeerCodecError> {
        self.uuid("peer_transport_id")
            .map(PeerTransportId::from_uuid)
    }

    fn stream_id(&mut self) -> Result<StreamId, PeerCodecError> {
        self.u64("stream_id").map(StreamId::from_raw)
    }

    fn handshake(&mut self) -> Result<PeerHandshake, PeerCodecError> {
        let gateway_name = PeerGatewayName::new(self.non_empty_string("gateway_name")?)
            .map_err(|_| PeerCodecError::InvalidField("gateway_name"))?;
        let internal_gateway_key =
            PeerGatewayKey::new(self.non_empty_string("internal_gateway_key")?)
                .map_err(|_| PeerCodecError::InvalidField("internal_gateway_key"))?;
        Ok(PeerHandshake {
            gateway_name,
            internal_gateway_key,
            gateway_id: self.gateway_id("gateway_id")?,
            expected_peer_gateway_id: self.gateway_id("expected_peer_gateway_id")?,
            dialer_gateway_id: self.gateway_id("dialer_gateway_id")?,
            peer_transport_id: self.peer_transport_id()?,
        })
    }

    fn error_code(&mut self) -> Result<ErrorCode, PeerCodecError> {
        let value = self.u8("error_code")?;
        match value {
            1 => Ok(ErrorCode::InvalidArgument),
            2 => Ok(ErrorCode::Unauthenticated),
            3 => Ok(ErrorCode::PermissionDenied),
            4 => Ok(ErrorCode::NotFound),
            5 => Ok(ErrorCode::FailedPrecondition),
            6 => Ok(ErrorCode::Unavailable),
            7 => Ok(ErrorCode::DeadlineExceeded),
            8 => Ok(ErrorCode::ResourceExhausted),
            9 => Ok(ErrorCode::Cancelled),
            10 => Ok(ErrorCode::ProtocolError),
            11 => Ok(ErrorCode::Internal),
            12 => Ok(ErrorCode::AlreadyExists),
            _ => Err(PeerCodecError::UnknownEnum {
                name: "ErrorCode",
                value,
            }),
        }
    }

    fn observation(&mut self) -> Result<PeerObservation, PeerCodecError> {
        let value = self.u8("peer_observation")?;
        match value {
            1 => Ok(PeerObservation::NotObserved),
            2 => Ok(PeerObservation::MaybeObserved),
            3 => Ok(PeerObservation::Observed),
            _ => Err(PeerCodecError::UnknownEnum {
                name: "PeerObservation",
                value,
            }),
        }
    }

    fn remaining(&mut self) -> Bytes {
        let remaining = self.payload.slice(self.position..);
        self.position = self.payload.len();
        remaining
    }

    fn finish(self) -> Result<(), PeerCodecError> {
        let trailing = self.payload.len().saturating_sub(self.position);
        if trailing == 0 {
            Ok(())
        } else {
            Err(PeerCodecError::TrailingBytes(trailing))
        }
    }
}
