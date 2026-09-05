use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use crate::{
    BindingId, ClusterToken, DestinationId, ErrorCode, Frame, PeerObservation, PipeId,
    ProtocolError, SessionId,
};

const MAGIC: [u8; 2] = *b"RG";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 8;
const MAX_STRING_LEN: usize = u16::MAX as usize;
pub const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_frame_len: usize,
}

impl FrameCodec {
    #[must_use]
    pub const fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_LEN)
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Frame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let mut payload = BytesMut::new();
        let kind = encode_payload(item, &mut payload)?;
        if payload.len() > self.max_frame_len {
            return Err(ProtocolError::FrameTooLarge {
                actual: payload.len(),
                maximum: self.max_frame_len,
            });
        }
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        destination.reserve(HEADER_LEN + payload.len());
        destination.extend_from_slice(&MAGIC);
        destination.put_u8(VERSION);
        destination.put_u8(kind);
        destination.put_u32(payload_len);
        destination.unsplit(payload);
        Ok(())
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }
        if source[..2] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }
        let version = source[2];
        if version != VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }
        let kind = source[3];
        let payload_len = u32::from_be_bytes([source[4], source[5], source[6], source[7]]) as usize;
        if payload_len > self.max_frame_len {
            return Err(ProtocolError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        if source.len() < HEADER_LEN + payload_len {
            source.reserve(HEADER_LEN + payload_len - source.len());
            return Ok(None);
        }
        source.advance(HEADER_LEN);
        let payload = source.split_to(payload_len).freeze();
        decode_payload(kind, payload).map(Some)
    }
}

fn encode_payload(frame: Frame, destination: &mut BytesMut) -> Result<u8, ProtocolError> {
    let kind = match frame {
        Frame::Hello { cluster_token } => {
            put_string(destination, "cluster_token", cluster_token.expose_secret())?;
            1
        }
        Frame::Welcome { session_id } => {
            put_session_id(destination, session_id);
            2
        }
        Frame::SessionRejected { code, message } => {
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
            3
        }
        Frame::Publish {
            request_id,
            destination_id,
        } => {
            destination.put_u64(request_id);
            put_destination_id(destination, destination_id);
            4
        }
        Frame::Published {
            request_id,
            binding_id,
        } => {
            destination.put_u64(request_id);
            put_binding_id(destination, binding_id);
            5
        }
        Frame::PublishFailed {
            request_id,
            code,
            message,
        } => {
            destination.put_u64(request_id);
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
            6
        }
        Frame::Unpublish {
            request_id,
            binding_id,
        } => {
            destination.put_u64(request_id);
            put_binding_id(destination, binding_id);
            7
        }
        Frame::Unpublished { request_id } => {
            destination.put_u64(request_id);
            8
        }
        Frame::Dial {
            connection_id,
            destination_id,
        } => {
            destination.put_u64(connection_id);
            put_destination_id(destination, destination_id);
            9
        }
        Frame::Offer {
            pipe_id,
            binding_id,
            destination_id,
        } => {
            put_pipe_id(destination, pipe_id);
            put_binding_id(destination, binding_id);
            put_destination_id(destination, destination_id);
            10
        }
        Frame::OfferAccepted { pipe_id } => {
            put_pipe_id(destination, pipe_id);
            11
        }
        Frame::OfferRejected {
            pipe_id,
            code,
            message,
        } => {
            put_pipe_id(destination, pipe_id);
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
            12
        }
        Frame::Opened { pipe_id } => {
            put_pipe_id(destination, pipe_id);
            13
        }
        Frame::DialFailed {
            connection_id,
            code,
            observation,
            message,
        } => {
            destination.put_u64(connection_id);
            destination.put_u8(code as u8);
            destination.put_u8(observation as u8);
            put_string(destination, "message", &message)?;
            14
        }
        Frame::Data { pipe_id, payload } => {
            put_pipe_id(destination, pipe_id);
            destination.extend_from_slice(&payload);
            15
        }
        Frame::Fin { pipe_id } => {
            put_pipe_id(destination, pipe_id);
            16
        }
        Frame::Close { pipe_id } => {
            put_pipe_id(destination, pipe_id);
            17
        }
        Frame::Reset {
            pipe_id,
            code,
            message,
        } => {
            put_pipe_id(destination, pipe_id);
            destination.put_u8(code as u8);
            put_string(destination, "message", &message)?;
            18
        }
        Frame::Ping { nonce } => {
            destination.put_u64(nonce);
            19
        }
        Frame::Pong { nonce } => {
            destination.put_u64(nonce);
            20
        }
        Frame::Cancel { pipe_id } => {
            put_pipe_id(destination, pipe_id);
            21
        }
    };
    Ok(kind)
}

fn decode_payload(kind: u8, payload: Bytes) -> Result<Frame, ProtocolError> {
    let mut reader = PayloadReader::new(payload);
    let frame = match kind {
        1 => Frame::Hello {
            cluster_token: ClusterToken::new(reader.string("cluster_token")?),
        },
        2 => Frame::Welcome {
            session_id: reader.session_id()?,
        },
        3 => Frame::SessionRejected {
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        4 => Frame::Publish {
            request_id: reader.u64("request_id")?,
            destination_id: reader.destination_id()?,
        },
        5 => Frame::Published {
            request_id: reader.u64("request_id")?,
            binding_id: reader.binding_id()?,
        },
        6 => Frame::PublishFailed {
            request_id: reader.u64("request_id")?,
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        7 => Frame::Unpublish {
            request_id: reader.u64("request_id")?,
            binding_id: reader.binding_id()?,
        },
        8 => Frame::Unpublished {
            request_id: reader.u64("request_id")?,
        },
        9 => Frame::Dial {
            connection_id: reader.u64("connection_id")?,
            destination_id: reader.destination_id()?,
        },
        10 => Frame::Offer {
            pipe_id: reader.pipe_id()?,
            binding_id: reader.binding_id()?,
            destination_id: reader.destination_id()?,
        },
        11 => Frame::OfferAccepted {
            pipe_id: reader.pipe_id()?,
        },
        12 => Frame::OfferRejected {
            pipe_id: reader.pipe_id()?,
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        13 => Frame::Opened {
            pipe_id: reader.pipe_id()?,
        },
        14 => Frame::DialFailed {
            connection_id: reader.u64("connection_id")?,
            code: reader.error_code()?,
            observation: reader.observation()?,
            message: reader.string("message")?,
        },
        15 => {
            let pipe_id = reader.pipe_id()?;
            let payload = reader.remaining();
            Frame::Data { pipe_id, payload }
        }
        16 => Frame::Fin {
            pipe_id: reader.pipe_id()?,
        },
        17 => Frame::Close {
            pipe_id: reader.pipe_id()?,
        },
        18 => Frame::Reset {
            pipe_id: reader.pipe_id()?,
            code: reader.error_code()?,
            message: reader.string("message")?,
        },
        19 => Frame::Ping {
            nonce: reader.u64("nonce")?,
        },
        20 => Frame::Pong {
            nonce: reader.u64("nonce")?,
        },
        21 => Frame::Cancel {
            pipe_id: reader.pipe_id()?,
        },
        other => return Err(ProtocolError::UnknownFrameKind(other)),
    };
    reader.finish()?;
    Ok(frame)
}

fn put_string(
    destination: &mut BytesMut,
    field: &'static str,
    value: &str,
) -> Result<(), ProtocolError> {
    let length = value.len();
    let wire_length = u16::try_from(length).map_err(|_| ProtocolError::FieldTooLong {
        field,
        actual: length,
        maximum: MAX_STRING_LEN,
    })?;
    destination.put_u16(wire_length);
    destination.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_session_id(destination: &mut BytesMut, value: SessionId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_binding_id(destination: &mut BytesMut, value: BindingId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_destination_id(destination: &mut BytesMut, value: DestinationId) {
    destination.extend_from_slice(value.as_uuid().as_bytes());
}

fn put_pipe_id(destination: &mut BytesMut, value: PipeId) {
    put_session_id(destination, value.origin_session_id());
    destination.put_u64(value.connection_id());
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

    fn take(&mut self, length: usize, field: &'static str) -> Result<&[u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ProtocolError::LengthOverflow)?;
        let Some(bytes) = self.payload.get(self.position..end) else {
            return Err(ProtocolError::Truncated(field));
        };
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, ProtocolError> {
        Ok(self.take(1, field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, ProtocolError> {
        let bytes = self.take(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, ProtocolError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn string(&mut self, field: &'static str) -> Result<String, ProtocolError> {
        let length = self.u16(field)? as usize;
        let bytes = self.take(length, field)?;
        let value = std::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidUtf8(field))?;
        Ok(value.to_owned())
    }

    fn uuid(&mut self, field: &'static str) -> Result<Uuid, ProtocolError> {
        let bytes = self.take(16, field)?;
        Uuid::from_slice(bytes).map_err(|_| ProtocolError::Truncated(field))
    }

    fn session_id(&mut self) -> Result<SessionId, ProtocolError> {
        self.uuid("session_id").map(SessionId::from_uuid)
    }

    fn binding_id(&mut self) -> Result<BindingId, ProtocolError> {
        self.uuid("binding_id").map(BindingId::from_uuid)
    }

    fn destination_id(&mut self) -> Result<DestinationId, ProtocolError> {
        DestinationId::try_from_uuid(self.uuid("destination_id")?)
            .ok_or(ProtocolError::InvalidDestinationId)
    }

    fn pipe_id(&mut self) -> Result<PipeId, ProtocolError> {
        let session_id = self.session_id()?;
        let connection_id = self.u64("connection_id")?;
        Ok(PipeId::new(session_id, connection_id))
    }

    fn error_code(&mut self) -> Result<ErrorCode, ProtocolError> {
        let value = self.u8("error_code")?;
        ErrorCode::from_wire(value).ok_or(ProtocolError::UnknownEnum {
            name: "ErrorCode",
            value,
        })
    }

    fn observation(&mut self) -> Result<PeerObservation, ProtocolError> {
        let value = self.u8("peer_observation")?;
        PeerObservation::from_wire(value).ok_or(ProtocolError::UnknownEnum {
            name: "PeerObservation",
            value,
        })
    }

    fn remaining(&mut self) -> Bytes {
        let remaining = self.payload.slice(self.position..);
        self.position = self.payload.len();
        remaining
    }

    fn finish(self) -> Result<(), ProtocolError> {
        let trailing = self.payload.len().saturating_sub(self.position);
        if trailing == 0 {
            Ok(())
        } else {
            Err(ProtocolError::TrailingBytes(trailing))
        }
    }
}
