use std::io::{self, Write};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::WireFrame;

const MAGIC: [u8; 2] = *b"RT";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 7;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CodecError {
    #[error("frame I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid RouteTable frame magic")]
    InvalidMagic,
    #[error("unsupported RouteTable protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("RouteTable frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("RouteTable frame length cannot be represented on the wire")]
    LengthOverflow,
    #[error("invalid RouteTable frame payload: {0}")]
    InvalidPayload(serde_json::Error),
}

impl CodecError {
    #[must_use]
    pub(crate) const fn is_io(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FrameCodec {
    max_frame_len: usize,
}

impl FrameCodec {
    pub(crate) const fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    pub(crate) fn validate(&self, frame: &WireFrame) -> Result<(), CodecError> {
        let mut counter = BoundedFrameCounter::new(self.max_frame_len);
        match serde_json::to_writer(&mut counter, frame) {
            Ok(()) => self.validate_payload_len(counter.len),
            Err(_) if counter.exceeded_at.is_some() => Err(CodecError::FrameTooLarge {
                actual: counter.exceeded_at.unwrap_or(self.max_frame_len),
                maximum: self.max_frame_len,
            }),
            Err(error) => Err(CodecError::InvalidPayload(error)),
        }
    }

    fn validate_payload_len(&self, payload_len: usize) -> Result<(), CodecError> {
        if payload_len > self.max_frame_len {
            return Err(CodecError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        let _ = u32::try_from(payload_len).map_err(|_| CodecError::LengthOverflow)?;
        Ok(())
    }
}

struct BoundedFrameCounter {
    len: usize,
    maximum: usize,
    exceeded_at: Option<usize>,
}

impl BoundedFrameCounter {
    const fn new(maximum: usize) -> Self {
        Self {
            len: 0,
            maximum,
            exceeded_at: None,
        }
    }
}

impl Write for BoundedFrameCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let attempted = self.len.saturating_add(buffer.len());
        if attempted > self.maximum {
            self.exceeded_at = Some(attempted);
            return Err(io::Error::other(
                "RouteTable frame exceeds configured maximum",
            ));
        }
        self.len = attempted;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Encoder<WireFrame> for FrameCodec {
    type Error = CodecError;

    fn encode(&mut self, frame: WireFrame, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = serde_json::to_vec(&frame).map_err(CodecError::InvalidPayload)?;
        self.validate_payload_len(payload.len())?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| CodecError::LengthOverflow)?;
        destination.reserve(HEADER_LEN + payload.len());
        destination.extend_from_slice(&MAGIC);
        destination.put_u8(VERSION);
        destination.put_u32(payload_len);
        destination.extend_from_slice(&payload);
        Ok(())
    }
}

impl Decoder for FrameCodec {
    type Item = WireFrame;
    type Error = CodecError;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }
        if source[..2] != MAGIC {
            return Err(CodecError::InvalidMagic);
        }
        let version = source[2];
        if version != VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        let payload_len = u32::from_be_bytes([source[3], source[4], source[5], source[6]]) as usize;
        if payload_len > self.max_frame_len {
            return Err(CodecError::FrameTooLarge {
                actual: payload_len,
                maximum: self.max_frame_len,
            });
        }
        let frame_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(CodecError::LengthOverflow)?;
        if source.len() < frame_len {
            source.reserve(frame_len - source.len());
            return Ok(None);
        }

        source.advance(HEADER_LEN);
        let payload = source.split_to(payload_len);
        serde_json::from_slice(&payload)
            .map(Some)
            .map_err(CodecError::InvalidPayload)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE};

    #[test]
    fn round_trip() -> Result<(), CodecError> {
        let mut codec = FrameCodec::new(1024);
        let mut bytes = BytesMut::new();
        codec.encode(
            WireFrame::Welcome {
                role: ROUTE_TABLE_ROLE.to_owned(),
            },
            &mut bytes,
        )?;

        let decoded = codec.decode(&mut bytes)?;
        assert!(matches!(
            decoded,
            Some(WireFrame::Welcome { role }) if role == ROUTE_TABLE_ROLE
        ));
        Ok(())
    }

    #[test]
    fn rejects_unsupported_version() -> Result<(), CodecError> {
        let mut codec = FrameCodec::new(1024);
        let mut bytes = BytesMut::new();
        codec.encode(
            WireFrame::Welcome {
                role: GATEWAY_ROLE.to_owned(),
            },
            &mut bytes,
        )?;
        bytes[2] = VERSION + 1;

        let error = codec.decode(&mut bytes).err();
        assert!(matches!(error, Some(CodecError::UnsupportedVersion(2))));
        Ok(())
    }

    #[test]
    fn rejects_oversized_declared_frame_before_payload_arrives() {
        let mut codec = FrameCodec::new(8);
        let mut bytes = BytesMut::from(&[MAGIC[0], MAGIC[1], VERSION, 0, 0, 0, 9][..]);

        let error = codec.decode(&mut bytes).err();
        assert!(matches!(
            error,
            Some(CodecError::FrameTooLarge {
                actual: 9,
                maximum: 8
            })
        ));
    }
}
