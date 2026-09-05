#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid frame magic")]
    InvalidMagic,
    #[error("unknown frame kind {0}")]
    UnknownFrameKind(u8),
    #[error("unknown enum value {value} for {name}")]
    UnknownEnum { name: &'static str, value: u8 },
    #[error("truncated {0}")]
    Truncated(&'static str),
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    #[error("DestinationId must be UUIDv4")]
    InvalidDestinationId,
    #[error("{field} is too long: {actual} bytes, maximum {maximum}")]
    FieldTooLong {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("frame has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("frame length cannot be represented on the wire")]
    LengthOverflow,
}
