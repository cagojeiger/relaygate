/// Failures raised while encoding or decoding an SDK–Gateway frame.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The underlying transport failed while a frame was being read or written.
    #[error("frame I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// A frame payload is longer than the codec's configured maximum.
    #[error("frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge {
        /// Payload length seen on the wire, in bytes.
        actual: usize,
        /// Configured payload limit, in bytes.
        maximum: usize,
    },
    /// The frame header names a protocol version this codec does not implement.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),
    /// The frame header does not begin with the expected magic bytes.
    #[error("invalid frame magic")]
    InvalidMagic,
    /// The header kind byte does not name a frame in this protocol version.
    #[error("unknown frame kind {0}")]
    UnknownFrameKind(u8),
    /// A byte-valued enum field carries a value outside the contract.
    #[error("unknown enum value {value} for {name}")]
    UnknownEnum {
        /// Name of the enum type that rejected the value.
        name: &'static str,
        /// The wire byte that matched no variant.
        value: u8,
    },
    /// The payload ended before the named field was complete.
    #[error("truncated {0}")]
    Truncated(&'static str),
    /// The named string field is not valid UTF-8.
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    /// The decoded namespace and name do not form a valid `Destination`.
    #[error("invalid Destination")]
    InvalidDestination,
    /// A length-prefixed field is longer than the wire format allows.
    #[error("{field} is too long: {actual} bytes, maximum {maximum}")]
    FieldTooLong {
        /// Name of the field that exceeded its limit.
        field: &'static str,
        /// Length of the offered value, in bytes.
        actual: usize,
        /// Largest length the field accepts, in bytes.
        maximum: usize,
    },
    /// The payload held bytes left over after the frame was decoded.
    #[error("frame has {0} trailing bytes")]
    TrailingBytes(usize),
    /// A frame length cannot be represented on the wire, or a decode offset
    /// would overflow.
    #[error("frame length cannot be represented on the wire")]
    LengthOverflow,
}
