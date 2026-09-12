/// Failure while encoding or decoding an SDK–Gateway frame.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The underlying byte transport reported an I/O error.
    #[error("frame I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// A frame payload exceeds the codec's configured limit.
    #[error("frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge {
        /// Encoded or declared payload length.
        actual: usize,
        /// Maximum payload length accepted by the codec.
        maximum: usize,
    },
    /// The frame declares a protocol version this implementation does not support.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),
    /// The frame does not begin with the RelayGate wire magic.
    #[error("invalid frame magic")]
    InvalidMagic,
    /// The frame kind byte is not assigned by this protocol version.
    #[error("unknown frame kind {0}")]
    UnknownFrameKind(u8),
    /// An enum-valued field contains an unknown wire value.
    #[error("unknown enum value {value} for {name}")]
    UnknownEnum {
        /// Name of the field being decoded.
        name: &'static str,
        /// Unrecognized wire value.
        value: u8,
    },
    /// The payload ends before the named field is complete.
    #[error("truncated {0}")]
    Truncated(&'static str),
    /// The named string field is not valid UTF-8.
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    /// Namespace or destination-name bytes do not form a valid [`Destination`](crate::Destination).
    #[error("invalid Destination")]
    InvalidDestination,
    /// A length-prefixed string cannot fit within its wire limit.
    #[error("{field} is too long: {actual} bytes, maximum {maximum}")]
    FieldTooLong {
        /// Name of the oversized field.
        field: &'static str,
        /// Encoded byte length of the field.
        actual: usize,
        /// Maximum byte length accepted by the wire format.
        maximum: usize,
    },
    /// Bytes remain after decoding a fixed-layout frame payload.
    #[error("frame has {0} trailing bytes")]
    TrailingBytes(usize),
    /// A payload length cannot be represented by the wire header.
    #[error("frame length cannot be represented on the wire")]
    LengthOverflow,
}
