/// Maximum UTF-8 byte length of an operation bearer token.
pub const MAX_BEARER_TOKEN_BYTES: usize = 4096;

/// Bounded operation credential carried only by `PUBLISH` and `DIAL`.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
    /// Creates a token when its UTF-8 byte length is within the wire limit.
    ///
    /// Returns [`ProtocolError::FieldTooLong`](crate::ProtocolError::FieldTooLong)
    /// when `value` exceeds [`MAX_BEARER_TOKEN_BYTES`].
    pub fn new(value: impl Into<String>) -> Result<Self, crate::ProtocolError> {
        let value = value.into();
        if value.len() > MAX_BEARER_TOKEN_BYTES {
            return Err(crate::ProtocolError::FieldTooLong {
                field: "access_token",
                actual: value.len(),
                maximum: MAX_BEARER_TOKEN_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Returns the raw credential text for authorization or wire encoding.
    ///
    /// Do not log or persist the returned value.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}
