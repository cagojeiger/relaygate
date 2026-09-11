pub const MAX_BEARER_TOKEN_BYTES: usize = 4096;

/// Bounded operation credential carried only by `PUBLISH` and `DIAL`.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
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
