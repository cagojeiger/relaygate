use std::fmt;

use crate::TransportError;

/// Stable logical Gateway name; transport authentication is owned by mTLS.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayName(String);

impl GatewayName {
    pub fn new(value: impl Into<String>) -> Result<Self, TransportError> {
        let value = value.into();
        if value.is_empty() {
            return Err(TransportError::invalid_argument(
                "GatewayName must not be empty",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GatewayName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
