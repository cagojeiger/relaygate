/// Listener registration credential carried only during registration.
///
/// `Debug` deliberately redacts the value so frame or state diagnostics cannot
/// accidentally print credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct ClientKey(String);

impl ClientKey {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ClientKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ClientKey([REDACTED])")
    }
}
