use std::net::IpAddr;

use crate::ConnectError;

/// Connection credentials and transport policy.
///
/// TLS is mandatory by default. Plaintext is available only through
/// [`Config::with_insecure_local`] and only for a loopback endpoint.
pub struct Config {
    pub(crate) endpoint: String,
    pub(crate) client_id: String,
    pub(crate) api_key_id: String,
    pub(crate) api_key: String,
    pub(crate) insecure_local: bool,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("endpoint", &self.endpoint)
            .field("client_id", &self.client_id)
            .field("api_key_id", &self.api_key_id)
            .field("api_key", &"[REDACTED]")
            .field("insecure_local", &self.insecure_local)
            .finish()
    }
}

impl Config {
    /// Creates a TLS-only connection configuration.
    pub fn new(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
        api_key_id: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            client_id: client_id.into(),
            api_key_id: api_key_id.into(),
            api_key: api_key.into(),
            insecure_local: false,
        }
    }

    /// Explicitly permits plaintext for a loopback-only local development endpoint.
    pub fn with_insecure_local(mut self) -> Self {
        self.insecure_local = true;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), ConnectError> {
        if self.client_id.is_empty() || self.api_key_id.is_empty() || self.api_key.is_empty() {
            return Err(ConnectError::InvalidConfig(
                "client_id, api_key_id, and api_key must be non-empty",
            ));
        }

        let uri: tonic::transport::Uri = self
            .endpoint
            .parse()
            .map_err(|_| ConnectError::InvalidConfig("endpoint must be a valid URI"))?;
        let scheme = uri.scheme_str().unwrap_or_default();
        match (scheme, self.insecure_local) {
            ("https", false) => Ok(()),
            ("https", true) => Err(ConnectError::InvalidConfig(
                "insecure local opt-in requires an http endpoint",
            )),
            ("http", true) if is_loopback_host(uri.host().unwrap_or_default()) => Ok(()),
            ("http", true) => Err(ConnectError::InvalidConfig(
                "insecure transport is restricted to loopback endpoints",
            )),
            ("http", false) => Err(ConnectError::InvalidConfig(
                "plaintext requires with_insecure_local()",
            )),
            _ => Err(ConnectError::InvalidConfig(
                "endpoint scheme must be https, or loopback http with explicit opt-in",
            )),
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn plaintext_requires_explicit_loopback_opt_in() {
        assert!(
            Config::new("http://127.0.0.1:1234", "c", "k", "s")
                .validate()
                .is_err()
        );
        assert!(
            Config::new("http://127.0.0.1:1234", "c", "k", "s")
                .with_insecure_local()
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("http://example.com:1234", "c", "k", "s")
                .with_insecure_local()
                .validate()
                .is_err()
        );
    }
}
