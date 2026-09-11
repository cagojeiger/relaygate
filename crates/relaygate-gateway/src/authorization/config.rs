use std::{collections::HashSet, sync::Arc, time::Duration};

use jsonwebtoken::DecodingKey;
use relaygate_address::NamespaceId;

use crate::GatewayError;

const MAX_AUDIENCE_BYTES: usize = 256;
const MAX_ISSUER_BYTES: usize = 2_048;
const MAX_KEY_ID_BYTES: usize = 128;
const MAX_ISSUERS: usize = 256;
const MAX_KEYS_PER_ISSUER: usize = 2;
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(300);

/// One static ES256 verification key selected by JWT `kid`.
#[derive(Clone)]
pub struct Es256PublicKey {
    pub(crate) kid: Arc<str>,
    pub(crate) decoding_key: DecodingKey,
}

impl Es256PublicKey {
    pub fn new(
        kid: impl Into<String>,
        x: impl AsRef<str>,
        y: impl AsRef<str>,
    ) -> Result<Self, GatewayError> {
        let kid = kid.into();
        if kid.is_empty() || kid.len() > MAX_KEY_ID_BYTES {
            return Err(invalid("ES256 key kid must be 1..=128 bytes"));
        }
        let decoding_key = DecodingKey::from_ec_components(x.as_ref(), y.as_ref())
            .map_err(|_| invalid("ES256 public key coordinates are invalid"))?;
        Ok(Self {
            kid: kid.into(),
            decoding_key,
        })
    }
}

impl std::fmt::Debug for Es256PublicKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Es256PublicKey")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

/// One issuer trusted to authorize operations inside exactly one Namespace.
#[derive(Clone)]
pub struct TrustedIssuer {
    pub(crate) namespace: NamespaceId,
    pub(crate) issuer: Arc<str>,
    pub(crate) keys: Vec<Es256PublicKey>,
}

impl TrustedIssuer {
    pub fn new(
        namespace: NamespaceId,
        issuer: impl Into<String>,
        keys: Vec<Es256PublicKey>,
    ) -> Result<Self, GatewayError> {
        let issuer = issuer.into();
        if issuer.is_empty() || issuer.len() > MAX_ISSUER_BYTES {
            return Err(invalid("JWT issuer must be 1..=2048 bytes"));
        }
        if keys.is_empty() || keys.len() > MAX_KEYS_PER_ISSUER {
            return Err(invalid("each issuer must configure one or two ES256 keys"));
        }
        let mut kids = HashSet::with_capacity(keys.len());
        if keys.iter().any(|key| !kids.insert(key.kid.as_ref())) {
            return Err(invalid("JWT key ids must be unique within an issuer"));
        }
        Ok(Self {
            namespace,
            issuer: issuer.into(),
            keys,
        })
    }
}

impl std::fmt::Debug for TrustedIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TrustedIssuer")
            .field("namespace", &self.namespace)
            .field("issuer", &self.issuer)
            .field("key_count", &self.keys.len())
            .finish()
    }
}

/// Immutable public-key trust configuration for operation admission.
#[derive(Clone)]
pub struct AuthorizationConfig {
    pub(crate) audience: Arc<str>,
    pub(crate) issuers: Vec<TrustedIssuer>,
    pub(crate) clock_skew: Duration,
}

impl AuthorizationConfig {
    pub fn new(
        audience: impl Into<String>,
        issuers: Vec<TrustedIssuer>,
    ) -> Result<Self, GatewayError> {
        let audience = audience.into();
        if audience.is_empty() || audience.len() > MAX_AUDIENCE_BYTES {
            return Err(invalid("JWT audience must be 1..=256 bytes"));
        }
        if issuers.is_empty() || issuers.len() > MAX_ISSUERS {
            return Err(invalid(
                "authorization must configure 1..=256 trusted issuers",
            ));
        }
        let mut namespaces = HashSet::with_capacity(issuers.len());
        if issuers
            .iter()
            .any(|trusted| !namespaces.insert(trusted.namespace.as_str()))
        {
            return Err(invalid(
                "each namespace must have exactly one trusted issuer",
            ));
        }
        Ok(Self {
            audience: audience.into(),
            issuers,
            clock_skew: Duration::from_secs(30),
        })
    }

    pub fn with_clock_skew(mut self, clock_skew: Duration) -> Result<Self, GatewayError> {
        if clock_skew > MAX_CLOCK_SKEW {
            return Err(invalid("JWT clock skew must not exceed 300 seconds"));
        }
        self.clock_skew = clock_skew;
        Ok(self)
    }
}

impl std::fmt::Debug for AuthorizationConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizationConfig")
            .field("audience", &self.audience)
            .field("issuer_count", &self.issuers.len())
            .field("clock_skew", &self.clock_skew)
            .finish()
    }
}

fn invalid(message: &str) -> GatewayError {
    GatewayError::InvalidConfig(message.to_owned())
}
