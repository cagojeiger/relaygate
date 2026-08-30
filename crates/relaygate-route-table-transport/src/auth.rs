use std::fmt;

use sha2::{Digest, Sha256};
use subtle::{Choice, ConstantTimeEq};

use crate::TransportError;

/// Stable configuration name used to select one local/CI Gateway credential.
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

/// Local/CI Gateway credential carried only during the TCP handshake.
///
/// `Debug` deliberately redacts the value. The RouteTable service stores only
/// its SHA-256 digest.
#[derive(Clone, PartialEq, Eq)]
pub struct InternalGatewayKey(String);

impl InternalGatewayKey {
    pub fn new(value: impl Into<String>) -> Result<Self, TransportError> {
        let value = value.into();
        if value.is_empty() {
            return Err(TransportError::invalid_argument(
                "InternalGatewayKey must not be empty",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn from_wire(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for InternalGatewayKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InternalGatewayKey([REDACTED])")
    }
}

/// Immutable startup allowlist for the local/CI authentication adapter.
#[derive(Clone)]
pub struct TrustedGatewayKeys {
    digests: Vec<TrustedGatewayDigest>,
}

#[derive(Clone)]
struct TrustedGatewayDigest {
    name: [u8; 32],
    key: [u8; 32],
}

impl TrustedGatewayKeys {
    pub fn new(
        entries: impl IntoIterator<Item = (GatewayName, InternalGatewayKey)>,
    ) -> Result<Self, TransportError> {
        let mut digests: Vec<TrustedGatewayDigest> = Vec::new();
        for (name, key) in entries {
            let name_digest = digest(name.as_str());
            if digests
                .iter()
                .any(|entry| bool::from(entry.name.ct_eq(&name_digest)))
            {
                return Err(TransportError::invalid_argument(
                    "TrustedGatewayKeys contains a duplicate GatewayName",
                ));
            }
            digests.push(TrustedGatewayDigest {
                name: name_digest,
                key: digest(key.expose_secret()),
            });
        }
        Ok(Self { digests })
    }

    pub(crate) fn authenticate(&self, name: &GatewayName, key: &InternalGatewayKey) -> bool {
        let candidate_name = digest(name.as_str());
        let candidate_key = digest(key.expose_secret());
        let mut matched = Choice::from(0);
        // Scan every configured entry and fold both fixed-size comparisons so
        // lookup timing does not reveal the matching GatewayName position.
        for entry in &self.digests {
            matched |= entry.name.ct_eq(&candidate_name) & entry.key.ct_eq(&candidate_key);
        }
        bool::from(matched)
    }
}

fn digest(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

impl fmt::Debug for TrustedGatewayKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedGatewayKeys")
            .field("gateway_count", &self.digests.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_and_allowlist_debug_are_redacted() -> Result<(), TransportError> {
        let name = GatewayName::new("gw-a")?;
        let key = InternalGatewayKey::new("do-not-print-this")?;
        let keys = TrustedGatewayKeys::new([(name, key.clone())])?;

        let key_debug = format!("{key:?}");
        let keys_debug = format!("{keys:?}");
        assert!(!key_debug.contains("do-not-print-this"));
        assert!(!keys_debug.contains("do-not-print-this"));
        Ok(())
    }

    #[test]
    fn unknown_and_wrong_keys_fail() -> Result<(), TransportError> {
        let known_name = GatewayName::new("gw-a")?;
        let correct_key = InternalGatewayKey::new("correct")?;
        let keys = TrustedGatewayKeys::new([(known_name.clone(), correct_key.clone())])?;

        assert!(keys.authenticate(&known_name, &correct_key));
        assert!(!keys.authenticate(&known_name, &InternalGatewayKey::new("wrong")?));
        assert!(!keys.authenticate(&GatewayName::new("gw-unknown")?, &correct_key));
        Ok(())
    }

    #[test]
    fn scans_fixed_name_and_key_digests_for_every_configured_position() -> Result<(), TransportError>
    {
        let keys = TrustedGatewayKeys::new([
            (
                GatewayName::new("gw-first")?,
                InternalGatewayKey::new("key-1")?,
            ),
            (
                GatewayName::new("gw-middle")?,
                InternalGatewayKey::new("key-2")?,
            ),
            (
                GatewayName::new("gw-last")?,
                InternalGatewayKey::new("key-3")?,
            ),
        ])?;

        assert!(keys.authenticate(
            &GatewayName::new("gw-first")?,
            &InternalGatewayKey::new("key-1")?
        ));
        assert!(keys.authenticate(
            &GatewayName::new("gw-last")?,
            &InternalGatewayKey::new("key-3")?
        ));
        assert!(!keys.authenticate(
            &GatewayName::new("gw-first")?,
            &InternalGatewayKey::new("key-3")?
        ));
        assert!(!keys.authenticate(
            &GatewayName::new("gw-absent")?,
            &InternalGatewayKey::new("key-2")?
        ));
        assert!(!format!("{keys:?}").contains("gw-first"));
        Ok(())
    }

    #[test]
    fn duplicate_gateway_name_is_rejected() -> Result<(), TransportError> {
        let duplicate = TrustedGatewayKeys::new([
            (GatewayName::new("gw-a")?, InternalGatewayKey::new("key-1")?),
            (GatewayName::new("gw-a")?, InternalGatewayKey::new("key-2")?),
        ]);
        assert!(duplicate.is_err());
        Ok(())
    }
}
