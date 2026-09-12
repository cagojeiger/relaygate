#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use p256::{
    ecdsa::SigningKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey},
};
use relaygate_destination::{Destination, DestinationName, Namespace};
use serde::Serialize;

const MAX_AUDIENCE_BYTES: usize = 256;
const MAX_ISSUER_BYTES: usize = 2_048;
const MAX_KEY_ID_BYTES: usize = 128;
const MAX_PERMISSIONS: usize = 128;
const MAX_TOKEN_BYTES: usize = 4_096;

/// Canonical explicit type used by RelayGate operation tokens.
pub const OPERATION_TOKEN_TYPE: &str = "relaygate-operation+jwt";

/// A RelayGate operation authorized by a permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// Authorizes `PUBLISH` for the permission's destination scope.
    Publish,
    /// Authorizes `DIAL` for the permission's destination scope.
    Dial,
}

/// One authorization rule encoded in an operation token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Permission {
    action: Action,
    namespace: Namespace,
    scope: DestinationScope,
}

impl Permission {
    /// Authorizes exactly one destination.
    #[must_use]
    pub fn exact(action: Action, destination: &Destination) -> Self {
        Self {
            action,
            namespace: destination.namespace().clone(),
            scope: DestinationScope::Exact {
                name: destination.name().clone(),
            },
        }
    }

    /// Authorizes a destination and all of its whole-label descendants.
    #[must_use]
    pub fn subtree(action: Action, namespace: Namespace, name: DestinationName) -> Self {
        Self {
            action,
            namespace,
            scope: DestinationScope::Subtree { name },
        }
    }

    /// Authorizes every destination in one namespace.
    #[must_use]
    pub fn all(action: Action, namespace: Namespace) -> Self {
        Self {
            action,
            namespace,
            scope: DestinationScope::All,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum DestinationScope {
    Exact { name: DestinationName },
    Subtree { name: DestinationName },
    All,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    aud: &'a str,
    nbf: u64,
    exp: u64,
    permissions: &'a [Permission],
}

/// A signed compact JWT and its expiration time.
pub struct IssuedToken {
    token: String,
    expires_at: u64,
}

impl IssuedToken {
    /// Returns the compact JWT for transport to the SDK.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.token
    }

    /// Returns the JWT `exp` value as Unix seconds.
    #[must_use]
    pub const fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Consumes the value and returns the compact JWT.
    #[must_use]
    pub fn into_string(self) -> String {
        self.token
    }
}

impl std::fmt::Debug for IssuedToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IssuedToken")
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Canonical ES256 signer used by an application-owned token endpoint.
pub struct TokenIssuer {
    issuer: Box<str>,
    audience: Box<str>,
    key_id: Box<str>,
    encoding_key: EncodingKey,
}

impl TokenIssuer {
    /// Loads one unencrypted P-256 PKCS#8 private key from PEM.
    pub fn from_es256_pem(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        key_id: impl Into<String>,
        private_key_pem: impl AsRef<[u8]>,
    ) -> Result<Self, TokenIssuerError> {
        let issuer = issuer.into();
        let audience = audience.into();
        let key_id = key_id.into();
        validate_text(&issuer, MAX_ISSUER_BYTES, "issuer")?;
        validate_text(&audience, MAX_AUDIENCE_BYTES, "audience")?;
        validate_text(&key_id, MAX_KEY_ID_BYTES, "key id")?;

        let private_key_pem = std::str::from_utf8(private_key_pem.as_ref())
            .map_err(|_| TokenIssuerError::InvalidPrivateKey)?;
        let signing_key = SigningKey::from_pkcs8_pem(private_key_pem)
            .map_err(|_| TokenIssuerError::InvalidPrivateKey)?;
        let private_key_der = signing_key
            .to_pkcs8_der()
            .map_err(|_| TokenIssuerError::InvalidPrivateKey)?;

        Ok(Self {
            issuer: issuer.into_boxed_str(),
            audience: audience.into_boxed_str(),
            key_id: key_id.into_boxed_str(),
            encoding_key: EncodingKey::from_ec_der(private_key_der.as_bytes()),
        })
    }

    /// Issues a least-privilege token for exactly one operation and destination.
    pub fn issue_exact(
        &self,
        action: Action,
        destination: &Destination,
        valid_for: Duration,
    ) -> Result<IssuedToken, TokenIssuerError> {
        self.issue([Permission::exact(action, destination)], valid_for)
    }

    /// Issues a token containing application-approved permissions.
    pub fn issue<I>(
        &self,
        permissions: I,
        valid_for: Duration,
    ) -> Result<IssuedToken, TokenIssuerError>
    where
        I: IntoIterator<Item = Permission>,
    {
        self.issue_at(permissions, SystemTime::now(), valid_for)
    }

    /// Issues a token from an explicit start time, primarily for an external clock or tests.
    pub fn issue_at<I>(
        &self,
        permissions: I,
        not_before: SystemTime,
        valid_for: Duration,
    ) -> Result<IssuedToken, TokenIssuerError>
    where
        I: IntoIterator<Item = Permission>,
    {
        let permissions = permissions.into_iter().collect::<Vec<_>>();
        if permissions.is_empty() {
            return Err(TokenIssuerError::EmptyPermissions);
        }
        if permissions.len() > MAX_PERMISSIONS {
            return Err(TokenIssuerError::TooManyPermissions {
                actual: permissions.len(),
                maximum: MAX_PERMISSIONS,
            });
        }

        let lifetime = valid_for.as_secs();
        if lifetime == 0 {
            return Err(TokenIssuerError::InvalidLifetime);
        }
        let not_before = not_before
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TokenIssuerError::InvalidNotBefore)?
            .as_secs();
        let expires_at = not_before
            .checked_add(lifetime)
            .ok_or(TokenIssuerError::InvalidLifetime)?;

        let mut header = Header::new(Algorithm::ES256);
        header.typ = Some(OPERATION_TOKEN_TYPE.to_owned());
        header.kid = Some(self.key_id.to_string());
        let claims = Claims {
            iss: &self.issuer,
            aud: &self.audience,
            nbf: not_before,
            exp: expires_at,
            permissions: &permissions,
        };
        let token = encode(&header, &claims, &self.encoding_key)
            .map_err(|_| TokenIssuerError::SigningFailed)?;
        if token.len() > MAX_TOKEN_BYTES {
            return Err(TokenIssuerError::TokenTooLong {
                actual: token.len(),
                maximum: MAX_TOKEN_BYTES,
            });
        }
        Ok(IssuedToken { token, expires_at })
    }
}

impl std::fmt::Debug for TokenIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TokenIssuer")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("key_id", &self.key_id)
            .field("encoding_key", &"[REDACTED]")
            .finish()
    }
}

/// Configuration or issuance failure that does not expose key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenIssuerError {
    /// A required text field is empty or exceeds its byte limit.
    #[error("{field} must contain 1..={maximum} bytes")]
    InvalidText {
        /// Name of the invalid configuration field.
        field: &'static str,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// The supplied key is not an unencrypted P-256 PKCS#8 PEM private key.
    #[error("ES256 private key must be an unencrypted P-256 PKCS#8 PEM key")]
    InvalidPrivateKey,
    /// No permission was supplied for the operation token.
    #[error("operation token must contain at least one permission")]
    EmptyPermissions,
    /// The permission count exceeds the closed token-profile limit.
    #[error("operation token contains {actual} permissions, maximum {maximum}")]
    TooManyPermissions {
        /// Number of permissions supplied by the application.
        actual: usize,
        /// Maximum number accepted by the token profile.
        maximum: usize,
    },
    /// The requested not-before time precedes the Unix epoch.
    #[error("token not-before time must be at or after the Unix epoch")]
    InvalidNotBefore,
    /// The requested lifetime has no whole second or overflows its timestamp.
    #[error("token lifetime must contain at least one whole second")]
    InvalidLifetime,
    /// Compact-JWT serialization or ES256 signing failed.
    #[error("operation token signing failed")]
    SigningFailed,
    /// The resulting compact JWT exceeds the token-profile size limit.
    #[error("operation token is {actual} bytes, maximum {maximum}")]
    TokenTooLong {
        /// Encoded token length in bytes.
        actual: usize,
        /// Maximum encoded length accepted by the token profile.
        maximum: usize,
    },
}

fn validate_text(value: &str, maximum: usize, field: &'static str) -> Result<(), TokenIssuerError> {
    if value.is_empty() || value.len() > maximum {
        return Err(TokenIssuerError::InvalidText { field, maximum });
    }
    Ok(())
}
