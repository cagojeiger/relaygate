use std::{fmt, str::FromStr};

use uuid::Uuid;

use crate::RouteTableError;

macro_rules! non_empty_string_id {
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, RouteTableError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(RouteTableError::InvalidArgument(concat!($label, " must not be empty").to_owned()));
                }
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl TryFrom<String> for $name {
            type Error = RouteTableError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = RouteTableError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

macro_rules! opaque_uuid {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

non_empty_string_id!(
    /// Logical RouteTable shard identity within one directory generation.
    ShardId,
    "ShardId"
);
non_empty_string_id!(
    /// Stable logical endpoint for one RouteTable shard.
    ShardEndpoint,
    "ShardEndpoint"
);
non_empty_string_id!(
    /// Routable location of one Gateway runtime.
    GatewayLocator,
    "GatewayLocator"
);

opaque_uuid!(
    /// Identifies one Gateway runtime incarnation.
    GatewayId
);
opaque_uuid!(
    /// Identifies one Relay session within its Gateway incarnation.
    RelaySessionId
);
opaque_uuid!(
    /// Identifies one Binding incarnation.
    BindingId
);
opaque_uuid!(
    /// Identifies one active registration lease issued as a UUIDv4 by a RouteTable shard.
    ///
    /// A shard rejects collisions with current active leases. Ended IDs are not
    /// retained as tombstones; non-reuse across ended leases relies on UUIDv4.
    LeaseId
);

/// SHA-256 of the exact immutable shard-directory artifact bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardDirectoryGeneration([u8; 32]);

impl ShardDirectoryGeneration {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ShardDirectoryGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ShardDirectoryGeneration {
    type Err = RouteTableError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            RouteTableError::InvalidArgument(
                "ShardDirectoryGeneration must be 64 hexadecimal characters".to_owned(),
            )
        };
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let mut bytes = [0_u8; 32];
        for (byte, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            let pair = std::str::from_utf8(pair).map_err(|_| invalid())?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
        }
        Ok(Self(bytes))
    }
}

/// Monotonic snapshot revision scoped to one active lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegistrationRevision(u64);

impl RegistrationRevision {
    pub const FIRST: Self = Self(1);

    pub fn new(value: u64) -> Result<Self, RouteTableError> {
        if value == 0 {
            return Err(RouteTableError::InvalidArgument(
                "RegistrationRevision must be greater than zero".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A Gateway identity already verified by the internal transport boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuthenticatedGatewayId(GatewayId);

impl AuthenticatedGatewayId {
    #[must_use]
    pub const fn from_verified_transport(gateway_id: GatewayId) -> Self {
        Self(gateway_id)
    }

    #[must_use]
    pub const fn gateway_id(self) -> GatewayId {
        self.0
    }
}

/// Per-operation context supplied after transport authentication succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestContext {
    authenticated_gateway_id: AuthenticatedGatewayId,
}

impl RequestContext {
    #[must_use]
    pub const fn new(authenticated_gateway_id: AuthenticatedGatewayId) -> Self {
        Self {
            authenticated_gateway_id,
        }
    }

    #[must_use]
    pub const fn authenticated_gateway_id(self) -> AuthenticatedGatewayId {
        self.authenticated_gateway_id
    }
}

#[cfg(test)]
mod tests {
    use super::ShardDirectoryGeneration;

    #[test]
    fn shard_directory_generation_round_trips_through_its_hex_display() {
        let generation = ShardDirectoryGeneration::from_bytes(core::array::from_fn(|i| i as u8));
        let text = generation.to_string();
        assert_eq!(text.len(), 64);
        assert_eq!(
            text.parse::<ShardDirectoryGeneration>().ok(),
            Some(generation)
        );
        assert_eq!(
            text.to_ascii_uppercase()
                .parse::<ShardDirectoryGeneration>()
                .ok(),
            Some(generation)
        );
    }

    #[test]
    fn shard_directory_generation_rejects_non_hex_text() {
        for text in [
            "",
            &"a".repeat(63),
            &"a".repeat(65),
            &("+f".repeat(32)),
            &("zz".repeat(32)),
            &("é".repeat(32)),
        ] {
            assert!(
                text.parse::<ShardDirectoryGeneration>().is_err(),
                "{text:?}"
            );
        }
    }
}
