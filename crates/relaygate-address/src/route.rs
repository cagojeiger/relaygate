use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{AddressError, DestinationName, NamespaceId};

/// A fully specified exact routing key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RouteAddress {
    namespace: NamespaceId,
    destination: DestinationName,
}

impl RouteAddress {
    #[must_use]
    pub const fn new(namespace: NamespaceId, destination: DestinationName) -> Self {
        Self {
            namespace,
            destination,
        }
    }

    #[must_use]
    pub const fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    #[must_use]
    pub const fn destination(&self) -> &DestinationName {
        &self.destination
    }

    /// Collision-unambiguous bytes used by the shard authority hash.
    ///
    /// Format: namespace byte length as big-endian u16, namespace bytes,
    /// destination byte length as big-endian u16, destination bytes.
    #[must_use]
    pub fn canonical_key(&self) -> Vec<u8> {
        let namespace = self.namespace.as_str().as_bytes();
        let destination = self.destination.as_str().as_bytes();
        let mut key = Vec::with_capacity(4 + namespace.len() + destination.len());
        key.extend_from_slice(&(namespace.len() as u16).to_be_bytes());
        key.extend_from_slice(namespace);
        key.extend_from_slice(&(destination.len() as u16).to_be_bytes());
        key.extend_from_slice(destination);
        key
    }
}

impl FromStr for RouteAddress {
    type Err = AddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, destination) = value.split_once('/').ok_or(AddressError::InvalidRoute)?;
        if destination.contains('/') {
            return Err(AddressError::InvalidRoute);
        }
        Ok(Self::new(
            NamespaceId::new(namespace)?,
            DestinationName::new(destination)?,
        ))
    }
}

impl TryFrom<String> for RouteAddress {
    type Error = AddressError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RouteAddress> for String {
    fn from(value: RouteAddress) -> Self {
        value.to_string()
    }
}

impl fmt::Display for RouteAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.destination)
    }
}
