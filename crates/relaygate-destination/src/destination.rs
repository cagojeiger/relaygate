use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{DestinationError, DestinationName, Namespace};

/// A fully specified exact routing key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Destination {
    namespace: Namespace,
    name: DestinationName,
}

impl Destination {
    /// Constructs a destination from an already validated namespace and name.
    #[must_use]
    pub const fn new(namespace: Namespace, name: DestinationName) -> Self {
        Self { namespace, name }
    }

    /// Returns the destination's namespace.
    #[must_use]
    pub const fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Returns the name within the destination's namespace.
    #[must_use]
    pub const fn name(&self) -> &DestinationName {
        &self.name
    }

    /// Collision-unambiguous bytes used by the shard authority hash.
    ///
    /// Format: namespace byte length as big-endian u16, namespace bytes,
    /// name byte length as big-endian u16, name bytes.
    #[must_use]
    pub fn canonical_key(&self) -> Vec<u8> {
        let namespace = self.namespace.as_str().as_bytes();
        let name = self.name.as_str().as_bytes();
        let mut key = Vec::with_capacity(4 + namespace.len() + name.len());
        key.extend_from_slice(&(namespace.len() as u16).to_be_bytes());
        key.extend_from_slice(namespace);
        key.extend_from_slice(&(name.len() as u16).to_be_bytes());
        key.extend_from_slice(name);
        key
    }
}

impl FromStr for Destination {
    type Err = DestinationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = value
            .split_once('/')
            .ok_or(DestinationError::InvalidDestination)?;
        if name.contains('/') {
            return Err(DestinationError::InvalidDestination);
        }
        Ok(Self::new(
            Namespace::new(namespace)?,
            DestinationName::new(name)?,
        ))
    }
}

impl TryFrom<String> for Destination {
    type Error = DestinationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Destination> for String {
    fn from(value: Destination) -> Self {
        value.to_string()
    }
}

impl fmt::Display for Destination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.name)
    }
}
