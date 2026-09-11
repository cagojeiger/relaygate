use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::AddressError;

pub const MAX_LABEL_BYTES: usize = 63;
pub const MAX_DESTINATION_BYTES: usize = 253;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NamespaceId(Box<str>);

impl NamespaceId {
    pub fn new(value: &str) -> Result<Self, AddressError> {
        if value.contains('.') {
            return Err(AddressError::InvalidNamespace);
        }
        validate_label(value)?;
        Ok(Self(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for NamespaceId {
    type Err = AddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for NamespaceId {
    type Error = AddressError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl From<NamespaceId> for String {
    fn from(value: NamespaceId) -> Self {
        value.0.into_string()
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DestinationName(Box<str>);

impl DestinationName {
    pub fn new(value: &str) -> Result<Self, AddressError> {
        if value.len() > MAX_DESTINATION_BYTES {
            return Err(AddressError::DestinationLength);
        }
        for label in value.split('.') {
            validate_label(label)?;
        }
        Ok(Self(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this name is below `prefix` on a whole-label boundary.
    /// The prefix itself is not its descendant.
    #[must_use]
    pub fn is_descendant_of(&self, prefix: &Self) -> bool {
        self.as_str()
            .strip_prefix(prefix.as_str())
            .is_some_and(|suffix| suffix.starts_with('.'))
    }
}

impl FromStr for DestinationName {
    type Err = AddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for DestinationName {
    type Error = AddressError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl From<DestinationName> for String {
    fn from(value: DestinationName) -> Self {
        value.0.into_string()
    }
}

impl fmt::Display for DestinationName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn validate_label(value: &str) -> Result<(), AddressError> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_LABEL_BYTES {
        return Err(AddressError::LabelLength);
    }

    let is_alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !bytes.first().is_some_and(is_alphanumeric) || !bytes.last().is_some_and(is_alphanumeric) {
        return Err(AddressError::LabelBoundary);
    }
    if !bytes
        .iter()
        .all(|byte| is_alphanumeric(byte) || *byte == b'-')
    {
        return Err(AddressError::LabelCharacter);
    }
    Ok(())
}
