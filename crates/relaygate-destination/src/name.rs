use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::DestinationError;

/// Maximum encoded length of one namespace or destination-name label.
pub const MAX_LABEL_BYTES: usize = 63;
/// Maximum encoded length of a complete [`DestinationName`].
pub const MAX_DESTINATION_NAME_BYTES: usize = 253;

/// A validated single-label tenant or application routing namespace.
///
/// Namespaces contain lowercase ASCII letters, digits, and interior hyphens.
/// Unlike [`DestinationName`], a namespace cannot contain dots.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Namespace(Box<str>);

impl Namespace {
    /// Validates and constructs a namespace.
    pub fn new(value: &str) -> Result<Self, DestinationError> {
        if value.contains('.') {
            return Err(DestinationError::InvalidNamespace);
        }
        validate_label(value)?;
        Ok(Self(value.into()))
    }

    #[must_use]
    /// Returns the validated namespace text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Namespace {
    type Err = DestinationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for Namespace {
    type Error = DestinationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl From<Namespace> for String {
    fn from(value: Namespace) -> Self {
        value.0.into_string()
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A validated dot-separated destination name within a [`Namespace`].
///
/// Each label contains lowercase ASCII letters, digits, and interior hyphens.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DestinationName(Box<str>);

impl DestinationName {
    /// Validates and constructs a destination name.
    pub fn new(value: &str) -> Result<Self, DestinationError> {
        if value.len() > MAX_DESTINATION_NAME_BYTES {
            return Err(DestinationError::DestinationNameLength);
        }
        for label in value.split('.') {
            validate_label(label)?;
        }
        Ok(Self(value.into()))
    }

    #[must_use]
    /// Returns the validated dot-separated destination name.
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
    type Err = DestinationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for DestinationName {
    type Error = DestinationError;

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

fn validate_label(value: &str) -> Result<(), DestinationError> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_LABEL_BYTES {
        return Err(DestinationError::LabelLength);
    }

    let is_alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !bytes.first().is_some_and(is_alphanumeric) || !bytes.last().is_some_and(is_alphanumeric) {
        return Err(DestinationError::LabelBoundary);
    }
    if !bytes
        .iter()
        .all(|byte| is_alphanumeric(byte) || *byte == b'-')
    {
        return Err(DestinationError::LabelCharacter);
    }
    Ok(())
}
