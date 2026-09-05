use relaygate_protocol::DestinationId as WireDestinationId;
use uuid::Uuid;

/// Application-owned UUIDv4 routing address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DestinationId(WireDestinationId);

impl DestinationId {
    /// Creates a new random UUIDv4 Destination.
    #[must_use]
    pub fn new() -> Self {
        Self(WireDestinationId::new())
    }

    pub fn from_uuid(value: Uuid) -> Result<Self, DestinationIdError> {
        WireDestinationId::try_from_uuid(value)
            .map(Self)
            .ok_or(DestinationIdError::NotVersion4)
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0.as_uuid()
    }

    pub(crate) const fn to_wire(self) -> WireDestinationId {
        self.0
    }

    pub(crate) const fn from_wire(value: WireDestinationId) -> Self {
        Self(value)
    }
}

impl Default for DestinationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DestinationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for DestinationId {
    type Err = DestinationIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = Uuid::parse_str(value).map_err(DestinationIdError::InvalidUuid)?;
        Self::from_uuid(value)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DestinationIdError {
    #[error("DestinationId is not a UUID: {0}")]
    InvalidUuid(uuid::Error),
    #[error("DestinationId must be UUIDv4")]
    NotVersion4,
}
