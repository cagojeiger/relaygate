use uuid::Uuid;

macro_rules! opaque_uuid {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

opaque_uuid!(
    /// Identifies one SDK-Gateway transport-session incarnation.
    ///
    /// Gateways issue a fresh UUIDv4 for every established session and the
    /// RelayGate cluster treats the value as globally unique.
    SessionId
);
opaque_uuid!(
    /// Identifies one live Listener binding.
    BindingId
);
/// Application-owned UUIDv4 logical routing address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DestinationId(Uuid);

impl DestinationId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub fn try_from_uuid(value: Uuid) -> Option<Self> {
        (value.get_version_num() == 4).then_some(Self(value))
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
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
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = Uuid::parse_str(value).map_err(|_| "DestinationId must be a UUID")?;
        Self::try_from_uuid(value).ok_or("DestinationId must be UUIDv4")
    }
}

/// Identifies one Pipe as its origin Relay session plus a session-local counter.
///
/// `connection_id` is monotonic only within its origin session. Combining
/// it with the cluster-unique session incarnation makes the `PipeId` globally
/// unique without exposing a Gateway identifier to the SDK wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipeId {
    origin_session_id: SessionId,
    connection_id: u64,
}

impl PipeId {
    #[must_use]
    pub const fn new(origin_session_id: SessionId, connection_id: u64) -> Self {
        Self {
            origin_session_id,
            connection_id,
        }
    }

    #[must_use]
    pub const fn origin_session_id(self) -> SessionId {
        self.origin_session_id
    }

    #[must_use]
    pub const fn connection_id(self) -> u64 {
        self.connection_id
    }
}
