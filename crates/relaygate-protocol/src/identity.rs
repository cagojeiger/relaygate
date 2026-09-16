use uuid::Uuid;

macro_rules! opaque_uuid {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates an identifier from a freshly generated UUIDv4.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID, such as one decoded from the wire.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the wrapped UUID for wire encoding or comparison.
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
    /// Identifies one live route binding.
    BindingId
);
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
    /// Creates an identifier from its origin session and connection counter.
    #[must_use]
    pub const fn new(origin_session_id: SessionId, connection_id: u64) -> Self {
        Self {
            origin_session_id,
            connection_id,
        }
    }

    /// Returns the session incarnation that originated the dial.
    #[must_use]
    pub const fn origin_session_id(self) -> SessionId {
        self.origin_session_id
    }

    /// Returns the counter the origin session assigned to the dial.
    #[must_use]
    pub const fn connection_id(self) -> u64 {
        self.connection_id
    }
}
