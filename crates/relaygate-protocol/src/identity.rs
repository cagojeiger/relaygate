use uuid::Uuid;

macro_rules! opaque_uuid {
    ($name:ident) => {
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
    };
}

opaque_uuid!(SessionId);
opaque_uuid!(BindingId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipeId {
    connector_session_id: SessionId,
    connection_id: u64,
}

impl PipeId {
    #[must_use]
    pub const fn new(connector_session_id: SessionId, connection_id: u64) -> Self {
        Self {
            connector_session_id,
            connection_id,
        }
    }

    #[must_use]
    pub const fn connector_session_id(self) -> SessionId {
        self.connector_session_id
    }

    #[must_use]
    pub const fn connection_id(self) -> u64 {
        self.connection_id
    }
}
