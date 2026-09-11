use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use crate::{
    BindingId, Destination, GatewayId, GatewayLocator, LeaseId, RegistrationRevision,
    RelaySessionId, RouteTableError, ShardId,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegistrationKey {
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
    shard_id: ShardId,
}

impl RegistrationKey {
    #[must_use]
    pub const fn new(
        gateway_id: GatewayId,
        relay_session_id: RelaySessionId,
        shard_id: ShardId,
    ) -> Self {
        Self {
            gateway_id,
            relay_session_id,
            shard_id,
        }
    }

    #[must_use]
    pub const fn gateway_id(&self) -> GatewayId {
        self.gateway_id
    }

    #[must_use]
    pub const fn relay_session_id(&self) -> RelaySessionId {
        self.relay_session_id
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindingIdentity {
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
    binding_id: BindingId,
}

impl BindingIdentity {
    #[must_use]
    pub const fn new(
        gateway_id: GatewayId,
        relay_session_id: RelaySessionId,
        binding_id: BindingId,
    ) -> Self {
        Self {
            gateway_id,
            relay_session_id,
            binding_id,
        }
    }

    #[must_use]
    pub const fn gateway_id(self) -> GatewayId {
        self.gateway_id
    }

    #[must_use]
    pub const fn relay_session_id(self) -> RelaySessionId {
        self.relay_session_id
    }

    #[must_use]
    pub const fn binding_id(self) -> BindingId {
        self.binding_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingProjection {
    destination: Destination,
    identity: BindingIdentity,
    gateway_locator: GatewayLocator,
}

impl BindingProjection {
    #[must_use]
    pub const fn new(
        destination: Destination,
        gateway_id: GatewayId,
        relay_session_id: RelaySessionId,
        binding_id: BindingId,
        gateway_locator: GatewayLocator,
    ) -> Self {
        Self {
            destination,
            identity: BindingIdentity::new(gateway_id, relay_session_id, binding_id),
            gateway_locator,
        }
    }

    #[must_use]
    pub fn destination(&self) -> &Destination {
        &self.destination
    }

    #[must_use]
    pub const fn identity(&self) -> BindingIdentity {
        self.identity
    }

    #[must_use]
    pub fn gateway_locator(&self) -> &GatewayLocator {
        &self.gateway_locator
    }
}

/// A non-empty, complete current binding snapshot for one registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingSnapshot {
    entries: BTreeMap<BindingIdentity, BindingProjection>,
}

impl BindingSnapshot {
    pub fn new(
        entries: impl IntoIterator<Item = BindingProjection>,
    ) -> Result<Self, RouteTableError> {
        let mut by_identity = BTreeMap::new();
        let mut by_session_destination = std::collections::HashSet::new();

        for entry in entries {
            let identity = entry.identity();
            let session_destination = (
                identity.gateway_id(),
                identity.relay_session_id(),
                entry.destination().clone(),
            );
            if !by_session_destination.insert(session_destination) {
                return Err(RouteTableError::InvalidArgument(
                    "snapshot contains duplicate Destination scope for one RelaySession".to_owned(),
                ));
            }
            if by_identity.insert(identity, entry).is_some() {
                return Err(RouteTableError::InvalidArgument(
                    "snapshot contains a duplicate BindingIdentity".to_owned(),
                ));
            }
        }

        if by_identity.is_empty() {
            return Err(RouteTableError::InvalidArgument(
                "Update snapshot must contain at least one binding".to_owned(),
            ));
        }
        Ok(Self {
            entries: by_identity,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = &BindingProjection> {
        self.entries.values()
    }

    pub(crate) fn as_map(&self) -> &BTreeMap<BindingIdentity, BindingProjection> {
        &self.entries
    }

    pub(crate) fn into_map(self) -> BTreeMap<BindingIdentity, BindingProjection> {
        self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingSet {
    entries: Vec<BindingProjection>,
}

impl BindingSet {
    pub(crate) fn new(entries: Vec<BindingProjection>) -> Self {
        Self { entries }
    }

    /// Reconstructs a Resolve result at a validated transport boundary.
    pub fn from_entries(entries: Vec<BindingProjection>) -> Result<Self, RouteTableError> {
        let Some(first) = entries.first() else {
            return Err(RouteTableError::InvalidArgument(
                "BindingSet must contain at least one binding".to_owned(),
            ));
        };
        let destination = first.destination();
        let mut identities = HashSet::with_capacity(entries.len());
        for entry in &entries {
            if entry.destination() != destination {
                return Err(RouteTableError::InvalidArgument(
                    "BindingSet bindings must share one Destination".to_owned(),
                ));
            }
            if !identities.insert(entry.identity()) {
                return Err(RouteTableError::InvalidArgument(
                    "BindingSet contains a duplicate BindingIdentity".to_owned(),
                ));
            }
        }
        Ok(Self { entries })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the current entries. Their order has no routing meaning.
    #[must_use]
    pub fn entries(&self) -> &[BindingProjection] {
        &self.entries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationAck {
    lease_id: LeaseId,
    accepted_revision: Option<RegistrationRevision>,
    expires_in: Duration,
}

impl RegistrationAck {
    pub(crate) const fn new(
        lease_id: LeaseId,
        accepted_revision: Option<RegistrationRevision>,
        expires_in: Duration,
    ) -> Self {
        Self {
            lease_id,
            accepted_revision,
            expires_in,
        }
    }

    /// Reconstructs an acknowledgement at a validated transport boundary.
    #[must_use]
    pub const fn from_parts(
        lease_id: LeaseId,
        accepted_revision: Option<RegistrationRevision>,
        expires_in: Duration,
    ) -> Self {
        Self::new(lease_id, accepted_revision, expires_in)
    }

    #[must_use]
    pub const fn lease_id(self) -> LeaseId {
        self.lease_id
    }

    #[must_use]
    pub const fn accepted_revision(self) -> Option<RegistrationRevision> {
        self.accepted_revision
    }

    #[must_use]
    pub const fn expires_in(self) -> Duration {
        self.expires_in
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTableStats {
    pub registration_count: usize,
    pub binding_count: usize,
    pub destination_count: usize,
    pub expiry_record_count: usize,
}
