use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant},
};

use crate::{
    BindingSet, ClientId, LeaseId, MappingEntry, MappingIdentity, MappingSnapshot, RegistrationAck,
    RegistrationKey, RegistrationRevision, RequestContext, RouteTableError, RouteTableStats,
    ShardDirectory, ShardDirectoryGeneration, ShardId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTableConfig {
    lease_ttl: Duration,
}

impl RouteTableConfig {
    pub fn new(lease_ttl: Duration) -> Result<Self, RouteTableError> {
        if lease_ttl.is_zero() {
            return Err(RouteTableError::InvalidArgument(
                "lease TTL must be greater than zero".to_owned(),
            ));
        }
        Ok(Self { lease_ttl })
    }

    #[must_use]
    pub const fn lease_ttl(self) -> Duration {
        self.lease_ttl
    }
}

#[derive(Debug)]
struct RegistrationState {
    lease_id: LeaseId,
    revision: Option<RegistrationRevision>,
    deadline: Instant,
    mappings: BTreeMap<MappingIdentity, MappingEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpiryKey {
    registration_key: RegistrationKey,
    lease_id: LeaseId,
}

/// One READY, memory-only logical RouteTable shard.
#[derive(Debug)]
pub struct RouteTableShard {
    directory: ShardDirectory,
    shard_id: ShardId,
    config: RouteTableConfig,
    route_index: HashMap<ClientId, BTreeMap<MappingIdentity, MappingEntry>>,
    registration_index: HashMap<RegistrationKey, RegistrationState>,
    active_lease_ids: HashSet<LeaseId>,
    expiry_index: BTreeMap<Instant, BTreeSet<ExpiryKey>>,
}

impl RouteTableShard {
    pub fn new(
        directory: ShardDirectory,
        shard_id: ShardId,
        config: RouteTableConfig,
    ) -> Result<Self, RouteTableError> {
        if directory.shard(&shard_id).is_none() {
            return Err(RouteTableError::InvalidArgument(
                "RouteTable shard is not present in ShardDirectory".to_owned(),
            ));
        }
        Ok(Self {
            directory,
            shard_id,
            config,
            route_index: HashMap::new(),
            registration_index: HashMap::new(),
            active_lease_ids: HashSet::new(),
            expiry_index: BTreeMap::new(),
        })
    }

    #[must_use]
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    #[must_use]
    pub const fn generation(&self) -> ShardDirectoryGeneration {
        self.directory.generation()
    }

    #[must_use]
    pub fn stats(&self) -> RouteTableStats {
        RouteTableStats {
            registration_count: self.registration_index.len(),
            mapping_count: self
                .registration_index
                .values()
                .map(|registration| registration.mappings.len())
                .sum(),
            route_count: self.route_index.len(),
            expiry_record_count: self.expiry_index.values().map(BTreeSet::len).sum(),
        }
    }

    pub fn register(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: RegistrationKey,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, &key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(&key)?;
        self.expire_due(now);

        if let Some(registration) = self.registration_index.get(&key) {
            return Ok(Self::ack(registration, now));
        }

        let deadline = self.deadline_from(now)?;
        let lease_id = self.unique_lease_id();
        self.registration_index.insert(
            key.clone(),
            RegistrationState {
                lease_id,
                revision: None,
                deadline,
                mappings: BTreeMap::new(),
            },
        );
        self.active_lease_ids.insert(lease_id);
        self.insert_expiry(&key, lease_id, deadline);

        Ok(RegistrationAck::new(lease_id, None, self.config.lease_ttl))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: MappingSnapshot,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.validate_snapshot(key, &snapshot)?;
        self.expire_due(now);

        let registration = self.current_registration(key, lease_id)?;
        match registration.revision {
            None if revision != RegistrationRevision::FIRST => {
                return Err(RouteTableError::FailedPrecondition(
                    "the first accepted RegistrationRevision must be 1".to_owned(),
                ));
            }
            Some(current) if revision == current => {
                if registration.mappings == *snapshot.as_map() {
                    return Ok(Self::ack(registration, now));
                }
                return Err(RouteTableError::FailedPrecondition(
                    "the same RegistrationRevision has a different snapshot".to_owned(),
                ));
            }
            Some(current) if revision < current => {
                return Err(RouteTableError::FailedPrecondition(
                    "RegistrationRevision is lower than the accepted revision".to_owned(),
                ));
            }
            None | Some(_) => {}
        }
        Self::validate_current_mapping_identity_stability(registration, &snapshot)?;

        let new_mappings = snapshot.into_map();
        let (old_mappings, deadline) = {
            let registration = self
                .registration_index
                .get(key)
                .ok_or_else(|| RouteTableError::FailedPrecondition("unknown lease".to_owned()))?;
            (registration.mappings.clone(), registration.deadline)
        };

        self.remove_route_mappings(&old_mappings);
        self.insert_route_mappings(&new_mappings);
        let registration = self
            .registration_index
            .get_mut(key)
            .ok_or_else(|| RouteTableError::FailedPrecondition("unknown lease".to_owned()))?;
        registration.revision = Some(revision);
        registration.mappings = new_mappings;

        Ok(RegistrationAck::new(
            lease_id,
            Some(revision),
            deadline.saturating_duration_since(now),
        ))
    }

    pub fn keep_alive(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        now: Instant,
    ) -> Result<RegistrationAck, RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.expire_due(now);

        let (old_deadline, revision) = {
            let registration = self.current_registration(key, lease_id)?;
            (registration.deadline, registration.revision)
        };
        let new_deadline = self.deadline_from(now)?;
        self.remove_expiry(key, lease_id, old_deadline);
        self.insert_expiry(key, lease_id, new_deadline);
        let registration = self
            .registration_index
            .get_mut(key)
            .ok_or_else(|| RouteTableError::FailedPrecondition("unknown lease".to_owned()))?;
        registration.deadline = new_deadline;

        Ok(RegistrationAck::new(
            lease_id,
            revision,
            self.config.lease_ttl,
        ))
    }

    pub fn deregister(
        &mut self,
        context: RequestContext,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        now: Instant,
    ) -> Result<(), RouteTableError> {
        self.validate_authenticated_owner(context, key)?;
        self.validate_generation(generation)?;
        self.validate_registration_scope(key)?;
        self.expire_due(now);

        let Some(registration) = self.registration_index.get(key) else {
            return Ok(());
        };
        if registration.lease_id != lease_id {
            return Err(RouteTableError::FailedPrecondition(
                "LeaseId is not the current active lease".to_owned(),
            ));
        }
        self.remove_registration(key);
        Ok(())
    }

    pub fn resolve(
        &mut self,
        _context: RequestContext,
        generation: ShardDirectoryGeneration,
        client_id: &ClientId,
        now: Instant,
    ) -> Result<BindingSet, RouteTableError> {
        self.validate_generation(generation)?;
        self.validate_client_authority(client_id)?;
        self.expire_due(now);

        let mappings = self
            .route_index
            .get(client_id)
            .ok_or(RouteTableError::NotFound)?;
        if mappings.is_empty() {
            return Err(RouteTableError::NotFound);
        }
        Ok(BindingSet::new(mappings.values().cloned().collect()))
    }

    /// Returns the earliest active lease deadline, if one exists.
    ///
    /// Runtime adapters use this monotonic deadline to drive expiry even when
    /// no RouteTable request arrives. The deadline is operational scheduling
    /// state, not a stable identity and must not be sent over the wire.
    #[must_use]
    pub fn next_expiry_deadline(&self) -> Option<Instant> {
        self.expiry_index
            .first_key_value()
            .map(|(deadline, _)| *deadline)
    }

    /// Removes all registrations whose current deadline is at or before `now`.
    pub fn expire_due(&mut self, now: Instant) -> usize {
        let mut expired = 0;
        while self
            .expiry_index
            .first_key_value()
            .is_some_and(|(deadline, _)| *deadline <= now)
        {
            let Some((_, candidates)) = self.expiry_index.pop_first() else {
                break;
            };
            for candidate in candidates {
                let is_current = self
                    .registration_index
                    .get(&candidate.registration_key)
                    .is_some_and(|registration| {
                        registration.lease_id == candidate.lease_id && registration.deadline <= now
                    });
                if is_current {
                    self.remove_registration(&candidate.registration_key);
                    expired += 1;
                }
            }
        }
        expired
    }

    fn validate_generation(
        &self,
        generation: ShardDirectoryGeneration,
    ) -> Result<(), RouteTableError> {
        if generation != self.directory.generation() {
            return Err(RouteTableError::FailedPrecondition(
                "ShardDirectoryGeneration mismatch".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_authenticated_owner(
        &self,
        context: RequestContext,
        key: &RegistrationKey,
    ) -> Result<(), RouteTableError> {
        if context.authenticated_gateway_id().gateway_id() != key.gateway_id() {
            return Err(RouteTableError::PermissionDenied);
        }
        Ok(())
    }

    fn validate_registration_scope(&self, key: &RegistrationKey) -> Result<(), RouteTableError> {
        if key.shard_id() != &self.shard_id {
            return Err(RouteTableError::InvalidArgument(
                "RegistrationKey targets a different shard".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_snapshot(
        &self,
        key: &RegistrationKey,
        snapshot: &MappingSnapshot,
    ) -> Result<(), RouteTableError> {
        for mapping in snapshot.entries() {
            let identity = mapping.identity();
            if identity.gateway_id() != key.gateway_id()
                || identity.listener_session_id() != key.listener_session_id()
            {
                return Err(RouteTableError::InvalidArgument(
                    "snapshot mapping is outside the RegistrationKey scope".to_owned(),
                ));
            }
            self.validate_client_authority(mapping.client_id())?;
        }
        Ok(())
    }

    fn validate_current_mapping_identity_stability(
        registration: &RegistrationState,
        snapshot: &MappingSnapshot,
    ) -> Result<(), RouteTableError> {
        for (identity, next) in snapshot.as_map() {
            if registration
                .mappings
                .get(identity)
                .is_some_and(|current| current != next)
            {
                return Err(RouteTableError::FailedPrecondition(
                    "an active MappingIdentity cannot change ClientId or GatewayLocator".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_client_authority(&self, client_id: &ClientId) -> Result<(), RouteTableError> {
        if self.directory.authority(client_id).id() != &self.shard_id {
            return Err(RouteTableError::InvalidArgument(
                "ClientId belongs to a different authority shard".to_owned(),
            ));
        }
        Ok(())
    }

    fn current_registration(
        &self,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<&RegistrationState, RouteTableError> {
        let registration = self.registration_index.get(key).ok_or_else(|| {
            RouteTableError::FailedPrecondition("registration lease is not active".to_owned())
        })?;
        if registration.lease_id != lease_id {
            return Err(RouteTableError::FailedPrecondition(
                "LeaseId is not the current active lease".to_owned(),
            ));
        }
        Ok(registration)
    }

    fn deadline_from(&self, now: Instant) -> Result<Instant, RouteTableError> {
        now.checked_add(self.config.lease_ttl)
            .ok_or(RouteTableError::DeadlineOverflow)
    }

    fn unique_lease_id(&self) -> LeaseId {
        loop {
            let candidate = LeaseId::new();
            if !self.active_lease_ids.contains(&candidate) {
                return candidate;
            }
        }
    }

    fn ack(registration: &RegistrationState, now: Instant) -> RegistrationAck {
        RegistrationAck::new(
            registration.lease_id,
            registration.revision,
            registration.deadline.saturating_duration_since(now),
        )
    }

    fn insert_expiry(&mut self, key: &RegistrationKey, lease_id: LeaseId, deadline: Instant) {
        self.expiry_index
            .entry(deadline)
            .or_default()
            .insert(ExpiryKey {
                registration_key: key.clone(),
                lease_id,
            });
    }

    fn remove_expiry(&mut self, key: &RegistrationKey, lease_id: LeaseId, deadline: Instant) {
        let expiry_key = ExpiryKey {
            registration_key: key.clone(),
            lease_id,
        };
        let remove_bucket = if let Some(bucket) = self.expiry_index.get_mut(&deadline) {
            bucket.remove(&expiry_key);
            bucket.is_empty()
        } else {
            false
        };
        if remove_bucket {
            self.expiry_index.remove(&deadline);
        }
    }

    fn remove_registration(&mut self, key: &RegistrationKey) {
        let Some(registration) = self.registration_index.remove(key) else {
            return;
        };
        self.active_lease_ids.remove(&registration.lease_id);
        self.remove_expiry(key, registration.lease_id, registration.deadline);
        self.remove_route_mappings(&registration.mappings);
    }

    fn insert_route_mappings(&mut self, mappings: &BTreeMap<MappingIdentity, MappingEntry>) {
        for (identity, mapping) in mappings {
            self.route_index
                .entry(mapping.client_id().clone())
                .or_default()
                .insert(*identity, mapping.clone());
        }
    }

    fn remove_route_mappings(&mut self, mappings: &BTreeMap<MappingIdentity, MappingEntry>) {
        for (identity, mapping) in mappings {
            let remove_route = if let Some(route) = self.route_index.get_mut(mapping.client_id()) {
                route.remove(identity);
                route.is_empty()
            } else {
                false
            };
            if remove_route {
                self.route_index.remove(mapping.client_id());
            }
        }
    }
}
