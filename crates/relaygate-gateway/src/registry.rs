use std::collections::{HashMap, HashSet};

use relaygate_protocol::{BindingId, RouteAddress, SessionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) id: BindingId,
    pub(crate) address: RouteAddress,
    pub(crate) session_id: SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Registration {
    Created(Binding),
    Existing(Binding),
}

#[derive(Debug, Default)]
pub(crate) struct LocalRegistry {
    by_id: HashMap<BindingId, Binding>,
    by_address: HashMap<RouteAddress, Vec<BindingId>>,
    by_session: HashMap<SessionId, HashSet<BindingId>>,
    next_selection: HashMap<RouteAddress, usize>,
}

impl LocalRegistry {
    pub(crate) fn register(
        &mut self,
        session_id: SessionId,
        address: RouteAddress,
    ) -> Registration {
        if let Some(existing) = self.binding_for_session_address(session_id, &address) {
            return Registration::Existing(existing);
        }

        let binding = Binding {
            id: self.unique_binding_id(),
            address,
            session_id,
        };
        self.by_id.insert(binding.id, binding.clone());
        self.by_address
            .entry(binding.address.clone())
            .or_default()
            .push(binding.id);
        self.by_session
            .entry(binding.session_id)
            .or_default()
            .insert(binding.id);
        Registration::Created(binding)
    }

    pub(crate) fn remove_owned(
        &mut self,
        session_id: SessionId,
        binding_id: BindingId,
    ) -> Option<Binding> {
        let binding = self.by_id.get(&binding_id)?;
        if binding.session_id != session_id {
            return None;
        }
        self.remove(binding_id)
    }

    pub(crate) fn remove_session(&mut self, session_id: SessionId) -> Vec<Binding> {
        let ids = self
            .by_session
            .get(&session_id)
            .cloned()
            .unwrap_or_default();
        ids.into_iter().filter_map(|id| self.remove(id)).collect()
    }

    pub(crate) fn bindings_for_session(&self, session_id: SessionId) -> Vec<Binding> {
        let mut bindings = self
            .by_session
            .get(&session_id)
            .into_iter()
            .flatten()
            .filter_map(|id| self.by_id.get(id).cloned())
            .collect::<Vec<_>>();
        bindings.sort_unstable_by_key(|binding| binding.id.as_uuid());
        bindings
    }

    /// Returns the current binding only when every incarnation component
    /// matches. Remote OPEN admission uses this after RouteTable lookup so a
    /// stale projection cannot attach to a replacement Listener binding.
    pub(crate) fn exact(
        &self,
        session_id: SessionId,
        binding_id: BindingId,
        address: &RouteAddress,
    ) -> Option<Binding> {
        self.by_id
            .get(&binding_id)
            .filter(|binding| binding.session_id == session_id && &binding.address == address)
            .cloned()
    }

    pub(crate) fn select_excluding(
        &mut self,
        address: &RouteAddress,
        excluded_session: SessionId,
    ) -> Option<Binding> {
        let ids = self.by_address.get(address)?;
        let eligible = ids
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .filter(|binding| binding.session_id != excluded_session)
            .cloned()
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return None;
        }
        let cursor = self.next_selection.entry(address.clone()).or_default();
        let index = *cursor % eligible.len();
        *cursor = cursor.wrapping_add(1);
        eligible.get(index).cloned()
    }

    pub(crate) fn binding_count(&self) -> usize {
        self.by_id.len()
    }

    pub(crate) fn contains_session_address(
        &self,
        session_id: SessionId,
        address: &RouteAddress,
    ) -> bool {
        self.binding_for_session_address(session_id, address)
            .is_some()
    }

    #[cfg(test)]
    pub(crate) fn address_binding_count(&self, address: &RouteAddress) -> usize {
        self.by_address.get(address).map_or(0, Vec::len)
    }

    #[cfg(test)]
    pub(crate) fn session_binding_count(&self, session_id: SessionId) -> usize {
        self.by_session.get(&session_id).map_or(0, HashSet::len)
    }

    fn binding_for_session_address(
        &self,
        session_id: SessionId,
        address: &RouteAddress,
    ) -> Option<Binding> {
        self.by_session.get(&session_id).and_then(|ids| {
            ids.iter().find_map(|id| {
                self.by_id
                    .get(id)
                    .filter(|binding| &binding.address == address)
                    .cloned()
            })
        })
    }

    fn remove(&mut self, binding_id: BindingId) -> Option<Binding> {
        let binding = self.by_id.remove(&binding_id)?;

        let remove_address_index = if let Some(ids) = self.by_address.get_mut(&binding.address) {
            ids.retain(|id| *id != binding_id);
            ids.is_empty()
        } else {
            false
        };
        if remove_address_index {
            self.by_address.remove(&binding.address);
            self.next_selection.remove(&binding.address);
        }

        let remove_session_index = if let Some(ids) = self.by_session.get_mut(&binding.session_id) {
            ids.remove(&binding_id);
            ids.is_empty()
        } else {
            false
        };
        if remove_session_index {
            self.by_session.remove(&binding.session_id);
        }

        Some(binding)
    }

    fn unique_binding_id(&self) -> BindingId {
        loop {
            let candidate = BindingId::new();
            if !self.by_id.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalRegistry, Registration};
    use relaygate_protocol::{RouteAddress, SessionId};

    #[allow(clippy::expect_used)]
    fn address(destination: &str) -> RouteAddress {
        format!("test/{destination}")
            .parse()
            .expect("test RouteAddress must be valid")
    }

    fn binding_id(registration: Registration) -> relaygate_protocol::BindingId {
        match registration {
            Registration::Created(binding) | Registration::Existing(binding) => binding.id,
        }
    }

    #[test]
    fn indexes_describe_the_same_live_bindings() {
        let first_session = SessionId::new();
        let second_session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let shared = address("shared");
        let other = address("other");

        let first = binding_id(registry.register(first_session, shared.clone()));
        let second = binding_id(registry.register(second_session, shared.clone()));
        let third = binding_id(registry.register(first_session, other));

        assert_eq!(registry.binding_count(), 3);
        assert_eq!(registry.address_binding_count(&shared), 2);
        assert_eq!(registry.session_binding_count(first_session), 2);
        assert!(registry.remove_owned(first_session, first).is_some());
        assert_eq!(registry.binding_count(), 2);
        assert_eq!(registry.address_binding_count(&shared), 1);
        assert_eq!(registry.session_binding_count(first_session), 1);
        assert!(registry.remove_owned(first_session, second).is_none());
        assert!(registry.remove_owned(first_session, third).is_some());
    }

    #[test]
    fn unregister_then_register_renews_binding_id() {
        let session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let destination = address("renewed");
        let first = binding_id(registry.register(session, destination.clone()));
        let repeated = binding_id(registry.register(session, destination.clone()));

        assert_eq!(first, repeated);
        assert!(registry.remove_owned(session, first).is_some());
        let renewed = binding_id(registry.register(session, destination));
        assert_ne!(first, renewed);
    }

    #[test]
    fn many_bindings_are_selected_one_at_a_time() -> Result<(), &'static str> {
        let first_session = SessionId::new();
        let second_session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let shared = address("balanced");
        registry.register(first_session, shared.clone());
        registry.register(second_session, shared.clone());

        let caller = SessionId::new();
        let first = registry
            .select_excluding(&shared, caller)
            .ok_or("missing first")?;
        let second = registry
            .select_excluding(&shared, caller)
            .ok_or("missing second")?;

        assert_ne!(first.id, second.id);
        assert_eq!(registry.address_binding_count(&shared), 2);
        Ok(())
    }

    #[test]
    fn session_cleanup_removes_only_its_bindings() {
        let first_session = SessionId::new();
        let second_session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let shared = address("cleanup");
        registry.register(first_session, shared.clone());
        registry.register(first_session, address("other-cleanup"));
        registry.register(second_session, shared.clone());

        let removed = registry.remove_session(first_session);

        assert_eq!(removed.len(), 2);
        assert_eq!(registry.binding_count(), 1);
        assert_eq!(registry.address_binding_count(&shared), 1);
        assert_eq!(registry.session_binding_count(second_session), 1);
    }

    #[test]
    fn session_snapshot_contains_only_current_complete_bindings() {
        let first_session = SessionId::new();
        let second_session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let shared = address("snapshot");
        let first = binding_id(registry.register(first_session, shared.clone()));
        let second = binding_id(registry.register(first_session, address("snapshot-other")));
        registry.register(second_session, shared);

        let snapshot = registry.bindings_for_session(first_session);

        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.iter().any(|binding| binding.id == first));
        assert!(snapshot.iter().any(|binding| binding.id == second));
        assert!(registry.remove_owned(first_session, first).is_some());
        assert_eq!(
            registry
                .bindings_for_session(first_session)
                .into_iter()
                .map(|binding| binding.id)
                .collect::<Vec<_>>(),
            vec![second]
        );
        assert!(registry.bindings_for_session(SessionId::new()).is_empty());
    }

    #[test]
    fn exact_lookup_rejects_every_stale_identity_component() {
        let session = SessionId::new();
        let mut registry = LocalRegistry::default();
        let destination = address("exact");
        let binding = match registry.register(session, destination.clone()) {
            Registration::Created(binding) | Registration::Existing(binding) => binding,
        };

        assert_eq!(
            registry.exact(session, binding.id, &destination),
            Some(binding.clone())
        );
        assert!(
            registry
                .exact(SessionId::new(), binding.id, &destination)
                .is_none()
        );
        assert!(
            registry
                .exact(session, relaygate_protocol::BindingId::new(), &destination)
                .is_none()
        );
        assert!(
            registry
                .exact(session, binding.id, &address("other-exact"))
                .is_none()
        );
    }
}
