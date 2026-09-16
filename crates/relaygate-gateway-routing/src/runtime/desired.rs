use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use relaygate_route_table::{BindingSnapshot, RelaySessionId, ShardId};

use super::super::{RoutingError, projection::ProjectedShardSnapshot};

#[derive(Debug, Clone)]
struct DesiredShardEntry {
    version: u64,
    snapshot: BindingSnapshot,
}

#[derive(Debug, Default)]
struct DesiredShard {
    sessions: HashMap<RelaySessionId, DesiredShardEntry>,
    // Sessions removed from this shard since the shard worker last looked,
    // keyed to the version that removed them. Drained by `shard_view_after`.
    removed: HashMap<RelaySessionId, u64>,
}

#[derive(Debug, Default)]
struct DesiredState {
    version: u64,
    by_shard: BTreeMap<ShardId, DesiredShard>,
}

#[derive(Debug, Default)]
pub(super) struct DesiredStore {
    // Mirrors `DesiredState::version` so the shard workers' scan tick can
    // skip the lock when nothing was committed.
    version: AtomicU64,
    state: RwLock<DesiredState>,
}

impl DesiredStore {
    pub(super) fn commit(
        &self,
        session_id: RelaySessionId,
        projected: Vec<ProjectedShardSnapshot>,
    ) -> Result<u64, RoutingError> {
        let mut state = self.state.write().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        let version = state.version.checked_add(1).ok_or_else(|| {
            RoutingError::WorkerFailed("routing desired version exhausted".to_owned())
        })?;
        state.version = version;
        self.version.store(version, Ordering::Release);
        for projected in projected {
            let shard = state.by_shard.entry(projected.shard_id).or_default();
            if let Some(snapshot) = projected.snapshot {
                shard.removed.remove(&session_id);
                shard
                    .sessions
                    .insert(session_id, DesiredShardEntry { version, snapshot });
            } else if shard.sessions.remove(&session_id).is_some() {
                shard.removed.insert(session_id, version);
            }
        }
        Ok(version)
    }

    /// Returns the sessions whose desired state changed after `observed_version`,
    /// or `None` when nothing was committed since. Each shard has exactly one
    /// worker, so the removals handed out here are dropped from the store.
    pub(super) fn shard_view_after(
        &self,
        shard_id: &ShardId,
        observed_version: u64,
    ) -> Result<Option<ShardDesiredView>, RoutingError> {
        if self.version.load(Ordering::Acquire) <= observed_version {
            return Ok(None);
        }
        let mut state = self.state.write().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        if state.version <= observed_version {
            return Ok(None);
        }
        let store_version = state.version;
        let Some(shard) = state.by_shard.get_mut(shard_id) else {
            return Ok(Some(ShardDesiredView {
                store_version,
                changed: Vec::new(),
                removed: Vec::new(),
            }));
        };
        let changed = shard
            .sessions
            .iter()
            .filter(|(_, desired)| desired.version > observed_version)
            .map(|(session_id, desired)| (*session_id, desired.version, desired.snapshot.clone()))
            .collect();
        let removed = shard.removed.drain().collect();
        Ok(Some(ShardDesiredView {
            store_version,
            changed,
            removed,
        }))
    }
}

pub(super) struct ShardDesiredView {
    pub(super) store_version: u64,
    pub(super) changed: Vec<(RelaySessionId, u64, BindingSnapshot)>,
    pub(super) removed: Vec<(RelaySessionId, u64)>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use relaygate_route_table::{
        BindingId, BindingProjection, BindingSnapshot, Destination, GatewayId, GatewayLocator,
        RelaySessionId, ShardId,
    };
    use uuid::Uuid;

    use super::{DesiredStore, ProjectedShardSnapshot, ShardDesiredView};

    type TestResult = Result<(), Box<dyn Error>>;

    fn shard(name: &str) -> Result<ShardId, Box<dyn Error>> {
        Ok(ShardId::new(name)?)
    }

    fn present(shard_id: &ShardId) -> Result<ProjectedShardSnapshot, Box<dyn Error>> {
        let binding = BindingProjection::new(
            "test/echo".parse::<Destination>()?,
            GatewayId::from_uuid(Uuid::from_u128(1)),
            RelaySessionId::from_uuid(Uuid::from_u128(2)),
            BindingId::from_uuid(Uuid::from_u128(3)),
            GatewayLocator::new("gw-a.internal:27431")?,
        );
        Ok(ProjectedShardSnapshot {
            shard_id: shard_id.clone(),
            snapshot: Some(BindingSnapshot::new([binding])?),
        })
    }

    fn absent(shard_id: &ShardId) -> ProjectedShardSnapshot {
        ProjectedShardSnapshot {
            shard_id: shard_id.clone(),
            snapshot: None,
        }
    }

    fn view_after(
        store: &DesiredStore,
        shard_id: &ShardId,
        observed: u64,
    ) -> Result<ShardDesiredView, Box<dyn Error>> {
        Ok(store
            .shard_view_after(shard_id, observed)?
            .ok_or("expected a view after the observed version")?)
    }

    #[test]
    fn view_reports_only_sessions_changed_after_the_observed_version() -> TestResult {
        let store = DesiredStore::default();
        let shard_id = shard("rt-0")?;
        let first = RelaySessionId::new();
        let second = RelaySessionId::new();

        let v1 = store.commit(first, vec![present(&shard_id)?])?;
        let v2 = store.commit(second, vec![present(&shard_id)?])?;

        let view = view_after(&store, &shard_id, v1)?;
        assert_eq!(view.store_version, v2);
        assert_eq!(
            view.changed
                .iter()
                .map(|(session_id, version, _)| (*session_id, *version))
                .collect::<Vec<_>>(),
            vec![(second, v2)]
        );
        assert!(view.removed.is_empty());
        assert!(store.shard_view_after(&shard_id, v2)?.is_none());
        Ok(())
    }

    #[test]
    fn removals_are_reported_once_with_their_version() -> TestResult {
        let store = DesiredStore::default();
        let shard_id = shard("rt-0")?;
        let session = RelaySessionId::new();

        let v1 = store.commit(session, vec![present(&shard_id)?])?;
        let v2 = store.commit(session, vec![absent(&shard_id)])?;

        let view = view_after(&store, &shard_id, v1)?;
        assert!(view.changed.is_empty());
        assert_eq!(view.removed, vec![(session, v2)]);

        let v3 = store.commit(RelaySessionId::new(), vec![absent(&shard("rt-1")?)])?;
        let view = view_after(&store, &shard_id, v2)?;
        assert_eq!(view.store_version, v3);
        assert!(view.changed.is_empty());
        assert!(
            view.removed.is_empty(),
            "removal must not be reported twice"
        );
        Ok(())
    }

    #[test]
    fn re_adding_a_removed_session_cancels_the_pending_removal() -> TestResult {
        let store = DesiredStore::default();
        let shard_id = shard("rt-0")?;
        let session = RelaySessionId::new();

        store.commit(session, vec![present(&shard_id)?])?;
        store.commit(session, vec![absent(&shard_id)])?;
        let v3 = store.commit(session, vec![present(&shard_id)?])?;

        let view = view_after(&store, &shard_id, 0)?;
        assert_eq!(view.changed.len(), 1);
        assert_eq!(view.changed[0].1, v3);
        assert!(view.removed.is_empty());
        Ok(())
    }

    #[test]
    fn removing_a_session_the_shard_never_held_records_nothing() -> TestResult {
        let store = DesiredStore::default();
        let shard_id = shard("rt-0")?;

        let v1 = store.commit(RelaySessionId::new(), vec![absent(&shard_id)])?;
        let view = view_after(&store, &shard_id, 0)?;
        assert_eq!(view.store_version, v1);
        assert!(view.changed.is_empty());
        assert!(view.removed.is_empty());
        Ok(())
    }
}
