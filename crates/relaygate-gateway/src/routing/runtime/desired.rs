use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

use relaygate_route_table::{MappingSnapshot, RelaySessionId, ShardId};

use super::super::{RoutingError, projection::ProjectedShardSnapshot};

#[derive(Debug, Clone)]
struct DesiredShardEntry {
    version: u64,
    snapshot: MappingSnapshot,
}

#[derive(Debug, Default)]
struct DesiredState {
    version: u64,
    by_shard: BTreeMap<ShardId, HashMap<RelaySessionId, DesiredShardEntry>>,
}

#[derive(Debug, Default)]
pub(super) struct DesiredStore(RwLock<DesiredState>);

impl DesiredStore {
    pub(super) fn commit(
        &self,
        session_id: RelaySessionId,
        projected: Vec<ProjectedShardSnapshot>,
    ) -> Result<u64, RoutingError> {
        let mut state = self.0.write().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        let version = state.version.checked_add(1).ok_or_else(|| {
            RoutingError::WorkerFailed("routing desired version exhausted".to_owned())
        })?;
        state.version = version;
        for projected in projected {
            let shard = state.by_shard.entry(projected.shard_id).or_default();
            if let Some(snapshot) = projected.snapshot {
                shard.insert(session_id, DesiredShardEntry { version, snapshot });
            } else {
                shard.remove(&session_id);
            }
        }
        Ok(version)
    }

    pub(super) fn shard_view_after(
        &self,
        shard_id: &ShardId,
        observed_version: u64,
    ) -> Result<Option<ShardDesiredView>, RoutingError> {
        let state = self.0.read().map_err(|_| {
            RoutingError::WorkerFailed("routing desired state lock is poisoned".to_owned())
        })?;
        if state.version <= observed_version {
            return Ok(None);
        }
        let sessions = state
            .by_shard
            .get(shard_id)
            .into_iter()
            .flatten()
            .map(|(session_id, desired)| {
                (
                    *session_id,
                    (desired.version, Some(desired.snapshot.clone())),
                )
            })
            .collect();
        Ok(Some(ShardDesiredView {
            store_version: state.version,
            sessions,
        }))
    }
}

pub(super) struct ShardDesiredView {
    pub(super) store_version: u64,
    pub(super) sessions: HashMap<RelaySessionId, (u64, Option<MappingSnapshot>)>,
}
