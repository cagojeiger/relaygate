use std::collections::BTreeMap;

use relaygate_protocol::{BindingId as ProtocolBindingId, SessionId};
use relaygate_route_table::{
    BindingId, DestinationId, GatewayId, GatewayLocator, MappingEntry, MappingSnapshot,
    RelaySessionId, ShardDirectory, ShardId,
};

use crate::registry::Binding;

use super::RoutingError;

#[derive(Debug, Clone)]
pub(super) struct ProjectedShardSnapshot {
    pub(super) shard_id: ShardId,
    pub(super) snapshot: Option<MappingSnapshot>,
}

/// Projects one complete local RelaySession snapshot into every configured
/// shard. An empty shard subset is explicit because it removes prior state.
pub(super) fn project_session(
    directory: &ShardDirectory,
    gateway_id: GatewayId,
    gateway_locator: &GatewayLocator,
    session_id: SessionId,
    bindings: Vec<Binding>,
) -> Result<Vec<ProjectedShardSnapshot>, RoutingError> {
    let relay_session_id = project_session_id(session_id);
    let mut by_shard = directory
        .shards()
        .iter()
        .map(|record| (record.id().clone(), Vec::new()))
        .collect::<BTreeMap<_, _>>();

    for binding in bindings {
        if binding.session_id != session_id {
            return Err(RoutingError::InvalidProjection(
                "binding belongs to a different RelaySession".to_owned(),
            ));
        }
        let destination_id = DestinationId::new(binding.destination_id.to_string())?;
        let shard_id = directory.authority(&destination_id).id();
        let entries = by_shard.get_mut(shard_id).ok_or_else(|| {
            RoutingError::InvalidProjection("authority shard is absent from directory".to_owned())
        })?;
        entries.push(MappingEntry::new(
            destination_id,
            gateway_id,
            relay_session_id,
            project_binding_id(binding.id),
            gateway_locator.clone(),
        ));
    }

    by_shard
        .into_iter()
        .map(|(shard_id, entries)| {
            let snapshot = if entries.is_empty() {
                None
            } else {
                Some(MappingSnapshot::new(entries)?)
            };
            Ok(ProjectedShardSnapshot { shard_id, snapshot })
        })
        .collect()
}

#[must_use]
pub(super) const fn project_session_id(value: SessionId) -> RelaySessionId {
    RelaySessionId::from_uuid(value.as_uuid())
}

#[must_use]
pub(super) const fn project_binding_id(value: ProtocolBindingId) -> BindingId {
    BindingId::from_uuid(value.as_uuid())
}
