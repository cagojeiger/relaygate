//! Per-shard registration reconciliation and lifecycle preparation. The
//! registration pass deliberately combines pruning, operation selection,
//! deadline discovery, and counts in one traversal.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use relaygate_route_table::{
    GatewayId, RegistrationKey, RelaySessionId, ShardDirectoryGeneration, ShardId,
};
use relaygate_route_table_transport::RouteTableClient;
use tokio::time::Instant;

use super::{
    super::{
        RoutingError,
        lifecycle::{OperationTicket, RegistrationPhase, RegistrationState},
    },
    desired::DesiredStore,
};

#[derive(Debug, Default)]
pub(super) struct WorkerCounts {
    pub(super) synced: AtomicUsize,
    pub(super) unsynced: AtomicUsize,
    pub(super) terminal: AtomicUsize,
}

impl WorkerCounts {
    pub(super) fn update(&self, registrations: &BTreeMap<RelaySessionId, RegistrationState>) {
        let counts = registrations
            .values()
            .filter(|state| state.is_desired())
            .fold(RegistrationCounts::default(), |mut counts, state| {
                counts.observe(state);
                counts
            });
        self.store(counts);
    }

    pub(super) fn clear(&self) {
        self.synced.store(0, Ordering::Relaxed);
        self.unsynced.store(0, Ordering::Relaxed);
        self.terminal.store(0, Ordering::Relaxed);
    }

    fn store(&self, counts: RegistrationCounts) {
        self.synced.store(counts.synced, Ordering::Relaxed);
        self.unsynced.store(counts.unsynced, Ordering::Relaxed);
        self.terminal.store(counts.terminal, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Default)]
struct RegistrationCounts {
    synced: usize,
    unsynced: usize,
    terminal: usize,
}

impl RegistrationCounts {
    fn observe(&mut self, state: &RegistrationState) {
        if !state.is_desired() {
            return;
        }
        match state.phase() {
            RegistrationPhase::Synced => self.synced += 1,
            RegistrationPhase::Terminal => {
                self.unsynced += 1;
                self.terminal += 1;
            }
            RegistrationPhase::Registering
            | RegistrationPhase::Leased
            | RegistrationPhase::Unsynced
            | RegistrationPhase::Deregistering
            | RegistrationPhase::Removed => self.unsynced += 1,
        }
    }
}

pub(super) struct RegistrationPass {
    pub(super) ticket: Option<OperationTicket>,
    pub(super) deadline: Option<Instant>,
    counts: RegistrationCounts,
    #[cfg(test)]
    visited: usize,
}

impl RegistrationPass {
    pub(super) fn update_counts(&self, counts: &WorkerCounts) {
        counts.store(self.counts);
    }
}

pub(super) fn reconcile_desired(
    desired: &DesiredStore,
    shard_id: &ShardId,
    gateway_id: GatewayId,
    (retry_initial, retry_max): (Duration, Duration),
    registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
    observed_version: &mut u64,
    now: Instant,
) -> Result<(), RoutingError> {
    let Some(view) = desired.shard_view_after(shard_id, *observed_version)? else {
        return Ok(());
    };
    *observed_version = view.store_version;
    for (session_id, version, snapshot) in view.changed {
        match registrations.entry(session_id) {
            Entry::Occupied(mut entry) => entry.get_mut().publish(version, Some(snapshot), now),
            Entry::Vacant(entry) => {
                let key = RegistrationKey::new(gateway_id, session_id, shard_id.clone());
                entry.insert(RegistrationState::new(
                    key,
                    version,
                    Some(snapshot),
                    now,
                    retry_initial,
                    retry_max,
                ));
            }
        }
    }
    for (session_id, version) in view.removed {
        if let Some(state) = registrations.get_mut(&session_id) {
            state.publish(version, None, now);
        }
    }
    Ok(())
}

pub(super) fn prepare_registration_pass(
    registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
    now: Instant,
) -> Result<RegistrationPass, &'static str> {
    let mut pass = RegistrationPass {
        ticket: None,
        deadline: None,
        counts: RegistrationCounts::default(),
        #[cfg(test)]
        visited: 0,
    };
    let mut error = None;
    registrations.retain(|_, state| {
        #[cfg(test)]
        {
            pass.visited += 1;
        }
        if state.is_removable() {
            return false;
        }
        if error.is_none() && pass.ticket.is_none() {
            match state.begin_next(now) {
                Ok(ticket) => pass.ticket = ticket,
                Err(message) => error = Some(message),
            }
        }
        pass.counts.observe(state);
        if pass.ticket.is_none() {
            pass.deadline = match (pass.deadline, state.next_deadline()) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (None, candidate) => candidate,
                (current, None) => current,
            };
        }
        true
    });
    if let Some(message) = error {
        return Err(message);
    }
    if pass.ticket.is_some() {
        pass.deadline = None;
    }
    Ok(pass)
}

pub(super) fn mark_connection_lost(
    registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
    now: Instant,
) {
    for state in registrations.values_mut() {
        state.connection_lost(now);
    }
}

pub(super) fn mark_all_terminal(registrations: &mut BTreeMap<RelaySessionId, RegistrationState>) {
    for state in registrations.values_mut() {
        state.mark_terminal();
    }
}

pub(super) async fn best_effort_deregister(
    client: &RouteTableClient,
    generation: ShardDirectoryGeneration,
    registrations: &BTreeMap<RelaySessionId, RegistrationState>,
    timeout: Duration,
) {
    let deregister = async {
        for state in registrations.values() {
            if let Some((key, lease_id)) = state.active_lease() {
                let _ = client.deregister(generation, key, lease_id).await;
            }
        }
    };
    let _ = tokio::time::timeout(timeout, deregister).await;
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, error::Error, time::Duration};

    use relaygate_route_table::{
        BindingId, BindingProjection, BindingSnapshot, Destination, GatewayId, GatewayLocator,
        RelaySessionId, ShardId,
    };
    use tokio::time::Instant;
    use uuid::Uuid;

    use super::{RegistrationState, prepare_registration_pass, reconcile_desired};
    use crate::{projection::ProjectedShardSnapshot, runtime::desired::DesiredStore};

    type TestResult = Result<(), Box<dyn Error>>;

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

    fn reconcile(
        desired: &DesiredStore,
        shard_id: &ShardId,
        registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
        observed: &mut u64,
    ) -> TestResult {
        reconcile_desired(
            desired,
            shard_id,
            GatewayId::from_uuid(Uuid::from_u128(1)),
            (Duration::from_millis(10), Duration::from_millis(40)),
            registrations,
            observed,
            Instant::now(),
        )?;
        Ok(())
    }

    #[test]
    fn reconcile_applies_inserts_and_removals_with_their_own_versions() -> TestResult {
        let desired = DesiredStore::default();
        let shard_id = ShardId::new("rt-0")?;
        let session = RelaySessionId::new();
        let mut registrations = BTreeMap::new();
        let mut observed = 0;

        let inserted = desired.commit(session, vec![present(&shard_id)?])?;
        reconcile(&desired, &shard_id, &mut registrations, &mut observed)?;
        assert_eq!(observed, inserted);
        let state = registrations.get(&session).ok_or("registration missing")?;
        assert_eq!(state.desired_version(), inserted);
        assert!(!state.is_removable());

        let removed = desired.commit(session, vec![absent(&shard_id)])?;
        reconcile(&desired, &shard_id, &mut registrations, &mut observed)?;
        assert_eq!(observed, removed);
        let state = registrations.get(&session).ok_or("registration missing")?;
        assert_eq!(state.desired_version(), removed);
        assert!(state.is_removable());
        Ok(())
    }

    #[test]
    fn reconcile_ignores_removals_for_sessions_it_never_registered() -> TestResult {
        let desired = DesiredStore::default();
        let shard_id = ShardId::new("rt-0")?;
        let session = RelaySessionId::new();
        let mut registrations = BTreeMap::new();
        let mut observed = 0;

        desired.commit(session, vec![present(&shard_id)?])?;
        let removed = desired.commit(session, vec![absent(&shard_id)])?;
        reconcile(&desired, &shard_id, &mut registrations, &mut observed)?;
        assert_eq!(observed, removed);
        assert!(registrations.is_empty());
        Ok(())
    }

    #[test]
    fn registration_pass_selects_and_counts_one_thousand_sessions_in_one_visit_each() -> TestResult
    {
        let now = Instant::now();
        let shard_id = ShardId::new("rt-0")?;
        let projected = present(&shard_id)?;
        let snapshot = projected.snapshot.ok_or("snapshot missing")?;
        let gateway_id = GatewayId::from_uuid(Uuid::from_u128(1));
        let mut registrations = (1..=1_000_u128)
            .map(|value| {
                let session_id = RelaySessionId::from_uuid(Uuid::from_u128(value));
                let key = relaygate_route_table::RegistrationKey::new(
                    gateway_id,
                    session_id,
                    shard_id.clone(),
                );
                (
                    session_id,
                    RegistrationState::new(
                        key,
                        1,
                        Some(snapshot.clone()),
                        now,
                        Duration::from_millis(10),
                        Duration::from_millis(40),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let started = std::time::Instant::now();
        let pass = prepare_registration_pass(&mut registrations, now)?;
        eprintln!(
            "registration pass: sessions=1000 elapsed_us={}",
            started.elapsed().as_micros()
        );

        assert!(pass.ticket.is_some());
        assert!(pass.deadline.is_none());
        assert_eq!(pass.counts.synced, 0);
        assert_eq!(pass.counts.unsynced, 1_000);
        assert_eq!(pass.counts.terminal, 0);
        assert_eq!(pass.visited, 1_000);
        assert_eq!(registrations.len(), 1_000);
        Ok(())
    }
}
