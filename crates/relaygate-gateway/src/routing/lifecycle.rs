use std::time::Duration;

use relaygate_route_table::{
    LeaseId, MappingSnapshot, RegistrationAck, RegistrationKey, RegistrationRevision,
};
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RegistrationAction {
    Register {
        key: RegistrationKey,
    },
    Update {
        key: RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: MappingSnapshot,
    },
    KeepAlive {
        key: RegistrationKey,
        lease_id: LeaseId,
    },
    Deregister {
        key: RegistrationKey,
        lease_id: LeaseId,
    },
}

impl RegistrationAction {
    #[must_use]
    pub(super) const fn lease_id(&self) -> Option<LeaseId> {
        match self {
            Self::Register { .. } => None,
            Self::Update { lease_id, .. }
            | Self::KeepAlive { lease_id, .. }
            | Self::Deregister { lease_id, .. } => Some(*lease_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OperationTicket {
    pub(super) desired_version: u64,
    pub(super) action: RegistrationAction,
}

#[derive(Debug, Clone)]
struct LeaseState {
    id: LeaseId,
    revision: Option<RegistrationRevision>,
    keep_alive_at: Instant,
}

/// Current desired state and synchronization metadata for one session-shard
/// registration. No mutation history or old snapshot is retained.
#[derive(Debug, Clone)]
pub(super) struct RegistrationState {
    key: RegistrationKey,
    desired_version: u64,
    snapshot: Option<MappingSnapshot>,
    lease: Option<LeaseState>,
    synced_version: Option<u64>,
    pending: Option<OperationTicket>,
    terminal: bool,
    validate_lease: bool,
    retry_at: Instant,
    retry_backoff: Duration,
    retry_initial: Duration,
    retry_max: Duration,
}

impl RegistrationState {
    pub(super) fn new(
        key: RegistrationKey,
        desired_version: u64,
        snapshot: Option<MappingSnapshot>,
        now: Instant,
        retry_initial: Duration,
        retry_max: Duration,
    ) -> Self {
        Self {
            key,
            desired_version,
            snapshot,
            lease: None,
            synced_version: None,
            pending: None,
            terminal: false,
            validate_lease: false,
            retry_at: now,
            retry_backoff: retry_initial,
            retry_initial,
            retry_max,
        }
    }

    pub(super) fn publish(
        &mut self,
        desired_version: u64,
        snapshot: Option<MappingSnapshot>,
        now: Instant,
    ) {
        if desired_version <= self.desired_version {
            return;
        }
        let unchanged = self.snapshot == snapshot;
        let was_synced = self.is_synced();
        self.desired_version = desired_version;
        self.snapshot = snapshot;
        if unchanged && was_synced {
            self.synced_version = Some(desired_version);
        } else {
            self.synced_version = None;
        }
        if self.terminal && self.snapshot.is_none() {
            // A terminal trust/generation failure cannot complete Deregister.
            // Drop the local lease reference and rely on RT expiry so removed
            // sessions do not accumulate in manager memory.
            self.lease = None;
            self.pending = None;
            self.validate_lease = false;
        }
        self.retry_at = now;
        self.retry_backoff = self.retry_initial;
    }

    #[must_use]
    pub(super) fn is_synced(&self) -> bool {
        self.snapshot.is_some()
            && !self.terminal
            && !self.validate_lease
            && self.synced_version == Some(self.desired_version)
    }

    #[must_use]
    pub(super) fn is_desired(&self) -> bool {
        self.snapshot.is_some()
    }

    #[must_use]
    pub(super) fn is_removable(&self) -> bool {
        self.snapshot.is_none() && self.lease.is_none() && self.pending.is_none()
    }

    #[must_use]
    #[cfg(test)]
    pub(super) const fn desired_version(&self) -> u64 {
        self.desired_version
    }

    #[must_use]
    pub(super) fn active_lease(&self) -> Option<(&RegistrationKey, LeaseId)> {
        self.lease.as_ref().map(|lease| (&self.key, lease.id))
    }

    #[must_use]
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        if self.pending.is_some() || self.terminal {
            return None;
        }
        if self.retry_at > Instant::now() {
            return Some(self.retry_at);
        }
        self.lease
            .as_ref()
            .filter(|_| self.is_synced())
            .map(|lease| lease.keep_alive_at)
    }

    pub(super) fn begin_next(
        &mut self,
        now: Instant,
    ) -> Result<Option<OperationTicket>, &'static str> {
        if self.pending.is_some() || self.terminal || now < self.retry_at {
            return Ok(None);
        }

        let action = match (&self.snapshot, &self.lease) {
            (None, None) => return Ok(None),
            (None, Some(lease)) => RegistrationAction::Deregister {
                key: self.key.clone(),
                lease_id: lease.id,
            },
            (Some(_), None) => RegistrationAction::Register {
                key: self.key.clone(),
            },
            (Some(_), Some(lease)) if self.validate_lease => RegistrationAction::KeepAlive {
                key: self.key.clone(),
                lease_id: lease.id,
            },
            (Some(snapshot), Some(lease)) if self.synced_version != Some(self.desired_version) => {
                let revision = match lease.revision {
                    Some(revision) => revision
                        .get()
                        .checked_add(1)
                        .ok_or("registration revision exhausted")?,
                    None => RegistrationRevision::FIRST.get(),
                };
                RegistrationAction::Update {
                    key: self.key.clone(),
                    lease_id: lease.id,
                    revision: RegistrationRevision::new(revision)
                        .map_err(|_| "registration revision is invalid")?,
                    snapshot: snapshot.clone(),
                }
            }
            (Some(_), Some(lease)) if now >= lease.keep_alive_at => RegistrationAction::KeepAlive {
                key: self.key.clone(),
                lease_id: lease.id,
            },
            (Some(_), Some(_)) => return Ok(None),
        };

        let ticket = OperationTicket {
            desired_version: self.desired_version,
            action,
        };
        self.pending = Some(ticket.clone());
        Ok(Some(ticket))
    }

    pub(super) fn register_succeeded(
        &mut self,
        ticket: &OperationTicket,
        ack: RegistrationAck,
        now: Instant,
    ) {
        if !self.take_current(ticket)
            || !matches!(ticket.action, RegistrationAction::Register { .. })
        {
            return;
        }
        self.lease = Some(LeaseState {
            id: ack.lease_id(),
            revision: ack.accepted_revision(),
            keep_alive_at: next_keep_alive(now, ack.expires_in()),
        });
        self.synced_version = None;
        self.validate_lease = false;
        self.reset_retry(now);
    }

    pub(super) fn update_succeeded(
        &mut self,
        ticket: &OperationTicket,
        ack: RegistrationAck,
        now: Instant,
    ) {
        let RegistrationAction::Update {
            lease_id, revision, ..
        } = &ticket.action
        else {
            return;
        };
        if !self.take_current(ticket) || ack.lease_id() != *lease_id {
            return;
        }
        let Some(lease) = &mut self.lease else {
            return;
        };
        if lease.id != *lease_id {
            return;
        }
        lease.revision = Some(*revision);
        lease.keep_alive_at = next_keep_alive(now, ack.expires_in());
        self.synced_version = Some(ticket.desired_version);
        self.validate_lease = false;
        self.reset_retry(now);
    }

    pub(super) fn keep_alive_succeeded(
        &mut self,
        ticket: &OperationTicket,
        ack: RegistrationAck,
        now: Instant,
    ) {
        let RegistrationAction::KeepAlive { lease_id, .. } = ticket.action else {
            return;
        };
        if !self.take_current(ticket) || ack.lease_id() != lease_id {
            return;
        }
        if let Some(lease) = &mut self.lease
            && lease.id == lease_id
        {
            lease.keep_alive_at = next_keep_alive(now, ack.expires_in());
            self.validate_lease = false;
            self.reset_retry(now);
        }
    }

    /// A stale/unknown lease can only invalidate the lease captured by this
    /// exact desired operation. Newer desired state is never rolled back.
    pub(super) fn stale_lease(&mut self, ticket: &OperationTicket, now: Instant) {
        if !self.take_current(ticket) {
            return;
        }
        let Some(expected) = ticket.action.lease_id() else {
            return;
        };
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| lease.id == expected)
        {
            self.lease = None;
            self.synced_version = None;
            self.validate_lease = false;
            self.reset_retry(now);
        }
    }

    pub(super) fn transient_failure(&mut self, ticket: &OperationTicket, now: Instant) {
        if !self.take_current(ticket) {
            return;
        }
        self.synced_version = None;
        self.retry_at = now + self.retry_backoff;
        self.retry_backoff = next_backoff(self.retry_backoff, self.retry_max);
    }

    pub(super) fn terminal_failure(&mut self, ticket: &OperationTicket) {
        if self.take_current(ticket) {
            self.synced_version = None;
            self.terminal = true;
        }
    }

    pub(super) fn connection_lost(&mut self, now: Instant) {
        if self.lease.is_some() {
            self.validate_lease = true;
        }
        self.retry_at = now;
        self.retry_backoff = self.retry_initial;
    }

    pub(super) fn mark_terminal(&mut self) {
        self.pending = None;
        self.synced_version = None;
        self.terminal = true;
    }

    pub(super) fn finish_deregister(&mut self, ticket: &OperationTicket) {
        if self.take_current(ticket)
            && matches!(ticket.action, RegistrationAction::Deregister { .. })
        {
            self.lease = None;
            self.synced_version = None;
            self.validate_lease = false;
        }
    }

    fn take_current(&mut self, ticket: &OperationTicket) -> bool {
        if self.pending.as_ref() != Some(ticket) {
            return false;
        }
        self.pending = None;
        self.desired_version == ticket.desired_version
            && self.current_lease_id() == ticket.action.lease_id()
    }

    fn current_lease_id(&self) -> Option<LeaseId> {
        self.lease.as_ref().map(|lease| lease.id)
    }

    fn reset_retry(&mut self, now: Instant) {
        self.retry_at = now;
        self.retry_backoff = self.retry_initial;
    }
}

fn next_keep_alive(now: Instant, expires_in: Duration) -> Instant {
    let interval = expires_in / 2;
    let interval = if interval.is_zero() {
        Duration::from_nanos(1)
    } else {
        interval
    };
    now.checked_add(interval)
        .unwrap_or_else(|| now + Duration::from_secs(60))
}

#[must_use]
pub(super) fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}
