use std::time::Duration;

use relaygate_protocol::{Frame, SessionId};
use tokio::time::Instant;

#[derive(Debug)]
pub(super) struct SessionHeartbeat {
    idle_timeout: Duration,
    response_timeout: Duration,
    last_inbound: Instant,
    pending: Option<PendingHeartbeat>,
    next_nonce: u64,
}

#[derive(Debug)]
struct PendingHeartbeat {
    nonce: u64,
    deadline: Instant,
}

impl SessionHeartbeat {
    pub(super) fn new(
        idle_timeout: Duration,
        response_timeout: Duration,
        session_id: SessionId,
        salt: u8,
    ) -> Self {
        Self {
            idle_timeout: jittered_duration(idle_timeout, session_id, salt),
            response_timeout,
            last_inbound: Instant::now(),
            pending: None,
            next_nonce: 1,
        }
    }

    pub(super) fn observe_inbound(&mut self, frame: &Frame) {
        match (&self.pending, frame) {
            (Some(pending), Frame::Pong { nonce })
                if pending.nonce == *nonce && Instant::now() < pending.deadline => {}
            (Some(_), _) | (None, Frame::Pong { .. }) => return,
            (None, Frame::Hello { .. })
            | (None, Frame::Welcome { .. })
            | (None, Frame::SessionRejected { .. })
            | (None, Frame::Publish { .. })
            | (None, Frame::Published { .. })
            | (None, Frame::PublishFailed { .. })
            | (None, Frame::Unpublish { .. })
            | (None, Frame::Unpublished { .. })
            | (None, Frame::Dial { .. })
            | (None, Frame::Offer { .. })
            | (None, Frame::OfferAccepted { .. })
            | (None, Frame::OfferRejected { .. })
            | (None, Frame::Opened { .. })
            | (None, Frame::DialFailed { .. })
            | (None, Frame::Data { .. })
            | (None, Frame::Fin { .. })
            | (None, Frame::Close { .. })
            | (None, Frame::Reset { .. })
            | (None, Frame::Ping { .. })
            | (None, Frame::Cancel { .. }) => {}
        }
        self.last_inbound = Instant::now();
        self.pending = None;
    }

    pub(super) fn response_timed_out(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= Instant::now())
    }

    pub(super) fn next_deadline(&self) -> Instant {
        self.pending
            .as_ref()
            .map_or(self.last_inbound + self.idle_timeout, |pending| {
                pending.deadline
            })
    }

    pub(super) fn on_deadline(&mut self) -> Option<Frame> {
        let now = Instant::now();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= now)
        {
            return None;
        }
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1).max(1);
        self.pending = Some(PendingHeartbeat {
            nonce,
            deadline: now + self.response_timeout,
        });
        Some(Frame::Ping { nonce })
    }

    pub(super) fn mark_probe_committed(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            pending.deadline = Instant::now() + self.response_timeout;
        }
    }
}

fn jittered_duration(duration: Duration, session_id: SessionId, salt: u8) -> Duration {
    let mut hash = u64::from(salt);
    for byte in session_id.as_uuid().as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    let offset_per_mille = (hash % 201) as u128;
    let factor_per_mille = 900 + offset_per_mille;
    let nanos = duration.as_nanos().saturating_mul(factor_per_mille) / 1_000;
    Duration::from_nanos(nanos.try_into().unwrap_or(u64::MAX)).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use relaygate_protocol::{Frame, PipeId, SessionId};
    use tokio::time::Instant;

    use super::SessionHeartbeat;

    fn heartbeat() -> SessionHeartbeat {
        SessionHeartbeat::new(
            Duration::from_secs(60),
            Duration::from_secs(20),
            SessionId::new(),
            0x47,
        )
    }

    #[test]
    fn activity_without_pending_probe_resets_idle_deadline() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(30);
        let previous_deadline = heartbeat.next_deadline();

        heartbeat.observe_inbound(&Frame::Ping { nonce: 7 });

        assert!(heartbeat.next_deadline() > previous_deadline);
        assert!(heartbeat.pending.is_none());
    }

    #[test]
    fn pending_probe_requires_matching_pong() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);

        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Data {
            pipe_id: PipeId::new(SessionId::new(), 1),
            payload: Bytes::from_static(b"x"),
        });
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Pong { nonce: 999 });
        assert!(heartbeat.pending.is_some());

        heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });
        assert!(heartbeat.pending.is_none());
    }

    #[test]
    fn probe_commit_starts_full_response_window() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        if let Some(pending) = heartbeat.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        let committed_at = Instant::now();
        heartbeat.mark_probe_committed();

        assert!(!heartbeat.response_timed_out());
        assert!(heartbeat.next_deadline() >= committed_at + heartbeat.response_timeout);
    }

    #[test]
    fn late_matching_pong_does_not_clear_pending_probe() {
        let mut heartbeat = heartbeat();
        heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            heartbeat.on_deadline(),
            Some(Frame::Ping { nonce: 1 })
        ));
        if let Some(pending) = heartbeat.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });

        assert!(heartbeat.pending.is_some());
        assert!(heartbeat.response_timed_out());
    }
}
