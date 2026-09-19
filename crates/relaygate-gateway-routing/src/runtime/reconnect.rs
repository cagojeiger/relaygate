//! Deterministic per-shard reconnect pacing with bounded jitter.

use std::time::Duration;

use relaygate_route_table::{GatewayId, ShardId};

pub(super) struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    maximum: Duration,
    entropy: u64,
}

impl ReconnectBackoff {
    pub(super) fn new(
        initial: Duration,
        maximum: Duration,
        gateway_id: GatewayId,
        shard_id: &ShardId,
    ) -> Self {
        let mut entropy = 14_695_981_039_346_656_037_u64;
        for byte in gateway_id
            .as_uuid()
            .as_bytes()
            .iter()
            .chain(shard_id.as_bytes())
        {
            entropy = entropy.wrapping_mul(1_099_511_628_211) ^ u64::from(*byte);
        }
        Self {
            initial,
            current: initial,
            maximum,
            entropy: entropy.max(1),
        }
    }

    pub(super) fn next_delay(&mut self) -> Duration {
        self.entropy ^= self.entropy << 13;
        self.entropy ^= self.entropy >> 7;
        self.entropy ^= self.entropy << 17;

        let base_nanos = self.current.as_nanos();
        let floor_nanos = base_nanos.saturating_mul(2) / 3;
        let jitter_nanos = (base_nanos - floor_nanos).saturating_mul(u128::from(self.entropy))
            / u128::from(u64::MAX);
        let delay = duration_from_nanos(floor_nanos.saturating_add(jitter_nanos));
        self.current = self.current.saturating_mul(2).min(self.maximum);
        delay
    }

    pub(super) fn reset(&mut self) {
        self.current = self.initial;
    }
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = (nanos / NANOS_PER_SECOND).min(u128::from(u64::MAX));
    let subsecond_nanos = if seconds == u128::from(u64::MAX) {
        999_999_999
    } else {
        nanos % NANOS_PER_SECOND
    };
    Duration::new(seconds as u64, subsecond_nanos as u32).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use relaygate_route_table::{GatewayId, RouteTableError, ShardId};
    use uuid::Uuid;

    use super::ReconnectBackoff;

    #[test]
    fn route_table_reconnect_backoff_uses_bounded_jitter_and_resets() -> Result<(), RouteTableError>
    {
        let initial = Duration::from_millis(100);
        let maximum = Duration::from_millis(400);
        let mut backoff = ReconnectBackoff::new(
            initial,
            maximum,
            GatewayId::from_uuid(Uuid::from_u128(1)),
            &ShardId::new("rt-0")?,
        );

        for (floor, ceiling) in [
            (Duration::from_millis(66), initial),
            (Duration::from_millis(133), Duration::from_millis(200)),
            (Duration::from_millis(266), maximum),
            (Duration::from_millis(266), maximum),
        ] {
            assert!((floor..=ceiling).contains(&backoff.next_delay()));
        }

        backoff.reset();
        assert!((Duration::from_millis(66)..=initial).contains(&backoff.next_delay()));
        Ok(())
    }
}
