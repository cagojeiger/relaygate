use std::time::Duration;

use uuid::Uuid;

/// Scales `duration` into `[0.9x, 1.1x]` with a deterministic FNV hash of
/// `seed` and `salt`, so peers with the same configuration do not probe in
/// lockstep.
pub fn staggered(duration: Duration, seed: Uuid, salt: u8) -> Duration {
    let mut hash = u64::from(salt);
    for byte in seed.as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    let factor_per_mille = 900 + u128::from(hash % 201);
    let nanos = duration.as_nanos().saturating_mul(factor_per_mille) / 1_000;
    Duration::from_nanos(nanos.try_into().unwrap_or(u64::MAX)).max(Duration::from_millis(1))
}
