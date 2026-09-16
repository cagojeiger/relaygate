use std::time::Duration;

/// Scales `duration` into `[0.9x, 1.1x]` with a deterministic FNV hash of
/// `seed` and `salt`, so sessions with the same configuration do not probe
/// in lockstep.
pub(crate) fn staggered(duration: Duration, seed: &[u8; 16], salt: u8) -> Duration {
    let mut hash = u64::from(salt);
    for byte in seed {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    let factor_per_mille = 900 + u128::from(hash % 201);
    let nanos = duration.as_nanos().saturating_mul(factor_per_mille) / 1_000;
    Duration::from_nanos(nanos.try_into().unwrap_or(u64::MAX)).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::staggered;

    #[test]
    fn stagger_is_deterministic_and_stays_within_ten_percent() {
        let base = Duration::from_secs(10);
        let seed = [7_u8; 16];
        let first = staggered(base, &seed, 1);
        assert_eq!(first, staggered(base, &seed, 1));
        assert!((Duration::from_secs(9)..=Duration::from_secs(11)).contains(&first));
        assert_ne!(first, staggered(base, &seed, 2), "salt varies the stagger");
        assert_eq!(
            staggered(Duration::ZERO, &seed, 1),
            Duration::from_millis(1),
            "the stagger never collapses to zero"
        );
    }
}
