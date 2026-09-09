use std::time::{Duration, Instant};

use super::TokenBucket;

#[test]
fn burst_and_fractional_refill_bound_admitted_attempts() {
    let now = Instant::now();
    let mut bucket = TokenBucket::new(2, 3, now);
    for _ in 0..3 {
        assert!(bucket.try_take(now));
    }
    assert!(!bucket.try_take(now));
    for millis in [100, 200, 300, 499] {
        assert!(!bucket.try_take(now + Duration::from_millis(millis)));
    }
    assert!(bucket.try_take(now + Duration::from_millis(500)));
    assert!(!bucket.try_take(now + Duration::from_millis(500)));
    assert!(bucket.try_take(now + Duration::from_secs(1)));
    assert!(!bucket.try_take(now + Duration::from_secs(1)));
}

#[test]
fn idle_refill_never_accumulates_more_than_burst() {
    let now = Instant::now();
    let mut bucket = TokenBucket::new(2, 3, now);
    let later = now + Duration::from_secs(3600);
    for _ in 0..3 {
        assert!(bucket.try_take(later));
    }
    assert!(!bucket.try_take(later));
    assert!(!bucket.try_take(later + Duration::from_millis(499)));
    assert!(bucket.try_take(later + Duration::from_millis(500)));
}

#[test]
fn extreme_refill_arithmetic_saturates_at_capacity() {
    let now = Instant::now();
    let mut bucket = TokenBucket::new(usize::MAX, usize::MAX, now);
    bucket.credit = 0;
    assert!(bucket.has_capacity(now + Duration::from_secs(u32::MAX as u64)));
    assert_eq!(bucket.credit, bucket.capacity);
}

#[test]
fn mixed_schedules_obey_every_window_rate_bound() {
    for rate in [1, 3, 7, 256] {
        for burst in [1, 2, 8] {
            let start = Instant::now();
            let mut bucket = TokenBucket::new(rate, burst, start);
            let mut elapsed = 0_u64;
            let mut admissions = Vec::new();
            for step in 0..240 {
                elapsed += [0, 1, 17, 250, 999, 1000][step % 6];
                let now = start + Duration::from_millis(elapsed);
                // Repeated observation must not spend tokens or erase fractional refill.
                let available = bucket.has_capacity(now);
                assert_eq!(available, bucket.has_capacity(now));
                assert_eq!(bucket.try_take(now), available);
                if available {
                    admissions.push(elapsed);
                }
                assert!(bucket.credit <= bucket.capacity);
                for &from in &admissions {
                    let count = admissions.iter().filter(|&&time| time >= from).count() as u64;
                    let bound = burst as u64 + rate as u64 * (elapsed - from) / 1000;
                    assert!(
                        count <= bound,
                        "rate={rate} burst={burst} window={from}..{elapsed}"
                    );
                }
            }
        }
    }
}
