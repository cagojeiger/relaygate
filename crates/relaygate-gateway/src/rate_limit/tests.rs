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
