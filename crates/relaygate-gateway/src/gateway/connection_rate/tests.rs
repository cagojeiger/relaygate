use std::time::Duration;

use super::{ConnectionRateLimit, TOKEN, TokenBucket};
use crate::{Gateway, GatewayConfig};
use tokio::time::Instant;

#[test]
fn burst_and_fractional_refill_bound_admitted_attempts() {
    let now = Instant::now();
    let mut bucket = TokenBucket {
        per_second: 2,
        capacity: 3 * TOKEN,
        credit: 3 * TOKEN,
        updated_at: now,
    };
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
    let limiter = ConnectionRateLimit::new(2, 3);
    let mut bucket = limiter.lock();
    let later = bucket.updated_at + Duration::from_secs(3600);
    for _ in 0..3 {
        assert!(bucket.try_take(later));
    }
    assert!(!bucket.try_take(later));
    assert!(!bucket.try_take(later + Duration::from_millis(499)));
    assert!(bucket.try_take(later + Duration::from_millis(500)));
}

#[test]
fn extreme_refill_arithmetic_saturates_at_capacity() {
    let limiter = ConnectionRateLimit::new(usize::MAX, usize::MAX);
    let mut bucket = limiter.lock();
    bucket.credit = 0;
    let later = bucket.updated_at + Duration::from_secs(u32::MAX as u64);
    bucket.refill(later);
    assert_eq!(bucket.credit, bucket.capacity);
}

#[tokio::test(start_paused = true)]
async fn gateway_clones_share_rate_budget_and_readiness_does_not_consume_it()
-> Result<(), Box<dyn std::error::Error>> {
    let gateway =
        Gateway::new(GatewayConfig::new("test-token").with_sdk_connection_rate_limit(2, 3))?;
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let clone = gateway.clone();
        tasks.push(tokio::spawn(async move {
            clone.inner.connection_rate.try_acquire()
        }));
    }
    let mut admitted = 0;
    for task in tasks {
        admitted += usize::from(task.await?);
    }
    assert_eq!(admitted, 3);
    assert!(!gateway.snapshot().sdk_admission_ready);
    assert_eq!(gateway.snapshot().pending_handshakes, 0);
    tokio::time::advance(Duration::from_millis(500)).await;
    for _ in 0..10 {
        assert!(gateway.snapshot().sdk_admission_ready);
    }
    assert!(gateway.inner.connection_rate.try_acquire());
    assert!(!gateway.inner.connection_rate.try_acquire());
    Ok(())
}
