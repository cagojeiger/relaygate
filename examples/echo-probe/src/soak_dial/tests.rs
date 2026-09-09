use std::{
    future::{pending, ready},
    sync::{Arc, atomic::AtomicBool},
};
use tokio::time::{Instant, advance};

use super::*;

#[test]
fn soak_retries_only_known_unobserved_transient_failures() {
    for code in [
        ErrorCode::NotFound,
        ErrorCode::Unavailable,
        ErrorCode::ResourceExhausted,
    ] {
        assert!(retryable(code, PeerObservation::NotObserved));
        assert!(!retryable(code, PeerObservation::MaybeObserved));
        assert!(!retryable(code, PeerObservation::Observed));
    }
    for code in [
        ErrorCode::InvalidArgument,
        ErrorCode::Unauthenticated,
        ErrorCode::PermissionDenied,
        ErrorCode::FailedPrecondition,
        ErrorCode::DeadlineExceeded,
        ErrorCode::Cancelled,
        ErrorCode::ProtocolError,
        ErrorCode::Internal,
        ErrorCode::AlreadyExists,
    ] {
        for observation in [
            PeerObservation::NotObserved,
            PeerObservation::MaybeObserved,
            PeerObservation::Observed,
        ] {
            assert!(!retryable(code, observation));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn immediate_success_and_terminal_error_do_not_retry() -> anyhow::Result<()> {
    for success in [true, false] {
        let start = Instant::now();
        let rejected = AtomicU64::new(0);
        let mut calls = 0;
        let result = retry_until_available(Duration::from_secs(1), &rejected, || {
            calls += 1;
            ready(Attempt::Complete(if success {
                Ok(7)
            } else {
                Err(anyhow::anyhow!("terminal"))
            }))
        })
        .await;
        if success {
            assert_eq!(result?, 7);
        } else {
            assert_eq!(
                result
                    .err()
                    .ok_or_else(|| anyhow::anyhow!("unexpected success"))?
                    .to_string(),
                "terminal"
            );
        }
        assert_eq!(calls, 1);
        assert_eq!(rejected.load(Ordering::Relaxed), 0);
        assert_eq!(Instant::now(), start);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn transient_rejections_recover_and_count_only_admission_failures() -> anyhow::Result<()> {
    let start = Instant::now();
    let rejected = AtomicU64::new(0);
    let mut calls = 0;
    let result = retry_until_available(Duration::from_secs(1), &rejected, || {
        calls += 1;
        ready(match calls {
            1 => Attempt::Retry {
                admission_rejected: false,
            },
            2 => Attempt::Retry {
                admission_rejected: true,
            },
            _ => Attempt::Complete(Ok(7)),
        })
    })
    .await?;
    assert_eq!(result, 7);
    assert_eq!(calls, 3);
    assert_eq!(rejected.load(Ordering::Relaxed), 1);
    assert_eq!(Instant::now() - start, Duration::from_millis(200));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn repeated_rejection_uses_one_deadline_not_one_per_retry() -> anyhow::Result<()> {
    let start = Instant::now();
    let rejected = AtomicU64::new(0);
    let mut calls = 0;
    let result = retry_until_available::<(), _, _>(Duration::from_millis(250), &rejected, || {
        calls += 1;
        ready(Attempt::Retry {
            admission_rejected: true,
        })
    })
    .await;
    let error = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("unexpected success"))?;
    assert!(
        error
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    );
    assert_eq!(calls, 3);
    assert_eq!(rejected.load(Ordering::Relaxed), 3);
    assert_eq!(Instant::now() - start, Duration::from_millis(250));
    advance(Duration::from_secs(10)).await;
    assert_eq!(rejected.load(Ordering::Relaxed), 3);
    Ok(())
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_drops_a_stalled_attempt() -> anyhow::Result<()> {
    let dropped = Arc::new(AtomicBool::new(false));
    let rejected = AtomicU64::new(0);
    let start = Instant::now();
    let result = retry_until_available(Duration::from_millis(250), &rejected, || {
        let guard = Dropped(Arc::clone(&dropped));
        async move {
            let _guard = guard;
            pending::<Attempt<()>>().await
        }
    })
    .await;
    assert!(
        result
            .err()
            .ok_or_else(|| anyhow::anyhow!("unexpected success"))?
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
    );
    assert!(dropped.load(Ordering::Relaxed));
    assert_eq!(rejected.load(Ordering::Relaxed), 0);
    assert_eq!(Instant::now() - start, Duration::from_millis(250));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_stops_backoff_without_background_retries() -> anyhow::Result<()> {
    let rejected = Arc::new(AtomicU64::new(0));
    let calls = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&rejected);
    let attempts = Arc::clone(&calls);
    let entered = Arc::new(tokio::sync::Notify::new());
    let started = Arc::clone(&entered);
    let task = tokio::spawn(async move {
        retry_until_available::<(), _, _>(Duration::from_secs(5), &counter, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            started.notify_one();
            ready(Attempt::Retry {
                admission_rejected: true,
            })
        })
        .await
    });
    entered.notified().await;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    task.abort();
    assert!(
        task.await
            .err()
            .ok_or_else(|| anyhow::anyhow!("unexpected task success"))?
            .is_cancelled()
    );
    advance(Duration::from_secs(10)).await;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(rejected.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn caller_cancellation_drops_a_stalled_attempt() -> anyhow::Result<()> {
    let dropped = Arc::new(AtomicBool::new(false));
    let guard_state = Arc::clone(&dropped);
    let entered = Arc::new(tokio::sync::Notify::new());
    let started = Arc::clone(&entered);
    let task = tokio::spawn(async move {
        let rejected = AtomicU64::new(0);
        retry_until_available(Duration::from_secs(5), &rejected, || {
            let guard = Dropped(Arc::clone(&guard_state));
            let started = Arc::clone(&started);
            async move {
                let _guard = guard;
                started.notify_one();
                pending::<Attempt<()>>().await
            }
        })
        .await
    });
    entered.notified().await;
    assert!(!dropped.load(Ordering::Relaxed));
    task.abort();
    assert!(
        task.await
            .err()
            .ok_or_else(|| anyhow::anyhow!("unexpected task success"))?
            .is_cancelled()
    );
    assert!(dropped.load(Ordering::Relaxed));
    Ok(())
}
