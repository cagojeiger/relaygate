use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak, atomic::AtomicU64},
    time::Duration,
};

use tokio::{
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{ListenerLifecycle, ListenerState, RelayInner};
use crate::{
    AccessToken, AccessTokenSource, Config, Destination, Error, ListenerStatus,
    lifetime::RuntimeLifetime, resource::RelayResources, session::ReconnectBackoff,
};

use super::super::RelaySession;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn precommit_session_end_keeps_initial_listener_retryable_with_original_deadline() -> TestResult {
    let destination: Destination = "inference/stt.seoul".parse()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let (status, _) = watch::channel(ListenerStatus::Registering);
    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    let state = Arc::new(ListenerState {
        destination,
        access_token_source: AccessTokenSource::static_token(AccessToken::new("grant")?),
        status,
        last_error: StdMutex::new(None),
        incoming_tx,
        incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        initial_deadline: deadline,
        lifecycle: StdMutex::new(ListenerLifecycle::Pending),
        registration_committed: StdMutex::new(false),
        live_pipe_slots: Arc::new(Semaphore::new(1)),
    });

    state.handle_precommit_session_end(Error::unavailable(
        "RelaySession ended before managed PUBLISH commit",
    ));

    assert_eq!(*state.status.borrow(), ListenerStatus::Registering);
    assert_eq!(state.initial_deadline, deadline);
    assert_eq!(
        *state
            .lifecycle
            .lock()
            .map_err(|_| "lifecycle lock poisoned")?,
        ListenerLifecycle::Pending
    );
    assert!(state.last_error().is_none());

    assert!(state.begin_registration_commit());
    assert!(state.activate());
    assert_eq!(*state.status.borrow(), ListenerStatus::Active);
    assert!(state.promote_returned());
    assert!(state.was_returned());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn repeated_republish_failures_share_one_bounded_retry_timer() -> TestResult {
    let initial = Duration::from_secs(10);
    let maximum = Duration::from_secs(40);
    let config =
        Config::new_insecure_for_tests("127.0.0.1:1").with_reconnect_backoff(initial, maximum);
    let limits = config.resource_limits;
    let (current, _) = watch::channel::<Option<Arc<RelaySession>>>(None);
    let cancel = CancellationToken::new();
    let inner = RelayInner {
        resources: RelayResources::new(limits),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(initial, maximum))),
        config,
        desired: StdMutex::new(HashMap::new()),
        current,
        reconcile: Arc::new(Notify::new()),
        cancel: cancel.clone(),
        lifetime: Weak::<RuntimeLifetime>::new(),
    };

    inner.schedule_reconcile();
    inner.schedule_reconcile();

    let next = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(next, initial.saturating_mul(2));
    assert!(
        inner
            .republish_retry_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            == 1
    );
    assert!(!inner.republish_retry_is_ready());

    tokio::time::advance(initial).await;
    inner.reconcile.notified().await;
    assert!(
        inner
            .republish_retry_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            == 0
    );
    assert!(inner.republish_retry_is_ready());

    inner.schedule_reconcile();
    let next = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(next, maximum);

    inner.reset_republish_backoff();
    assert!(inner.republish_retry_is_ready());
    let reset = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(reset, initial);
    tokio::time::advance(maximum).await;
    assert!(inner.republish_retry_is_ready());
    cancel.cancel();
    tokio::task::yield_now().await;
    Ok(())
}
