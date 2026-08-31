use std::{collections::HashMap, sync::Arc, time::Duration};

use relaygate_protocol::{PipeId, SessionId};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{Listener, ListenerRuntimeInner, ListenerState, ListenerStatus};
use crate::{Config, Error, ErrorCode, PeerObservation, pipe::PipeState};

fn listener_state(client_id: &str) -> Arc<ListenerState> {
    let (status, _) = watch::channel(ListenerStatus::Active);
    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    Arc::new(ListenerState {
        client_id: client_id.to_owned(),
        client_key: "test-key".to_owned(),
        status,
        last_error: std::sync::Mutex::new(None),
        incoming_tx,
        incoming_rx: Mutex::new(incoming_rx),
        initial_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        lifecycle: std::sync::Mutex::new(super::ListenerLifecycle::Returned),
        registration_committed: std::sync::Mutex::new(false),
    })
}

#[test]
fn return_and_session_end_share_one_lifecycle_linearization()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    {
        let mut lifecycle = state
            .lifecycle
            .lock()
            .map_err(|_| std::io::Error::other("lifecycle lock is poisoned"))?;
        *lifecycle = super::ListenerLifecycle::Pending;
    }
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let promote = std::thread::spawn({
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        move || {
            barrier.wait();
            state.promote_returned()
        }
    });
    let session_end = std::thread::spawn({
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        move || {
            barrier.wait();
            state.suspend_or_fail_initial(
                Error::unavailable("recovery"),
                Error::new(
                    ErrorCode::Unavailable,
                    PeerObservation::Observed,
                    "initial terminal",
                ),
            )
        }
    });
    barrier.wait();

    let promoted = promote
        .join()
        .map_err(|_| std::io::Error::other("promote thread panicked"))?;
    let initial_terminated = session_end
        .join()
        .map_err(|_| std::io::Error::other("session-end thread panicked"))?;
    if promoted {
        assert!(!initial_terminated);
        assert_eq!(*state.status.borrow(), ListenerStatus::Suspended);
    } else {
        assert!(initial_terminated);
        assert_eq!(*state.status.borrow(), ListenerStatus::Closed);
    }
    Ok(())
}

#[tokio::test]
async fn closed_listener_does_not_return_an_already_queued_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([(
            state.client_id.clone(),
            Arc::clone(&state),
        )])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let listener = Listener {
        inner,
        _lifetime: Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
        state: Arc::clone(&state),
    };
    let (outbound, _outbound_rx) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 8);
    let (pipe, _pipe_state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state
        .incoming_tx
        .send(pipe)
        .await
        .map_err(|_| "failed to enqueue test Pipe")?;
    state.set_status(ListenerStatus::Closed, None);

    let error = listener
        .accept()
        .await
        .err()
        .ok_or("closed Listener unexpectedly accepted a Pipe")?;
    assert_eq!(error.code(), ErrorCode::Cancelled);
    listener.close().await?;
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    Ok(())
}

#[tokio::test]
async fn blocked_listener_discards_queued_pipe_before_returning_registration_error()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([(
            state.client_id.clone(),
            Arc::clone(&state),
        )])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let listener = Listener {
        inner,
        _lifetime: Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
        state: Arc::clone(&state),
    };
    let (outbound, _outbound_rx) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 10);
    let (pipe, pipe_state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let pipe_state_weak = Arc::downgrade(&pipe_state);
    state
        .incoming_tx
        .send(pipe)
        .await
        .map_err(|_| "failed to enqueue test Pipe")?;
    pipe_state.fail(Error::unavailable("old ListenerSession ended"));
    drop(pipe_state);
    state.block(Error::new(
        ErrorCode::Unauthenticated,
        PeerObservation::NotObserved,
        "credential was rejected during recovery",
    ));

    let error = listener
        .accept()
        .await
        .err()
        .ok_or("blocked Listener unexpectedly returned its queued Pipe")?;
    assert_eq!(error.code(), ErrorCode::Unauthenticated);
    assert!(pipe_state_weak.upgrade().is_none());
    assert!(abandoned_rx.try_recv().is_err());
    assert!(state.incoming_tx.is_closed());
    listener.close().await?;
    Ok(())
}

#[tokio::test]
async fn suspended_listener_waits_for_a_pipe_from_the_recovered_session()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([(
            state.client_id.clone(),
            Arc::clone(&state),
        )])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let listener = Arc::new(Listener {
        inner,
        _lifetime: Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
        state: Arc::clone(&state),
    });
    let (outbound, _outbound_rx) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let old_pipe_id = PipeId::new(SessionId::new(), 11);
    let (old_pipe, old_pipe_state) =
        PipeState::pair(old_pipe_id, outbound.clone(), 1, abandoned.clone());
    let old_pipe_state_weak = Arc::downgrade(&old_pipe_state);
    state
        .incoming_tx
        .send(old_pipe)
        .await
        .map_err(|_| "failed to enqueue old-session test Pipe")?;
    old_pipe_state.fail(Error::unavailable("old ListenerSession ended"));
    drop(old_pipe_state);
    assert!(!state.suspend_or_fail_initial(
        Error::unavailable("ListenerSession transport ended"),
        Error::unavailable("unused initial failure"),
    ));
    state.drain_unaccepted(false).await;
    assert!(old_pipe_state_weak.upgrade().is_none());
    assert!(abandoned_rx.try_recv().is_err());

    let mut accepting = {
        let listener = Arc::clone(&listener);
        tokio::spawn(async move { listener.accept().await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut accepting)
            .await
            .is_err(),
        "suspended Listener returned instead of waiting for recovery"
    );

    assert!(state.activate());
    let new_pipe_id = PipeId::new(SessionId::new(), 12);
    let (new_pipe, _new_pipe_state) = PipeState::pair(new_pipe_id, outbound, 1, abandoned);
    state
        .incoming_tx
        .send(new_pipe)
        .await
        .map_err(|_| "failed to enqueue recovered-session test Pipe")?;
    let accepted = tokio::time::timeout(Duration::from_secs(1), &mut accepting).await???;
    drop(accepted);
    assert_eq!(abandoned_rx.recv().await, Some(new_pipe_id));
    listener.close().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_close_removes_desired_and_drops_unaccepted_pipes()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([(
            state.client_id.clone(),
            Arc::clone(&state),
        )])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let listener = Listener {
        inner,
        _lifetime: Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
        state: Arc::clone(&state),
    };
    let (outbound, _outbound_rx) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 7);
    let (pipe, _pipe_state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state
        .incoming_tx
        .send(pipe)
        .await
        .map_err(|_| "failed to enqueue test Pipe")?;

    listener.close().await?;

    assert_eq!(*state.status.borrow(), ListenerStatus::Closed);
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    Ok(())
}

#[tokio::test]
async fn pending_accept_observes_close_before_a_racing_pipe()
-> Result<(), Box<dyn std::error::Error>> {
    let state = listener_state("echo.alpha");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([(
            state.client_id.clone(),
            Arc::clone(&state),
        )])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let listener = Arc::new(Listener {
        inner,
        _lifetime: Arc::new(crate::lifetime::RuntimeLifetime::new(
            CancellationToken::new(),
        )),
        state: Arc::clone(&state),
    });

    let accepting = {
        let listener = Arc::clone(&listener);
        tokio::spawn(async move { listener.accept().await })
    };
    while let Ok(receiver) = state.incoming_rx.try_lock() {
        drop(receiver);
        tokio::task::yield_now().await;
    }
    let (outbound, _outbound_rx) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 9);
    let (pipe, _pipe_state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state.set_status(ListenerStatus::Closed, None);
    state
        .incoming_tx
        .try_send(pipe)
        .map_err(|_| "failed to enqueue racing test Pipe")?;

    let error = accepting
        .await?
        .err()
        .ok_or("closed Listener unexpectedly accepted the racing Pipe")?;
    assert_eq!(error.code(), ErrorCode::Cancelled);
    listener.close().await?;
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_accept_preserves_other_accept_sibling_and_queued_pipes()
-> Result<(), Box<dyn std::error::Error>> {
    let alpha_state = listener_state("echo.alpha");
    let beta_state = listener_state("echo.beta");
    let inner = Arc::new(ListenerRuntimeInner {
        config: Config::new("unused").with_operation_timeout(Duration::from_secs(2)),
        desired: std::sync::Mutex::new(HashMap::from([
            (alpha_state.client_id.clone(), Arc::clone(&alpha_state)),
            (beta_state.client_id.clone(), Arc::clone(&beta_state)),
        ])),
        reconcile: Arc::new(tokio::sync::Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: std::sync::Weak::new(),
    });
    let lifetime = Arc::new(crate::lifetime::RuntimeLifetime::new(
        CancellationToken::new(),
    ));
    let alpha = Arc::new(Listener {
        inner: Arc::clone(&inner),
        _lifetime: Arc::clone(&lifetime),
        state: Arc::clone(&alpha_state),
    });
    let beta = Arc::new(Listener {
        inner,
        _lifetime: lifetime,
        state: Arc::clone(&beta_state),
    });

    let (outbound, _outbound_rx) = mpsc::channel(2);
    let (alpha_abandoned, mut alpha_abandoned_rx) = mpsc::unbounded_channel();
    let alpha_pipe_id = PipeId::new(SessionId::new(), 13);
    let (alpha_pipe, _alpha_pipe_state) =
        PipeState::pair(alpha_pipe_id, outbound.clone(), 1, alpha_abandoned);
    let (beta_abandoned, mut beta_abandoned_rx) = mpsc::unbounded_channel();
    let beta_pipe_id = PipeId::new(SessionId::new(), 14);
    let (beta_pipe, _beta_pipe_state) = PipeState::pair(beta_pipe_id, outbound, 1, beta_abandoned);
    beta_state
        .incoming_tx
        .send(beta_pipe)
        .await
        .map_err(|_| "failed to enqueue sibling test Pipe")?;

    let first_accept = {
        let alpha = Arc::clone(&alpha);
        tokio::spawn(async move { alpha.accept().await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Ok(receiver) = alpha_state.incoming_rx.try_lock() {
            drop(receiver);
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let other_accept = {
        let alpha = Arc::clone(&alpha);
        tokio::spawn(async move { alpha.accept().await })
    };
    tokio::task::yield_now().await;
    assert!(!other_accept.is_finished());

    first_accept.abort();
    let cancellation = first_accept
        .await
        .err()
        .ok_or("aborted accept unexpectedly completed")?;
    assert!(cancellation.is_cancelled());
    assert_eq!(alpha.status(), ListenerStatus::Active);
    assert_eq!(beta.status(), ListenerStatus::Active);

    alpha_state
        .incoming_tx
        .send(alpha_pipe)
        .await
        .map_err(|_| "failed to enqueue replacement accept test Pipe")?;
    let accepted_alpha = tokio::time::timeout(Duration::from_secs(1), other_accept).await???;
    let accepted_beta = tokio::time::timeout(Duration::from_secs(1), beta.accept()).await??;

    drop(accepted_alpha);
    drop(accepted_beta);
    assert_eq!(alpha_abandoned_rx.recv().await, Some(alpha_pipe_id));
    assert_eq!(beta_abandoned_rx.recv().await, Some(beta_pipe_id));
    alpha.close().await?;
    beta.close().await?;
    Ok(())
}
