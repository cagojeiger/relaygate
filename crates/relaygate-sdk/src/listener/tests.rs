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
        registration_attempt: std::sync::Mutex::new(None),
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
async fn blocked_listener_returns_previously_admitted_pipe_then_rejects_accept()
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
    let (pipe, _pipe_state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state
        .incoming_tx
        .send(pipe)
        .await
        .map_err(|_| "failed to enqueue test Pipe")?;
    state.block(Error::new(
        ErrorCode::PermissionDenied,
        PeerObservation::NotObserved,
        "credential was revoked",
    ));

    let admitted = listener.accept().await?;
    drop(admitted);
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    let error = listener
        .accept()
        .await
        .err()
        .ok_or("blocked Listener unexpectedly waited for a new Pipe")?;
    assert_eq!(error.code(), ErrorCode::PermissionDenied);
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
