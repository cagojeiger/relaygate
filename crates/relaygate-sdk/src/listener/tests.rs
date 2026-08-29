use std::{collections::HashMap, sync::Arc, time::Duration};

use relaygate_protocol::{PipeId, SessionId};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{Listener, ListenerRuntimeInner, ListenerState, ListenerStatus};
use crate::{Config, ErrorCode, pipe::PipeState};

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
    })
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
    });
    let listener = Listener {
        inner,
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
    });
    let listener = Listener {
        inner,
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
    });
    let listener = Arc::new(Listener {
        inner,
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
