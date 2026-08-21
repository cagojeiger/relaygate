use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;

use super::*;
use crate::{
    Session, SessionError,
    runtime::{OUTBOUND_CAPACITY, Shared, dispatch_response},
    wire::{self, connect_request, connect_response},
};

struct FakeSession {
    number: usize,
    shared: Arc<Shared>,
    outbound: mpsc::Receiver<wire::ConnectRequest>,
}

struct FakeConnector {
    attempts: AtomicUsize,
    sessions: mpsc::UnboundedSender<FakeSession>,
}

impl Connector for FakeConnector {
    fn connect(
        &self,
        _config: Config,
    ) -> Pin<Box<dyn Future<Output = Result<Client, ConnectError>> + Send + '_>> {
        let number = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
        let sessions = self.sessions.clone();
        Box::pin(async move {
            let (outbound, receiver) = mpsc::channel(OUTBOUND_CAPACITY);
            let shared = Arc::new(Shared::new(
                outbound,
                Session {
                    client_session_id: format!("managed-session-{number}"),
                    client_id: "client-1".into(),
                    api_key_id: "key-1".into(),
                    auth_revision: "revision-1".into(),
                },
            ));
            sessions
                .send(FakeSession {
                    number,
                    shared: Arc::clone(&shared),
                    outbound: receiver,
                })
                .map_err(|_| ConnectError::Protocol("managed test session receiver closed"))?;
            Ok(Client { shared })
        })
    }
}

fn harness() -> (ManagedClient, mpsc::UnboundedReceiver<FakeSession>) {
    let (sessions_tx, sessions_rx) = mpsc::unbounded_channel();
    let (state_tx, _) = watch::channel(ManagedState::Connecting);
    let (cancel_tx, _) = watch::channel(false);
    let core = Arc::new(ManagedCore {
        config: Config::new("https://relay.test", "client-1", "key-1", "secret"),
        data: StdMutex::new(ManagedData {
            state: ManagedState::Connecting,
            current: None,
            generation: 0,
            bindings: HashMap::new(),
            failure: None,
        }),
        state_tx,
        cancel_tx,
        task: StdMutex::new(None),
        connector: Arc::new(FakeConnector {
            attempts: AtomicUsize::new(0),
            sessions: sessions_tx,
        }),
    });
    let task_core = Arc::clone(&core);
    let task = tokio::spawn(async move { task_core.run().await });
    *core.task.lock().expect("managed task lock poisoned") = Some(task);
    (ManagedClient { core }, sessions_rx)
}

async fn next_message(
    outbound: &mut mpsc::Receiver<wire::ConnectRequest>,
) -> connect_request::Message {
    outbound
        .recv()
        .await
        .expect("managed outbound request")
        .message
        .expect("managed request message")
}

async fn acknowledge_bind(session: &FakeSession, endpoint: &str, target_id: &str) {
    dispatch_response(
        &session.shared,
        wire::ConnectResponse {
            message: Some(connect_response::Message::ListenerBound(
                wire::ListenerBound {
                    binding: Some(wire::ListenerBinding {
                        listener_binding_id: format!("managed-binding-{}", session.number),
                        endpoint_pattern: endpoint.into(),
                        target_id: target_id.into(),
                    }),
                },
            )),
        },
    )
    .await
    .expect("dispatch ListenerBound");
}

#[tokio::test]
async fn reconnect_redeclares_current_listener_and_does_not_queue_open() {
    let (client, mut sessions) = harness();
    let mut first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");

    let bind = client.bind("/echo", "server");
    let acknowledge = async {
        assert!(matches!(
            next_message(&mut first.outbound).await,
            connect_request::Message::BindListener(_)
        ));
        acknowledge_bind(&first, "/echo", "server").await;
    };
    let (listener, ()) = tokio::join!(bind, acknowledge);
    let mut listener = listener.expect("managed bind");

    first
        .shared
        .terminate(SessionError::Transport("injected loss".into()));
    let mut state = client.core.state_tx.subscribe();
    while *state.borrow() != ManagedState::Backoff {
        state.changed().await.expect("state transition");
    }
    assert!(matches!(
        client.open("/echo", "server").await,
        Err(ManagedError::NotReady)
    ));

    let mut second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
        .await
        .expect("reconnect timeout")
        .expect("second session");
    assert!(matches!(
        next_message(&mut second.outbound).await,
        connect_request::Message::BindListener(_)
    ));
    acknowledge_bind(&second, "/echo", "server").await;
    client.wait_ready().await.expect("ready after reconnect");
    dispatch_response(
        &second.shared,
        wire::ConnectResponse {
            message: Some(connect_response::Message::ListenerOffer(
                wire::ListenerOffer {
                    attempt_id: "managed-attempt".into(),
                    listener_binding_id: "managed-binding-2".into(),
                    endpoint: "/echo".into(),
                    target_id: "server".into(),
                    caller_session_id: "caller-session".into(),
                },
            )),
        },
    )
    .await
    .expect("dispatch ListenerOffer");
    let offer = listener.next().await.expect("managed next").expect("offer");
    assert_eq!(offer.metadata().attempt_id(), "managed-attempt");
    client.close().await;
}

#[tokio::test]
async fn close_cancels_backoff_and_joins_supervisor() {
    let (client, mut sessions) = harness();
    let first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");
    first
        .shared
        .terminate(SessionError::Transport("injected loss".into()));
    let mut state = client.core.state_tx.subscribe();
    while *state.borrow() != ManagedState::Backoff {
        state.changed().await.expect("state transition");
    }
    tokio::time::timeout(Duration::from_millis(200), client.close())
        .await
        .expect("Close must cancel backoff promptly");
}

#[tokio::test]
async fn cancelled_bind_does_not_leave_a_desired_listener() {
    let (client, mut sessions) = harness();
    let mut first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");
    let mut bind = Box::pin(client.bind("/cancelled", "server"));
    let request = tokio::select! {
        request = next_message(&mut first.outbound) => request,
        _ = &mut bind => panic!("Bind completed before response"),
    };
    assert!(matches!(request, connect_request::Message::BindListener(_)));
    drop(bind);
    assert!(
        client
            .core
            .data
            .lock()
            .expect("managed data lock poisoned")
            .bindings
            .is_empty()
    );
    client.close().await;
}

#[tokio::test]
async fn unbind_during_backoff_is_not_redeclared() {
    let (client, mut sessions) = harness();
    let mut first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");
    let bind = client.bind("/temporary", "server");
    let acknowledge = async {
        assert!(matches!(
            next_message(&mut first.outbound).await,
            connect_request::Message::BindListener(_)
        ));
        acknowledge_bind(&first, "/temporary", "server").await;
    };
    let (listener, ()) = tokio::join!(bind, acknowledge);
    let listener = listener.expect("managed bind");
    first
        .shared
        .terminate(SessionError::Transport("injected loss".into()));
    let mut state = client.core.state_tx.subscribe();
    while *state.borrow() != ManagedState::Backoff {
        state.changed().await.expect("state transition");
    }
    listener.unbind().await.expect("unbind during backoff");
    let mut second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
        .await
        .expect("reconnect timeout")
        .expect("second session");
    client
        .wait_ready()
        .await
        .expect("ready without removed bind");
    assert!(matches!(
        second.outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    client.close().await;
}

#[tokio::test]
async fn protocol_failure_stops_without_reconnect() {
    let (client, mut sessions) = harness();
    let first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");
    first
        .shared
        .terminate(SessionError::Protocol("injected protocol failure"));
    assert!(matches!(client.done().await, ManagedError::Failed(_)));
    assert_eq!(client.state(), ManagedState::Failed);
    assert!(sessions.try_recv().is_err());
    client.close().await;
}

#[tokio::test]
async fn wrapped_protocol_failure_during_rebind_stops_without_reconnect() {
    let (client, mut sessions) = harness();
    let mut first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");

    let bind = client.bind("/rebind", "server");
    let acknowledge = async {
        assert!(matches!(
            next_message(&mut first.outbound).await,
            connect_request::Message::BindListener(_)
        ));
        acknowledge_bind(&first, "/rebind", "server").await;
    };
    let (listener, ()) = tokio::join!(bind, acknowledge);
    let _listener = listener.expect("managed bind");

    first
        .shared
        .terminate(SessionError::Transport("injected loss".into()));
    let mut second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
        .await
        .expect("reconnect timeout")
        .expect("second session");
    let connect_request::Message::BindListener(rebind) = next_message(&mut second.outbound).await
    else {
        panic!("expected rebind request");
    };
    assert_eq!(rebind.endpoint_pattern, "/rebind");
    assert_eq!(rebind.target_id, "server");

    let protocol_error = dispatch_response(
        &second.shared,
        wire::ConnectResponse {
            message: Some(connect_response::Message::ListenerBindFailed(
                wire::ListenerBindFailed {
                    endpoint_pattern: rebind.endpoint_pattern,
                    target_id: rebind.target_id,
                    failure: wire::ListenerBindingFailure::Unspecified as i32,
                },
            )),
        },
    )
    .await
    .expect_err("UNSPECIFIED rebind failure must be protocol-fatal");
    assert!(matches!(protocol_error, SessionError::Protocol(_)));
    second.shared.terminate(protocol_error);

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), client.done())
            .await
            .expect("wrapped rebind protocol failure must stop supervisor"),
        ManagedError::Failed(_)
    ));
    assert_eq!(client.state(), ManagedState::Failed);
    assert!(sessions.try_recv().is_err());
    client.close().await;
}

#[tokio::test]
async fn permanent_rpc_session_failure_stops_without_reconnect() {
    for code in [
        Code::InvalidArgument,
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::FailedPrecondition,
    ] {
        let (client, mut sessions) = harness();
        let first = sessions.recv().await.expect("first session");
        client.wait_ready().await.expect("initial ready");
        first.shared.terminate(SessionError::Rpc {
            code,
            message: "injected permanent failure".into(),
        });
        assert!(matches!(client.done().await, ManagedError::Failed(_)));
        assert_eq!(client.state(), ManagedState::Failed);
        assert!(sessions.try_recv().is_err());
        client.close().await;
    }
}

#[tokio::test]
async fn transient_rpc_session_failure_reconnects() {
    let (client, mut sessions) = harness();
    let first = sessions.recv().await.expect("first session");
    client.wait_ready().await.expect("initial ready");
    first.shared.terminate(SessionError::Rpc {
        code: Code::Unavailable,
        message: "injected transient failure".into(),
    });
    let second = tokio::time::timeout(Duration::from_secs(2), sessions.recv())
        .await
        .expect("reconnect timeout")
        .expect("second session");
    assert_eq!(second.number, 2);
    client.close().await;
}

#[test]
fn binding_retry_classification_matches_operation_contract() {
    for error in [
        BindError::InvalidRequest,
        BindError::CapacityReached,
        BindError::Conflict,
    ] {
        assert!(!retryable_managed_error(&ManagedError::Bind(error)));
    }
    assert!(retryable_managed_error(&ManagedError::Bind(
        BindError::Unavailable,
    )));
}
