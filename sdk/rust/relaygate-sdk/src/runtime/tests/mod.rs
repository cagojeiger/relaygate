use std::sync::{Arc, atomic::AtomicBool};

use tokio::sync::mpsc;

use crate::DeliveryOutcome;

use super::*;

fn harness() -> (Arc<Client>, mpsc::Receiver<wire::ConnectRequest>) {
    let (outbound, receiver) = mpsc::channel(OUTBOUND_CAPACITY);
    let shared = Arc::new(Shared::new(
        outbound,
        Session {
            client_session_id: "session-1".into(),
            client_id: "client-1".into(),
            api_key_id: "key-1".into(),
            auth_revision: "revision-1".into(),
        },
    ));
    (Arc::new(Client { shared }), receiver)
}

fn response(message: connect_response::Message) -> wire::ConnectResponse {
    wire::ConnectResponse {
        message: Some(message),
    }
}

async fn dispatch_as_receive_loop(
    shared: &Arc<Shared>,
    response: wire::ConnectResponse,
) -> SessionError {
    let error = dispatch_response(shared, response)
        .await
        .expect_err("protocol-invalid response must fail dispatch");
    shared.terminate(error.clone());
    error
}

async fn next_message(
    outbound: &mut mpsc::Receiver<wire::ConnectRequest>,
) -> connect_request::Message {
    outbound
        .recv()
        .await
        .expect("outbound request")
        .message
        .expect("request message")
}

async fn bind_listener(
    client: &Arc<Client>,
    outbound: &mut mpsc::Receiver<wire::ConnectRequest>,
    binding_id: &str,
) -> Listener {
    let client_for_bind = Arc::clone(client);
    let bind = tokio::spawn(async move { client_for_bind.bind("service", "target").await });
    assert!(matches!(
        next_message(outbound).await,
        connect_request::Message::BindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: binding_id.into(),
                    endpoint_pattern: "service".into(),
                    target_id: "target".into(),
                }),
            },
        )),
    )
    .await
    .expect("dispatch ListenerBound");
    bind.await.expect("bind task").expect("bind result")
}

async fn offer(client: &Arc<Client>, listener: &mut Listener, attempt_id: &str) -> Offer {
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerOffer(
            wire::ListenerOffer {
                attempt_id: attempt_id.into(),
                listener_binding_id: listener.binding_id().into(),
                endpoint: "service".into(),
                target_id: "target".into(),
                caller_session_id: "caller".into(),
            },
        )),
    )
    .await
    .expect("dispatch ListenerOffer");
    listener.next().await.expect("next offer").expect("offer")
}

async fn start_accept(
    client: &Arc<Client>,
    outbound: &mut mpsc::Receiver<wire::ConnectRequest>,
    listener: &mut Listener,
    attempt_id: &str,
) -> tokio::task::JoinHandle<Result<Pipe, AcceptError>> {
    let offer = offer(client, listener, attempt_id).await;
    let accept = tokio::spawn(async move { offer.accept().await });
    let sent = match next_message(outbound).await {
        connect_request::Message::ListenerAccept(accept) => accept,
        other => panic!("unexpected listener decision: {other:?}"),
    };
    assert_eq!(sent.attempt_id, attempt_id);
    accept
}

fn register_pipe(shared: &Arc<Shared>, pipe_id: &str) -> Pipe {
    assert!(shared.reserve_pipe_slot());
    let reservation = AtomicBool::new(true);
    shared
        .register_pipe(pipe_id, "test-attempt", &reservation)
        .expect("register pipe")
}

async fn assert_invalid_open_terminal_preserves_pending(
    case: &str,
    terminal: impl FnOnce(&wire::Open) -> connect_response::Message,
) {
    let (client, mut outbound) = harness();
    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("service", "target").await });
    let request = match next_message(&mut outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("{case}: unexpected Open request: {other:?}"),
    };
    let pending = client
        .shared
        .opens
        .lock()
        .expect("opens lock poisoned")
        .get(&request.request_id)
        .cloned()
        .expect("pending Open correlation");

    let error = dispatch_response(&client.shared, response(terminal(&request)))
        .await
        .expect_err("invalid Open terminal enum must fail dispatch");
    assert!(
        matches!(error, SessionError::Protocol(_)),
        "{case}: {error:?}"
    );

    {
        let opens = client.shared.opens.lock().expect("opens lock poisoned");
        assert_eq!(opens.len(), 1);
        assert!(
            opens
                .get(&request.request_id)
                .is_some_and(|current| Arc::ptr_eq(current, &pending)),
            "{case}: invalid terminal response replaced or removed pending Open"
        );
    }
    assert!(pending.response.lock().unwrap().is_some());
    assert!(!pending.cancelled.load(Ordering::Acquire));
    assert!(pending.slot_reserved.load(Ordering::Acquire));
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 1);
    assert!(client.shared.open_history.lock().unwrap().is_empty());
    assert!(!open.is_finished(), "{case}: pending Open completed");

    client.shared.terminate(error);
    assert!(matches!(
        open.await.expect("Open task"),
        Err(OpenError::Session(SessionError::Protocol(_)))
    ));
}

mod accept_lifecycle;
mod binding;
mod cancellation;
mod listener_decision;
mod open;
mod payload;
mod pipe;
