use std::sync::{Arc, atomic::AtomicBool};

use tokio::sync::mpsc;

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

fn register_pipe(shared: &Arc<Shared>, pipe_id: &str) -> Pipe {
    assert!(shared.reserve_pipe_slot());
    let reservation = AtomicBool::new(true);
    shared
        .register_pipe(pipe_id, "test-attempt", &reservation)
        .expect("register pipe")
}

#[test]
fn authentication_metadata_and_debug_output_do_not_retain_secret() {
    let config = Config::new("https://relay.example", "client", "key", "super-secret");
    let debug = format!("{config:?}");
    assert!(!debug.contains("super-secret"));
    assert!(debug.contains("[REDACTED]"));

    let (client, _) = harness();
    assert_eq!(client.session().client_id(), "client-1");
    assert_eq!(client.session().api_key_id(), "key-1");
    assert_eq!(client.session().auth_revision(), "revision-1");
}

#[tokio::test]
async fn bind_operations_are_serialized() {
    let (client, mut outbound) = harness();
    let first_client = Arc::clone(&client);
    let first = tokio::spawn(async move { first_client.bind("one", "target-1").await });
    let second_client = Arc::clone(&client);
    let second = tokio::spawn(async move { second_client.bind("two", "target-2").await });

    let first_request = match next_message(&mut outbound).await {
        connect_request::Message::BindListener(bind) => bind,
        other => panic!("unexpected first request: {other:?}"),
    };
    assert!(matches!(
        outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "binding-1".into(),
                    endpoint_pattern: first_request.endpoint_pattern,
                    target_id: first_request.target_id,
                }),
            },
        )),
    )
    .await
    .unwrap();
    let first_listener = first.await.unwrap().unwrap();
    first_listener.state.active.store(false, Ordering::Release);

    let second_request = match next_message(&mut outbound).await {
        connect_request::Message::BindListener(bind) => bind,
        other => panic!("unexpected second request: {other:?}"),
    };
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "binding-2".into(),
                    endpoint_pattern: second_request.endpoint_pattern,
                    target_id: second_request.target_id,
                }),
            },
        )),
    )
    .await
    .unwrap();
    let second_listener = second.await.unwrap().unwrap();
    second_listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn stale_binding_responses_are_bounded_noops_without_consuming_current_operation() {
    let (client, mut outbound) = harness();
    let bind_client = Arc::clone(&client);
    let bind = tokio::spawn(async move { bind_client.bind("current", "target").await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::BindListener(_)
    ));

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "stale-binding".into(),
                    endpoint_pattern: "stale".into(),
                    target_id: "target".into(),
                }),
            },
        )),
    )
    .await
    .unwrap();
    assert!(!bind.is_finished());
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "current-binding".into(),
                    endpoint_pattern: "current".into(),
                    target_id: "target".into(),
                }),
            },
        )),
    )
    .await
    .unwrap();
    let listener = Arc::new(bind.await.unwrap().unwrap());

    let stale_cleanup = match next_message(&mut outbound).await {
        connect_request::Message::UnbindListener(unbind) => unbind,
        other => panic!("unexpected stale cleanup request: {other:?}"),
    };
    assert_eq!(stale_cleanup.listener_binding_id, "stale-binding");
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "stale-binding".into(),
            },
        )),
    )
    .await
    .unwrap();

    let listener_for_unbind = Arc::clone(&listener);
    let unbind = tokio::spawn(async move { listener_for_unbind.unbind().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "older-binding".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(!unbind.is_finished());
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "current-binding".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(unbind.await.unwrap(), Ok(()));

    for _ in 0..3 {
        for binding_id in ["stale-binding", "older-binding", "current-binding"] {
            dispatch_response(
                &client.shared,
                response(connect_response::Message::ListenerUnbound(
                    wire::ListenerUnbound {
                        listener_binding_id: binding_id.into(),
                    },
                )),
            )
            .await
            .unwrap();
        }
    }
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerBound(
                wire::ListenerBound {
                    binding: Some(wire::ListenerBinding {
                        listener_binding_id: "stale-binding".into(),
                        endpoint_pattern: "stale".into(),
                        target_id: "target".into(),
                    }),
                },
            )),
        )
        .await
        .unwrap();
    }
    tokio::task::yield_now().await;
    assert!(matches!(
        outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(client.shared.terminal().is_none());
    assert!(client.shared.retired_bindings.lock().unwrap().len() <= MAX_LISTENERS);
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerBound(
                wire::ListenerBound {
                    binding: Some(wire::ListenerBinding {
                        listener_binding_id: "stale-binding".into(),
                        endpoint_pattern: "conflicting".into(),
                        target_id: "target".into(),
                    }),
                },
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
}

#[tokio::test]
async fn offer_accept_registers_dispatch_before_confirmation_and_waits_for_ack() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let offer = offer(&client, &mut listener, "attempt-1").await;
    let accept = tokio::spawn(async move { offer.accept().await });

    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ListenerAccept(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-1".into(),
                pipe_id: "pipe-1".into(),
            },
        )),
    )
    .await
    .unwrap();
    let confirmed = match next_message(&mut outbound).await {
        connect_request::Message::ListenerConfirmed(confirmed) => confirmed,
        other => panic!("unexpected confirmation request: {other:?}"),
    };
    assert_eq!(confirmed.pipe_id, "pipe-1");
    assert!(client.shared.pipes.lock().unwrap().contains_key("pipe-1"));
    tokio::task::yield_now().await;
    assert!(!accept.is_finished());

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: "attempt-1".into(),
                pipe_id: "pipe-1".into(),
            },
        )),
    )
    .await
    .unwrap();
    let pipe = accept.await.unwrap().unwrap();
    assert_eq!(pipe.id(), "pipe-1");
    listener.state.active.store(false, Ordering::Release);
    client
        .shared
        .terminalize_pipe("pipe-1", PipeError::Terminal);
}

#[tokio::test]
async fn cancelled_provisional_accept_confirms_then_closes_late_pipe() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let offer = offer(&client, &mut listener, "attempt-late").await;
    let accept = tokio::spawn(async move { offer.accept().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ListenerAccept(_)
    ));
    accept.abort();
    assert!(accept.await.unwrap_err().is_cancelled());

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-late".into(),
                pipe_id: "pipe-late".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ListenerConfirmed(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: "attempt-late".into(),
                pipe_id: "pipe-late".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ClosePipe(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeCloseAcknowledged(
            wire::PipeCloseAcknowledged {
                pipe_id: "pipe-late".into(),
                owned: true,
            },
        )),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(
        !client
            .shared
            .pipes
            .lock()
            .unwrap()
            .contains_key("pipe-late")
    );
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn confirmation_ack_and_listener_terminal_are_ordered_without_resurrection() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;

    let first_offer = offer(&client, &mut listener, "attempt-a").await;
    let first_accept = tokio::spawn(async move { first_offer.accept().await });
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-a".into(),
                pipe_id: "pipe-a".into(),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: "attempt-a".into(),
                pipe_id: "pipe-a".into(),
            },
        )),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerConfirmationAcknowledged(
                wire::ListenerConfirmationAcknowledged {
                    attempt_id: "attempt-a".into(),
                    pipe_id: "pipe-a".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerTerminated(
            wire::ListenerTerminated {
                attempt_id: "attempt-a".into(),
                pipe_id: "pipe-a".into(),
            },
        )),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerTerminated(
                wire::ListenerTerminated {
                    attempt_id: "attempt-a".into(),
                    pipe_id: "pipe-a".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    let first_pipe = first_accept.await.unwrap().unwrap();
    assert_eq!(first_pipe.done().await, PipeError::Terminal);

    let second_offer = offer(&client, &mut listener, "attempt-b").await;
    let second_accept = tokio::spawn(async move { second_offer.accept().await });
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-b".into(),
                pipe_id: "pipe-b".into(),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerTerminated(
            wire::ListenerTerminated {
                attempt_id: "attempt-b".into(),
                pipe_id: "pipe-b".into(),
            },
        )),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerTerminated(
                wire::ListenerTerminated {
                    attempt_id: "attempt-b".into(),
                    pipe_id: "pipe-b".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: "attempt-b".into(),
                pipe_id: "pipe-b".into(),
            },
        )),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerConfirmationAcknowledged(
                wire::ListenerConfirmationAcknowledged {
                    attempt_id: "attempt-b".into(),
                    pipe_id: "pipe-b".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        second_accept.await.unwrap().unwrap_err(),
        AcceptError::NotPending
    );
    assert!(client.shared.terminal().is_none());
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerConfirmationAcknowledged(
                wire::ListenerConfirmationAcknowledged {
                    attempt_id: "attempt-b".into(),
                    pipe_id: "different-pipe".into(),
                },
            ),),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn confirmation_ack_and_pipe_terminal_are_ordered_without_resurrection() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-pipe-order").await;

    let terminal_first_offer = offer(&client, &mut listener, "attempt-terminal-first").await;
    let terminal_first = tokio::spawn(async move { terminal_first_offer.accept().await });
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-terminal-first".into(),
                pipe_id: "pipe-terminal-first".into(),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_message(&mut outbound).await;
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeTerminated(
                wire::PipeTerminated {
                    pipe_id: "pipe-terminal-first".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerConfirmationAcknowledged(
                wire::ListenerConfirmationAcknowledged {
                    attempt_id: "attempt-terminal-first".into(),
                    pipe_id: "pipe-terminal-first".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        terminal_first.await.unwrap().unwrap_err(),
        AcceptError::NotPending
    );

    let ack_first_offer = offer(&client, &mut listener, "attempt-ack-first").await;
    let ack_first = tokio::spawn(async move { ack_first_offer.accept().await });
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-ack-first".into(),
                pipe_id: "pipe-ack-first-offer".into(),
            },
        )),
    )
    .await
    .unwrap();
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: "attempt-ack-first".into(),
                pipe_id: "pipe-ack-first-offer".into(),
            },
        )),
    )
    .await
    .unwrap();
    let pipe = ack_first.await.unwrap().unwrap();
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeTerminated(
                wire::PipeTerminated {
                    pipe_id: "pipe-ack-first-offer".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerConfirmationAcknowledged(
                wire::ListenerConfirmationAcknowledged {
                    attempt_id: "attempt-ack-first".into(),
                    pipe_id: "pipe-ack-first-offer".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    assert_eq!(pipe.done().await, PipeError::Terminal);
    assert!(client.shared.terminal().is_none());
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn concurrent_open_outcomes_preserve_cancelled_vs_unknown() {
    let (client, mut outbound) = harness();
    let first_client = Arc::clone(&client);
    let first = tokio::spawn(async move { first_client.open("one", "target-1").await });
    let second_client = Arc::clone(&client);
    let second = tokio::spawn(async move { second_client.open("two", "target-2").await });

    let mut requests = HashMap::new();
    for _ in 0..2 {
        let open = match next_message(&mut outbound).await {
            connect_request::Message::Open(open) => open,
            other => panic!("unexpected Open request: {other:?}"),
        };
        requests.insert(open.endpoint.clone(), open);
    }
    let second_request = requests.remove("two").unwrap();
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeOpenUnknown(
            wire::PipeOpenUnknown {
                request_id: second_request.request_id,
                endpoint: second_request.endpoint,
                target_id: second_request.target_id,
            },
        )),
    )
    .await
    .unwrap();
    let first_request = requests.remove("one").unwrap();
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeOpenFailed(
            wire::PipeOpenFailed {
                request_id: first_request.request_id,
                endpoint: first_request.endpoint,
                target_id: first_request.target_id,
                failure: wire::OpenFailure::Cancelled as i32,
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(first.await.unwrap().unwrap_err(), OpenError::Cancelled);
    assert_eq!(second.await.unwrap().unwrap_err(), OpenError::Unknown);
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn open_terminal_replays_remain_noops_and_conflicts_fail_closed() {
    let (opened_client, mut opened_outbound) = harness();
    let open_client = Arc::clone(&opened_client);
    let open = tokio::spawn(async move { open_client.open("opened", "target").await });
    let request = match next_message(&mut opened_outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    let opened = wire::PipeOpened {
        request_id: request.request_id,
        attempt_id: "attempt-opened".into(),
        pipe_id: "pipe-opened".into(),
        endpoint: request.endpoint,
        target_id: request.target_id,
    };
    dispatch_response(
        &opened_client.shared,
        response(connect_response::Message::PipeOpened(opened.clone())),
    )
    .await
    .unwrap();
    let pipe = open.await.unwrap().unwrap();
    for _ in 0..3 {
        dispatch_response(
            &opened_client.shared,
            response(connect_response::Message::PipeOpened(opened.clone())),
        )
        .await
        .unwrap();
    }
    let mut conflicting_opened = opened;
    conflicting_opened.pipe_id = "different-pipe".into();
    assert!(matches!(
        dispatch_response(
            &opened_client.shared,
            response(connect_response::Message::PipeOpened(conflicting_opened)),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
    opened_client
        .shared
        .terminalize_pipe(pipe.id(), PipeError::Terminal);

    let (failed_client, mut failed_outbound) = harness();
    let open_client = Arc::clone(&failed_client);
    let open = tokio::spawn(async move { open_client.open("failed", "target").await });
    let request = match next_message(&mut failed_outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    let failed = wire::PipeOpenFailed {
        request_id: request.request_id,
        endpoint: request.endpoint,
        target_id: request.target_id,
        failure: wire::OpenFailure::Unavailable as i32,
    };
    dispatch_response(
        &failed_client.shared,
        response(connect_response::Message::PipeOpenFailed(failed.clone())),
    )
    .await
    .unwrap();
    assert_eq!(
        open.await.unwrap().unwrap_err(),
        OpenError::Failed(OpenFailure::Unavailable)
    );
    for _ in 0..3 {
        dispatch_response(
            &failed_client.shared,
            response(connect_response::Message::PipeOpenFailed(failed.clone())),
        )
        .await
        .unwrap();
    }
    let mut conflicting_failed = failed;
    conflicting_failed.failure = wire::OpenFailure::RouteNotFound as i32;
    assert!(matches!(
        dispatch_response(
            &failed_client.shared,
            response(connect_response::Message::PipeOpenFailed(
                conflicting_failed
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));

    let (unknown_client, mut unknown_outbound) = harness();
    let open_client = Arc::clone(&unknown_client);
    let open = tokio::spawn(async move { open_client.open("unknown", "target").await });
    let request = match next_message(&mut unknown_outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    let unknown = wire::PipeOpenUnknown {
        request_id: request.request_id,
        endpoint: request.endpoint,
        target_id: request.target_id,
    };
    dispatch_response(
        &unknown_client.shared,
        response(connect_response::Message::PipeOpenUnknown(unknown.clone())),
    )
    .await
    .unwrap();
    assert_eq!(open.await.unwrap().unwrap_err(), OpenError::Unknown);
    for _ in 0..3 {
        dispatch_response(
            &unknown_client.shared,
            response(connect_response::Message::PipeOpenUnknown(unknown.clone())),
        )
        .await
        .unwrap();
    }
    let mut conflicting_unknown = unknown;
    conflicting_unknown.endpoint = "different-endpoint".into();
    assert!(matches!(
        dispatch_response(
            &unknown_client.shared,
            response(connect_response::Message::PipeOpenUnknown(
                conflicting_unknown,
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));

    let (rejected_client, mut rejected_outbound) = harness();
    let open_client = Arc::clone(&rejected_client);
    let open = tokio::spawn(async move { open_client.open("rejected", "target").await });
    let request = match next_message(&mut rejected_outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    let rejected = wire::OpenRequestRejected {
        request_id: request.request_id,
        failure: wire::OpenRequestFailure::DuplicateInFlight as i32,
    };
    dispatch_response(
        &rejected_client.shared,
        response(connect_response::Message::OpenRequestRejected(
            rejected.clone(),
        )),
    )
    .await
    .unwrap();
    assert_eq!(
        open.await.unwrap().unwrap_err(),
        OpenError::DuplicateInFlight
    );
    for _ in 0..3 {
        dispatch_response(
            &rejected_client.shared,
            response(connect_response::Message::OpenRequestRejected(
                rejected.clone(),
            )),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cancelled_open_closes_a_late_opened_pipe() {
    let (client, mut outbound) = harness();
    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("service", "target").await });
    let request = match next_message(&mut outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected request: {other:?}"),
    };
    open.abort();
    assert!(open.await.unwrap_err().is_cancelled());
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::CancelOpen(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeOpened(wire::PipeOpened {
            request_id: request.request_id,
            attempt_id: "attempt".into(),
            pipe_id: "late-pipe".into(),
            endpoint: request.endpoint,
            target_id: request.target_id,
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ClosePipe(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeCloseAcknowledged(
            wire::PipeCloseAcknowledged {
                pipe_id: "late-pipe".into(),
                owned: true,
            },
        )),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(
        !client
            .shared
            .pipes
            .lock()
            .unwrap()
            .contains_key("late-pipe")
    );
}

#[tokio::test]
async fn cancel_acknowledgements_require_exact_requested_identity_and_fingerprint() {
    let (client, mut outbound) = harness();
    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("cancelled", "target").await });
    let request = match next_message(&mut outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    open.abort();
    assert!(open.await.unwrap_err().is_cancelled());
    let cancel = match next_message(&mut outbound).await {
        connect_request::Message::CancelOpen(cancel) => cancel,
        other => panic!("unexpected cancel request: {other:?}"),
    };
    assert_eq!(cancel.request_id, request.request_id);

    let acknowledged = wire::OpenCancelAcknowledged {
        request_id: request.request_id.clone(),
        was_pending: true,
    };
    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::OpenCancelAcknowledged(
                acknowledged.clone(),
            )),
        )
        .await
        .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeOpenFailed(
            wire::PipeOpenFailed {
                request_id: request.request_id,
                endpoint: request.endpoint,
                target_id: request.target_id,
                failure: wire::OpenFailure::Cancelled as i32,
            },
        )),
    )
    .await
    .unwrap();
    dispatch_response(
        &client.shared,
        response(connect_response::Message::OpenCancelAcknowledged(
            acknowledged.clone(),
        )),
    )
    .await
    .unwrap();
    assert!(client.shared.opens.lock().unwrap().is_empty());
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);

    let mut conflicting = acknowledged;
    conflicting.was_pending = false;
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::OpenCancelAcknowledged(
                conflicting,
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::OpenCancelAcknowledged(
                wire::OpenCancelAcknowledged {
                    request_id: "foreign-request".into(),
                    was_pending: false,
                },
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
}

#[tokio::test]
async fn pipe_payload_is_bidirectional_and_async_rejection_is_terminal() {
    let (client, mut outbound) = harness();
    let mut pipe = register_pipe(&client.shared, "pipe-1");
    pipe.send(b"caller-to-listener".to_vec()).await.unwrap();
    let sent = match next_message(&mut outbound).await {
        connect_request::Message::PipePayload(payload) => payload,
        other => panic!("unexpected payload request: {other:?}"),
    };
    assert_eq!(sent.pipe_id, "pipe-1");
    assert_eq!(sent.payload, b"caller-to-listener");

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "pipe-1".into(),
            payload: b"listener-to-caller".to_vec(),
        })),
    )
    .await
    .unwrap();
    assert_eq!(pipe.recv().await.unwrap(), b"listener-to-caller");

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayloadRejected(
            wire::PipePayloadRejected {
                pipe_id: "pipe-1".into(),
                failure: wire::PipePayloadFailure::Backpressure as i32,
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(pipe.done().await, PipeError::Backpressure);
}

#[tokio::test]
async fn malformed_unknown_and_retired_pipe_payloads_fail_the_session_closed() {
    let malformed = [
        ("", b"payload".to_vec()),
        (&"x".repeat(MAX_IDENTITY_BYTES + 1), b"payload".to_vec()),
        ("pipe-empty", Vec::new()),
        ("pipe-oversize", vec![0; MAX_PAYLOAD_BYTES + 1]),
    ];
    for (pipe_id, payload) in malformed {
        let (client, _) = harness();
        let error = dispatch_as_receive_loop(
            &client.shared,
            response(connect_response::Message::PipePayload(wire::PipePayload {
                pipe_id: pipe_id.into(),
                payload,
            })),
        )
        .await;
        assert_eq!(error, SessionError::Protocol("invalid PipePayload"));
        assert_eq!(client.shared.terminal(), Some(error));
    }

    let (unknown_client, _) = harness();
    let error = dispatch_as_receive_loop(
        &unknown_client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "unknown-pipe".into(),
            payload: b"payload".to_vec(),
        })),
    )
    .await;
    assert_eq!(error, SessionError::Protocol("foreign PipePayload"));
    assert_eq!(unknown_client.shared.terminal(), Some(error));

    let (retired_client, _) = harness();
    let retired = register_pipe(&retired_client.shared, "retired-pipe");
    retired_client
        .shared
        .terminalize_pipe(retired.id(), PipeError::Terminal);
    let error = dispatch_as_receive_loop(
        &retired_client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "retired-pipe".into(),
            payload: b"payload".to_vec(),
        })),
    )
    .await;
    assert_eq!(error, SessionError::Protocol("foreign PipePayload"));
    assert_eq!(retired_client.shared.terminal(), Some(error));
}

#[tokio::test]
async fn live_pipe_payload_backpressure_closes_only_that_pipe() {
    let (client, mut outbound) = harness();
    let mut congested = register_pipe(&client.shared, "congested-pipe");
    let mut healthy = register_pipe(&client.shared, "healthy-pipe");

    for index in 0..PIPE_PAYLOAD_CAPACITY {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayload(wire::PipePayload {
                pipe_id: "congested-pipe".into(),
                payload: vec![index as u8],
            })),
        )
        .await
        .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "congested-pipe".into(),
            payload: b"overflow".to_vec(),
        })),
    )
    .await
    .unwrap();

    assert_eq!(congested.done().await, PipeError::Backpressure);
    assert_eq!(congested.recv().await.unwrap_err(), PipeError::Backpressure);
    assert!(client.shared.terminal().is_none());
    assert!(
        client
            .shared
            .pipes
            .lock()
            .unwrap()
            .contains_key("healthy-pipe")
    );
    let close = match next_message(&mut outbound).await {
        connect_request::Message::ClosePipe(close) => close,
        other => panic!("unexpected backpressure cleanup request: {other:?}"),
    };
    assert_eq!(close.pipe_id, "congested-pipe");

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "healthy-pipe".into(),
            payload: b"still-live".to_vec(),
        })),
    )
    .await
    .unwrap();
    assert_eq!(healthy.recv().await.unwrap(), b"still-live");
    client
        .shared
        .terminalize_pipe(healthy.id(), PipeError::Terminal);
}

#[tokio::test]
async fn duplicate_close_preserves_first_waiter_and_terminal_order_is_idempotent() {
    let (client, mut outbound) = harness();
    let pipe = Arc::new(register_pipe(&client.shared, "pipe-close"));
    let first_pipe = Arc::clone(&pipe);
    let first = tokio::spawn(async move { first_pipe.close().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ClosePipe(_)
    ));
    assert_eq!(
        pipe.send(b"payload-after-close".to_vec()).await,
        Err(PipeError::Terminal)
    );
    assert!(matches!(
        outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(pipe.close().await.unwrap_err(), CloseError::AlreadyPending);

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeTerminated(
            wire::PipeTerminated {
                pipe_id: "pipe-close".into(),
            },
        )),
    )
    .await
    .unwrap();
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeCloseAcknowledged(
            wire::PipeCloseAcknowledged {
                pipe_id: "pipe-close".into(),
                owned: true,
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(first.await.unwrap(), Ok(()));

    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeCloseAcknowledged(
                wire::PipeCloseAcknowledged {
                    pipe_id: "pipe-close".into(),
                    owned: true,
                },
            )),
        )
        .await
        .unwrap();
    }

    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeTerminated(
                wire::PipeTerminated {
                    pipe_id: "pipe-close".into(),
                },
            )),
        )
        .await
        .unwrap();
    }
    assert!(client.shared.terminal().is_none());

    let second = Arc::new(register_pipe(&client.shared, "pipe-ack-first"));
    let close_pipe = Arc::clone(&second);
    let close = tokio::spawn(async move { close_pipe.close().await });
    let _ = next_message(&mut outbound).await;
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeCloseAcknowledged(
            wire::PipeCloseAcknowledged {
                pipe_id: "pipe-ack-first".into(),
                owned: true,
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(close.await.unwrap(), Ok(()));
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeCloseAcknowledged(
                wire::PipeCloseAcknowledged {
                    pipe_id: "pipe-ack-first".into(),
                    owned: true,
                },
            )),
        )
        .await
        .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeTerminated(
            wire::PipeTerminated {
                pipe_id: "pipe-ack-first".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(client.shared.terminal().is_none());
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipeCloseAcknowledged(
                wire::PipeCloseAcknowledged {
                    pipe_id: "pipe-ack-first".into(),
                    owned: false,
                },
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));
}

#[tokio::test]
async fn reservations_and_transport_shutdown_are_bounded_and_terminal() {
    let (client, mut outbound) = harness();
    for _ in 0..MAX_PIPES {
        assert!(client.shared.reserve_pipe_slot());
    }
    assert!(!client.shared.reserve_pipe_slot());
    for _ in 0..MAX_PIPES {
        client.shared.release_pipe_slot();
    }

    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("service", "target").await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::Open(_)
    ));
    client
        .shared
        .terminate(SessionError::Transport("injected loss".into()));
    assert_eq!(
        open.await.unwrap().unwrap_err(),
        OpenError::Session(SessionError::Transport("injected loss".into()))
    );
    assert!(client.shared.opens.lock().unwrap().is_empty());
    assert!(client.shared.pipes.lock().unwrap().is_empty());
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
    assert_eq!(
        client.done().await,
        SessionError::Transport("injected loss".into())
    );
}

#[tokio::test]
async fn max_pipes_is_shared_by_pending_open_provisional_accept_and_live_pipe() {
    let (client, mut outbound) = harness();
    let live_pipe = register_pipe(&client.shared, "live-pipe");
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 1);

    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("service", "target").await });
    let open_request = match next_message(&mut outbound).await {
        connect_request::Message::Open(open) => open,
        other => panic!("unexpected Open request: {other:?}"),
    };
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 2);

    let mut listener = bind_listener(&client, &mut outbound, "binding-cap").await;
    let offer = offer(&client, &mut listener, "attempt-cap").await;
    let accept = tokio::spawn(async move { offer.accept().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ListenerAccept(_)
    ));
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 3);

    for _ in 3..MAX_PIPES {
        assert!(client.shared.reserve_pipe_slot());
    }
    assert_eq!(
        client.open("overflow", "target").await.unwrap_err(),
        OpenError::CapacityReached
    );
    assert!(matches!(
        outbound.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerTerminated(
            wire::ListenerTerminated {
                attempt_id: "attempt-cap".into(),
                pipe_id: String::new(),
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(accept.await.unwrap().unwrap_err(), AcceptError::NotPending);
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeOpenFailed(
            wire::PipeOpenFailed {
                request_id: open_request.request_id,
                endpoint: open_request.endpoint,
                target_id: open_request.target_id,
                failure: wire::OpenFailure::Unavailable as i32,
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(
        open.await.unwrap().unwrap_err(),
        OpenError::Failed(OpenFailure::Unavailable)
    );
    client
        .shared
        .terminalize_pipe(live_pipe.id(), PipeError::Terminal);
    for _ in 3..MAX_PIPES {
        client.shared.release_pipe_slot();
    }
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn cancelled_confirmation_send_is_taken_over_and_closes_without_orphaning() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-confirm-cancel").await;
    let offer = offer(&client, &mut listener, "attempt-confirm-cancel").await;
    let accept = tokio::spawn(async move { offer.accept().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ListenerAccept(_)
    ));

    for _ in 0..OUTBOUND_CAPACITY {
        client
            .shared
            .outbound
            .try_send(request(connect_request::Message::CancelOpen(
                wire::CancelOpen {
                    request_id: "filler".into(),
                },
            )))
            .unwrap();
    }
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-confirm-cancel".into(),
                pipe_id: "pipe-confirm-cancel".into(),
            },
        )),
    )
    .await
    .unwrap();
    while !client
        .shared
        .pipes
        .lock()
        .unwrap()
        .contains_key("pipe-confirm-cancel")
    {
        tokio::task::yield_now().await;
    }
    accept.abort();
    assert!(accept.await.unwrap_err().is_cancelled());

    let mut confirmation = None;
    for _ in 0..=OUTBOUND_CAPACITY {
        match next_message(&mut outbound).await {
            connect_request::Message::ListenerConfirmed(confirmed) => {
                confirmation = Some(confirmed);
                break;
            }
            connect_request::Message::CancelOpen(_) => {}
            other => panic!("unexpected cleanup request: {other:?}"),
        }
    }
    let confirmation = confirmation.expect("cleanup confirmation");
    assert_eq!(confirmation.attempt_id, "attempt-confirm-cancel");
    assert_eq!(confirmation.pipe_id, "pipe-confirm-cancel");
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerConfirmationAcknowledged(
            wire::ListenerConfirmationAcknowledged {
                attempt_id: confirmation.attempt_id,
                pipe_id: confirmation.pipe_id,
            },
        )),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::ClosePipe(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeCloseAcknowledged(
            wire::PipeCloseAcknowledged {
                pipe_id: "pipe-confirm-cancel".into(),
                owned: true,
            },
        )),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(client.shared.pipes.lock().unwrap().is_empty());
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn pre_send_open_cancellation_releases_exact_correlation_and_slot() {
    let (client, mut outbound) = harness();
    for _ in 0..OUTBOUND_CAPACITY {
        client
            .shared
            .outbound
            .try_send(request(connect_request::Message::CancelOpen(
                wire::CancelOpen {
                    request_id: "filler".into(),
                },
            )))
            .unwrap();
    }
    let open_client = Arc::clone(&client);
    let open = tokio::spawn(async move { open_client.open("blocked", "target").await });
    while client.shared.opens.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    open.abort();
    assert!(open.await.unwrap_err().is_cancelled());
    assert!(client.shared.opens.lock().unwrap().is_empty());
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
    assert!(client.shared.terminal().is_none());
    for _ in 0..OUTBOUND_CAPACITY {
        assert!(matches!(
            next_message(&mut outbound).await,
            connect_request::Message::CancelOpen(_)
        ));
    }
}

#[tokio::test]
async fn cancelled_binding_operations_clear_or_finish_the_exact_pending_operation() {
    let (client, mut outbound) = harness();
    for _ in 0..OUTBOUND_CAPACITY {
        client
            .shared
            .outbound
            .try_send(request(connect_request::Message::CancelOpen(
                wire::CancelOpen {
                    request_id: "filler".into(),
                },
            )))
            .unwrap();
    }
    let pre_send_client = Arc::clone(&client);
    let pre_send_bind =
        tokio::spawn(async move { pre_send_client.bind("blocked", "target").await });
    while client.shared.binding_pending.lock().unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    pre_send_bind.abort();
    assert!(matches!(pre_send_bind.await, Err(error) if error.is_cancelled()));
    assert!(client.shared.binding_pending.lock().unwrap().is_none());
    for _ in 0..OUTBOUND_CAPACITY {
        let _ = outbound.recv().await.unwrap();
    }

    let post_send_client = Arc::clone(&client);
    let post_send_bind = tokio::spawn(async move { post_send_client.bind("late", "target").await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::BindListener(_)
    ));
    post_send_bind.abort();
    assert!(matches!(post_send_bind.await, Err(error) if error.is_cancelled()));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "late-binding".into(),
                    endpoint_pattern: "late".into(),
                    target_id: "target".into(),
                }),
            },
        )),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "late-binding".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(
        !client
            .shared
            .listeners
            .lock()
            .unwrap()
            .contains_key("late-binding")
    );

    let listener = Arc::new(bind_listener(&client, &mut outbound, "kept-binding").await);
    for _ in 0..OUTBOUND_CAPACITY {
        client
            .shared
            .outbound
            .try_send(request(connect_request::Message::CancelOpen(
                wire::CancelOpen {
                    request_id: "filler".into(),
                },
            )))
            .unwrap();
    }
    let pre_send_listener = Arc::clone(&listener);
    let pre_send_unbind = tokio::spawn(async move { pre_send_listener.unbind().await });
    while client.shared.binding_pending.lock().unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    pre_send_unbind.abort();
    assert!(pre_send_unbind.await.unwrap_err().is_cancelled());
    assert!(client.shared.binding_pending.lock().unwrap().is_none());
    assert!(listener.state.active.load(Ordering::Acquire));
    assert!(
        client
            .shared
            .listeners
            .lock()
            .unwrap()
            .contains_key("kept-binding")
    );
    for _ in 0..OUTBOUND_CAPACITY {
        let _ = outbound.recv().await.unwrap();
    }

    let post_send_listener = Arc::clone(&listener);
    let post_send_unbind = tokio::spawn(async move { post_send_listener.unbind().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    post_send_unbind.abort();
    assert!(post_send_unbind.await.unwrap_err().is_cancelled());
    assert!(!listener.state.active.load(Ordering::Acquire));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "kept-binding".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert!(client.shared.binding_pending.lock().unwrap().is_none());
    assert!(
        !client
            .shared
            .listeners
            .lock()
            .unwrap()
            .contains_key("kept-binding")
    );
    assert!(client.shared.terminal().is_none());
}

#[test]
fn dropping_public_handles_outside_a_runtime_never_panics() {
    let (client, mut outbound) = harness();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime");
    let (listener, offer) = runtime.block_on(async {
        let mut listener = bind_listener(&client, &mut outbound, "binding-drop").await;
        let offer = offer(&client, &mut listener, "attempt-drop").await;
        (listener, offer)
    });
    drop(runtime);

    drop(offer);
    assert!(matches!(
        outbound.try_recv().expect("drop rejection").message,
        Some(connect_request::Message::ListenerReject(_))
    ));
    drop(listener);
    assert!(matches!(
        outbound.try_recv().expect("drop unbind").message,
        Some(connect_request::Message::UnbindListener(_))
    ));
    assert!(client.shared.terminal().is_none());
}
