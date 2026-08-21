use super::*;

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
