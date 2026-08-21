use super::*;

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
