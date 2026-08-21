use super::*;

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
        pipe.send(b"payload-after-close".to_vec())
            .await
            .unwrap_err()
            .outcome(),
        DeliveryOutcome::NotSent
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
