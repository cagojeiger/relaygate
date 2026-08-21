use super::*;

#[tokio::test]
async fn pipe_payload_is_bidirectional_and_async_rejection_is_terminal() {
    let (client, mut outbound) = harness();
    let mut pipe = register_pipe(&client.shared, "pipe-1");
    {
        let send = pipe.send(b"caller-to-listener".to_vec());
        tokio::pin!(send);
        let sent = tokio::select! {
            message = next_message(&mut outbound) => match message {
                connect_request::Message::PipePayload(payload) => payload,
                other => panic!("unexpected payload request: {other:?}"),
            },
            result = &mut send => panic!("Send completed before receipt: {result:?}"),
        };
        assert_eq!(sent.pipe_id, "pipe-1");
        assert_eq!(sent.payload, b"caller-to-listener");
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayloadReceived(
                wire::PipePayloadReceived {
                    pipe_id: sent.pipe_id.clone(),
                    payload_id: sent.payload_id.clone(),
                },
            )),
        )
        .await
        .unwrap();
        send.await.unwrap();
    }

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "pipe-1".into(),
            payload_id: "payload-inbound".into(),
            payload: b"listener-to-caller".to_vec(),
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::PipePayloadReceived(received)
            if received.payload_id == "payload-inbound"
    ));
    assert_eq!(pipe.recv().await.unwrap(), b"listener-to-caller");

    let rejected_send = pipe.send(b"rejected".to_vec());
    tokio::pin!(rejected_send);
    let rejected_payload = tokio::select! {
        message = next_message(&mut outbound) => match message {
            connect_request::Message::PipePayload(payload) => payload,
            other => panic!("unexpected payload request: {other:?}"),
        },
        result = &mut rejected_send => panic!("Send completed before rejection: {result:?}"),
    };
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayloadRejected(
            wire::PipePayloadRejected {
                pipe_id: "pipe-1".into(),
                failure: wire::PipePayloadFailure::Backpressure as i32,
                payload_id: rejected_payload.payload_id,
            },
        )),
    )
    .await
    .unwrap();
    let rejection = rejected_send.await.unwrap_err();
    assert_eq!(rejection.outcome(), DeliveryOutcome::Rejected);
    assert_eq!(rejection.failure(), Some(DeliveryFailure::Backpressure));
    assert_eq!(pipe.done().await, PipeError::Backpressure);
}

#[tokio::test]
async fn payload_replay_is_enqueued_once_and_conflicts_fail_closed() {
    let (client, mut outbound) = harness();
    let mut pipe = register_pipe(&client.shared, "pipe-replay");
    let payload = wire::PipePayload {
        pipe_id: "pipe-replay".into(),
        payload_id: "payload-replay".into(),
        payload: b"same".to_vec(),
    };
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayload(payload.clone())),
        )
        .await
        .expect("dispatch exact payload");
        assert!(matches!(
            next_message(&mut outbound).await,
            connect_request::Message::PipePayloadReceived(received)
                if received.payload_id == payload.payload_id
        ));
    }

    let interleaved = wire::PipePayload {
        pipe_id: "pipe-replay".into(),
        payload_id: "payload-interleaved".into(),
        payload: b"second".to_vec(),
    };
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(interleaved.clone())),
    )
    .await
    .expect("dispatch interleaved payload");
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::PipePayloadReceived(received)
            if received.payload_id == interleaved.payload_id
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(payload.clone())),
    )
    .await
    .expect("dispatch non-adjacent exact replay");
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::PipePayloadReceived(received)
            if received.payload_id == payload.payload_id
    ));
    assert_eq!(pipe.recv().await.unwrap(), b"same");
    assert_eq!(pipe.recv().await.unwrap(), b"second");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), pipe.recv())
            .await
            .is_err(),
        "exact replay was enqueued twice"
    );
    let mut conflicting = payload;
    conflicting.payload = b"different".to_vec();
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayload(conflicting)),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));

    for index in 0..=MAX_RECEIVED_PAYLOADS {
        let payload_id = format!("payload-bounded-{index}");
        match pipe
            .state
            .deliver(payload_id.clone(), payload_id.as_bytes().to_vec())
        {
            IncomingPayload::Accepted { permit, payload } => {
                permit.send(payload);
            }
            _ => panic!("bounded delivery {index} was not accepted"),
        }
        assert_eq!(pipe.recv().await.unwrap(), payload_id.as_bytes());
    }
    let received = pipe
        .state
        .received
        .lock()
        .expect("received payload lock poisoned");
    assert_eq!(received.fingerprints.len(), MAX_RECEIVED_PAYLOADS);
    assert_eq!(received.order.len(), MAX_RECEIVED_PAYLOADS);
}

#[tokio::test]
async fn payload_receipt_replay_is_exact_and_cancelled_wait_is_bounded() {
    let (client, mut outbound) = harness();
    let pipe = Arc::new(register_pipe(&client.shared, "pipe-receipt"));
    let sending = Arc::clone(&pipe);
    let send = tokio::spawn(async move { sending.send(b"payload".to_vec()).await });
    let payload = match next_message(&mut outbound).await {
        connect_request::Message::PipePayload(payload) => payload,
        other => panic!("unexpected payload request: {other:?}"),
    };
    let receipt = wire::PipePayloadReceived {
        pipe_id: payload.pipe_id.clone(),
        payload_id: payload.payload_id.clone(),
    };
    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayloadReceived(
                receipt.clone(),
            )),
        )
        .await
        .expect("dispatch exact receipt");
    }
    send.await.unwrap().unwrap();
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayloadRejected(
                wire::PipePayloadRejected {
                    pipe_id: payload.pipe_id.clone(),
                    payload_id: payload.payload_id.clone(),
                    failure: wire::PipePayloadFailure::Backpressure as i32,
                },
            )),
        )
        .await,
        Err(SessionError::Protocol(_))
    ));

    let (cancel_client, mut cancel_outbound) = harness();
    let cancel_pipe = Arc::new(register_pipe(&cancel_client.shared, "pipe-cancelled-send"));
    let sending = Arc::clone(&cancel_pipe);
    let send = tokio::spawn(async move { sending.send(b"payload".to_vec()).await });
    let payload = match next_message(&mut cancel_outbound).await {
        connect_request::Message::PipePayload(payload) => payload,
        other => panic!("unexpected payload request: {other:?}"),
    };
    send.abort();
    let _ = send.await;
    assert_eq!(cancel_pipe.done().await, PipeError::Terminal);
    assert!(matches!(
        next_message(&mut cancel_outbound).await,
        connect_request::Message::ClosePipe(close) if close.pipe_id == payload.pipe_id
    ));
    let cancelled_payload_id = payload.payload_id.clone();
    dispatch_response(
        &cancel_client.shared,
        response(connect_response::Message::PipePayloadReceived(
            wire::PipePayloadReceived {
                pipe_id: payload.pipe_id,
                payload_id: payload.payload_id,
            },
        )),
    )
    .await
    .expect("late exact receipt after cancelled wait");
    let history = cancel_client
        .shared
        .delivery_history
        .lock()
        .expect("delivery history lock poisoned");
    assert!(history.iter().any(|(pipe_id, payload_id, terminal)| {
        pipe_id == "pipe-cancelled-send"
            && payload_id == &cancelled_payload_id
            && *terminal == DeliveryTerminal::Unknown
    }));
    assert!(cancel_client.shared.terminal().is_none());
}

#[tokio::test]
async fn every_payload_rejection_is_pipe_terminal_and_invalid_failure_is_protocol_fatal() {
    for (failure, expected) in [
        (
            wire::PipePayloadFailure::InvalidRequest,
            PipeError::InvalidPayload,
        ),
        (wire::PipePayloadFailure::NotOwned, PipeError::NotOwned),
        (
            wire::PipePayloadFailure::Backpressure,
            PipeError::Backpressure,
        ),
        (
            wire::PipePayloadFailure::Unavailable,
            PipeError::Unavailable,
        ),
    ] {
        let (client, mut outbound) = harness();
        let pipe = Arc::new(register_pipe(&client.shared, "pipe-rejected"));
        let sending = Arc::clone(&pipe);
        let send = tokio::spawn(async move { sending.send(b"payload".to_vec()).await });
        let payload_id = match next_message(&mut outbound).await {
            connect_request::Message::PipePayload(payload) => payload.payload_id,
            other => panic!("unexpected payload request: {other:?}"),
        };
        dispatch_response(
            &client.shared,
            response(connect_response::Message::PipePayloadRejected(
                wire::PipePayloadRejected {
                    pipe_id: "pipe-rejected".into(),
                    failure: failure as i32,
                    payload_id,
                },
            )),
        )
        .await
        .expect("dispatch operation-local payload rejection");
        assert_eq!(
            send.await.unwrap().unwrap_err().outcome(),
            DeliveryOutcome::Rejected
        );
        assert_eq!(pipe.done().await, expected);
        assert!(client.shared.terminal().is_none());
    }

    for failure in [wire::PipePayloadFailure::Unspecified as i32, i32::MAX] {
        let (client, mut outbound) = harness();
        let pipe = Arc::new(register_pipe(&client.shared, "pipe-invalid-failure"));
        let sending = Arc::clone(&pipe);
        let send = tokio::spawn(async move { sending.send(b"payload".to_vec()).await });
        let payload_id = match next_message(&mut outbound).await {
            connect_request::Message::PipePayload(payload) => payload.payload_id,
            other => panic!("unexpected payload request: {other:?}"),
        };
        assert!(matches!(
            dispatch_response(
                &client.shared,
                response(connect_response::Message::PipePayloadRejected(
                    wire::PipePayloadRejected {
                        pipe_id: "pipe-invalid-failure".into(),
                        failure,
                        payload_id,
                    },
                )),
            )
            .await,
            Err(SessionError::Protocol(_))
        ));
        send.abort();
    }

    let (client, mut outbound) = harness();
    let pipe = Arc::new(register_pipe(&client.shared, "pipe-terminal-first"));
    let sending = Arc::clone(&pipe);
    let send = tokio::spawn(async move { sending.send(b"payload".to_vec()).await });
    let payload_id = match next_message(&mut outbound).await {
        connect_request::Message::PipePayload(payload) => payload.payload_id,
        other => panic!("unexpected payload request: {other:?}"),
    };
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipeTerminated(
            wire::PipeTerminated {
                pipe_id: "pipe-terminal-first".into(),
            },
        )),
    )
    .await
    .expect("dispatch first terminal");
    assert_eq!(
        send.await.unwrap().unwrap_err().outcome(),
        DeliveryOutcome::Unknown
    );
    assert_eq!(pipe.done().await, PipeError::Terminal);
    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayloadRejected(
            wire::PipePayloadRejected {
                pipe_id: "pipe-terminal-first".into(),
                failure: wire::PipePayloadFailure::InvalidRequest as i32,
                payload_id,
            },
        )),
    )
    .await
    .expect("absorb exact payload rejection after terminal");
    assert!(client.shared.terminal().is_none());
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
                payload_id: "payload-malformed".into(),
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
            payload_id: "payload-unknown".into(),
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
            payload_id: "payload-retired".into(),
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
                payload_id: format!("payload-{index}"),
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
            payload_id: "payload-overflow".into(),
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
    for index in 0..PIPE_PAYLOAD_CAPACITY {
        assert!(matches!(
            next_message(&mut outbound).await,
            connect_request::Message::PipePayloadReceived(received)
                if received.payload_id == format!("payload-{index}")
        ));
    }
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::PipePayloadRejected(rejected)
            if rejected.payload_id == "payload-overflow"
    ));
    let close = match next_message(&mut outbound).await {
        connect_request::Message::ClosePipe(close) => close,
        other => panic!("unexpected backpressure cleanup request: {other:?}"),
    };
    assert_eq!(close.pipe_id, "congested-pipe");

    dispatch_response(
        &client.shared,
        response(connect_response::Message::PipePayload(wire::PipePayload {
            pipe_id: "healthy-pipe".into(),
            payload_id: "payload-healthy".into(),
            payload: b"still-live".to_vec(),
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::PipePayloadReceived(received)
            if received.payload_id == "payload-healthy"
    ));
    assert_eq!(healthy.recv().await.unwrap(), b"still-live");
    client
        .shared
        .terminalize_pipe(healthy.id(), PipeError::Terminal);
}
