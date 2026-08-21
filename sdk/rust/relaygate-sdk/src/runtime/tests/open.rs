use super::*;

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
async fn invalid_pipe_open_failed_enums_preserve_pending_open_until_protocol_termination() {
    for (case, failure) in [
        ("unspecified", wire::OpenFailure::Unspecified as i32),
        ("unknown", i32::MAX),
    ] {
        assert_invalid_open_terminal_preserves_pending(case, |request| {
            connect_response::Message::PipeOpenFailed(wire::PipeOpenFailed {
                request_id: request.request_id.clone(),
                endpoint: request.endpoint.clone(),
                target_id: request.target_id.clone(),
                failure,
            })
        })
        .await;
    }
}

#[tokio::test]
async fn invalid_open_request_rejected_enums_preserve_pending_open_until_protocol_termination() {
    for (case, failure) in [
        ("unspecified", wire::OpenRequestFailure::Unspecified as i32),
        ("unknown_non_duplicate", i32::MAX),
    ] {
        assert_invalid_open_terminal_preserves_pending(case, |request| {
            connect_response::Message::OpenRequestRejected(wire::OpenRequestRejected {
                request_id: request.request_id.clone(),
                failure,
            })
        })
        .await;
    }
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
