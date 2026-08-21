use super::*;

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
async fn every_listener_decision_rejection_is_exact_and_operation_local() {
    for failure in [
        wire::ListenerDecisionFailure::InvalidRequest,
        wire::ListenerDecisionFailure::AttemptNotPending,
        wire::ListenerDecisionFailure::WrongPhase,
    ] {
        let (client, mut outbound) = harness();
        let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
        let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-rejected").await;
        let untouched = offer(&client, &mut listener, "attempt-untouched").await;
        assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 1);

        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerDecisionRejected(
                wire::ListenerDecisionRejected {
                    attempt_id: "attempt-rejected".into(),
                    failure: failure as i32,
                },
            )),
        )
        .await
        .expect("dispatch operation-local listener decision rejection");

        assert!(matches!(
            accept.await.unwrap(),
            Err(AcceptError::NotPending)
        ));
        let offers = client.shared.offers.lock().unwrap();
        assert!(!offers.contains_key("attempt-rejected"));
        assert!(offers.contains_key("attempt-untouched"));
        drop(offers);
        assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 0);
        assert!(client.shared.terminal().is_none());
        drop(untouched);
        listener.state.active.store(false, Ordering::Release);
    }
}

#[tokio::test]
async fn listener_decision_rejection_cleans_only_its_provisional_pipe() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let other_pipe = register_pipe(&client.shared, "pipe-other");
    let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-rejected").await;
    let untouched_offer = offer(&client, &mut listener, "attempt-untouched").await;

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerEstablished(
            wire::ListenerEstablished {
                attempt_id: "attempt-rejected".into(),
                pipe_id: "pipe-rejected".into(),
            },
        )),
    )
    .await
    .unwrap();
    let confirmed = match next_message(&mut outbound).await {
        connect_request::Message::ListenerConfirmed(confirmed) => confirmed,
        other => panic!("unexpected listener decision: {other:?}"),
    };
    assert_eq!(confirmed.attempt_id, "attempt-rejected");
    assert_eq!(confirmed.pipe_id, "pipe-rejected");
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 2);

    let rejected = wire::ListenerDecisionRejected {
        attempt_id: "attempt-rejected".into(),
        failure: wire::ListenerDecisionFailure::WrongPhase as i32,
    };
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            rejected.clone(),
        )),
    )
    .await
    .expect("dispatch rejection after provisional Pipe registration");

    assert!(matches!(
        accept.await.unwrap(),
        Err(AcceptError::NotPending)
    ));
    {
        let pipes = client.shared.pipes.lock().unwrap();
        assert!(!pipes.contains_key("pipe-rejected"));
        assert!(pipes.contains_key("pipe-other"));
    }
    assert!(
        client
            .shared
            .offers
            .lock()
            .unwrap()
            .contains_key("attempt-untouched")
    );
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 1);
    assert!(other_pipe.state.terminal.borrow().is_none());
    assert!(client.shared.terminal().is_none());

    for _ in 0..2 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerDecisionRejected(
                rejected.clone(),
            )),
        )
        .await
        .expect("exact rejection replay must be idempotent");
    }
    assert!(
        client
            .shared
            .pipes
            .lock()
            .unwrap()
            .contains_key("pipe-other")
    );
    assert!(
        client
            .shared
            .offers
            .lock()
            .unwrap()
            .contains_key("attempt-untouched")
    );
    assert_eq!(client.shared.pipe_slots.load(Ordering::Acquire), 1);
    assert!(other_pipe.state.terminal.borrow().is_none());
    assert!(client.shared.terminal().is_none());

    drop(untouched_offer);
    client
        .shared
        .terminalize_pipe("pipe-other", PipeError::Terminal);
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn invalid_listener_decision_rejection_failures_are_protocol_fatal() {
    for failure in [wire::ListenerDecisionFailure::Unspecified as i32, i32::MAX] {
        let (client, mut outbound) = harness();
        let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
        let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-current").await;

        let error = dispatch_as_receive_loop(
            &client.shared,
            response(connect_response::Message::ListenerDecisionRejected(
                wire::ListenerDecisionRejected {
                    attempt_id: "attempt-current".into(),
                    failure,
                },
            )),
        )
        .await;

        assert!(matches!(error, SessionError::Protocol(_)));
        assert!(matches!(
            accept.await.unwrap(),
            Err(AcceptError::Session(SessionError::Protocol(_)))
        ));
    }
}

#[tokio::test]
async fn invalid_listener_decision_rejection_identities_are_protocol_fatal() {
    for attempt_id in [String::new(), "x".repeat(MAX_IDENTITY_BYTES + 1)] {
        let (client, mut outbound) = harness();
        let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
        let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-current").await;

        let error = dispatch_as_receive_loop(
            &client.shared,
            response(connect_response::Message::ListenerDecisionRejected(
                wire::ListenerDecisionRejected {
                    attempt_id,
                    failure: wire::ListenerDecisionFailure::InvalidRequest as i32,
                },
            )),
        )
        .await;

        assert!(matches!(error, SessionError::Protocol(_)));
        assert!(matches!(
            accept.await.unwrap(),
            Err(AcceptError::Session(SessionError::Protocol(_)))
        ));
    }
}

#[tokio::test]
async fn foreign_listener_decision_rejection_is_protocol_fatal() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-current").await;

    let error = dispatch_as_receive_loop(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-foreign".into(),
                failure: wire::ListenerDecisionFailure::AttemptNotPending as i32,
            },
        )),
    )
    .await;

    assert!(matches!(error, SessionError::Protocol(_)));
    assert!(matches!(
        accept.await.unwrap(),
        Err(AcceptError::Session(SessionError::Protocol(_)))
    ));
}

#[tokio::test]
async fn exact_listener_decision_rejection_replay_is_noop() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-current").await;
    let rejected = wire::ListenerDecisionRejected {
        attempt_id: "attempt-current".into(),
        failure: wire::ListenerDecisionFailure::AttemptNotPending as i32,
    };

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            rejected.clone(),
        )),
    )
    .await
    .expect("dispatch current listener decision rejection");
    assert!(matches!(
        accept.await.unwrap(),
        Err(AcceptError::NotPending)
    ));

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            rejected,
        )),
    )
    .await
    .expect("exact ListenerDecisionRejected replay");
    assert!(client.shared.terminal().is_none());
    assert_eq!(client.shared.offer_history.lock().unwrap().len(), 1);
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn conflicting_listener_decision_rejection_replay_is_protocol_fatal() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-current").await;

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-current".into(),
                failure: wire::ListenerDecisionFailure::AttemptNotPending as i32,
            },
        )),
    )
    .await
    .expect("first ListenerDecisionRejected");
    assert!(matches!(
        accept.await.unwrap(),
        Err(AcceptError::NotPending)
    ));

    let error = dispatch_as_receive_loop(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-current".into(),
                failure: wire::ListenerDecisionFailure::WrongPhase as i32,
            },
        )),
    )
    .await;
    assert!(matches!(error, SessionError::Protocol(_)));
}

#[tokio::test]
async fn listener_offer_cannot_reuse_an_attempt_in_terminal_history() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let accept = start_accept(&client, &mut outbound, &mut listener, "attempt-retired").await;

    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-retired".into(),
                failure: wire::ListenerDecisionFailure::AttemptNotPending as i32,
            },
        )),
    )
    .await
    .expect("first ListenerDecisionRejected");
    assert!(matches!(
        accept.await.unwrap(),
        Err(AcceptError::NotPending)
    ));

    let error = dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerOffer(
            wire::ListenerOffer {
                attempt_id: "attempt-retired".into(),
                listener_binding_id: listener.binding_id().into(),
                endpoint: "service".into(),
                target_id: "target".into(),
                caller_session_id: "caller-new".into(),
            },
        )),
    )
    .await
    .expect_err("ListenerOffer must not reuse a terminal attempt identity");
    assert!(matches!(error, SessionError::Protocol(_)));
    assert!(
        !client
            .shared
            .offers
            .lock()
            .unwrap()
            .contains_key("attempt-retired")
    );
    assert_eq!(client.shared.offer_history.lock().unwrap().len(), 1);
    listener.state.active.store(false, Ordering::Release);
}

#[tokio::test]
async fn listener_terminal_families_cannot_alias_each_other() {
    let (client, _) = harness();
    let failure = wire::ListenerDecisionFailure::AttemptNotPending as i32;

    for (attempt_id, pipe_id) in [
        ("attempt-without-pipe", ""),
        ("attempt-with-pipe", "pipe-rejected"),
    ] {
        client
            .shared
            .remember_decision_rejection(attempt_id.to_owned(), failure);
        assert_eq!(
            client.shared.confirmation_matches(attempt_id, pipe_id),
            Some(false)
        );

        let error = dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerTerminated(
                wire::ListenerTerminated {
                    attempt_id: attempt_id.into(),
                    pipe_id: pipe_id.into(),
                },
            )),
        )
        .await
        .expect_err("ListenerTerminated must not alias decision-rejection history");
        assert!(matches!(error, SessionError::Protocol(_)));
        assert_eq!(
            client
                .shared
                .decision_rejection_matches(attempt_id, failure),
            Some(true)
        );
    }

    client
        .shared
        .remember_confirmation("attempt-retired".into(), String::new());
    assert_eq!(
        client
            .shared
            .decision_rejection_matches("attempt-retired", failure),
        Some(false)
    );
    let error = dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-retired".into(),
                failure,
            },
        )),
    )
    .await
    .expect_err("ListenerDecisionRejected must not alias retired history");
    assert!(matches!(error, SessionError::Protocol(_)));
}

#[test]
fn listener_decision_rejection_history_is_bounded() {
    let (client, _) = harness();
    for index in 0..=MAX_OFFERS {
        client.shared.remember_decision_rejection(
            format!("attempt-{index}"),
            wire::ListenerDecisionFailure::AttemptNotPending as i32,
        );
    }

    let history = client.shared.offer_history.lock().unwrap();
    assert_eq!(history.len(), MAX_OFFERS);
    assert!(
        history
            .iter()
            .all(|(attempt_id, _)| attempt_id != "attempt-0")
    );
    assert!(
        history
            .iter()
            .any(|(attempt_id, _)| attempt_id == "attempt-1")
    );
    assert!(
        history
            .iter()
            .any(|(attempt_id, _)| attempt_id == &format!("attempt-{MAX_OFFERS}"))
    );
}

#[tokio::test]
async fn listener_decision_rejection_requires_an_accept_in_progress() {
    let (client, mut outbound) = harness();
    let mut listener = bind_listener(&client, &mut outbound, "binding-1").await;
    let pending = offer(&client, &mut listener, "attempt-pending").await;

    let error = dispatch_as_receive_loop(
        &client.shared,
        response(connect_response::Message::ListenerDecisionRejected(
            wire::ListenerDecisionRejected {
                attempt_id: "attempt-pending".into(),
                failure: wire::ListenerDecisionFailure::WrongPhase as i32,
            },
        )),
    )
    .await;

    assert!(matches!(error, SessionError::Protocol(_)));
    drop(pending);
}
