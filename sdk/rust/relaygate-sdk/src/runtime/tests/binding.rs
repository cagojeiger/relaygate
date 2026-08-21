use super::*;

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
async fn bind_and_unbind_failures_are_operation_local() {
    let (client, mut outbound) = harness();
    let first_client = Arc::clone(&client);
    let first = tokio::spawn(async move { first_client.bind("conflict", "target").await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::BindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerBindFailed(
            wire::ListenerBindFailed {
                endpoint_pattern: "conflict".into(),
                target_id: "target".into(),
                failure: wire::ListenerBindingFailure::Conflict as i32,
            },
        )),
    )
    .await
    .expect("dispatch operation-local Bind failure");
    assert!(matches!(first.await.unwrap(), Err(BindError::Conflict)));
    assert!(client.shared.terminal().is_none());

    let listener = Arc::new(bind_listener(&client, &mut outbound, "binding-1").await);
    let listener_for_unbind = Arc::clone(&listener);
    let first_unbind = tokio::spawn(async move { listener_for_unbind.unbind().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbindFailed(
            wire::ListenerUnbindFailed {
                listener_binding_id: "binding-1".into(),
                failure: wire::ListenerBindingFailure::Unavailable as i32,
            },
        )),
    )
    .await
    .expect("dispatch operation-local Unbind failure");
    assert!(matches!(
        first_unbind.await.unwrap(),
        Err(UnbindError::Unavailable)
    ));
    assert!(listener.state.active.load(Ordering::Acquire));
    assert!(client.shared.terminal().is_none());

    let listener_for_retry = Arc::clone(&listener);
    let retry = tokio::spawn(async move { listener_for_retry.unbind().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    dispatch_response(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "binding-1".into(),
            },
        )),
    )
    .await
    .expect("dispatch retry Unbind acknowledgement");
    retry.await.unwrap().expect("retry Unbind");
}

#[tokio::test]
async fn exact_retired_binding_responses_are_bounded_noops() {
    let (client, mut outbound) = harness();
    let listener = Arc::new(bind_listener(&client, &mut outbound, "binding-1").await);
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
                listener_binding_id: "binding-1".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(unbind.await.unwrap(), Ok(()));

    for _ in 0..3 {
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerBound(
                wire::ListenerBound {
                    binding: Some(wire::ListenerBinding {
                        listener_binding_id: "binding-1".into(),
                        endpoint_pattern: "service".into(),
                        target_id: "target".into(),
                    }),
                },
            )),
        )
        .await
        .unwrap();
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerUnbound(
                wire::ListenerUnbound {
                    listener_binding_id: "binding-1".into(),
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
}

#[tokio::test]
async fn foreign_binding_responses_fail_closed() {
    let (client, mut outbound) = harness();
    let bind_client = Arc::clone(&client);
    let bind = tokio::spawn(async move { bind_client.bind("current", "target").await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::BindListener(_)
    ));
    let error = dispatch_as_receive_loop(
        &client.shared,
        response(connect_response::Message::ListenerBound(
            wire::ListenerBound {
                binding: Some(wire::ListenerBinding {
                    listener_binding_id: "foreign-binding".into(),
                    endpoint_pattern: "foreign".into(),
                    target_id: "target".into(),
                }),
            },
        )),
    )
    .await;
    assert!(matches!(error, SessionError::Protocol(_)));
    assert!(matches!(
        bind.await.unwrap(),
        Err(BindError::Session(SessionError::Protocol(_)))
    ));

    let (client, mut outbound) = harness();
    let listener = Arc::new(bind_listener(&client, &mut outbound, "binding-1").await);
    let listener_for_unbind = Arc::clone(&listener);
    let unbind = tokio::spawn(async move { listener_for_unbind.unbind().await });
    assert!(matches!(
        next_message(&mut outbound).await,
        connect_request::Message::UnbindListener(_)
    ));
    let error = dispatch_as_receive_loop(
        &client.shared,
        response(connect_response::Message::ListenerUnbound(
            wire::ListenerUnbound {
                listener_binding_id: "foreign-binding".into(),
            },
        )),
    )
    .await;
    assert!(matches!(error, SessionError::Protocol(_)));
    assert!(matches!(
        unbind.await.unwrap(),
        Err(UnbindError::Session(SessionError::Protocol(_)))
    ));
}

#[tokio::test]
async fn retired_binding_metadata_conflict_fails_closed() {
    let (client, mut outbound) = harness();
    let listener = Arc::new(bind_listener(&client, &mut outbound, "binding-1").await);
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
                listener_binding_id: "binding-1".into(),
            },
        )),
    )
    .await
    .unwrap();
    assert_eq!(unbind.await.unwrap(), Ok(()));
    assert!(matches!(
        dispatch_response(
            &client.shared,
            response(connect_response::Message::ListenerBound(
                wire::ListenerBound {
                    binding: Some(wire::ListenerBinding {
                        listener_binding_id: "binding-1".into(),
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
