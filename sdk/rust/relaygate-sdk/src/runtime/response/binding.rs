use super::*;

pub(super) fn binding_failure(failure: i32) -> Option<wire::ListenerBindingFailure> {
    match wire::ListenerBindingFailure::try_from(failure).ok()? {
        wire::ListenerBindingFailure::InvalidRequest => {
            Some(wire::ListenerBindingFailure::InvalidRequest)
        }
        wire::ListenerBindingFailure::CapacityReached => {
            Some(wire::ListenerBindingFailure::CapacityReached)
        }
        wire::ListenerBindingFailure::Conflict => Some(wire::ListenerBindingFailure::Conflict),
        wire::ListenerBindingFailure::Unavailable => {
            Some(wire::ListenerBindingFailure::Unavailable)
        }
        wire::ListenerBindingFailure::Unspecified => None,
    }
}

pub(super) fn listener_bind_failed(
    shared: &Arc<Shared>,
    failed: wire::ListenerBindFailed,
) -> Result<(), SessionError> {
    let Some(failure) = binding_failure(failed.failure) else {
        return Err(SessionError::Protocol(
            "ListenerBindFailed contained invalid failure",
        ));
    };
    if !valid_text(&failed.endpoint_pattern, MAX_ENDPOINT_BYTES)
        || !valid_text(&failed.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerBindFailed contained invalid identity",
        ));
    }
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Bind { endpoint_pattern, target_id, .. })
                if endpoint_pattern == &failed.endpoint_pattern && target_id == &failed.target_id
        );
        matches_current.then(|| pending.take().expect("matching Bind must exist"))
    };
    let Some(BindingPending::Bind { response, .. }) = pending else {
        return Err(SessionError::Protocol("foreign ListenerBindFailed"));
    };
    let error = match failure {
        wire::ListenerBindingFailure::InvalidRequest => BindError::InvalidRequest,
        wire::ListenerBindingFailure::CapacityReached => BindError::CapacityReached,
        wire::ListenerBindingFailure::Conflict => BindError::Conflict,
        wire::ListenerBindingFailure::Unavailable => BindError::Unavailable,
        wire::ListenerBindingFailure::Unspecified => unreachable!("validated binding failure"),
    };
    let _ = response.send(Err(error));
    Ok(())
}

pub(super) fn listener_unbind_failed(
    shared: &Arc<Shared>,
    failed: wire::ListenerUnbindFailed,
) -> Result<(), SessionError> {
    let Some(failure) = binding_failure(failed.failure) else {
        return Err(SessionError::Protocol(
            "ListenerUnbindFailed contained invalid failure",
        ));
    };
    if !valid_text(&failed.listener_binding_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "ListenerUnbindFailed contained invalid identity",
        ));
    }
    let listener = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&failed.listener_binding_id)
        .cloned();
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Unbind { binding_id, .. })
                if binding_id == &failed.listener_binding_id
        );
        matches_current.then(|| pending.take().expect("matching Unbind must exist"))
    };
    let (Some(listener), Some(BindingPending::Unbind { response, .. })) = (listener, pending)
    else {
        return Err(SessionError::Protocol("foreign ListenerUnbindFailed"));
    };
    listener.active.store(true, Ordering::Release);
    let error = match failure {
        wire::ListenerBindingFailure::InvalidRequest => UnbindError::InvalidRequest,
        wire::ListenerBindingFailure::CapacityReached => UnbindError::CapacityReached,
        wire::ListenerBindingFailure::Conflict => UnbindError::Conflict,
        wire::ListenerBindingFailure::Unavailable => UnbindError::Unavailable,
        wire::ListenerBindingFailure::Unspecified => unreachable!("validated binding failure"),
    };
    let _ = response.send(Err(error));
    Ok(())
}

pub(super) fn listener_bound(
    shared: &Arc<Shared>,
    bound: wire::ListenerBound,
) -> Result<(), SessionError> {
    let binding = bound
        .binding
        .ok_or(SessionError::Protocol("ListenerBound omitted binding"))?;
    if !valid_text(&binding.listener_binding_id, MAX_IDENTITY_BYTES)
        || !valid_text(&binding.endpoint_pattern, MAX_ENDPOINT_BYTES)
        || !valid_text(&binding.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerBound contained invalid identity",
        ));
    }
    if let Some(existing) = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&binding.listener_binding_id)
        .cloned()
    {
        if existing.endpoint_pattern == binding.endpoint_pattern
            && existing.target_id == binding.target_id
        {
            return Ok(());
        }
        return Err(SessionError::Protocol(
            "ListenerBound reused an existing identity with different metadata",
        ));
    }
    match shared.binding_fingerprint_matches(&binding) {
        Some(true) => {
            let ambiguous = shared
                .binding_pending
                .lock()
                .expect("binding pending lock poisoned")
                .as_ref()
                .is_some_and(|pending| {
                    matches!(
                        pending,
                        BindingPending::Bind {
                            endpoint_pattern,
                            target_id,
                            ..
                        } if endpoint_pattern == &binding.endpoint_pattern
                            && target_id == &binding.target_id
                    )
                });
            if ambiguous {
                return Err(SessionError::Protocol(
                    "ListenerBound reused an ambiguous retired identity",
                ));
            }
            return Ok(());
        }
        Some(false) => {
            return Err(SessionError::Protocol(
                "ListenerBound conflicted with a retired binding fingerprint",
            ));
        }
        None => {}
    }
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Bind { endpoint_pattern, target_id, .. })
                if endpoint_pattern == &binding.endpoint_pattern && target_id == &binding.target_id
        );
        matches_current.then(|| pending.take().expect("matching Bind must exist"))
    };
    let Some(BindingPending::Bind { response, .. }) = pending else {
        return Err(SessionError::Protocol("foreign ListenerBound"));
    };
    shared.forget_retired_binding(&binding.listener_binding_id);
    let (offers_tx, offers_rx) = mpsc::channel(OFFER_QUEUE_CAPACITY);
    let state = Arc::new(ListenerState {
        binding_id: binding.listener_binding_id.clone(),
        endpoint_pattern: binding.endpoint_pattern.clone(),
        target_id: binding.target_id.clone(),
        offers_tx: StdMutex::new(Some(offers_tx)),
        offers_rx: Mutex::new(offers_rx),
        active: AtomicBool::new(true),
    });
    {
        let mut listeners = shared.listeners.lock().expect("listeners lock poisoned");
        if listeners.len() >= MAX_LISTENERS {
            drop(listeners);
            shared.remember_binding_fingerprint(&binding);
            shared.record_retired_binding(&binding.listener_binding_id);
            shared.auto_unbind(binding.listener_binding_id);
            let _ = response.send(Err(BindError::CapacityReached));
            return Ok(());
        }
        listeners.insert(binding.listener_binding_id.clone(), Arc::clone(&state));
    }
    let listener = Listener {
        state,
        shared: Arc::clone(shared),
    };
    let _ = response.send(Ok(listener));
    Ok(())
}

pub(super) fn listener_unbound(
    shared: &Arc<Shared>,
    unbound: wire::ListenerUnbound,
) -> Result<(), SessionError> {
    if !valid_text(&unbound.listener_binding_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "ListenerUnbound contained invalid identity",
        ));
    }
    if shared.binding_is_retired(&unbound.listener_binding_id) {
        return Ok(());
    }
    let listener = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&unbound.listener_binding_id)
        .cloned();
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Unbind { binding_id, .. })
                if binding_id == &unbound.listener_binding_id
        );
        matches_current.then(|| pending.take().expect("matching Unbind must exist"))
    };
    let auto_unbind = pending.is_none()
        && listener
            .as_ref()
            .is_some_and(|listener| !listener.active.load(Ordering::Acquire));
    if listener.is_none() || (pending.is_none() && !auto_unbind) {
        return Err(SessionError::Protocol("foreign ListenerUnbound"));
    }
    let Some(listener) = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .remove(&unbound.listener_binding_id)
    else {
        return Err(SessionError::Protocol("foreign ListenerUnbound"));
    };
    shared.remember_binding_fingerprint(&wire::ListenerBinding {
        listener_binding_id: listener.binding_id.clone(),
        endpoint_pattern: listener.endpoint_pattern.clone(),
        target_id: listener.target_id.clone(),
    });
    listener.retire();
    shared.record_retired_binding(&unbound.listener_binding_id);
    if let Some(BindingPending::Unbind { response, .. }) = pending {
        let _ = response.send(Ok(()));
    }
    Ok(())
}
