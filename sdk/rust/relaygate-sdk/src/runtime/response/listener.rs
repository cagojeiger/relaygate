use super::*;

pub(super) async fn listener_offer(
    shared: &Arc<Shared>,
    offer: wire::ListenerOffer,
) -> Result<(), SessionError> {
    if !valid_text(&offer.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.listener_binding_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&offer.target_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.caller_session_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerOffer contained invalid identity",
        ));
    }
    if shared.offer_terminal_exists(&offer.attempt_id) {
        return Err(SessionError::Protocol("retired ListenerOffer attempt"));
    }
    let listener = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&offer.listener_binding_id)
        .cloned();
    let Some(listener) = listener else {
        shared.retire_offer_identity(&offer.attempt_id, "");
        shared
            .send(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: offer.attempt_id,
                },
            )))
            .await?;
        return Ok(());
    };
    let (events, _) = watch::channel(OfferEvent::Pending);
    let state = Arc::new(OfferState {
        attempt_id: offer.attempt_id.clone(),
        decision: AtomicU8::new(0),
        cancelled: AtomicBool::new(false),
        confirm_sent: AtomicBool::new(false),
        acknowledged: AtomicBool::new(false),
        ended: AtomicBool::new(false),
        cleanup_started: AtomicBool::new(false),
        slot_reserved: AtomicBool::new(false),
        pipe_id: StdMutex::new(None),
        events,
        shared: Arc::downgrade(shared),
    });
    let rejected_for_capacity = {
        let history = shared
            .offer_history
            .lock()
            .expect("offer history lock poisoned");
        let mut offers = shared.offers.lock().expect("offers lock poisoned");
        if history
            .iter()
            .any(|(known_attempt, _)| known_attempt == &offer.attempt_id)
            || offers.contains_key(&offer.attempt_id)
        {
            return Err(SessionError::Protocol("duplicate ListenerOffer attempt"));
        }
        if offers.len() >= MAX_OFFERS {
            true
        } else {
            offers.insert(offer.attempt_id.clone(), Arc::clone(&state));
            false
        }
    };
    if rejected_for_capacity {
        shared.retire_offer_identity(&offer.attempt_id, "");
        shared
            .send(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: offer.attempt_id,
                },
            )))
            .await?;
        return Ok(());
    }
    let sdk_offer = Offer {
        metadata: OfferMetadata {
            attempt_id: offer.attempt_id.clone(),
            listener_binding_id: offer.listener_binding_id,
            endpoint: offer.endpoint,
            target_id: offer.target_id,
            caller_session_id: offer.caller_session_id,
        },
        state: Some(state),
        shared: Arc::clone(shared),
    };
    let sender = listener
        .offers_tx
        .lock()
        .expect("listener sender lock poisoned")
        .clone();
    match sender {
        Some(sender) if sender.try_send(sdk_offer).is_ok() => Ok(()),
        _ => {
            shared.retire_offer_identity(&offer.attempt_id, "");
            shared.remove_offer(&offer.attempt_id);
            shared
                .send(request(connect_request::Message::ListenerReject(
                    wire::ListenerReject {
                        attempt_id: offer.attempt_id,
                    },
                )))
                .await
        }
    }
}

pub(super) fn listener_established(
    shared: &Arc<Shared>,
    established: wire::ListenerEstablished,
) -> Result<(), SessionError> {
    if !valid_text(&established.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&established.pipe_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerEstablished contained invalid identity",
        ));
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&established.attempt_id)
        .cloned()
        .ok_or(SessionError::Protocol(
            "ListenerEstablished had no pending offer",
        ))?;
    state.publish(OfferEvent::Established(established.pipe_id));
    if state.cancelled.load(Ordering::Acquire) {
        shared.start_accept_cleanup(state);
    }
    Ok(())
}

pub(super) fn listener_confirmation_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::ListenerConfirmationAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&acknowledged.pipe_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerConfirmationAcknowledged contained invalid identity",
        ));
    }
    match shared.confirmation_matches(&acknowledged.attempt_id, &acknowledged.pipe_id) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "ListenerConfirmationAcknowledged conflicted with terminal history",
            ));
        }
        None => {}
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&acknowledged.attempt_id)
        .cloned();
    let Some(state) = state else {
        return Err(SessionError::Protocol(
            "foreign ListenerConfirmationAcknowledged",
        ));
    };
    if state
        .pipe_id
        .lock()
        .expect("offer pipe lock poisoned")
        .as_deref()
        != Some(acknowledged.pipe_id.as_str())
        || !shared
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .contains_key(&acknowledged.pipe_id)
    {
        return Err(SessionError::Protocol(
            "ListenerConfirmationAcknowledged identity or dispatch mismatch",
        ));
    }
    shared.remember_confirmation(
        acknowledged.attempt_id.clone(),
        acknowledged.pipe_id.clone(),
    );
    state.acknowledged.store(true, Ordering::Release);
    state.publish(OfferEvent::Acknowledged(acknowledged.pipe_id));
    if state.cancelled.load(Ordering::Acquire) {
        shared.start_accept_cleanup(state);
    }
    Ok(())
}

pub(super) fn listener_terminated(
    shared: &Arc<Shared>,
    terminated: wire::ListenerTerminated,
) -> Result<(), SessionError> {
    if !valid_text(&terminated.attempt_id, MAX_IDENTITY_BYTES)
        || (!terminated.pipe_id.is_empty() && !valid_text(&terminated.pipe_id, MAX_IDENTITY_BYTES))
    {
        return Err(SessionError::Protocol(
            "ListenerTerminated contained invalid identity",
        ));
    }
    let history_match = shared.confirmation_matches(&terminated.attempt_id, &terminated.pipe_id);
    if let Some(false) = history_match {
        return Err(SessionError::Protocol(
            "ListenerTerminated conflicted with terminal history",
        ));
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&terminated.attempt_id)
        .cloned();
    let pipe = (!terminated.pipe_id.is_empty())
        .then(|| {
            shared
                .pipes
                .lock()
                .expect("pipes lock poisoned")
                .get(&terminated.pipe_id)
                .cloned()
        })
        .flatten();
    if let Some(pipe) = &pipe
        && pipe.attempt_id != terminated.attempt_id
    {
        return Err(SessionError::Protocol(
            "ListenerTerminated did not own the referenced Pipe",
        ));
    }
    if state.is_none() && history_match.is_none() {
        return Err(SessionError::Protocol("foreign ListenerTerminated"));
    }
    if let Some(state) = state {
        state.ended.store(true, Ordering::Release);
        if state
            .pipe_id
            .lock()
            .expect("offer pipe lock poisoned")
            .as_ref()
            .is_some_and(|pipe_id| pipe_id != &terminated.pipe_id)
        {
            return Err(SessionError::Protocol(
                "ListenerTerminated identity did not match provisional Pipe",
            ));
        }
        shared.remove_offer(&terminated.attempt_id);
        state.publish(OfferEvent::Terminated);
    }
    if history_match.is_none() {
        shared.remember_confirmation(terminated.attempt_id.clone(), terminated.pipe_id.clone());
    }
    if !terminated.pipe_id.is_empty() {
        shared.terminalize_pipe(&terminated.pipe_id, PipeError::Terminal);
    }
    Ok(())
}

pub(super) fn listener_decision_rejected(
    shared: &Arc<Shared>,
    rejected: wire::ListenerDecisionRejected,
) -> Result<(), SessionError> {
    if !valid_text(&rejected.attempt_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "ListenerDecisionRejected contained invalid identity",
        ));
    }
    match wire::ListenerDecisionFailure::try_from(rejected.failure).ok() {
        Some(wire::ListenerDecisionFailure::InvalidRequest)
        | Some(wire::ListenerDecisionFailure::AttemptNotPending)
        | Some(wire::ListenerDecisionFailure::WrongPhase) => {}
        Some(wire::ListenerDecisionFailure::Unspecified) | None => {
            return Err(SessionError::Protocol(
                "ListenerDecisionRejected contained invalid failure",
            ));
        }
    }
    match shared.decision_rejection_matches(&rejected.attempt_id, rejected.failure) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "ListenerDecisionRejected conflicted with terminal history",
            ));
        }
        None => {}
    }
    let state = {
        let mut offers = shared.offers.lock().expect("offers lock poisoned");
        let state = offers
            .get(&rejected.attempt_id)
            .cloned()
            .ok_or(SessionError::Protocol("foreign ListenerDecisionRejected"))?;
        if state.decision.load(Ordering::Acquire) != 1
            || state.ended.load(Ordering::Acquire)
            || state.acknowledged.load(Ordering::Acquire)
        {
            return Err(SessionError::Protocol(
                "ListenerDecisionRejected arrived in the wrong offer phase",
            ));
        }
        state.ended.store(true, Ordering::Release);
        offers
            .remove(&rejected.attempt_id)
            .expect("matching offer must remain present")
    };
    state.release_slot(shared);
    let pipe_id = state
        .pipe_id
        .lock()
        .expect("offer pipe lock poisoned")
        .clone()
        .unwrap_or_default();
    shared.remember_decision_rejection(rejected.attempt_id.clone(), rejected.failure);
    if !pipe_id.is_empty() {
        shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
    }
    state.publish(OfferEvent::Rejected);
    Ok(())
}
