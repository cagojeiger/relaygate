use super::*;

pub(super) fn pipe_close_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::PipeCloseAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.pipe_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "PipeCloseAcknowledged contained invalid identity",
        ));
    }
    match shared.close_ack_matches(&acknowledged.pipe_id, acknowledged.owned) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "PipeCloseAcknowledged conflicted with terminal history",
            ));
        }
        None => {}
    }
    let response = shared
        .closes
        .lock()
        .expect("closes lock poisoned")
        .remove(&acknowledged.pipe_id);
    let Some(response) = response else {
        return Err(SessionError::Protocol("foreign PipeCloseAcknowledged"));
    };
    shared.remember_close_ack(acknowledged.pipe_id.clone(), acknowledged.owned);
    if acknowledged.owned {
        shared.terminalize_pipe(&acknowledged.pipe_id, PipeError::Terminal);
        let _ = response.send(Ok(()));
    } else {
        shared.terminalize_pipe(&acknowledged.pipe_id, PipeError::NotOwned);
        let _ = response.send(Err(CloseError::NotOwned));
    }
    Ok(())
}

pub(super) fn pipe_terminated(
    shared: &Arc<Shared>,
    terminated: wire::PipeTerminated,
) -> Result<(), SessionError> {
    if !valid_text(&terminated.pipe_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "PipeTerminated contained invalid identity",
        ));
    }
    let offers = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let offer = offers.into_iter().find(|offer| {
        offer
            .pipe_id
            .lock()
            .expect("offer pipe lock poisoned")
            .as_deref()
            == Some(terminated.pipe_id.as_str())
    });
    let pipe_exists = shared
        .pipes
        .lock()
        .expect("pipes lock poisoned")
        .contains_key(&terminated.pipe_id);
    if offer.is_none() && !pipe_exists && !shared.pipe_was_retired(&terminated.pipe_id) {
        return Err(SessionError::Protocol("foreign PipeTerminated"));
    }
    if let Some(offer) = offer {
        match shared.confirmation_matches(&offer.attempt_id, &terminated.pipe_id) {
            Some(false) => {
                return Err(SessionError::Protocol(
                    "PipeTerminated conflicted with confirmation history",
                ));
            }
            Some(true) => {}
            None => {
                shared.remember_confirmation(offer.attempt_id.clone(), terminated.pipe_id.clone())
            }
        }
        offer.ended.store(true, Ordering::Release);
        shared.remove_offer(&offer.attempt_id);
        offer.publish(OfferEvent::Terminated);
    }
    shared.terminalize_pipe(&terminated.pipe_id, PipeError::Terminal);
    Ok(())
}

pub(super) fn pipe_payload(
    shared: &Arc<Shared>,
    payload: wire::PipePayload,
) -> Result<(), SessionError> {
    if !valid_text(&payload.pipe_id, MAX_IDENTITY_BYTES)
        || !valid_text(&payload.payload_id, MAX_IDENTITY_BYTES)
        || payload.payload.is_empty()
        || payload.payload.len() > MAX_PAYLOAD_BYTES
    {
        return Err(SessionError::Protocol("invalid PipePayload"));
    }
    let pipe = shared
        .pipes
        .lock()
        .expect("pipes lock poisoned")
        .get(&payload.pipe_id)
        .cloned();
    let Some(pipe) = pipe else {
        return Err(SessionError::Protocol("foreign PipePayload"));
    };
    let pipe_id = payload.pipe_id;
    let payload_id = payload.payload_id;
    match pipe.deliver(payload_id.clone(), payload.payload) {
        IncomingPayload::Accepted {
            permit,
            payload: bytes,
        } => {
            shared.try_send_background(request(connect_request::Message::PipePayloadReceived(
                wire::PipePayloadReceived {
                    pipe_id: pipe_id.clone(),
                    payload_id: payload_id.clone(),
                },
            )))?;
            permit.send(bytes);
        }
        IncomingPayload::Duplicate => {
            shared.try_send_background(request(connect_request::Message::PipePayloadReceived(
                wire::PipePayloadReceived {
                    pipe_id,
                    payload_id,
                },
            )))?;
        }
        IncomingPayload::Full => {
            shared.send_background(request(connect_request::Message::PipePayloadRejected(
                wire::PipePayloadRejected {
                    pipe_id: pipe_id.clone(),
                    failure: wire::PipePayloadFailure::Backpressure.into(),
                    payload_id,
                },
            )));
            shared.terminalize_pipe(&pipe_id, PipeError::Backpressure);
            shared.send_background(request(connect_request::Message::ClosePipe(
                wire::ClosePipe { pipe_id },
            )));
        }
        IncomingPayload::Conflict => {
            return Err(SessionError::Protocol(
                "PipePayload reused PayloadId with different bytes",
            ));
        }
        IncomingPayload::Terminal => {
            return Err(SessionError::Protocol("PipePayload targeted terminal Pipe"));
        }
    }
    Ok(())
}

pub(super) fn pipe_payload_received(
    shared: &Arc<Shared>,
    received: wire::PipePayloadReceived,
) -> Result<(), SessionError> {
    if !valid_text(&received.pipe_id, MAX_IDENTITY_BYTES)
        || !valid_text(&received.payload_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipePayloadReceived contained invalid identity",
        ));
    }
    shared.complete_delivery(
        &received.pipe_id,
        &received.payload_id,
        DeliveryTerminal::Received,
    )
}

pub(super) fn pipe_payload_rejected(
    shared: &Arc<Shared>,
    rejected: wire::PipePayloadRejected,
) -> Result<(), SessionError> {
    if !valid_text(&rejected.pipe_id, MAX_IDENTITY_BYTES)
        || !valid_text(&rejected.payload_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipePayloadRejected contained invalid identity",
        ));
    }
    let (failure, error) = match wire::PipePayloadFailure::try_from(rejected.failure).ok() {
        Some(wire::PipePayloadFailure::InvalidRequest) => {
            (DeliveryFailure::InvalidRequest, PipeError::InvalidPayload)
        }
        Some(wire::PipePayloadFailure::NotOwned) => {
            (DeliveryFailure::NotOwned, PipeError::NotOwned)
        }
        Some(wire::PipePayloadFailure::Backpressure) => {
            (DeliveryFailure::Backpressure, PipeError::Backpressure)
        }
        Some(wire::PipePayloadFailure::Unavailable) => {
            (DeliveryFailure::Unavailable, PipeError::Unavailable)
        }
        Some(wire::PipePayloadFailure::Unspecified) | None => {
            return Err(SessionError::Protocol(
                "PipePayloadRejected contained invalid failure",
            ));
        }
    };
    shared.complete_delivery(
        &rejected.pipe_id,
        &rejected.payload_id,
        DeliveryTerminal::Rejected(failure),
    )?;
    if shared
        .pipes
        .lock()
        .expect("pipes lock poisoned")
        .contains_key(&rejected.pipe_id)
    {
        shared.terminalize_pipe(&rejected.pipe_id, error);
    }
    Ok(())
}
