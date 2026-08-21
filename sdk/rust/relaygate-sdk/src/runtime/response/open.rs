use super::*;

pub(super) async fn pipe_opened(
    shared: &Arc<Shared>,
    opened: wire::PipeOpened,
) -> Result<(), SessionError> {
    if !valid_text(&opened.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.pipe_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&opened.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpened contained invalid identity",
        ));
    }
    let terminal = OpenTerminal::Opened {
        attempt_id: opened.attempt_id.clone(),
        pipe_id: opened.pipe_id.clone(),
        endpoint: opened.endpoint.clone(),
        target_id: opened.target_id.clone(),
    };
    let Some(pending) = pending_open_or_replay(shared, &opened.request_id, &terminal)? else {
        return Ok(());
    };
    if opened.endpoint != pending.endpoint || opened.target_id != pending.target_id {
        pending.release_slot(shared);
        return Err(SessionError::Protocol("PipeOpened identity mismatch"));
    }
    let pipe = match shared.register_open_pipe(
        &opened.pipe_id,
        &opened.attempt_id,
        &pending.slot_reserved,
    ) {
        Ok(pipe) => pipe,
        Err(error) => {
            pending.complete(Err(error));
            return Ok(());
        }
    };
    shared.remember_open_terminal(opened.request_id, terminal);
    if pending.cancelled.load(Ordering::Acquire) || !pending.complete(Ok(pipe)) {
        let pipe_id = opened.pipe_id;
        let shared = Arc::clone(shared);
        shared
            .clone()
            .spawn_task("cancelled Open cleanup", async move {
                let _ = shared.close_pipe(&pipe_id).await;
                shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
            });
    }
    Ok(())
}

pub(super) fn pipe_open_failed(
    shared: &Arc<Shared>,
    failed: wire::PipeOpenFailed,
) -> Result<(), SessionError> {
    if !valid_text(&failed.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&failed.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&failed.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpenFailed contained invalid identity",
        ));
    }
    let result = match wire::OpenFailure::try_from(failed.failure).ok() {
        Some(wire::OpenFailure::InvalidRequest) => {
            Err(OpenError::Failed(OpenFailure::InvalidRequest))
        }
        Some(wire::OpenFailure::RouteNotFound) => {
            Err(OpenError::Failed(OpenFailure::RouteNotFound))
        }
        Some(wire::OpenFailure::Unavailable) => Err(OpenError::Failed(OpenFailure::Unavailable)),
        Some(wire::OpenFailure::CapacityReached) => {
            Err(OpenError::Failed(OpenFailure::CapacityReached))
        }
        Some(wire::OpenFailure::ListenerRejected) => {
            Err(OpenError::Failed(OpenFailure::ListenerRejected))
        }
        Some(wire::OpenFailure::DeadlineExceeded) => {
            Err(OpenError::Failed(OpenFailure::DeadlineExceeded))
        }
        Some(wire::OpenFailure::Cancelled) => Err(OpenError::Cancelled),
        Some(wire::OpenFailure::Unspecified) | None => {
            return Err(SessionError::Protocol(
                "PipeOpenFailed used unspecified failure",
            ));
        }
    };
    let terminal = OpenTerminal::Failed {
        endpoint: failed.endpoint.clone(),
        target_id: failed.target_id.clone(),
        failure: failed.failure,
    };
    let Some(pending) = pending_open_or_replay(shared, &failed.request_id, &terminal)? else {
        return Ok(());
    };
    pending.release_slot(shared);
    if failed.endpoint != pending.endpoint || failed.target_id != pending.target_id {
        return Err(SessionError::Protocol("PipeOpenFailed identity mismatch"));
    }
    shared.remember_open_terminal(failed.request_id, terminal);
    pending.complete(result);
    Ok(())
}

pub(super) fn pipe_open_unknown(
    shared: &Arc<Shared>,
    unknown: wire::PipeOpenUnknown,
) -> Result<(), SessionError> {
    if !valid_text(&unknown.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&unknown.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&unknown.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpenUnknown contained invalid identity",
        ));
    }
    let terminal = OpenTerminal::Unknown {
        endpoint: unknown.endpoint.clone(),
        target_id: unknown.target_id.clone(),
    };
    let Some(pending) = pending_open_or_replay(shared, &unknown.request_id, &terminal)? else {
        return Ok(());
    };
    if unknown.endpoint != pending.endpoint || unknown.target_id != pending.target_id {
        pending.release_slot(shared);
        return Err(SessionError::Protocol("PipeOpenUnknown identity mismatch"));
    }
    shared.remember_open_terminal(unknown.request_id, terminal);
    pending.release_slot(shared);
    pending.complete(Err(OpenError::Unknown));
    Ok(())
}

pub(super) fn open_request_rejected(
    shared: &Arc<Shared>,
    rejected: wire::OpenRequestRejected,
) -> Result<(), SessionError> {
    if !valid_text(&rejected.request_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "OpenRequestRejected contained invalid identity",
        ));
    }
    let result = match wire::OpenRequestFailure::try_from(rejected.failure).ok() {
        Some(wire::OpenRequestFailure::DuplicateInFlight) => Err(OpenError::DuplicateInFlight),
        _ => {
            return Err(SessionError::Protocol(
                "OpenRequestRejected used unspecified failure",
            ));
        }
    };
    let terminal = OpenTerminal::RequestRejected {
        failure: rejected.failure,
    };
    let Some(pending) = pending_open_or_replay(shared, &rejected.request_id, &terminal)? else {
        return Ok(());
    };
    shared.remember_open_terminal(rejected.request_id, terminal);
    pending.release_slot(shared);
    pending.complete(result);
    Ok(())
}

pub(super) fn pending_open_or_replay(
    shared: &Arc<Shared>,
    request_id: &str,
    terminal: &OpenTerminal,
) -> Result<Option<Arc<PendingOpen>>, SessionError> {
    if let Some(pending) = shared.remove_open(request_id) {
        return Ok(Some(pending));
    }
    match shared.open_terminal_matches(request_id, terminal) {
        Some(true) => Ok(None),
        Some(false) => Err(SessionError::Protocol(
            "Open terminal conflicted with retired history",
        )),
        None => Err(SessionError::Protocol("foreign Open terminal")),
    }
}

pub(super) fn open_cancel_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::OpenCancelAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.request_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "OpenCancelAcknowledged contained invalid identity",
        ));
    }
    match shared.acknowledge_cancel(&acknowledged.request_id, acknowledged.was_pending) {
        Some(true) => Ok(()),
        Some(false) => Err(SessionError::Protocol(
            "OpenCancelAcknowledged conflicted with retired history",
        )),
        None => Err(SessionError::Protocol("foreign OpenCancelAcknowledged")),
    }
}
