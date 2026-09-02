use relaygate_protocol::SessionId;

use super::ListenerSessionState;
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerStatus, is_current_desired},
};

pub(super) async fn cleanup_listener_session(
    session_id: SessionId,
    inner: &crate::listener::ListenerRuntimeInner,
    state: ListenerSessionState,
    timed_out_request: Option<u64>,
    registration_succeeded: bool,
) -> bool {
    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ended",
        role = "listener",
        session_id = %session_id.as_uuid(),
        pending_registrations = state.pending.len(),
        active_registrations = state.registrations.len(),
        live_pipes = state.pipes.len(),
        registration_timed_out = timed_out_request.is_some(),
        "Listener session ended"
    );
    // Fail every Pipe first. Once the Listener status leaves ACTIVE, pending
    // accept calls release the receiver lane and the old session queue can be
    // drained before this function permits a replacement session to start.
    for pipe in state.pipes.values() {
        pipe.state
            .fail(Error::unavailable("ListenerSession transport ended"));
    }
    let mut queues_to_drain = Vec::new();
    for (request_id, pending) in state.pending {
        if *pending.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((pending.state, true));
            continue;
        }
        if pending.committed {
            let recovery_error = if timed_out_request == Some(request_id) {
                Error::deadline(PeerObservation::MaybeObserved)
            } else {
                Error::maybe_observed("ListenerSession ended during managed REGISTER")
            };
            let initial_error = if timed_out_request == Some(request_id) {
                Error::deadline(PeerObservation::MaybeObserved)
            } else {
                Error::maybe_observed("ListenerSession ended after REGISTER commit")
            };
            if pending
                .state
                .suspend_or_fail_initial(recovery_error, initial_error)
            {
                inner.remove_terminal_listener(&pending.state);
            }
        } else if is_current_desired(inner, &pending.state) {
            pending
                .state
                .handle_precommit_session_end(Error::unavailable(
                    "ListenerSession ended before managed REGISTER commit",
                ));
        }
        let close_queue = matches!(
            *pending.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((pending.state, close_queue));
    }
    for (_, registration) in state.registrations {
        if *registration.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((registration.state, true));
            continue;
        }
        if registration.state.suspend_or_fail_initial(
            Error::unavailable("ListenerSession transport ended"),
            Error::new(
                ErrorCode::Unavailable,
                PeerObservation::Observed,
                "ListenerSession ended after REGISTERED before listen returned",
            ),
        ) {
            inner.remove_terminal_listener(&registration.state);
        }
        let close_queue = matches!(
            *registration.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((registration.state, close_queue));
    }
    for (listener, close_queue) in queues_to_drain {
        listener.drain_unaccepted(close_queue).await;
    }
    registration_succeeded
}
