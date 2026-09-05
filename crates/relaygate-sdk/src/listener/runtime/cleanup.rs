use relaygate_protocol::SessionId;

use super::RelaySessionState;
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerStatus, is_current_desired},
};

pub(super) async fn cleanup_relay_session(
    session_id: SessionId,
    inner: &crate::listener::RelayInner,
    state: RelaySessionState,
    timed_out_request: Option<u64>,
    registration_succeeded: bool,
) -> bool {
    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ended",
        session_id = %session_id.as_uuid(),
        pending_registrations = state.pending.len(),
        active_registrations = state.registrations.len(),
        pending_dials = state.pending_dials.len(),
        live_pipes = state.pipes.len(),
        registration_timed_out = timed_out_request.is_some(),
        "Relay session ended"
    );
    for response in state.pending_dials.into_values() {
        let _ = response.send(Err(Error::maybe_observed(
            "RelaySession transport ended after DIAL commit",
        )));
    }
    // Fail every Pipe first. Once the Listener status leaves ACTIVE, pending
    // accept calls release the receiver lane and the old session queue can be
    // drained before this function permits a replacement session to start.
    for pipe in state.pipes.values() {
        pipe.state
            .fail(Error::unavailable("RelaySession transport ended"));
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
                Error::maybe_observed("RelaySession ended during managed PUBLISH")
            };
            let initial_error = if timed_out_request == Some(request_id) {
                Error::deadline(PeerObservation::MaybeObserved)
            } else {
                Error::maybe_observed("RelaySession ended after PUBLISH commit")
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
                    "RelaySession ended before managed PUBLISH commit",
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
            Error::unavailable("RelaySession transport ended"),
            Error::new(
                ErrorCode::Unavailable,
                PeerObservation::Observed,
                "RelaySession ended after PUBLISHED before listen returned",
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
