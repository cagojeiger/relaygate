use std::{collections::HashMap, sync::Arc};

use relaygate_protocol::Frame;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use super::{PendingRegistration, RelaySessionState};
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerState, ListenerStatus, RelayInner, is_current_desired},
    session::{EstablishedSession, send_bounded},
};

pub(super) async fn reconcile_registrations(
    inner: &RelayInner,
    established: &mut EstablishedSession,
    session: &mut RelaySessionState,
    session_cancel: &CancellationToken,
) -> bool {
    let Some(desired) = snapshot_desired_by_client(inner) else {
        return false;
    };
    let abandoned_committed_registration = session.pending.values().any(|pending| {
        pending.committed
            && (!desired
                .get(&pending.state.destination_id)
                .is_some_and(|current| Arc::ptr_eq(current, &pending.state))
                || *pending.state.status.borrow() == ListenerStatus::Closed)
    });
    if abandoned_committed_registration {
        return false;
    }
    let registered_destinations = session.registrations.keys().copied().collect::<Vec<_>>();
    for destination_id in registered_destinations {
        let stale = session
            .registrations
            .get(&destination_id)
            .is_some_and(|registration| {
                !desired
                    .get(&destination_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() == ListenerStatus::Closed
            });
        if !stale {
            continue;
        }
        let Some(registration) = session.registrations.remove(&destination_id) else {
            continue;
        };
        let Some(request_id) = session.next_request_id() else {
            return false;
        };
        if send_bounded(
            &mut established.transport,
            Frame::Unpublish {
                request_id,
                binding_id: registration.binding_id,
            },
            inner.config.operation_timeout,
            session_cancel,
        )
        .await
        .is_err()
        {
            return false;
        }
    }

    for state in desired.values() {
        if !is_current_desired(inner, state) {
            continue;
        }
        if matches!(
            *state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        ) || session.registrations.contains_key(&state.destination_id)
            || session
                .pending_by_client
                .contains_key(&state.destination_id)
        {
            continue;
        }
        let deadline = if state.was_returned() {
            match inner.config.operation_deadline() {
                Ok(deadline) => deadline,
                Err(error) => {
                    state.block(error);
                    state.drain_unaccepted(true).await;
                    continue;
                }
            }
        } else {
            state.initial_deadline
        };
        if deadline <= Instant::now() {
            if state.was_returned() {
                state.set_status(
                    ListenerStatus::Suspended,
                    Some(Error::deadline(PeerObservation::NotObserved)),
                );
            } else {
                inner.fail_initial_listener(state, Error::deadline(PeerObservation::NotObserved));
            }
            continue;
        }
        let Some(request_id) = session.next_request_id() else {
            let error = Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "RelaySession exhausted request IDs",
            );
            if state.was_returned() {
                state.block(error);
                state.drain_unaccepted(true).await;
            } else {
                inner.fail_initial_listener(state, error);
            }
            continue;
        };
        session.pending.insert(
            request_id,
            PendingRegistration {
                state: Arc::clone(state),
                committed: false,
                deadline,
            },
        );
        session
            .pending_by_client
            .insert(state.destination_id, request_id);
        if !state.begin_registration_commit() {
            session.pending.remove(&request_id);
            session.pending_by_client.remove(&state.destination_id);
            continue;
        }
        if let Some(pending) = session.pending.get_mut(&request_id) {
            pending.committed = true;
        }
        if send_bounded(
            &mut established.transport,
            Frame::Publish {
                request_id,
                destination_id: state.destination_id.to_wire(),
            },
            deadline
                .saturating_duration_since(Instant::now())
                .min(inner.config.operation_timeout),
            session_cancel,
        )
        .await
        .is_err()
        {
            return false;
        }
    }

    !inner.cancel.is_cancelled()
}

fn snapshot_desired_by_client(
    inner: &RelayInner,
) -> Option<HashMap<crate::DestinationId, Arc<ListenerState>>> {
    match inner.desired.lock() {
        Ok(desired) => Some(
            desired
                .iter()
                .map(|(destination_id, state)| (*destination_id, Arc::clone(state)))
                .collect(),
        ),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            None
        }
    }
}

pub(super) async fn wait_for_registration_deadline(deadline: Option<(u64, Instant)>) {
    if let Some((_, deadline)) = deadline {
        sleep_until(deadline).await;
    }
}
