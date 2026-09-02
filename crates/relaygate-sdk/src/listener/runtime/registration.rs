use std::{collections::HashMap, sync::Arc};

use relaygate_protocol::{ClientKey, Frame};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use super::{ListenerSessionState, PendingRegistration};
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerRuntimeInner, ListenerState, ListenerStatus, is_current_desired},
    session::{EstablishedSession, send_bounded},
};

pub(super) async fn reconcile_registrations(
    inner: &ListenerRuntimeInner,
    established: &mut EstablishedSession,
    session: &mut ListenerSessionState,
    session_cancel: &CancellationToken,
) -> bool {
    let Some(desired) = snapshot_desired_by_client(inner) else {
        return false;
    };
    let abandoned_committed_registration = session.pending.values().any(|pending| {
        pending.committed
            && (!desired
                .get(&pending.state.client_id)
                .is_some_and(|current| Arc::ptr_eq(current, &pending.state))
                || *pending.state.status.borrow() == ListenerStatus::Closed)
    });
    if abandoned_committed_registration {
        return false;
    }
    let registered_clients = session.registrations.keys().cloned().collect::<Vec<_>>();
    for client_id in registered_clients {
        let stale = session
            .registrations
            .get(&client_id)
            .is_some_and(|registration| {
                !desired
                    .get(&client_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() == ListenerStatus::Closed
            });
        if !stale {
            continue;
        }
        let Some(registration) = session.registrations.remove(&client_id) else {
            continue;
        };
        let Some(request_id) = session.next_request_id() else {
            return false;
        };
        if send_bounded(
            &mut established.transport,
            Frame::Unregister {
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
        ) || session.registrations.contains_key(&state.client_id)
            || session.pending_by_client.contains_key(&state.client_id)
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
                "ListenerSession exhausted request IDs",
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
            .insert(state.client_id.clone(), request_id);
        if !state.begin_registration_commit() {
            session.pending.remove(&request_id);
            session.pending_by_client.remove(&state.client_id);
            continue;
        }
        if let Some(pending) = session.pending.get_mut(&request_id) {
            pending.committed = true;
        }
        if send_bounded(
            &mut established.transport,
            Frame::Register {
                request_id,
                client_id: state.client_id.clone(),
                client_key: ClientKey::new(state.client_key.clone()),
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
    inner: &ListenerRuntimeInner,
) -> Option<HashMap<String, Arc<ListenerState>>> {
    match inner.desired.lock() {
        Ok(desired) => Some(
            desired
                .iter()
                .map(|(client_id, state)| (client_id.clone(), Arc::clone(state)))
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
