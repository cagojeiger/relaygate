use std::{collections::HashMap, sync::Arc};

use futures_util::FutureExt;
use relaygate_protocol::Frame;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{PendingRegistration, RelaySessionState};
use crate::{
    AccessAction, AccessTokenRequest, Error, ErrorCode, PeerObservation, RouteAddress,
    listener::{ListenerState, ListenerStatus, RelayInner, is_current_desired},
    session::{EstablishedSession, send_bounded},
};

pub(super) async fn reconcile_registrations(
    inner: &RelayInner,
    established: &mut EstablishedSession,
    session: &mut RelaySessionState,
    session_cancel: &CancellationToken,
) -> bool {
    let Some(desired) = snapshot_desired_by_address(inner) else {
        return false;
    };
    let abandoned_committed_registration = session.pending.values().any(|pending| {
        pending.committed
            && (!desired
                .get(&pending.state.address)
                .is_some_and(|current| Arc::ptr_eq(current, &pending.state))
                || *pending.state.status.borrow() == ListenerStatus::Closed)
    });
    if abandoned_committed_registration {
        return false;
    }
    let registered_addresses = session.registrations.keys().cloned().collect::<Vec<_>>();
    for address in registered_addresses {
        let stale = session
            .registrations
            .get(&address)
            .is_some_and(|registration| {
                !desired
                    .get(&address)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() == ListenerStatus::Closed
            });
        if !stale {
            continue;
        }
        let Some(registration) = session.registrations.remove(&address) else {
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
        ) || session.registrations.contains_key(&state.address)
            || session.pending_by_address.contains_key(&state.address)
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
            .pending_by_address
            .insert(state.address.clone(), request_id);
        let source = state.access_token_source.clone();
        let address = state.address.clone();
        session.token_supplies.push(
            async move {
                let result = timeout_at(
                    deadline,
                    source.supply(AccessTokenRequest {
                        action: AccessAction::Publish,
                        address,
                    }),
                )
                .await
                .map_err(|_| Error::deadline(PeerObservation::NotObserved))
                .and_then(|result| result);
                (request_id, result)
            }
            .boxed(),
        );
    }

    !inner.cancel.is_cancelled()
}

pub(super) async fn commit_registration_token(
    request_id: u64,
    token: crate::Result<relaygate_protocol::BearerToken>,
    inner: &RelayInner,
    established: &mut EstablishedSession,
    session: &mut RelaySessionState,
    session_cancel: &CancellationToken,
) -> bool {
    let Some(mut pending) = session.pending.remove(&request_id) else {
        return true;
    };
    let state = Arc::clone(&pending.state);
    session.pending_by_address.remove(&state.address);
    if !is_current_desired(inner, &state) || *state.status.borrow() == ListenerStatus::Closed {
        return true;
    }
    let token = match token {
        Ok(token) if pending.deadline > Instant::now() => token,
        Ok(_) => {
            handle_token_source_error(inner, &state, Error::deadline(PeerObservation::NotObserved));
            return true;
        }
        Err(error) => {
            handle_token_source_error(inner, &state, error);
            return true;
        }
    };
    if !state.begin_registration_commit() {
        return true;
    }
    pending.committed = true;
    let deadline = pending.deadline;
    session.pending.insert(request_id, pending);
    session
        .pending_by_address
        .insert(state.address.clone(), request_id);
    send_bounded(
        &mut established.transport,
        Frame::Publish {
            request_id,
            address: state.address.clone(),
            access_token: token,
        },
        deadline
            .saturating_duration_since(Instant::now())
            .min(inner.config.operation_timeout),
        session_cancel,
    )
    .await
    .is_ok()
}

fn handle_token_source_error(inner: &RelayInner, state: &Arc<ListenerState>, error: Error) {
    if !state.was_returned() {
        inner.fail_initial_listener(state, error);
        return;
    }
    state.set_status(ListenerStatus::Suspended, Some(error));
    inner.schedule_reconcile();
}

fn snapshot_desired_by_address(
    inner: &RelayInner,
) -> Option<HashMap<RouteAddress, Arc<ListenerState>>> {
    match inner.desired.lock() {
        Ok(desired) => Some(
            desired
                .iter()
                .map(|(address, state)| (address.clone(), Arc::clone(state)))
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
