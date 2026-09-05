use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use tokio::{
    sync::{Notify, mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{ListenerStatus, RelaySession};
use crate::{
    Config, DestinationId, Error, ErrorCode, PeerObservation, Pipe, lifetime::RuntimeLifetime,
};

pub(super) struct RelayInner {
    pub(super) config: Config,
    pub(super) desired: StdMutex<HashMap<DestinationId, Arc<ListenerState>>>,
    pub(super) current: watch::Sender<Option<Arc<RelaySession>>>,
    pub(super) reconcile: Arc<Notify>,
    pub(super) cancel: CancellationToken,
    pub(super) lifetime: Weak<RuntimeLifetime>,
}

pub(super) struct ListenerState {
    pub(super) destination_id: DestinationId,
    pub(super) status: watch::Sender<ListenerStatus>,
    pub(super) last_error: StdMutex<Option<Error>>,
    pub(super) incoming_tx: mpsc::Sender<Pipe>,
    pub(super) incoming_rx: tokio::sync::Mutex<mpsc::Receiver<Pipe>>,
    pub(super) initial_deadline: Instant,
    pub(super) lifecycle: StdMutex<ListenerLifecycle>,
    pub(super) registration_committed: StdMutex<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ListenerLifecycle {
    Pending,
    Returned,
    Terminal,
}

impl RelayInner {
    pub(super) fn desired_is_converged(&self) -> bool {
        self.desired.lock().is_ok_and(|desired| {
            desired
                .values()
                .all(|state| *state.status.borrow() == ListenerStatus::Active)
        })
    }

    pub(super) fn detach_listener(&self, state: &Arc<ListenerState>) {
        state.close(None);
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.destination_id)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.destination_id);
            self.reconcile.notify_one();
        }
    }

    pub(super) fn drop_listener(&self, state: &Arc<ListenerState>) {
        self.detach_listener(state);
    }

    pub(super) fn close_all(&self) {
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => poisoned.into_inner(),
        };
        let states = desired.drain().map(|(_, state)| state).collect::<Vec<_>>();
        for state in states {
            state.close(None);
        }
        self.reconcile.notify_one();
    }

    pub(super) fn fail_initial_listener(&self, state: &Arc<ListenerState>, error: Error) {
        if !state.fail_initial(error) {
            return;
        }
        self.remove_terminal_listener(state);
    }

    pub(super) fn terminate_initial_listener(
        &self,
        state: &Arc<ListenerState>,
        code: ErrorCode,
        message: &str,
    ) -> Error {
        let error = state
            .terminate_initial_operation(code, message)
            .or_else(|| state.last_error())
            .unwrap_or_else(|| Error::new(code, PeerObservation::NotObserved, message));
        self.remove_terminal_listener(state);
        error
    }

    pub(super) fn remove_terminal_listener(&self, state: &Arc<ListenerState>) {
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.destination_id)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.destination_id);
        }
        self.reconcile.notify_one();
    }
}

impl ListenerState {
    pub(super) fn set_status(&self, status: ListenerStatus, error: Option<Error>) {
        let previous = *self.status.borrow();
        if previous != status {
            if let Some(error) = error.as_ref() {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener.status_changed",
                    destination_id = %self.destination_id,
                    previous = ?previous,
                    status = ?status,
                    error_code = ?error.code(),
                    observation = ?error.observation(),
                    "Listener status changed"
                );
            } else {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener.status_changed",
                    destination_id = %self.destination_id,
                    previous = ?previous,
                    status = ?status,
                    "Listener status changed"
                );
            }
        }
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = error;
        }
        self.status.send_replace(status);
    }

    pub(super) fn last_error(&self) -> Option<Error> {
        self.last_error.lock().ok().and_then(|error| error.clone())
    }

    pub(super) fn blocked_error(&self) -> Error {
        self.last_error().unwrap_or_else(|| {
            Error::new(
                ErrorCode::PermissionDenied,
                PeerObservation::NotObserved,
                "Listener registration is blocked",
            )
        })
    }

    pub(super) fn was_returned(&self) -> bool {
        self.lifecycle
            .lock()
            .is_ok_and(|lifecycle| *lifecycle == ListenerLifecycle::Returned)
    }

    pub(super) fn promote_returned(&self) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        match *lifecycle {
            ListenerLifecycle::Pending => {
                *lifecycle = ListenerLifecycle::Returned;
                true
            }
            ListenerLifecycle::Returned => true,
            ListenerLifecycle::Terminal => false,
        }
    }

    fn fail_initial(&self, error: Error) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle != ListenerLifecycle::Pending {
            return false;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Closed, Some(error));
        true
    }

    pub(super) fn suspend_or_fail_initial(
        &self,
        recovery_error: Error,
        initial_error: Error,
    ) -> bool {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        match *lifecycle {
            ListenerLifecycle::Pending => {
                *lifecycle = ListenerLifecycle::Terminal;
                self.finish_registration_attempt();
                self.set_status(ListenerStatus::Closed, Some(initial_error));
                true
            }
            ListenerLifecycle::Returned => {
                self.finish_registration_attempt();
                self.set_status(ListenerStatus::Suspended, Some(recovery_error));
                false
            }
            ListenerLifecycle::Terminal => false,
        }
    }

    pub(super) fn handle_precommit_session_end(&self, recovery_error: Error) {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return;
        };
        self.finish_registration_attempt();
        match *lifecycle {
            ListenerLifecycle::Pending => {
                self.set_status(ListenerStatus::Registering, None);
            }
            ListenerLifecycle::Returned => {
                self.set_status(ListenerStatus::Suspended, Some(recovery_error));
            }
            ListenerLifecycle::Terminal => {}
        }
    }

    pub(super) fn block(&self, error: Error) {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Blocked, Some(error));
    }

    pub(super) fn activate(&self) -> bool {
        self.activate_while_locked(|| {})
    }

    pub(super) fn activate_while_locked(&self, before_publish: impl FnOnce()) -> bool {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        before_publish();
        self.finish_registration_attempt();
        self.set_status(ListenerStatus::Active, None);
        drop(lifecycle);
        true
    }

    pub(super) fn close(&self, error: Option<Error>) {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return;
        };
        if *lifecycle == ListenerLifecycle::Terminal
            && *self.status.borrow() == ListenerStatus::Closed
        {
            return;
        }
        *lifecycle = ListenerLifecycle::Terminal;
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = false;
        }
        self.set_status(ListenerStatus::Closed, error);
    }

    pub(super) fn begin_registration_commit(&self) -> bool {
        let Ok(lifecycle) = self.lifecycle.lock() else {
            return false;
        };
        if *lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = true;
            self.set_status(ListenerStatus::Registering, None);
            true
        } else {
            false
        }
    }

    pub(super) fn finish_registration_attempt(&self) {
        if let Ok(mut committed) = self.registration_committed.lock() {
            *committed = false;
        }
    }

    fn terminate_initial_operation(&self, code: ErrorCode, message: &str) -> Option<Error> {
        let Ok(mut lifecycle) = self.lifecycle.lock() else {
            return None;
        };
        if *lifecycle != ListenerLifecycle::Pending {
            return None;
        }
        let observation = self.registration_committed.lock().map_or(
            PeerObservation::NotObserved,
            |mut committed| {
                if std::mem::take(&mut *committed) {
                    PeerObservation::MaybeObserved
                } else {
                    PeerObservation::NotObserved
                }
            },
        );
        *lifecycle = ListenerLifecycle::Terminal;
        let error = Error::new(code, observation, message);
        self.set_status(ListenerStatus::Closed, Some(error.clone()));
        Some(error)
    }

    /// Drops terminal queued Pipes without waiting for an application accept.
    ///
    /// A busy receiver means an accept call owns the queue and will either
    /// consume the terminal entry or release the lane before the next Offer.
    /// Live entries are reinserted in FIFO order while the receiver lock keeps
    /// application accepts from observing the temporary compaction.
    pub(super) fn try_compact_terminal_queue(&self) -> bool {
        let Ok(mut incoming) = self.incoming_rx.try_lock() else {
            return true;
        };
        let mut live = Vec::new();
        while let Ok(pipe) = incoming.try_recv() {
            if pipe.is_terminal() {
                drop(pipe);
            } else {
                live.push(pipe);
            }
        }
        for pipe in live {
            if self.incoming_tx.try_send(pipe).is_err() {
                return false;
            }
        }
        true
    }

    pub(super) async fn drain_unaccepted(&self, close: bool) {
        let mut incoming = self.incoming_rx.lock().await;
        if close {
            incoming.close();
        }
        while let Ok(pipe) = incoming.try_recv() {
            drop(pipe);
        }
    }
}

pub(super) fn is_current_desired(inner: &RelayInner, state: &Arc<ListenerState>) -> bool {
    match inner.desired.lock() {
        Ok(desired) => desired
            .get(&state.destination_id)
            .is_some_and(|current| Arc::ptr_eq(current, state)),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            false
        }
    }
}
