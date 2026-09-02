mod cleanup;
mod frame;
mod registration;
mod session;

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use relaygate_protocol::{BindingId, PipeId, SessionRole};
use tokio::time::Instant;

use super::{ListenerRuntimeInner, ListenerState};
use crate::{
    pipe::PipeState,
    session::{EstablishedSession, establish, next_backoff},
};

use self::session::run_listener_session;

pub(super) async fn listener_supervisor(
    inner: Arc<ListenerRuntimeInner>,
    initial: EstablishedSession,
) {
    let mut established = Some(initial);
    let mut backoff = inner.config.reconnect_initial;
    loop {
        if inner.cancel.is_cancelled() {
            inner.close_all();
            return;
        }
        let next = match established.take() {
            Some(session) => session,
            None => match establish(&inner.config, SessionRole::Listener).await {
                Ok(session) => session,
                Err(error) => {
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.reconnect_failed",
                        role = "listener",
                        error_code = ?error.code(),
                        observation = ?error.observation(),
                        "Listener session reconnect failed"
                    );
                    tokio::select! {
                        _ = inner.cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = next_backoff(backoff, inner.config.reconnect_maximum);
                    continue;
                }
            },
        };
        let session_cancel = inner.cancel.child_token();
        let registration_succeeded = run_listener_session(next, &inner, session_cancel).await;
        if inner.cancel.is_cancelled() {
            inner.close_all();
            return;
        }
        if registration_succeeded {
            backoff = inner.config.reconnect_initial;
        }
        tokio::select! {
            _ = inner.cancel.cancelled() => {
                inner.close_all();
                return;
            }
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = next_backoff(backoff, inner.config.reconnect_maximum);
    }
}

struct PendingRegistration {
    state: Arc<ListenerState>,
    committed: bool,
    deadline: Instant,
}

struct Registration {
    state: Arc<ListenerState>,
    binding_id: BindingId,
}

struct LivePipe {
    state: Arc<PipeState>,
    listener: Weak<ListenerState>,
}

impl LivePipe {
    fn compact_listener_queue(&self) -> bool {
        self.listener
            .upgrade()
            .is_none_or(|listener| listener.try_compact_terminal_queue())
    }
}

struct ListenerSessionState {
    next_request_id: u64,
    pending: HashMap<u64, PendingRegistration>,
    pending_by_client: HashMap<String, u64>,
    registrations: HashMap<String, Registration>,
    pipes: HashMap<PipeId, LivePipe>,
}

impl ListenerSessionState {
    fn new() -> Self {
        Self {
            next_request_id: 1,
            pending: HashMap::new(),
            pending_by_client: HashMap::new(),
            registrations: HashMap::new(),
            pipes: HashMap::new(),
        }
    }

    fn next_request_id(&mut self) -> Option<u64> {
        let current = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1)?;
        Some(current)
    }
}

enum ListenerFrameAction {
    Continue,
    RegistrationSucceeded,
    Reconcile,
    Stop,
}
