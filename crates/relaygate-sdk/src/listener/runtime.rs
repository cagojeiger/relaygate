mod cleanup;
mod frame;
mod registration;
mod session;

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use relaygate_protocol::{BindingId, PipeId};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::Instant,
};

use super::{ListenerState, RelayInner, RelaySession};
use crate::{
    DestinationId, Pipe, Result,
    observability::{ReconnectEpisode, close_reconnect_episode},
    pipe::PipeState,
    session::{EstablishedSession, ReconnectBackoff, establish},
};

use self::session::run_relay_session;

pub(super) async fn relay_supervisor(inner: Arc<RelayInner>, initial: EstablishedSession) {
    let mut established = Some(initial);
    let mut backoff = ReconnectBackoff::new(
        inner.config.reconnect_initial,
        inner.config.reconnect_maximum,
    );
    let mut reconnect_episode: Option<ReconnectEpisode> = None;
    loop {
        if inner.cancel.is_cancelled() {
            close_reconnect_episode(&mut reconnect_episode);
            inner.close_all();
            return;
        }
        let next = match established.take() {
            Some(session) => session,
            None => match establish(&inner.config).await {
                Ok(session) => {
                    if let Some(episode) = reconnect_episode.as_mut() {
                        episode.record_attempt("success");
                    }
                    session
                }
                Err(error) => {
                    if let Some(episode) = reconnect_episode.as_mut() {
                        episode.record_attempt("error");
                    }
                    tracing::debug!(
                        component = "sdk",
                        event = "sdk.session.reconnect_failed",
                        error_code = error.code().metric_name(),
                        observation = ?error.observation(),
                        "Relay session reconnect failed"
                    );
                    tokio::select! {
                        _ = inner.cancel.cancelled() => {
                            close_reconnect_episode(&mut reconnect_episode);
                            return;
                        },
                        _ = tokio::time::sleep(backoff.next_delay()) => {}
                    }
                    continue;
                }
            },
        };
        let started_at = Instant::now();
        let (commands_tx, commands_rx) = mpsc::channel(inner.config.outbound_capacity);
        let (cancellations_tx, cancellations_rx) = mpsc::unbounded_channel();
        let session_cancel = inner.cancel.child_token();
        let session = Arc::new(RelaySession {
            id: next.id,
            next_connection_id: Mutex::new(1),
            commands: commands_tx,
            cancellations: cancellations_tx,
            cancel: session_cancel.clone(),
        });
        inner.current.send_replace(Some(Arc::clone(&session)));
        let registration_succeeded = run_relay_session(
            next,
            &inner,
            commands_rx,
            cancellations_rx,
            session_cancel,
            &mut reconnect_episode,
        )
        .await;
        if inner
            .current
            .borrow()
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &session))
        {
            inner.current.send_replace(None);
        }
        if inner.cancel.is_cancelled() {
            close_reconnect_episode(&mut reconnect_episode);
            inner.close_all();
            return;
        }
        if registration_succeeded {
            backoff.reset();
        }
        if reconnect_episode.is_none() {
            reconnect_episode = Some(ReconnectEpisode::start());
        }
        if started_at.elapsed() >= inner.config.reconnect_maximum {
            backoff.reset();
        }
        tokio::select! {
            _ = inner.cancel.cancelled() => {
                close_reconnect_episode(&mut reconnect_episode);
                inner.close_all();
                return;
            }
            _ = tokio::time::sleep(backoff.next_delay()) => {}
        }
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
    listener: Option<Weak<ListenerState>>,
}

impl LivePipe {
    fn compact_listener_queue(&self) -> bool {
        self.listener
            .as_ref()
            .and_then(Weak::upgrade)
            .is_none_or(|listener| listener.try_compact_terminal_queue())
    }
}

struct RelaySessionState {
    next_request_id: u64,
    pending: HashMap<u64, PendingRegistration>,
    pending_by_client: HashMap<DestinationId, u64>,
    registrations: HashMap<DestinationId, Registration>,
    pending_dials: HashMap<u64, oneshot::Sender<Result<Pipe>>>,
    pipes: HashMap<PipeId, LivePipe>,
}

impl RelaySessionState {
    fn new() -> Self {
        Self {
            next_request_id: 1,
            pending: HashMap::new(),
            pending_by_client: HashMap::new(),
            registrations: HashMap::new(),
            pending_dials: HashMap::new(),
            pipes: HashMap::new(),
        }
    }

    fn next_request_id(&mut self) -> Option<u64> {
        let current = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1)?;
        Some(current)
    }
}

enum RelayFrameAction {
    Continue,
    RegistrationSucceeded,
    Reconcile,
    Stop,
}
