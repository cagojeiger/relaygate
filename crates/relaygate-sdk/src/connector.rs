mod runtime;
#[cfg(test)]
mod tests;

use std::sync::{Arc, Weak};

use relaygate_protocol::{PipeId, SessionId, SessionRole};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::{Instant, sleep_until, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    Config, Error, ErrorCode, PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime,
    session::{establish, valid_identity},
};

use self::runtime::connector_supervisor;

#[derive(Clone)]
pub struct Connector {
    inner: Arc<ConnectorInner>,
    _lifetime: Arc<RuntimeLifetime>,
}

pub(super) struct ConnectorInner {
    pub(super) config: Config,
    pub(super) current: watch::Sender<Option<Arc<ConnectorSession>>>,
    pub(super) cancel: CancellationToken,
    pub(super) lifetime: Weak<RuntimeLifetime>,
}

pub(super) struct ConnectorSession {
    pub(super) id: SessionId,
    pub(super) next_connection_id: Mutex<u64>,
    pub(super) control: mpsc::Sender<ConnectorCommand>,
    // Each committed attempt owns one guard and can emit at most one cancel.
    // The receiver removes the corresponding current attempt when it consumes it.
    pub(super) cancellations: mpsc::UnboundedSender<PipeId>,
    pub(super) cancel: CancellationToken,
}

pub(super) enum ConnectorCommand {
    Open {
        connection_id: u64,
        client_id: String,
        response: oneshot::Sender<Result<Pipe>>,
    },
}

struct OpenGuard {
    cancellations: mpsc::UnboundedSender<PipeId>,
    pipe_id: PipeId,
    armed: bool,
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cancellations.send(self.pipe_id);
        }
    }
}

impl Connector {
    /// Connects the initial managed Connector session.
    ///
    /// After this succeeds, the SDK reconnects in the background when the
    /// Gateway transport is lost. A committed `open` is never replayed on a
    /// replacement session.
    pub async fn connect(config: Config) -> Result<Self> {
        config.validate()?;
        let established = establish(&config, SessionRole::Connector).await?;
        let (current, _) = watch::channel(None);
        let cancel = CancellationToken::new();
        let lifetime = Arc::new(RuntimeLifetime::new(cancel.clone()));
        let inner = Arc::new(ConnectorInner {
            config,
            current,
            cancel,
            lifetime: Arc::downgrade(&lifetime),
        });
        tokio::spawn(connector_supervisor(Arc::clone(&inner), established));
        Ok(Self {
            inner,
            _lifetime: lifetime,
        })
    }

    /// Opens one Pipe to a logical `ClientId`.
    pub async fn open(&self, client_id: impl Into<String>) -> Result<Pipe> {
        let client_id = client_id.into();
        if !valid_identity(&client_id) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "ClientId must be non-empty and fit the wire limit",
            ));
        }

        let deadline = Instant::now() + self.inner.config.operation_timeout;
        let mut current = self.inner.current.subscribe();
        loop {
            if self.inner.cancel.is_cancelled() {
                return Err(Error::closed());
            }
            let session = current.borrow().clone();
            if let Some(session) = session {
                let mut next_connection_id =
                    timeout_at(deadline, session.next_connection_id.lock())
                        .await
                        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?;
                let connection_id = *next_connection_id;
                *next_connection_id = connection_id.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::ResourceExhausted,
                        PeerObservation::NotObserved,
                        "ConnectorSession exhausted ConnectionId space",
                    )
                })?;
                let pipe_id = PipeId::new(session.id, connection_id);
                let (response_tx, response_rx) = oneshot::channel();
                let command = ConnectorCommand::Open {
                    connection_id,
                    client_id: client_id.clone(),
                    response: response_tx,
                };
                let committed = timeout_at(deadline, session.control.send(command)).await;
                drop(next_connection_id);
                match committed {
                    Ok(Ok(())) => {
                        let mut guard = OpenGuard {
                            cancellations: session.cancellations.clone(),
                            pipe_id,
                            armed: true,
                        };
                        let result = match timeout_at(deadline, response_rx).await {
                            Ok(Ok(result)) => {
                                guard.armed = false;
                                result
                            }
                            Ok(Err(_)) => {
                                guard.armed = false;
                                Err(Error::maybe_observed(
                                    "ConnectorSession ended after OPEN commit",
                                ))
                            }
                            Err(_) => {
                                session.cancel.cancel();
                                Err(Error::deadline(PeerObservation::MaybeObserved))
                            }
                        };
                        return result;
                    }
                    Ok(Err(_)) => {
                        // The request did not enter a live session's bounded
                        // path, so this same call may wait for a new session.
                        if current
                            .borrow()
                            .as_ref()
                            .is_some_and(|active| Arc::ptr_eq(active, &session))
                        {
                            tokio::select! {
                                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                                _ = sleep_until(deadline) => {
                                    return Err(Error::deadline(PeerObservation::NotObserved));
                                }
                                changed = current.changed() => {
                                    if changed.is_err() {
                                        return Err(Error::closed());
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    Err(_) => {
                        return Err(Error::deadline(PeerObservation::NotObserved));
                    }
                }
            }

            tokio::select! {
                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                _ = sleep_until(deadline) => {
                    return Err(Error::deadline(PeerObservation::NotObserved));
                }
                changed = current.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
            }
        }
    }

    /// Stops reconnection and terminates current Connector Pipes.
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }
}
