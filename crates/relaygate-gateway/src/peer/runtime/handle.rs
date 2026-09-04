//! Cloneable Gateway-facing command and emergency-close surface.

use std::sync::Arc;

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation};
use tokio::sync::{mpsc, oneshot};

use super::{ManagerCommand, SharedCounts, TransportRegistry};
#[cfg(test)]
use crate::peer::config::ResetCommitGate;
use crate::peer::{
    event::{PeerCounts, PeerFailure, PeerOpenRequest, PeerStreamKey},
    identity::OpenIdentity,
};

/// Cloneable bounded command surface used by the Gateway state/effect layer.
#[derive(Clone)]
pub(crate) struct PeerHandle {
    commands: mpsc::Sender<ManagerCommand>,
    transports: TransportRegistry,
    counts: Arc<SharedCounts>,
    #[cfg(test)]
    reset_commit_gate: Option<ResetCommitGate>,
}

impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerHandle")
            .field("counts", &self.counts.snapshot())
            .finish_non_exhaustive()
    }
}

impl PeerHandle {
    pub(super) fn new(
        commands: mpsc::Sender<ManagerCommand>,
        transports: TransportRegistry,
        counts: Arc<SharedCounts>,
        #[cfg(test)] reset_commit_gate: Option<ResetCommitGate>,
    ) -> Self {
        Self {
            commands,
            transports,
            counts,
            #[cfg(test)]
            reset_commit_gate,
        }
    }

    /// Returns only after this OPEN is committed to the selected transport's
    /// ordered writer queue. The returned oneshot and [`PeerEvents`] are
    /// independent bounded channels, so a very fast `Opened`/`Failed` event may
    /// be scheduled before the caller observes this return value. Both paths
    /// carry the same [`PeerStreamKey`] and the Gateway state layer correlates
    /// them without assuming cross-channel delivery order.
    pub(crate) async fn open(
        &self,
        request: PeerOpenRequest,
    ) -> Result<PeerStreamKey, PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Open { request, reply },
            PeerObservation::NotObserved,
        )?;
        response.await.map_err(|_| {
            PeerFailure::not_observed(ErrorCode::Unavailable, "peer manager stopped before OPEN")
        })?
    }

    pub(crate) async fn cancel_open(&self, open_identity: OpenIdentity) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Cancel {
                open_identity,
                reply,
            },
            PeerObservation::MaybeObserved,
        )?;
        await_command_response(response).await
    }

    pub(crate) async fn send_opened(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Opened { key, reply },
            PeerObservation::MaybeObserved,
        )?;
        await_command_response(response).await
    }

    pub(crate) async fn send_failed(
        &self,
        key: PeerStreamKey,
        failure: PeerFailure,
    ) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        if let Err(error) = self.try_manager_send(
            ManagerCommand::Failed {
                key,
                failure,
                reply,
            },
            PeerObservation::MaybeObserved,
        ) {
            self.close_transport(key);
            return Err(error);
        }
        let result = await_command_response(response).await;
        if result.is_err() {
            self.close_transport(key);
        }
        result
    }

    pub(crate) async fn send_data(
        &self,
        key: PeerStreamKey,
        payload: Bytes,
    ) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Data {
                key,
                payload,
                reply,
            },
            PeerObservation::MaybeObserved,
        )?;
        await_command_response(response).await
    }

    pub(crate) async fn send_fin(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Fin { key, reply },
            PeerObservation::MaybeObserved,
        )?;
        await_command_response(response).await
    }

    pub(crate) async fn send_close(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        if let Err(error) = self.try_manager_send(
            ManagerCommand::Close { key, reply },
            PeerObservation::MaybeObserved,
        ) {
            self.close_transport(key);
            return Err(error);
        }
        let result = await_command_response(response).await;
        if result.is_err() {
            self.close_transport(key);
        }
        result
    }

    pub(crate) async fn send_reset(
        &self,
        key: PeerStreamKey,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Result<(), PeerFailure> {
        #[cfg(test)]
        if self
            .reset_commit_gate
            .as_ref()
            .is_some_and(ResetCommitGate::trip)
        {
            return Err(PeerFailure::maybe_observed(
                ErrorCode::ResourceExhausted,
                "test reset commit gate rejected RESET before manager commit",
            ));
        }

        let (reply, response) = oneshot::channel();
        if let Err(error) = self.try_manager_send(
            ManagerCommand::Reset {
                key,
                code,
                message: message.into(),
                reply,
            },
            PeerObservation::MaybeObserved,
        ) {
            self.close_transport(key);
            return Err(error);
        }
        let result = await_command_response(response).await;
        if result.is_err() {
            self.close_transport(key);
        }
        result
    }

    /// Force-closes the containing transport without going through the bounded
    /// manager queue. Gateway cleanup uses this when a terminal frame cannot be
    /// committed, avoiding an ambiguously reusable transport.
    pub(crate) fn close_transport(&self, key: PeerStreamKey) -> bool {
        let Ok(transports) = self.transports.read() else {
            return false;
        };
        let Some(transport) = transports.get(&key.peer_transport_id()) else {
            return false;
        };
        transport.force_close(crate::peer::transport::TransportCloseReason::WriterFailed);
        true
    }

    #[must_use]
    pub(crate) fn counts(&self) -> PeerCounts {
        self.counts.snapshot()
    }

    fn try_manager_send(
        &self,
        command: ManagerCommand,
        observation: PeerObservation,
    ) -> Result<(), PeerFailure> {
        self.commands.try_send(command).map_err(|error| {
            let (code, message) = match error {
                mpsc::error::TrySendError::Full(_) => (
                    ErrorCode::ResourceExhausted,
                    "peer manager command queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    (ErrorCode::Unavailable, "peer manager is closed")
                }
            };
            PeerFailure::new(code, observation, message)
        })
    }
}

async fn await_command_response(
    response: oneshot::Receiver<Result<(), PeerFailure>>,
) -> Result<(), PeerFailure> {
    response.await.map_err(|_| {
        PeerFailure::maybe_observed(
            ErrorCode::Unavailable,
            "peer manager stopped before command result",
        )
    })?
}
