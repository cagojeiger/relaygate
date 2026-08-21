use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::{
    CloseError, DeliveryError, PipeError, SessionError,
    runtime::{DeliveryTerminal, MAX_PAYLOAD_BYTES, PipeState, Shared, request},
    wire::{self, connect_request},
};
use uuid::Uuid;

/// One volatile, bidirectional, message-framed Pipe.
pub struct Pipe {
    pub(crate) state: Arc<PipeState>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) payloads: Mutex<mpsc::Receiver<Vec<u8>>>,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pipe")
            .field("pipe_id", &self.state.pipe_id)
            .finish_non_exhaustive()
    }
}

impl Pipe {
    pub fn id(&self) -> &str {
        &self.state.pipe_id
    }

    /// Delivers one frame to the remote SDK's bounded receive queue.
    ///
    /// Success means the exact remote queue-admission receipt was observed. It
    /// does not mean that application code processed or durably stored it.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<(), DeliveryError> {
        let payload = payload.into();
        if payload.is_empty() || payload.len() > MAX_PAYLOAD_BYTES {
            return Err(DeliveryError::not_sent(None, None));
        }
        let payload_id = Uuid::new_v4().to_string();
        let _enqueue = self.state.enqueue_gate.lock().await;
        if self
            .state
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
            || self.state.terminal.borrow().is_some()
        {
            return Err(DeliveryError::not_sent(Some(payload_id), None));
        }
        let receipt = self
            .state
            .begin_delivery(payload_id.clone())
            .map_err(|error| DeliveryError::not_sent(Some(payload_id.clone()), Some(error)))?;
        let mut guard = DeliveryGuard {
            shared: Arc::clone(&self.shared),
            state: Arc::clone(&self.state),
            payload_id: payload_id.clone(),
            sent: false,
            armed: true,
        };
        let message = request(connect_request::Message::PipePayload(wire::PipePayload {
            pipe_id: self.state.pipe_id.clone(),
            payload,
            payload_id: payload_id.clone(),
        }));
        if self.shared.outbound.send(message).await.is_err() {
            let error = SessionError::Transport("request stream ended".into());
            let _ = self
                .state
                .finish_delivery(&payload_id, DeliveryTerminal::NotSent);
            guard.armed = false;
            self.shared.terminate(error.clone());
            return receipt
                .await
                .unwrap_or_else(|_| Err(DeliveryError::not_sent(Some(payload_id), Some(error))));
        }
        guard.sent = true;
        let result = receipt.await.unwrap_or_else(|_| {
            Err(DeliveryError::unknown(
                payload_id,
                Some(self.shared.terminal_or_transport()),
            ))
        });
        guard.armed = false;
        result
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, PipeError> {
        let mut terminal = self.state.terminal.subscribe();
        if let Some(error) = terminal.borrow().clone() {
            return Err(error);
        }
        let mut payloads = self.payloads.lock().await;
        tokio::select! {
            biased;
            changed = terminal.changed() => {
                if changed.is_err() {
                    Err(PipeError::Terminal)
                } else {
                    Err(terminal.borrow().clone().unwrap_or(PipeError::Terminal))
                }
            }
            payload = payloads.recv() => payload.ok_or(PipeError::Terminal),
        }
    }

    pub async fn close(&self) -> Result<(), CloseError> {
        self.shared.close_pipe(&self.state.pipe_id).await
    }

    pub async fn done(&self) -> PipeError {
        let mut terminal = self.state.terminal.subscribe();
        loop {
            if let Some(error) = terminal.borrow().clone() {
                return error;
            }
            if terminal.changed().await.is_err() {
                return PipeError::Terminal;
            }
        }
    }
}

struct DeliveryGuard {
    shared: Arc<Shared>,
    state: Arc<PipeState>,
    payload_id: String,
    sent: bool,
    armed: bool,
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !self.sent {
            let _ = self
                .state
                .finish_delivery(&self.payload_id, DeliveryTerminal::NotSent);
            return;
        }
        let _ = self
            .state
            .finish_delivery(&self.payload_id, DeliveryTerminal::Unknown);
        self.shared
            .terminalize_pipe(&self.state.pipe_id, PipeError::Terminal);
        self.shared
            .send_background(request(connect_request::Message::ClosePipe(
                wire::ClosePipe {
                    pipe_id: self.state.pipe_id.clone(),
                },
            )));
    }
}
