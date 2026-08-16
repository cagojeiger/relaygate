use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::{
    CloseError, PipeError,
    runtime::{MAX_PAYLOAD_BYTES, PipeState, Shared, request},
    wire::{self, connect_request},
};

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

    /// Enqueues one frame to the bounded local authenticated-stream writer.
    /// Success is a local write only, not a peer-application acknowledgement.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<(), PipeError> {
        let payload = payload.into();
        if payload.is_empty() || payload.len() > MAX_PAYLOAD_BYTES {
            return Err(PipeError::InvalidPayload);
        }
        let _enqueue = self.state.enqueue_gate.lock().await;
        if self
            .state
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
            || self.state.terminal.borrow().is_some()
        {
            return Err(PipeError::Terminal);
        }
        self.shared
            .send(request(connect_request::Message::PipePayload(
                wire::PipePayload {
                    pipe_id: self.state.pipe_id.clone(),
                    payload,
                },
            )))
            .await?;
        Ok(())
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
