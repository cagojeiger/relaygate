use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use futures_util::{future::poll_fn, task::AtomicWaker};
use relaygate_protocol::{ErrorCode as WireErrorCode, PipeId};
use tokio::sync::{mpsc, watch};

use crate::{
    Error, ErrorCode, PeerObservation, Result, lifetime::RuntimeLifetime, session::SessionOutbound,
};

mod io;

#[cfg(test)]
mod tests;

const DATA_CHUNK_LEN: usize = 64 * 1024;

#[derive(Clone, Debug)]
enum Terminal {
    Closed,
    Failed(Error),
}

pub(crate) struct PipeState {
    id: PipeId,
    inbound: mpsc::Sender<Bytes>,
    remote_fin: AtomicBool,
    local_fin: AtomicBool,
    terminal: watch::Sender<Option<Terminal>>,
    read_waker: AtomicWaker,
    write_waker: AtomicWaker,
    abandoned: mpsc::UnboundedSender<PipeId>,
}

struct PipeOwner {
    state: Arc<PipeState>,
    _lifetime: Option<Arc<RuntimeLifetime>>,
}

struct PipeWriter {
    outbound: SessionOutbound,
}

struct PipeReader {
    inbound: mpsc::Receiver<Bytes>,
    current: Bytes,
    read_eof: bool,
    #[cfg(test)]
    after_inbound_pending: Option<Box<dyn FnOnce() + Send>>,
}

/// One ordered, opaque, bidirectional byte stream.
///
/// I/O errors do not report payload delivery. Do not use their
/// [`Error::is_retryable`] hint to replay bytes on this or a replacement Pipe.
pub struct Pipe {
    owner: Arc<PipeOwner>,
    reader: PipeReader,
    writer: PipeWriter,
}

/// The unique read side of an owned [`Pipe`] split.
///
/// Dropping this half does not send `FIN`. The Pipe is abandoned only after
/// both owned halves have been dropped without an explicit terminal action.
pub struct PipeReadHalf {
    owner: Arc<PipeOwner>,
    reader: PipeReader,
}

/// The unique write side of an owned [`Pipe`] split.
///
/// Call [`PipeWriteHalf::shutdown_write`] or [`tokio::io::AsyncWriteExt::shutdown`]
/// to send `FIN`; dropping this half alone never half-closes the Pipe.
pub struct PipeWriteHalf {
    owner: Arc<PipeOwner>,
    writer: PipeWriter,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Pipe").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PipeReadHalf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipeReadHalf")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PipeWriteHalf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipeWriteHalf")
            .finish_non_exhaustive()
    }
}

impl PipeState {
    #[cfg(test)]
    pub(crate) fn pair(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(id, outbound, inbound_capacity, abandoned, None)
    }

    pub(crate) fn pair_with_lifetime(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Arc<RuntimeLifetime>,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(id, outbound, inbound_capacity, abandoned, Some(lifetime))
    }

    fn pair_inner(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Option<Arc<RuntimeLifetime>>,
    ) -> (Pipe, Arc<Self>) {
        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_capacity);
        let (terminal, _) = watch::channel(None);
        let state = Arc::new(Self {
            id,
            inbound: inbound_tx,
            remote_fin: AtomicBool::new(false),
            local_fin: AtomicBool::new(false),
            terminal,
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
            abandoned,
        });
        let owner = Arc::new(PipeOwner {
            state: Arc::clone(&state),
            _lifetime: lifetime,
        });
        let pipe = Pipe {
            owner,
            reader: PipeReader {
                inbound: inbound_rx,
                current: Bytes::new(),
                read_eof: false,
                #[cfg(test)]
                after_inbound_pending: None,
            },
            writer: PipeWriter { outbound },
        };
        (pipe, state)
    }

    pub(crate) fn push_data(&self, payload: Bytes) -> Result<()> {
        if self.remote_fin.load(Ordering::Acquire) || self.terminal.borrow().is_some() {
            return Err(Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::Observed,
                "DATA arrived after the remote direction closed",
            ));
        }
        self.inbound.try_send(payload).map_err(|error| {
            Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::Observed,
                format!("Pipe inbound buffer is full or closed: {error}"),
            )
        })
    }

    pub(crate) fn remote_fin(&self) {
        self.remote_fin.store(true, Ordering::Release);
        self.read_waker.wake();
        if self.local_fin.load(Ordering::Acquire) {
            self.close_normal();
        }
    }

    pub(crate) fn close_normal(&self) -> bool {
        self.try_set_terminal(Terminal::Closed)
    }

    pub(crate) fn fail(&self, error: Error) -> bool {
        self.try_set_terminal(Terminal::Failed(error))
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.terminal.borrow().is_some()
            || (self.local_fin.load(Ordering::Acquire) && self.remote_fin.load(Ordering::Acquire))
    }

    fn try_set_terminal(&self, terminal: Terminal) -> bool {
        let terminal_event = match &terminal {
            Terminal::Closed => None,
            Terminal::Failed(error) => Some((error.code(), error.observation())),
        };
        let changed = self.terminal.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(terminal);
            true
        });
        if changed {
            self.read_waker.wake();
            self.write_waker.wake();
            if let Some((error_code, observation)) = terminal_event {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.pipe.terminal",
                    connector_session_id = %self.id.origin_session_id().as_uuid(),
                    connection_id = self.id.connection_id(),
                    outcome = "failed",
                    error_code = ?error_code,
                    observation = ?observation,
                    "Pipe reached a terminal failure"
                );
            } else {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.pipe.terminal",
                    connector_session_id = %self.id.origin_session_id().as_uuid(),
                    connection_id = self.id.connection_id(),
                    outcome = "closed",
                    "Pipe closed"
                );
            }
        }
        changed
    }

    fn write_error(&self) -> Option<Error> {
        match self.terminal.borrow().clone() {
            Some(Terminal::Failed(error)) => Some(error),
            Some(Terminal::Closed) => Some(Error::new(
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "Pipe write direction is closed",
            )),
            None if self.local_fin.load(Ordering::Acquire) => Some(Error::new(
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "Pipe write direction is closed",
            )),
            None => None,
        }
    }

    fn terminal_failure(&self) -> Option<Error> {
        match self.terminal.borrow().clone() {
            Some(Terminal::Failed(error)) => Some(error),
            Some(Terminal::Closed) | None => None,
        }
    }
}

impl Pipe {
    pub(crate) fn is_terminal(&self) -> bool {
        self.owner.state.terminal.borrow().is_some()
    }

    /// Splits this Pipe into independently owned read and write halves.
    ///
    /// The halves share one protocol state and cannot be cloned. The read half
    /// retains the only inbound cursor, while the write half retains all write,
    /// half-close, close, and reset operations.
    #[must_use]
    pub fn into_split(self) -> (PipeReadHalf, PipeWriteHalf) {
        let Self {
            owner,
            reader,
            writer,
        } = self;
        let write_owner = Arc::clone(&owner);
        (
            PipeReadHalf { owner, reader },
            PipeWriteHalf {
                owner: write_owner,
                writer,
            },
        )
    }

    /// Reads ordered bytes while preserving RelayGate's structured [`Error`].
    /// `0` means graceful EOF.
    ///
    /// Prefer Tokio's [`tokio::io::AsyncReadExt`] helpers for ordinary I/O.
    pub async fn read_into(&mut self, destination: &mut [u8]) -> Result<usize> {
        poll_fn(|context| {
            self.reader
                .poll_read(&self.owner.state, context, destination)
        })
        .await
    }

    /// Enqueues all bytes to the bounded session path in order.
    ///
    /// Success is not a peer application delivery acknowledgement.
    /// Prefer Tokio's [`tokio::io::AsyncWriteExt`] helpers for ordinary I/O.
    pub async fn write_all_bytes(&mut self, payload: &[u8]) -> Result<()> {
        self.writer.write_all(&self.owner.state, payload).await
    }

    /// Gracefully closes only this endpoint's write direction.
    pub async fn shutdown_write(&mut self) -> Result<()> {
        self.writer.shutdown_write(&self.owner.state).await
    }

    /// Closes both directions. Repeated calls are safe.
    pub async fn close(&mut self) -> Result<()> {
        self.writer.close(&self.owner.state).await
    }

    /// Fails both directions. Repeated calls are safe.
    pub async fn reset(&mut self, code: ErrorCode, message: impl Into<String>) -> Result<()> {
        self.writer
            .reset(&self.owner.state, code, message.into())
            .await
    }
}

impl PipeReadHalf {
    /// Split-read equivalent of [`Pipe::read_into`].
    pub async fn read_into(&mut self, destination: &mut [u8]) -> Result<usize> {
        poll_fn(|context| {
            self.reader
                .poll_read(&self.owner.state, context, destination)
        })
        .await
    }
}

impl PipeWriteHalf {
    /// Split-write equivalent of [`Pipe::write_all_bytes`].
    pub async fn write_all_bytes(&mut self, payload: &[u8]) -> Result<()> {
        self.writer.write_all(&self.owner.state, payload).await
    }

    /// Split-write equivalent of [`Pipe::shutdown_write`].
    pub async fn shutdown_write(&mut self) -> Result<()> {
        self.writer.shutdown_write(&self.owner.state).await
    }

    /// Split-write equivalent of [`Pipe::close`].
    pub async fn close(&mut self) -> Result<()> {
        self.writer.close(&self.owner.state).await
    }

    /// Split-write equivalent of [`Pipe::reset`].
    pub async fn reset(&mut self, code: ErrorCode, message: impl Into<String>) -> Result<()> {
        self.writer
            .reset(&self.owner.state, code, message.into())
            .await
    }
}

impl PipeWriter {
    async fn write_all(&mut self, state: &PipeState, payload: &[u8]) -> Result<()> {
        if payload.is_empty() {
            self.outbound.cancel_wait();
            return state.write_error().map_or(Ok(()), Err);
        }
        let mut written = 0;
        while written < payload.len() {
            written +=
                poll_fn(|context| self.poll_write(state, context, &payload[written..])).await?;
        }
        Ok(())
    }

    async fn shutdown_write(&mut self, state: &PipeState) -> Result<()> {
        poll_fn(|context| self.poll_shutdown(state, context)).await
    }

    async fn close(&mut self, state: &PipeState) -> Result<()> {
        poll_fn(|context| self.poll_close(state, context)).await
    }

    async fn reset(&mut self, state: &PipeState, code: ErrorCode, message: String) -> Result<()> {
        poll_fn(|context| self.poll_reset(state, context, code, &message)).await
    }
}

impl Drop for PipeOwner {
    fn drop(&mut self) {
        if self.state.close_normal() {
            let _ = self.state.abandoned.send(self.state.id);
        }
    }
}

pub(crate) fn to_wire_code(code: ErrorCode) -> WireErrorCode {
    match code {
        ErrorCode::InvalidArgument => WireErrorCode::InvalidArgument,
        ErrorCode::Unauthenticated => WireErrorCode::Unauthenticated,
        ErrorCode::PermissionDenied => WireErrorCode::PermissionDenied,
        ErrorCode::NotFound => WireErrorCode::NotFound,
        ErrorCode::FailedPrecondition => WireErrorCode::FailedPrecondition,
        ErrorCode::Unavailable => WireErrorCode::Unavailable,
        ErrorCode::DeadlineExceeded => WireErrorCode::DeadlineExceeded,
        ErrorCode::ResourceExhausted => WireErrorCode::ResourceExhausted,
        ErrorCode::Cancelled => WireErrorCode::Cancelled,
        ErrorCode::ProtocolError => WireErrorCode::ProtocolError,
        ErrorCode::Internal => WireErrorCode::Internal,
        ErrorCode::AlreadyExists => WireErrorCode::AlreadyExists,
    }
}
