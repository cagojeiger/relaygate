use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::{Buf, Bytes};
use relaygate_protocol::{ErrorCode as WireErrorCode, Frame, PipeId};
use tokio::sync::{Mutex, mpsc, watch};

use crate::{Error, ErrorCode, PeerObservation, Result, lifetime::RuntimeLifetime};

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
    outbound: mpsc::Sender<Frame>,
    inbound: mpsc::Sender<Bytes>,
    remote_fin: AtomicBool,
    remote_fin_signal: watch::Sender<bool>,
    local_fin: AtomicBool,
    terminal: watch::Sender<Option<Terminal>>,
    write_lane: Mutex<()>,
    // A Pipe value is unique and Drop emits its id at most once. The session
    // actor retains that id only while the corresponding PipeState is current.
    abandoned: mpsc::UnboundedSender<PipeId>,
}

/// One ordered, opaque, bidirectional byte stream.
pub struct Pipe {
    state: Arc<PipeState>,
    _lifetime: Option<Arc<RuntimeLifetime>>,
    inbound: mpsc::Receiver<Bytes>,
    current: Bytes,
    read_eof: bool,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Pipe").finish_non_exhaustive()
    }
}

impl PipeState {
    #[cfg(test)]
    pub(crate) fn pair(
        id: PipeId,
        outbound: mpsc::Sender<Frame>,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(id, outbound, inbound_capacity, abandoned, None)
    }

    pub(crate) fn pair_with_lifetime(
        id: PipeId,
        outbound: mpsc::Sender<Frame>,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Arc<RuntimeLifetime>,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(id, outbound, inbound_capacity, abandoned, Some(lifetime))
    }

    fn pair_inner(
        id: PipeId,
        outbound: mpsc::Sender<Frame>,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Option<Arc<RuntimeLifetime>>,
    ) -> (Pipe, Arc<Self>) {
        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_capacity);
        let (terminal, _) = watch::channel(None);
        let (remote_fin_signal, _) = watch::channel(false);
        let state = Arc::new(Self {
            id,
            outbound,
            inbound: inbound_tx,
            remote_fin: AtomicBool::new(false),
            remote_fin_signal,
            local_fin: AtomicBool::new(false),
            terminal,
            write_lane: Mutex::new(()),
            abandoned,
        });
        let pipe = Pipe {
            state: Arc::clone(&state),
            _lifetime: lifetime,
            inbound: inbound_rx,
            current: Bytes::new(),
            read_eof: false,
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
        if !self.remote_fin.swap(true, Ordering::AcqRel) {
            self.remote_fin_signal.send_replace(true);
        }
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
        self.terminal.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(terminal);
            true
        })
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
}

impl Pipe {
    pub(crate) fn is_terminal(&self) -> bool {
        self.state.terminal.borrow().is_some()
    }

    /// Reads ordered bytes. `0` means graceful EOF.
    pub async fn read(&mut self, destination: &mut [u8]) -> Result<usize> {
        if destination.is_empty() {
            return Ok(0);
        }
        loop {
            if self.current.has_remaining() {
                let count = destination.len().min(self.current.remaining());
                self.current.copy_to_slice(&mut destination[..count]);
                return Ok(count);
            }
            if self.read_eof {
                return Ok(0);
            }
            match self.inbound.try_recv() {
                Ok(payload) => {
                    self.current = payload;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.read_eof = true;
                    return Ok(0);
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            match self.state.terminal.borrow().clone() {
                Some(Terminal::Failed(error)) => return Err(error),
                Some(Terminal::Closed) => {
                    self.read_eof = true;
                    return Ok(0);
                }
                None => {}
            }
            if self.state.remote_fin.load(Ordering::Acquire) {
                self.read_eof = true;
                return Ok(0);
            }
            let mut terminal = self.state.terminal.subscribe();
            let mut remote_fin = self.state.remote_fin_signal.subscribe();
            tokio::select! {
                biased;
                payload = self.inbound.recv() => {
                    match payload {
                        Some(payload) => self.current = payload,
                        None => {
                            self.read_eof = true;
                            if let Some(Terminal::Failed(error)) = terminal.borrow().clone() {
                                return Err(error);
                            }
                            return Ok(0);
                        }
                    }
                }
                changed = terminal.changed() => {
                    if changed.is_err() {
                        self.read_eof = true;
                        return Ok(0);
                    }
                    match terminal.borrow().clone() {
                        Some(Terminal::Failed(error)) => return Err(error),
                        Some(Terminal::Closed) => {
                            self.read_eof = true;
                            return Ok(0);
                        }
                        None => {}
                    }
                }
                changed = remote_fin.changed() => {
                    if changed.is_err() || *remote_fin.borrow() {
                        continue;
                    }
                }
            }
        }
    }

    /// Enqueues all bytes to the bounded session path in order.
    ///
    /// Success is not a peer application delivery acknowledgement.
    pub async fn write_all(&self, payload: &[u8]) -> Result<()> {
        let _lane = self.state.write_lane.lock().await;
        if let Some(error) = self.state.write_error() {
            return Err(error);
        }
        for chunk in payload.chunks(DATA_CHUNK_LEN) {
            let permit = self
                .state
                .outbound
                .reserve()
                .await
                .map_err(|_| Error::maybe_observed("session ended while writing Pipe data"))?;
            if let Some(error) = self.state.write_error() {
                return Err(error);
            }
            permit.send(Frame::Data {
                pipe_id: self.state.id,
                payload: Bytes::copy_from_slice(chunk),
            });
        }
        Ok(())
    }

    /// Gracefully closes only this endpoint's write direction.
    pub async fn shutdown_write(&self) -> Result<()> {
        let _lane = self.state.write_lane.lock().await;
        if self.state.terminal.borrow().is_some() {
            return Ok(());
        }
        if self.state.local_fin.load(Ordering::Acquire) {
            return Ok(());
        }
        let permit = self
            .state
            .outbound
            .reserve()
            .await
            .map_err(|_| Error::maybe_observed("session ended while sending FIN"))?;
        if self.state.terminal.borrow().is_some() {
            return Ok(());
        }
        self.state.local_fin.store(true, Ordering::Release);
        permit.send(Frame::Fin {
            pipe_id: self.state.id,
        });
        if self.state.remote_fin.load(Ordering::Acquire) {
            self.state.close_normal();
        }
        Ok(())
    }

    /// Closes both directions. Repeated calls are safe.
    pub async fn close(&self) -> Result<()> {
        let _lane = self.state.write_lane.lock().await;
        if self.state.terminal.borrow().is_some() {
            return Ok(());
        }
        let permit = self
            .state
            .outbound
            .reserve()
            .await
            .map_err(|_| Error::maybe_observed("session ended while sending CLOSE"))?;
        if !self.state.close_normal() {
            return Ok(());
        }
        permit.send(Frame::Close {
            pipe_id: self.state.id,
        });
        Ok(())
    }

    /// Fails both directions. Repeated calls are safe.
    pub async fn reset(&self, code: ErrorCode, message: impl Into<String>) -> Result<()> {
        let _lane = self.state.write_lane.lock().await;
        if self.state.terminal.borrow().is_some() {
            return Ok(());
        }
        let message = message.into();
        let permit = self
            .state
            .outbound
            .reserve()
            .await
            .map_err(|_| Error::maybe_observed("session ended while sending RESET"))?;
        if !self
            .state
            .fail(Error::new(code, PeerObservation::Observed, message.clone()))
        {
            return Ok(());
        }
        permit.send(Frame::Reset {
            pipe_id: self.state.id,
            code: to_wire_code(code),
            message,
        });
        Ok(())
    }
}

impl Drop for Pipe {
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
