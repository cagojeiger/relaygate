use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};

use crate::{
    AcceptError, BindError, CloseError, Listener, Offer, OfferMetadata, OpenError, OpenFailure,
    Pipe, PipeError, Session, SessionError, UnbindError,
    wire::{self, connect_request, connect_response},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

#[cfg(test)]
use crate::{Client, Config};

pub(crate) const OUTBOUND_CAPACITY: usize = 64;
const OFFER_QUEUE_CAPACITY: usize = 32;
const PIPE_PAYLOAD_CAPACITY: usize = 32;
pub(crate) const MAX_LISTENERS: usize = 512;
const MAX_OFFERS: usize = 1_024;
pub(crate) const MAX_OPEN_REQUESTS: usize = 1_024;
const MAX_PIPES: usize = 1_024;
pub(crate) const MAX_IDENTITY_BYTES: usize = 128;
pub(crate) const MAX_ENDPOINT_BYTES: usize = 1_024;
pub(crate) const MAX_PAYLOAD_BYTES: usize = 60 << 10;

#[derive(Clone, Debug, PartialEq, Eq)]
struct BindingFingerprint {
    binding_id: String,
    endpoint_pattern: String,
    target_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OpenTerminal {
    Opened {
        attempt_id: String,
        pipe_id: String,
        endpoint: String,
        target_id: String,
    },
    Failed {
        endpoint: String,
        target_id: String,
        failure: i32,
    },
    Unknown {
        endpoint: String,
        target_id: String,
    },
    RequestRejected {
        failure: i32,
    },
}

pub(crate) struct Shared {
    pub(crate) outbound: mpsc::Sender<wire::ConnectRequest>,
    pub(crate) session: Session,
    terminal_tx: watch::Sender<Option<SessionError>>,
    pub(crate) binding_lane: Mutex<()>,
    pub(crate) binding_pending: StdMutex<Option<BindingPending>>,
    pub(crate) listeners: StdMutex<HashMap<String, Arc<ListenerState>>>,
    offers: StdMutex<HashMap<String, Arc<OfferState>>>,
    pub(crate) opens: StdMutex<HashMap<String, Arc<PendingOpen>>>,
    pipes: StdMutex<HashMap<String, Arc<PipeState>>>,
    closes: StdMutex<HashMap<String, oneshot::Sender<Result<(), CloseError>>>>,
    retired_bindings: StdMutex<VecDeque<String>>,
    binding_fingerprints: StdMutex<VecDeque<BindingFingerprint>>,
    open_history: StdMutex<VecDeque<(String, OpenTerminal)>>,
    confirmation_history: StdMutex<VecDeque<(String, String)>>,
    close_history: StdMutex<VecDeque<(String, bool)>>,
    cancel_history: StdMutex<VecDeque<(String, Option<bool>)>>,
    pipe_history: StdMutex<VecDeque<(String, String)>>,
    pipe_slots: AtomicUsize,
    pub(crate) dispatcher: StdMutex<Option<JoinHandle<()>>>,
}

impl Shared {
    pub(crate) fn new(outbound: mpsc::Sender<wire::ConnectRequest>, session: Session) -> Self {
        let (terminal_tx, _) = watch::channel(None);
        Self {
            outbound,
            session,
            terminal_tx,
            binding_lane: Mutex::new(()),
            binding_pending: StdMutex::new(None),
            listeners: StdMutex::new(HashMap::new()),
            offers: StdMutex::new(HashMap::new()),
            opens: StdMutex::new(HashMap::new()),
            pipes: StdMutex::new(HashMap::new()),
            closes: StdMutex::new(HashMap::new()),
            retired_bindings: StdMutex::new(VecDeque::new()),
            binding_fingerprints: StdMutex::new(VecDeque::new()),
            open_history: StdMutex::new(VecDeque::new()),
            confirmation_history: StdMutex::new(VecDeque::new()),
            close_history: StdMutex::new(VecDeque::new()),
            cancel_history: StdMutex::new(VecDeque::new()),
            pipe_history: StdMutex::new(VecDeque::new()),
            pipe_slots: AtomicUsize::new(0),
            dispatcher: StdMutex::new(None),
        }
    }

    pub(crate) fn terminal(&self) -> Option<SessionError> {
        self.terminal_tx.borrow().clone()
    }

    pub(crate) fn terminal_or_transport(&self) -> SessionError {
        self.terminal()
            .unwrap_or_else(|| SessionError::Transport("response dispatcher ended".into()))
    }

    pub(crate) fn ensure_active(&self) -> Result<(), SessionError> {
        match self.terminal() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn wait_done(&self) -> SessionError {
        let mut terminal = self.terminal_tx.subscribe();
        loop {
            if let Some(error) = terminal.borrow().clone() {
                return error;
            }
            if terminal.changed().await.is_err() {
                return SessionError::Transport("session terminal channel closed".into());
            }
        }
    }

    pub(crate) async fn send(&self, message: wire::ConnectRequest) -> Result<(), SessionError> {
        self.ensure_active()?;
        self.outbound.send(message).await.map_err(|_| {
            let error = SessionError::Transport("request stream ended".into());
            self.terminate(error.clone());
            error
        })
    }

    pub(crate) fn send_background(self: &Arc<Self>, message: wire::ConnectRequest) {
        if self.terminal().is_some() {
            return;
        }
        if let Err(error) = self.outbound.try_send(message) {
            let detail = match error {
                mpsc::error::TrySendError::Full(_) => "outbound control queue exhausted",
                mpsc::error::TrySendError::Closed(_) => "request stream ended",
            };
            self.terminate(SessionError::Transport(detail.into()));
        }
    }

    fn spawn_task(
        self: &Arc<Self>,
        purpose: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) {
        match Handle::try_current() {
            Ok(handle) => {
                handle.spawn(task);
            }
            Err(_) => self.terminate(SessionError::Transport(format!(
                "no Tokio runtime available for {purpose}"
            ))),
        }
    }

    pub(crate) fn terminate(&self, error: SessionError) {
        if self.terminal_tx.send_if_modified(|state| {
            if state.is_none() {
                *state = Some(error.clone());
                true
            } else {
                false
            }
        }) {
            if let Some(pending) = self
                .binding_pending
                .lock()
                .expect("binding pending lock poisoned")
                .take()
            {
                pending.fail(error.clone());
            }
            for (_, pending) in self.opens.lock().expect("opens lock poisoned").drain() {
                pending.release_slot(self);
                pending.complete(Err(OpenError::Session(error.clone())));
            }
            for (_, offer) in self.offers.lock().expect("offers lock poisoned").drain() {
                offer.ended.store(true, Ordering::Release);
                offer.release_slot(self);
                offer.publish(OfferEvent::Session(error.clone()));
            }
            for listener in self
                .listeners
                .lock()
                .expect("listeners lock poisoned")
                .values()
            {
                listener.retire();
            }
            self.listeners
                .lock()
                .expect("listeners lock poisoned")
                .clear();
            for (_, pipe) in self.pipes.lock().expect("pipes lock poisoned").drain() {
                pipe.release_slot(self);
                pipe.terminate(PipeError::Session(error.clone()));
            }
            for (_, close) in self.closes.lock().expect("closes lock poisoned").drain() {
                let _ = close.send(Err(CloseError::Session(error.clone())));
            }
        }
    }

    fn clear_binding_pending_if(&self, operation_id: &str) {
        let mut pending = self
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        if pending
            .as_ref()
            .is_some_and(|pending| pending.operation_id() == operation_id)
        {
            pending.take();
        }
    }

    fn record_retired_binding(&self, binding_id: &str) -> bool {
        let mut retired = self
            .retired_bindings
            .lock()
            .expect("retired bindings lock poisoned");
        if retired.iter().any(|known| known == binding_id) {
            return false;
        }
        if retired.len() == MAX_LISTENERS {
            retired.pop_front();
        }
        retired.push_back(binding_id.to_owned());
        true
    }

    fn binding_is_retired(&self, binding_id: &str) -> bool {
        self.retired_bindings
            .lock()
            .expect("retired bindings lock poisoned")
            .iter()
            .any(|known| known == binding_id)
    }

    fn forget_retired_binding(&self, binding_id: &str) {
        let mut retired = self
            .retired_bindings
            .lock()
            .expect("retired bindings lock poisoned");
        if let Some(index) = retired.iter().position(|known| known == binding_id) {
            retired.remove(index);
        }
    }

    fn remember_binding_fingerprint(&self, binding: &wire::ListenerBinding) {
        let mut history = self
            .binding_fingerprints
            .lock()
            .expect("binding fingerprint lock poisoned");
        if history
            .iter()
            .any(|known| known.binding_id == binding.listener_binding_id)
        {
            return;
        }
        if history.len() == MAX_LISTENERS {
            history.pop_front();
        }
        history.push_back(BindingFingerprint {
            binding_id: binding.listener_binding_id.clone(),
            endpoint_pattern: binding.endpoint_pattern.clone(),
            target_id: binding.target_id.clone(),
        });
    }

    fn binding_fingerprint_matches(&self, binding: &wire::ListenerBinding) -> Option<bool> {
        self.binding_fingerprints
            .lock()
            .expect("binding fingerprint lock poisoned")
            .iter()
            .find(|known| known.binding_id == binding.listener_binding_id)
            .map(|known| {
                known.endpoint_pattern == binding.endpoint_pattern
                    && known.target_id == binding.target_id
            })
    }

    fn remove_open_if(&self, request_id: &str, expected: &Arc<PendingOpen>) -> bool {
        let mut opens = self.opens.lock().expect("opens lock poisoned");
        if opens
            .get(request_id)
            .is_some_and(|pending| Arc::ptr_eq(pending, expected))
        {
            opens.remove(request_id);
            true
        } else {
            false
        }
    }

    fn open_terminal_matches(&self, request_id: &str, terminal: &OpenTerminal) -> Option<bool> {
        self.open_history
            .lock()
            .expect("open history lock poisoned")
            .iter()
            .find(|(known_id, _)| known_id == request_id)
            .map(|(_, known)| known == terminal)
    }

    fn remember_open_terminal(&self, request_id: String, terminal: OpenTerminal) {
        let mut history = self
            .open_history
            .lock()
            .expect("open history lock poisoned");
        if history.iter().any(|(known_id, _)| known_id == &request_id) {
            return;
        }
        if history.len() == MAX_OPEN_REQUESTS {
            history.pop_front();
        }
        history.push_back((request_id, terminal));
    }

    fn confirmation_matches(&self, attempt_id: &str, pipe_id: &str) -> Option<bool> {
        self.confirmation_history
            .lock()
            .expect("confirmation history lock poisoned")
            .iter()
            .find(|(known_attempt, _)| known_attempt == attempt_id)
            .map(|(_, known_pipe)| known_pipe == pipe_id)
    }

    fn remember_confirmation(&self, attempt_id: String, pipe_id: String) {
        let mut history = self
            .confirmation_history
            .lock()
            .expect("confirmation history lock poisoned");
        if history
            .iter()
            .any(|(known_attempt, _)| known_attempt == &attempt_id)
        {
            return;
        }
        if history.len() == MAX_OFFERS {
            history.pop_front();
        }
        history.push_back((attempt_id, pipe_id));
    }

    pub(crate) fn retire_offer_identity(&self, attempt_id: &str, pipe_id: &str) {
        self.remember_confirmation(attempt_id.to_owned(), pipe_id.to_owned());
    }

    fn close_ack_matches(&self, pipe_id: &str, owned: bool) -> Option<bool> {
        self.close_history
            .lock()
            .expect("close history lock poisoned")
            .iter()
            .find(|(known_pipe, _)| known_pipe == pipe_id)
            .map(|(_, known_owned)| *known_owned == owned)
    }

    fn remember_close_ack(&self, pipe_id: String, owned: bool) {
        let mut history = self
            .close_history
            .lock()
            .expect("close history lock poisoned");
        if history.iter().any(|(known_pipe, _)| known_pipe == &pipe_id) {
            return;
        }
        if history.len() == MAX_PIPES {
            history.pop_front();
        }
        history.push_back((pipe_id, owned));
    }

    fn remember_cancel_request(&self, request_id: &str) {
        let mut history = self
            .cancel_history
            .lock()
            .expect("cancel history lock poisoned");
        if history
            .iter()
            .any(|(known_request, _)| known_request == request_id)
        {
            return;
        }
        if history.len() == MAX_OPEN_REQUESTS {
            history.pop_front();
        }
        history.push_back((request_id.to_owned(), None));
    }

    fn acknowledge_cancel(&self, request_id: &str, was_pending: bool) -> Option<bool> {
        let mut history = self
            .cancel_history
            .lock()
            .expect("cancel history lock poisoned");
        let (_, acknowledged) = history
            .iter_mut()
            .find(|(known_request, _)| known_request == request_id)?;
        match acknowledged {
            Some(known) => Some(*known == was_pending),
            slot @ None => {
                *slot = Some(was_pending);
                Some(true)
            }
        }
    }

    fn pipe_was_retired(&self, pipe_id: &str) -> bool {
        self.pipe_history
            .lock()
            .expect("pipe history lock poisoned")
            .iter()
            .any(|(known_pipe, _)| known_pipe == pipe_id)
    }

    fn remember_pipe(&self, pipe_id: String, attempt_id: String) {
        let mut history = self
            .pipe_history
            .lock()
            .expect("pipe history lock poisoned");
        if history.iter().any(|(known_pipe, _)| known_pipe == &pipe_id) {
            return;
        }
        if history.len() == MAX_PIPES {
            history.pop_front();
        }
        history.push_back((pipe_id, attempt_id));
    }

    pub(crate) fn remove_open(&self, request_id: &str) -> Option<Arc<PendingOpen>> {
        self.opens
            .lock()
            .expect("opens lock poisoned")
            .remove(request_id)
    }

    pub(crate) fn remove_offer(&self, attempt_id: &str) -> Option<Arc<OfferState>> {
        let offer = self
            .offers
            .lock()
            .expect("offers lock poisoned")
            .remove(attempt_id);
        if let Some(offer) = &offer {
            offer.release_slot(self);
        }
        offer
    }

    pub(crate) fn reserve_pipe_slot(&self) -> bool {
        self.pipe_slots
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used < MAX_PIPES).then_some(used + 1)
            })
            .is_ok()
    }

    fn release_pipe_slot(&self) {
        let previous = self.pipe_slots.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "pipe slot count underflow");
    }

    pub(crate) fn register_pipe(
        self: &Arc<Self>,
        pipe_id: &str,
        attempt_id: &str,
        reservation: &AtomicBool,
    ) -> Result<Pipe, AcceptError> {
        if let Some(error) = self.terminal() {
            if reservation.swap(false, Ordering::AcqRel) {
                self.release_pipe_slot();
            }
            return Err(AcceptError::Session(error));
        }
        if !valid_text(pipe_id, MAX_IDENTITY_BYTES) || !valid_text(attempt_id, MAX_IDENTITY_BYTES) {
            if reservation.swap(false, Ordering::AcqRel) {
                self.release_pipe_slot();
            }
            return Err(AcceptError::NotPending);
        }
        if self.pipe_was_retired(pipe_id) {
            if reservation.swap(false, Ordering::AcqRel) {
                self.release_pipe_slot();
            }
            let error = SessionError::Protocol("reused retired PipeId");
            self.terminate(error.clone());
            return Err(AcceptError::Session(error));
        }
        if !reservation.swap(false, Ordering::AcqRel) {
            return Err(AcceptError::NotPending);
        }
        let (payload_tx, payload_rx) = mpsc::channel(PIPE_PAYLOAD_CAPACITY);
        let (terminal, _) = watch::channel(None);
        let state = Arc::new(PipeState {
            pipe_id: pipe_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            payload_tx,
            terminal,
            closing: AtomicBool::new(false),
            enqueue_gate: Mutex::new(()),
            slot_released: AtomicBool::new(false),
        });
        {
            let mut pipes = self.pipes.lock().expect("pipes lock poisoned");
            if pipes.contains_key(pipe_id) {
                self.release_pipe_slot();
                self.terminate(SessionError::Protocol("duplicate PipeId"));
                return Err(AcceptError::Session(SessionError::Protocol(
                    "duplicate PipeId",
                )));
            }
            pipes.insert(pipe_id.to_owned(), Arc::clone(&state));
        }
        if let Some(error) = self.terminal() {
            self.terminalize_pipe(pipe_id, PipeError::Session(error.clone()));
            return Err(AcceptError::Session(error));
        }
        Ok(Pipe {
            state,
            shared: Arc::clone(self),
            payloads: Mutex::new(payload_rx),
        })
    }

    fn register_open_pipe(
        self: &Arc<Self>,
        pipe_id: &str,
        attempt_id: &str,
        reservation: &AtomicBool,
    ) -> Result<Pipe, OpenError> {
        self.register_pipe(pipe_id, attempt_id, reservation)
            .map_err(|error| match error {
                AcceptError::CapacityReached => OpenError::CapacityReached,
                AcceptError::Session(error) => OpenError::Session(error),
                AcceptError::NotPending => OpenError::Unknown,
            })
    }

    pub(crate) fn terminalize_pipe(&self, pipe_id: &str, error: PipeError) {
        if let Some(pipe) = self
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .remove(pipe_id)
        {
            self.remember_pipe(pipe.pipe_id.clone(), pipe.attempt_id.clone());
            pipe.release_slot(self);
            pipe.terminate(error);
        }
    }

    pub(crate) async fn close_pipe(self: &Arc<Self>, pipe_id: &str) -> Result<(), CloseError> {
        self.ensure_active().map_err(CloseError::Session)?;
        let pipe = self
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .get(pipe_id)
            .cloned();
        let enqueue = match &pipe {
            Some(pipe) => Some(pipe.enqueue_gate.lock().await),
            None => None,
        };
        self.ensure_active().map_err(CloseError::Session)?;
        let permit = self.outbound.reserve().await.map_err(|_| {
            let error = SessionError::Transport("request stream ended".into());
            self.terminate(error.clone());
            CloseError::Session(error)
        })?;
        self.ensure_active().map_err(CloseError::Session)?;
        let (tx, rx) = oneshot::channel();
        {
            let mut closes = self.closes.lock().expect("closes lock poisoned");
            if closes.len() >= MAX_PIPES || closes.contains_key(pipe_id) {
                return Err(CloseError::AlreadyPending);
            }
            if pipe
                .as_ref()
                .is_some_and(|pipe| pipe.closing.swap(true, Ordering::AcqRel))
            {
                return Err(CloseError::AlreadyPending);
            }
            closes.insert(pipe_id.to_owned(), tx);
        }
        permit.send(request(connect_request::Message::ClosePipe(
            wire::ClosePipe {
                pipe_id: pipe_id.to_owned(),
            },
        )));
        drop(enqueue);
        rx.await
            .unwrap_or_else(|_| Err(self.terminal_or_transport().into()))
    }

    pub(crate) fn start_accept_cleanup(self: &Arc<Self>, state: Arc<OfferState>) {
        if state.cleanup_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = Arc::clone(self);
        self.spawn_task("provisional accept cleanup", async move {
            let mut events = state.events.subscribe();
            loop {
                if state.acknowledged.load(Ordering::Acquire) {
                    let pipe_id = state
                        .pipe_id
                        .lock()
                        .expect("offer pipe lock poisoned")
                        .clone();
                    if let Some(pipe_id) = pipe_id {
                        let _ = shared.close_pipe(&pipe_id).await;
                        shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
                    }
                    break;
                }
                let event = events.borrow().clone();
                match event {
                    OfferEvent::Pending => {}
                    OfferEvent::Established(pipe_id) => {
                        if state
                            .pipe_id
                            .lock()
                            .expect("offer pipe lock poisoned")
                            .is_none()
                        {
                            *state.pipe_id.lock().expect("offer pipe lock poisoned") =
                                Some(pipe_id.clone());
                            let pipe = match shared.register_pipe(
                                &pipe_id,
                                &state.attempt_id,
                                &state.slot_reserved,
                            ) {
                                Ok(pipe) => pipe,
                                Err(_) => {
                                    shared.remove_offer(&state.attempt_id);
                                    break;
                                }
                            };
                            if pipe.state.terminal.borrow().is_some() {
                                state.ended.store(true, Ordering::Release);
                            }
                        }
                        if state.ended.load(Ordering::Acquire) {
                            shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
                            break;
                        }
                        if !state.confirm_sent.swap(true, Ordering::AcqRel)
                            && shared
                                .send(request(connect_request::Message::ListenerConfirmed(
                                    wire::ListenerConfirmed {
                                        attempt_id: state.attempt_id.clone(),
                                        pipe_id,
                                    },
                                )))
                                .await
                                .is_err()
                        {
                            break;
                        }
                    }
                    OfferEvent::Acknowledged(pipe_id) => {
                        let _ = shared.close_pipe(&pipe_id).await;
                        shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
                        break;
                    }
                    OfferEvent::Terminated | OfferEvent::Rejected | OfferEvent::Session(_) => break,
                }
                if events.changed().await.is_err() {
                    break;
                }
            }
            shared.remove_offer(&state.attempt_id);
        });
    }

    pub(crate) fn auto_unbind(self: &Arc<Self>, binding_id: String) {
        self.send_background(request(connect_request::Message::UnbindListener(
            wire::UnbindListener {
                listener_binding_id: binding_id,
            },
        )));
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        if let Some(dispatcher) = self
            .dispatcher
            .lock()
            .expect("dispatcher lock poisoned")
            .take()
        {
            dispatcher.abort();
        }
    }
}

pub(crate) struct BindingOperationGuard {
    pub(crate) shared: Weak<Shared>,
    pub(crate) operation_id: String,
    pub(crate) sent: bool,
    pub(crate) armed: bool,
}

impl Drop for BindingOperationGuard {
    fn drop(&mut self) {
        if self.armed
            && !self.sent
            && let Some(shared) = self.shared.upgrade()
        {
            shared.clear_binding_pending_if(&self.operation_id);
        }
    }
}

pub(crate) enum BindingPending {
    Bind {
        operation_id: String,
        endpoint_pattern: String,
        target_id: String,
        response: oneshot::Sender<Result<Listener, BindError>>,
    },
    Unbind {
        operation_id: String,
        binding_id: String,
        response: oneshot::Sender<Result<(), UnbindError>>,
    },
}

impl BindingPending {
    fn operation_id(&self) -> &str {
        match self {
            Self::Bind { operation_id, .. } | Self::Unbind { operation_id, .. } => operation_id,
        }
    }

    fn fail(self, error: SessionError) {
        match self {
            Self::Bind { response, .. } => {
                let _ = response.send(Err(BindError::Session(error)));
            }
            Self::Unbind { response, .. } => {
                let _ = response.send(Err(UnbindError::Session(error)));
            }
        }
    }
}

pub(crate) struct ListenerState {
    pub(crate) binding_id: String,
    pub(crate) endpoint_pattern: String,
    pub(crate) target_id: String,
    offers_tx: StdMutex<Option<mpsc::Sender<Offer>>>,
    pub(crate) offers_rx: Mutex<mpsc::Receiver<Offer>>,
    pub(crate) active: AtomicBool,
}

impl ListenerState {
    fn retire(&self) {
        self.active.store(false, Ordering::Release);
        self.offers_tx
            .lock()
            .expect("listener sender lock poisoned")
            .take();
    }
}

#[derive(Clone, Debug)]
pub(crate) enum OfferEvent {
    Pending,
    Established(String),
    Acknowledged(String),
    Terminated,
    Rejected,
    Session(SessionError),
}

pub(crate) struct OfferState {
    pub(crate) attempt_id: String,
    pub(crate) decision: AtomicU8,
    pub(crate) cancelled: AtomicBool,
    pub(crate) confirm_sent: AtomicBool,
    pub(crate) acknowledged: AtomicBool,
    pub(crate) ended: AtomicBool,
    cleanup_started: AtomicBool,
    pub(crate) slot_reserved: AtomicBool,
    pub(crate) pipe_id: StdMutex<Option<String>>,
    pub(crate) events: watch::Sender<OfferEvent>,
    shared: Weak<Shared>,
}

impl OfferState {
    fn publish(&self, event: OfferEvent) {
        self.events.send_replace(event);
    }

    fn release_slot(&self, shared: &Shared) {
        if self.slot_reserved.swap(false, Ordering::AcqRel) {
            shared.release_pipe_slot();
        }
    }
}

pub(crate) struct AcceptGuard {
    pub(crate) state: Arc<OfferState>,
    pub(crate) sent: bool,
    pub(crate) armed: bool,
}

impl Drop for AcceptGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(shared) = self.state.shared.upgrade() {
            if !self.sent {
                shared.retire_offer_identity(&self.state.attempt_id, "");
                shared.remove_offer(&self.state.attempt_id);
                shared.send_background(request(connect_request::Message::ListenerReject(
                    wire::ListenerReject {
                        attempt_id: self.state.attempt_id.clone(),
                    },
                )));
                return;
            }
            self.state.cancelled.store(true, Ordering::Release);
            shared.start_accept_cleanup(Arc::clone(&self.state));
        }
    }
}

pub(crate) struct PendingOpen {
    pub(crate) request_id: String,
    pub(crate) endpoint: String,
    pub(crate) target_id: String,
    pub(crate) response: StdMutex<Option<oneshot::Sender<Result<Pipe, OpenError>>>>,
    pub(crate) cancelled: AtomicBool,
    pub(crate) slot_reserved: AtomicBool,
}

impl PendingOpen {
    fn complete(&self, result: Result<Pipe, OpenError>) -> bool {
        self.response
            .lock()
            .expect("open response lock poisoned")
            .take()
            .is_some_and(|response| response.send(result).is_ok())
    }

    pub(crate) fn release_slot(&self, shared: &Shared) {
        if self.slot_reserved.swap(false, Ordering::AcqRel) {
            shared.release_pipe_slot();
        }
    }
}

pub(crate) struct OpenGuard {
    pub(crate) shared: Weak<Shared>,
    pub(crate) pending: Arc<PendingOpen>,
    pub(crate) sent: bool,
    pub(crate) armed: bool,
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(shared) = self.shared.upgrade() {
            if !self.sent {
                if shared.remove_open_if(&self.pending.request_id, &self.pending) {
                    self.pending.release_slot(&shared);
                }
                return;
            }
            self.pending.cancelled.store(true, Ordering::Release);
            shared.remember_cancel_request(&self.pending.request_id);
            shared.send_background(request(connect_request::Message::CancelOpen(
                wire::CancelOpen {
                    request_id: self.pending.request_id.clone(),
                },
            )));
        }
    }
}

pub(crate) struct PipeState {
    pub(crate) pipe_id: String,
    pub(crate) attempt_id: String,
    payload_tx: mpsc::Sender<Vec<u8>>,
    pub(crate) terminal: watch::Sender<Option<PipeError>>,
    pub(crate) closing: AtomicBool,
    pub(crate) enqueue_gate: Mutex<()>,
    slot_released: AtomicBool,
}

impl PipeState {
    fn terminate(&self, error: PipeError) {
        self.terminal.send_if_modified(|terminal| {
            if terminal.is_none() {
                *terminal = Some(error);
                true
            } else {
                false
            }
        });
    }

    fn release_slot(&self, shared: &Shared) {
        if !self.slot_released.swap(true, Ordering::AcqRel) {
            shared.release_pipe_slot();
        }
    }
}

pub(crate) async fn receive_responses(
    shared: Weak<Shared>,
    inbound: &mut tonic::codec::Streaming<wire::ConnectResponse>,
) {
    loop {
        let response = match inbound.message().await {
            Ok(Some(response)) => response,
            Ok(None) => {
                if let Some(shared) = shared.upgrade() {
                    shared.terminate(SessionError::Transport("relay stream reached EOF".into()));
                }
                return;
            }
            Err(status) => {
                if let Some(shared) = shared.upgrade() {
                    shared.terminate(SessionError::Transport(status.to_string()));
                }
                return;
            }
        };
        let Some(shared) = shared.upgrade() else {
            return;
        };
        if let Err(error) = dispatch_response(&shared, response).await {
            shared.terminate(error);
            return;
        }
    }
}

pub(crate) async fn dispatch_response(
    shared: &Arc<Shared>,
    response: wire::ConnectResponse,
) -> Result<(), SessionError> {
    let message = response
        .message
        .ok_or(SessionError::Protocol("ConnectResponse omitted message"))?;
    match message {
        connect_response::Message::ClientSessionOpened(_) => {
            return Err(SessionError::Protocol("duplicate ClientSessionOpened"));
        }
        connect_response::Message::ListenerBound(bound) => listener_bound(shared, bound)?,
        connect_response::Message::ListenerUnbound(unbound) => listener_unbound(shared, unbound)?,
        connect_response::Message::ListenerOffer(offer) => listener_offer(shared, offer).await?,
        connect_response::Message::ListenerEstablished(established) => {
            listener_established(shared, established)?;
        }
        connect_response::Message::ListenerConfirmationAcknowledged(acknowledged) => {
            listener_confirmation_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::ListenerTerminated(terminated) => {
            listener_terminated(shared, terminated)?;
        }
        connect_response::Message::PipeOpened(opened) => pipe_opened(shared, opened).await?,
        connect_response::Message::PipeOpenFailed(failed) => pipe_open_failed(shared, failed)?,
        connect_response::Message::PipeOpenUnknown(unknown) => pipe_open_unknown(shared, unknown)?,
        connect_response::Message::ListenerDecisionRejected(rejected) => {
            listener_decision_rejected(shared, rejected);
        }
        connect_response::Message::OpenCancelAcknowledged(acknowledged) => {
            open_cancel_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::PipeCloseAcknowledged(acknowledged) => {
            pipe_close_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::OpenRequestRejected(rejected) => {
            open_request_rejected(shared, rejected)?;
        }
        connect_response::Message::PipePayload(payload) => pipe_payload(shared, payload)?,
        connect_response::Message::PipeTerminated(terminated) => {
            pipe_terminated(shared, terminated)?;
        }
        connect_response::Message::PipePayloadRejected(rejected) => {
            pipe_payload_rejected(shared, rejected);
        }
    }
    Ok(())
}

fn listener_bound(shared: &Arc<Shared>, bound: wire::ListenerBound) -> Result<(), SessionError> {
    let binding = bound
        .binding
        .ok_or(SessionError::Protocol("ListenerBound omitted binding"))?;
    if !valid_text(&binding.listener_binding_id, MAX_IDENTITY_BYTES)
        || !valid_text(&binding.endpoint_pattern, MAX_ENDPOINT_BYTES)
        || !valid_text(&binding.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerBound contained invalid identity",
        ));
    }
    if let Some(existing) = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&binding.listener_binding_id)
        .cloned()
    {
        if existing.endpoint_pattern == binding.endpoint_pattern
            && existing.target_id == binding.target_id
        {
            return Ok(());
        }
        return Err(SessionError::Protocol(
            "ListenerBound reused an existing identity with different metadata",
        ));
    }
    match shared.binding_fingerprint_matches(&binding) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "ListenerBound conflicted with a retired binding fingerprint",
            ));
        }
        None if shared.binding_is_retired(&binding.listener_binding_id) => return Ok(()),
        None => {}
    }
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Bind { endpoint_pattern, target_id, .. })
                if endpoint_pattern == &binding.endpoint_pattern && target_id == &binding.target_id
        );
        matches_current.then(|| pending.take().expect("matching Bind must exist"))
    };
    let Some(BindingPending::Bind { response, .. }) = pending else {
        shared.remember_binding_fingerprint(&binding);
        if shared.record_retired_binding(&binding.listener_binding_id) {
            shared.auto_unbind(binding.listener_binding_id);
        }
        return Ok(());
    };
    shared.forget_retired_binding(&binding.listener_binding_id);
    let (offers_tx, offers_rx) = mpsc::channel(OFFER_QUEUE_CAPACITY);
    let state = Arc::new(ListenerState {
        binding_id: binding.listener_binding_id.clone(),
        endpoint_pattern: binding.endpoint_pattern.clone(),
        target_id: binding.target_id.clone(),
        offers_tx: StdMutex::new(Some(offers_tx)),
        offers_rx: Mutex::new(offers_rx),
        active: AtomicBool::new(true),
    });
    {
        let mut listeners = shared.listeners.lock().expect("listeners lock poisoned");
        if listeners.len() >= MAX_LISTENERS {
            drop(listeners);
            shared.remember_binding_fingerprint(&binding);
            shared.record_retired_binding(&binding.listener_binding_id);
            shared.auto_unbind(binding.listener_binding_id);
            let _ = response.send(Err(BindError::CapacityReached));
            return Ok(());
        }
        listeners.insert(binding.listener_binding_id.clone(), Arc::clone(&state));
    }
    let listener = Listener {
        state,
        shared: Arc::clone(shared),
    };
    let _ = response.send(Ok(listener));
    Ok(())
}

fn listener_unbound(
    shared: &Arc<Shared>,
    unbound: wire::ListenerUnbound,
) -> Result<(), SessionError> {
    if !valid_text(&unbound.listener_binding_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "ListenerUnbound contained invalid identity",
        ));
    }
    let pending = {
        let mut pending = shared
            .binding_pending
            .lock()
            .expect("binding pending lock poisoned");
        let matches_current = matches!(
            pending.as_ref(),
            Some(BindingPending::Unbind { binding_id, .. })
                if binding_id == &unbound.listener_binding_id
        );
        matches_current.then(|| pending.take().expect("matching Unbind must exist"))
    };
    if let Some(listener) = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .remove(&unbound.listener_binding_id)
    {
        shared.remember_binding_fingerprint(&wire::ListenerBinding {
            listener_binding_id: listener.binding_id.clone(),
            endpoint_pattern: listener.endpoint_pattern.clone(),
            target_id: listener.target_id.clone(),
        });
        listener.retire();
    }
    shared.record_retired_binding(&unbound.listener_binding_id);
    if let Some(BindingPending::Unbind { response, .. }) = pending {
        let _ = response.send(Ok(()));
    }
    Ok(())
}

async fn listener_offer(
    shared: &Arc<Shared>,
    offer: wire::ListenerOffer,
) -> Result<(), SessionError> {
    if !valid_text(&offer.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.listener_binding_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&offer.target_id, MAX_IDENTITY_BYTES)
        || !valid_text(&offer.caller_session_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerOffer contained invalid identity",
        ));
    }
    let listener = shared
        .listeners
        .lock()
        .expect("listeners lock poisoned")
        .get(&offer.listener_binding_id)
        .cloned();
    let Some(listener) = listener else {
        shared.retire_offer_identity(&offer.attempt_id, "");
        shared
            .send(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: offer.attempt_id,
                },
            )))
            .await?;
        return Ok(());
    };
    let (events, _) = watch::channel(OfferEvent::Pending);
    let state = Arc::new(OfferState {
        attempt_id: offer.attempt_id.clone(),
        decision: AtomicU8::new(0),
        cancelled: AtomicBool::new(false),
        confirm_sent: AtomicBool::new(false),
        acknowledged: AtomicBool::new(false),
        ended: AtomicBool::new(false),
        cleanup_started: AtomicBool::new(false),
        slot_reserved: AtomicBool::new(false),
        pipe_id: StdMutex::new(None),
        events,
        shared: Arc::downgrade(shared),
    });
    let rejected_for_capacity = {
        let mut offers = shared.offers.lock().expect("offers lock poisoned");
        if offers.contains_key(&offer.attempt_id) {
            return Err(SessionError::Protocol("duplicate ListenerOffer attempt"));
        }
        if offers.len() >= MAX_OFFERS {
            true
        } else {
            offers.insert(offer.attempt_id.clone(), Arc::clone(&state));
            false
        }
    };
    if rejected_for_capacity {
        shared.retire_offer_identity(&offer.attempt_id, "");
        shared
            .send(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: offer.attempt_id,
                },
            )))
            .await?;
        return Ok(());
    }
    let sdk_offer = Offer {
        metadata: OfferMetadata {
            attempt_id: offer.attempt_id.clone(),
            listener_binding_id: offer.listener_binding_id,
            endpoint: offer.endpoint,
            target_id: offer.target_id,
            caller_session_id: offer.caller_session_id,
        },
        state: Some(state),
        shared: Arc::clone(shared),
    };
    let sender = listener
        .offers_tx
        .lock()
        .expect("listener sender lock poisoned")
        .clone();
    match sender {
        Some(sender) if sender.try_send(sdk_offer).is_ok() => Ok(()),
        _ => {
            shared.retire_offer_identity(&offer.attempt_id, "");
            shared.remove_offer(&offer.attempt_id);
            shared
                .send(request(connect_request::Message::ListenerReject(
                    wire::ListenerReject {
                        attempt_id: offer.attempt_id,
                    },
                )))
                .await
        }
    }
}

fn listener_established(
    shared: &Arc<Shared>,
    established: wire::ListenerEstablished,
) -> Result<(), SessionError> {
    if !valid_text(&established.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&established.pipe_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerEstablished contained invalid identity",
        ));
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&established.attempt_id)
        .cloned()
        .ok_or(SessionError::Protocol(
            "ListenerEstablished had no pending offer",
        ))?;
    state.publish(OfferEvent::Established(established.pipe_id));
    if state.cancelled.load(Ordering::Acquire) {
        shared.start_accept_cleanup(state);
    }
    Ok(())
}

fn listener_confirmation_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::ListenerConfirmationAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&acknowledged.pipe_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "ListenerConfirmationAcknowledged contained invalid identity",
        ));
    }
    match shared.confirmation_matches(&acknowledged.attempt_id, &acknowledged.pipe_id) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "ListenerConfirmationAcknowledged conflicted with terminal history",
            ));
        }
        None => {}
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&acknowledged.attempt_id)
        .cloned();
    let Some(state) = state else {
        return Err(SessionError::Protocol(
            "foreign ListenerConfirmationAcknowledged",
        ));
    };
    if state
        .pipe_id
        .lock()
        .expect("offer pipe lock poisoned")
        .as_deref()
        != Some(acknowledged.pipe_id.as_str())
        || !shared
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .contains_key(&acknowledged.pipe_id)
    {
        return Err(SessionError::Protocol(
            "ListenerConfirmationAcknowledged identity or dispatch mismatch",
        ));
    }
    shared.remember_confirmation(
        acknowledged.attempt_id.clone(),
        acknowledged.pipe_id.clone(),
    );
    state.acknowledged.store(true, Ordering::Release);
    state.publish(OfferEvent::Acknowledged(acknowledged.pipe_id));
    if state.cancelled.load(Ordering::Acquire) {
        shared.start_accept_cleanup(state);
    }
    Ok(())
}

fn listener_terminated(
    shared: &Arc<Shared>,
    terminated: wire::ListenerTerminated,
) -> Result<(), SessionError> {
    if !valid_text(&terminated.attempt_id, MAX_IDENTITY_BYTES)
        || (!terminated.pipe_id.is_empty() && !valid_text(&terminated.pipe_id, MAX_IDENTITY_BYTES))
    {
        return Err(SessionError::Protocol(
            "ListenerTerminated contained invalid identity",
        ));
    }
    let history_match = shared.confirmation_matches(&terminated.attempt_id, &terminated.pipe_id);
    if let Some(false) = history_match {
        return Err(SessionError::Protocol(
            "ListenerTerminated conflicted with terminal history",
        ));
    }
    let state = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .get(&terminated.attempt_id)
        .cloned();
    let pipe = (!terminated.pipe_id.is_empty())
        .then(|| {
            shared
                .pipes
                .lock()
                .expect("pipes lock poisoned")
                .get(&terminated.pipe_id)
                .cloned()
        })
        .flatten();
    if let Some(pipe) = &pipe
        && pipe.attempt_id != terminated.attempt_id
    {
        return Err(SessionError::Protocol(
            "ListenerTerminated did not own the referenced Pipe",
        ));
    }
    if state.is_none() && history_match.is_none() {
        return Err(SessionError::Protocol("foreign ListenerTerminated"));
    }
    if let Some(state) = state {
        state.ended.store(true, Ordering::Release);
        if state
            .pipe_id
            .lock()
            .expect("offer pipe lock poisoned")
            .as_ref()
            .is_some_and(|pipe_id| pipe_id != &terminated.pipe_id)
        {
            return Err(SessionError::Protocol(
                "ListenerTerminated identity did not match provisional Pipe",
            ));
        }
        shared.remove_offer(&terminated.attempt_id);
        state.publish(OfferEvent::Terminated);
    }
    if history_match.is_none() {
        shared.remember_confirmation(terminated.attempt_id.clone(), terminated.pipe_id.clone());
    }
    if !terminated.pipe_id.is_empty() {
        shared.terminalize_pipe(&terminated.pipe_id, PipeError::Terminal);
    }
    Ok(())
}

async fn pipe_opened(shared: &Arc<Shared>, opened: wire::PipeOpened) -> Result<(), SessionError> {
    if !valid_text(&opened.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.attempt_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.pipe_id, MAX_IDENTITY_BYTES)
        || !valid_text(&opened.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&opened.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpened contained invalid identity",
        ));
    }
    let terminal = OpenTerminal::Opened {
        attempt_id: opened.attempt_id.clone(),
        pipe_id: opened.pipe_id.clone(),
        endpoint: opened.endpoint.clone(),
        target_id: opened.target_id.clone(),
    };
    let Some(pending) = pending_open_or_replay(shared, &opened.request_id, &terminal)? else {
        return Ok(());
    };
    if opened.endpoint != pending.endpoint || opened.target_id != pending.target_id {
        pending.release_slot(shared);
        return Err(SessionError::Protocol("PipeOpened identity mismatch"));
    }
    let pipe = match shared.register_open_pipe(
        &opened.pipe_id,
        &opened.attempt_id,
        &pending.slot_reserved,
    ) {
        Ok(pipe) => pipe,
        Err(error) => {
            pending.complete(Err(error));
            return Ok(());
        }
    };
    shared.remember_open_terminal(opened.request_id, terminal);
    if pending.cancelled.load(Ordering::Acquire) || !pending.complete(Ok(pipe)) {
        let pipe_id = opened.pipe_id;
        let shared = Arc::clone(shared);
        shared
            .clone()
            .spawn_task("cancelled Open cleanup", async move {
                let _ = shared.close_pipe(&pipe_id).await;
                shared.terminalize_pipe(&pipe_id, PipeError::Terminal);
            });
    }
    Ok(())
}

fn pipe_open_failed(
    shared: &Arc<Shared>,
    failed: wire::PipeOpenFailed,
) -> Result<(), SessionError> {
    if !valid_text(&failed.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&failed.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&failed.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpenFailed contained invalid identity",
        ));
    }
    let result = match wire::OpenFailure::try_from(failed.failure).ok() {
        Some(wire::OpenFailure::InvalidRequest) => {
            Err(OpenError::Failed(OpenFailure::InvalidRequest))
        }
        Some(wire::OpenFailure::RouteNotFound) => {
            Err(OpenError::Failed(OpenFailure::RouteNotFound))
        }
        Some(wire::OpenFailure::Unavailable) => Err(OpenError::Failed(OpenFailure::Unavailable)),
        Some(wire::OpenFailure::CapacityReached) => {
            Err(OpenError::Failed(OpenFailure::CapacityReached))
        }
        Some(wire::OpenFailure::ListenerRejected) => {
            Err(OpenError::Failed(OpenFailure::ListenerRejected))
        }
        Some(wire::OpenFailure::DeadlineExceeded) => {
            Err(OpenError::Failed(OpenFailure::DeadlineExceeded))
        }
        Some(wire::OpenFailure::Cancelled) => Err(OpenError::Cancelled),
        Some(wire::OpenFailure::Unspecified) | None => {
            return Err(SessionError::Protocol(
                "PipeOpenFailed used unspecified failure",
            ));
        }
    };
    let terminal = OpenTerminal::Failed {
        endpoint: failed.endpoint.clone(),
        target_id: failed.target_id.clone(),
        failure: failed.failure,
    };
    let Some(pending) = pending_open_or_replay(shared, &failed.request_id, &terminal)? else {
        return Ok(());
    };
    pending.release_slot(shared);
    if failed.endpoint != pending.endpoint || failed.target_id != pending.target_id {
        return Err(SessionError::Protocol("PipeOpenFailed identity mismatch"));
    }
    shared.remember_open_terminal(failed.request_id, terminal);
    pending.complete(result);
    Ok(())
}

fn pipe_open_unknown(
    shared: &Arc<Shared>,
    unknown: wire::PipeOpenUnknown,
) -> Result<(), SessionError> {
    if !valid_text(&unknown.request_id, MAX_IDENTITY_BYTES)
        || !valid_text(&unknown.endpoint, MAX_ENDPOINT_BYTES)
        || !valid_text(&unknown.target_id, MAX_IDENTITY_BYTES)
    {
        return Err(SessionError::Protocol(
            "PipeOpenUnknown contained invalid identity",
        ));
    }
    let terminal = OpenTerminal::Unknown {
        endpoint: unknown.endpoint.clone(),
        target_id: unknown.target_id.clone(),
    };
    let Some(pending) = pending_open_or_replay(shared, &unknown.request_id, &terminal)? else {
        return Ok(());
    };
    if unknown.endpoint != pending.endpoint || unknown.target_id != pending.target_id {
        pending.release_slot(shared);
        return Err(SessionError::Protocol("PipeOpenUnknown identity mismatch"));
    }
    shared.remember_open_terminal(unknown.request_id, terminal);
    pending.release_slot(shared);
    pending.complete(Err(OpenError::Unknown));
    Ok(())
}

fn open_request_rejected(
    shared: &Arc<Shared>,
    rejected: wire::OpenRequestRejected,
) -> Result<(), SessionError> {
    if !valid_text(&rejected.request_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "OpenRequestRejected contained invalid identity",
        ));
    }
    let result = match wire::OpenRequestFailure::try_from(rejected.failure).ok() {
        Some(wire::OpenRequestFailure::DuplicateInFlight) => Err(OpenError::DuplicateInFlight),
        _ => {
            return Err(SessionError::Protocol(
                "OpenRequestRejected used unspecified failure",
            ));
        }
    };
    let terminal = OpenTerminal::RequestRejected {
        failure: rejected.failure,
    };
    let Some(pending) = pending_open_or_replay(shared, &rejected.request_id, &terminal)? else {
        return Ok(());
    };
    shared.remember_open_terminal(rejected.request_id, terminal);
    pending.release_slot(shared);
    pending.complete(result);
    Ok(())
}

fn pending_open_or_replay(
    shared: &Arc<Shared>,
    request_id: &str,
    terminal: &OpenTerminal,
) -> Result<Option<Arc<PendingOpen>>, SessionError> {
    if let Some(pending) = shared.remove_open(request_id) {
        return Ok(Some(pending));
    }
    match shared.open_terminal_matches(request_id, terminal) {
        Some(true) => Ok(None),
        Some(false) => Err(SessionError::Protocol(
            "Open terminal conflicted with retired history",
        )),
        None => Err(SessionError::Protocol("foreign Open terminal")),
    }
}

fn open_cancel_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::OpenCancelAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.request_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "OpenCancelAcknowledged contained invalid identity",
        ));
    }
    match shared.acknowledge_cancel(&acknowledged.request_id, acknowledged.was_pending) {
        Some(true) => Ok(()),
        Some(false) => Err(SessionError::Protocol(
            "OpenCancelAcknowledged conflicted with retired history",
        )),
        None => Err(SessionError::Protocol("foreign OpenCancelAcknowledged")),
    }
}

fn listener_decision_rejected(shared: &Arc<Shared>, rejected: wire::ListenerDecisionRejected) {
    if let Some(state) = shared.remove_offer(&rejected.attempt_id) {
        state.ended.store(true, Ordering::Release);
        let pipe_id = state
            .pipe_id
            .lock()
            .expect("offer pipe lock poisoned")
            .clone()
            .unwrap_or_default();
        shared.retire_offer_identity(&rejected.attempt_id, &pipe_id);
        state.publish(OfferEvent::Rejected);
    }
}

fn pipe_close_acknowledged(
    shared: &Arc<Shared>,
    acknowledged: wire::PipeCloseAcknowledged,
) -> Result<(), SessionError> {
    if !valid_text(&acknowledged.pipe_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "PipeCloseAcknowledged contained invalid identity",
        ));
    }
    match shared.close_ack_matches(&acknowledged.pipe_id, acknowledged.owned) {
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(SessionError::Protocol(
                "PipeCloseAcknowledged conflicted with terminal history",
            ));
        }
        None => {}
    }
    let response = shared
        .closes
        .lock()
        .expect("closes lock poisoned")
        .remove(&acknowledged.pipe_id);
    let Some(response) = response else {
        return Err(SessionError::Protocol("foreign PipeCloseAcknowledged"));
    };
    shared.remember_close_ack(acknowledged.pipe_id.clone(), acknowledged.owned);
    if acknowledged.owned {
        shared.terminalize_pipe(&acknowledged.pipe_id, PipeError::Terminal);
        let _ = response.send(Ok(()));
    } else {
        shared.terminalize_pipe(&acknowledged.pipe_id, PipeError::NotOwned);
        let _ = response.send(Err(CloseError::NotOwned));
    }
    Ok(())
}

fn pipe_terminated(
    shared: &Arc<Shared>,
    terminated: wire::PipeTerminated,
) -> Result<(), SessionError> {
    if !valid_text(&terminated.pipe_id, MAX_IDENTITY_BYTES) {
        return Err(SessionError::Protocol(
            "PipeTerminated contained invalid identity",
        ));
    }
    let offers = shared
        .offers
        .lock()
        .expect("offers lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let offer = offers.into_iter().find(|offer| {
        offer
            .pipe_id
            .lock()
            .expect("offer pipe lock poisoned")
            .as_deref()
            == Some(terminated.pipe_id.as_str())
    });
    let pipe_exists = shared
        .pipes
        .lock()
        .expect("pipes lock poisoned")
        .contains_key(&terminated.pipe_id);
    if offer.is_none() && !pipe_exists && !shared.pipe_was_retired(&terminated.pipe_id) {
        return Err(SessionError::Protocol("foreign PipeTerminated"));
    }
    if let Some(offer) = offer {
        match shared.confirmation_matches(&offer.attempt_id, &terminated.pipe_id) {
            Some(false) => {
                return Err(SessionError::Protocol(
                    "PipeTerminated conflicted with confirmation history",
                ));
            }
            Some(true) => {}
            None => {
                shared.remember_confirmation(offer.attempt_id.clone(), terminated.pipe_id.clone())
            }
        }
        offer.ended.store(true, Ordering::Release);
        shared.remove_offer(&offer.attempt_id);
        offer.publish(OfferEvent::Terminated);
    }
    shared.terminalize_pipe(&terminated.pipe_id, PipeError::Terminal);
    Ok(())
}

fn pipe_payload(shared: &Arc<Shared>, payload: wire::PipePayload) -> Result<(), SessionError> {
    if !valid_text(&payload.pipe_id, MAX_IDENTITY_BYTES)
        || payload.payload.is_empty()
        || payload.payload.len() > MAX_PAYLOAD_BYTES
    {
        return Err(SessionError::Protocol("invalid PipePayload"));
    }
    let pipe = shared
        .pipes
        .lock()
        .expect("pipes lock poisoned")
        .get(&payload.pipe_id)
        .cloned();
    let Some(pipe) = pipe else {
        return Err(SessionError::Protocol("foreign PipePayload"));
    };
    if pipe.payload_tx.try_send(payload.payload).is_err() {
        shared.terminalize_pipe(&payload.pipe_id, PipeError::Backpressure);
        shared.send_background(request(connect_request::Message::ClosePipe(
            wire::ClosePipe {
                pipe_id: payload.pipe_id,
            },
        )));
    }
    Ok(())
}

fn pipe_payload_rejected(shared: &Arc<Shared>, rejected: wire::PipePayloadRejected) {
    let error = match wire::PipePayloadFailure::try_from(rejected.failure).ok() {
        Some(wire::PipePayloadFailure::InvalidRequest) => PipeError::InvalidPayload,
        Some(wire::PipePayloadFailure::NotOwned) => PipeError::NotOwned,
        Some(wire::PipePayloadFailure::Backpressure) => PipeError::Backpressure,
        Some(wire::PipePayloadFailure::Unavailable)
        | Some(wire::PipePayloadFailure::Unspecified)
        | None => PipeError::Unavailable,
    };
    shared.terminalize_pipe(&rejected.pipe_id, error);
}

pub(crate) async fn wait_for_established(
    events: &mut watch::Receiver<OfferEvent>,
) -> Result<String, AcceptError> {
    loop {
        let event = events.borrow().clone();
        match event {
            OfferEvent::Pending => {}
            OfferEvent::Established(pipe_id) => return Ok(pipe_id),
            OfferEvent::Acknowledged(_) => return Err(AcceptError::NotPending),
            OfferEvent::Terminated | OfferEvent::Rejected => return Err(AcceptError::NotPending),
            OfferEvent::Session(error) => return Err(AcceptError::Session(error)),
        }
        if events.changed().await.is_err() {
            return Err(AcceptError::NotPending);
        }
    }
}

pub(crate) async fn wait_for_acknowledged(
    state: &OfferState,
    events: &mut watch::Receiver<OfferEvent>,
    expected_pipe_id: &str,
) -> Result<(), AcceptError> {
    loop {
        if state.acknowledged.load(Ordering::Acquire) {
            return Ok(());
        }
        let event = events.borrow().clone();
        match event {
            OfferEvent::Pending | OfferEvent::Established(_) => {}
            OfferEvent::Acknowledged(pipe_id) if pipe_id == expected_pipe_id => return Ok(()),
            OfferEvent::Acknowledged(_) | OfferEvent::Terminated | OfferEvent::Rejected => {
                return Err(AcceptError::NotPending);
            }
            OfferEvent::Session(error) => return Err(AcceptError::Session(error)),
        }
        if events.changed().await.is_err() {
            return Err(AcceptError::NotPending);
        }
    }
}

pub(crate) fn request(message: connect_request::Message) -> wire::ConnectRequest {
    wire::ConnectRequest {
        message: Some(message),
    }
}

pub(crate) fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes
}

#[cfg(test)]
mod tests;
