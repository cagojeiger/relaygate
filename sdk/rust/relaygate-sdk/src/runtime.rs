use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};

use crate::{
    AcceptError, BindError, CloseError, DeliveryError, DeliveryFailure, Listener, Offer,
    OfferMetadata, OpenError, OpenFailure, Pipe, PipeError, Session, SessionError, UnbindError,
    wire::{self, connect_request, connect_response},
};
use ring::digest::{SHA256, digest};
use tokio::{
    runtime::Handle,
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

mod response;
mod shared;
pub(crate) use response::dispatch_response;

#[cfg(test)]
use crate::{Client, Config};

pub(crate) const OUTBOUND_CAPACITY: usize = 64;
const OFFER_QUEUE_CAPACITY: usize = 32;
const PIPE_PAYLOAD_CAPACITY: usize = 32;
pub(crate) const MAX_LISTENERS: usize = 512;
const MAX_OFFERS: usize = 1_024;
pub(crate) const MAX_OPEN_REQUESTS: usize = 1_024;
const MAX_PIPES: usize = 1_024;
const MAX_RECEIVED_PAYLOADS: usize = 1_024;
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum OfferTerminal {
    Retired { pipe_id: String },
    DecisionRejected { failure: i32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryTerminal {
    Received,
    NotSent,
    Rejected(DeliveryFailure),
    Unknown,
}

struct PendingDelivery {
    payload_id: String,
    response: oneshot::Sender<Result<(), DeliveryError>>,
}

#[derive(Default)]
struct PipeDeliveryState {
    pending: Option<PendingDelivery>,
    last: Option<(String, DeliveryTerminal)>,
}

enum IncomingPayload {
    Accepted {
        permit: mpsc::OwnedPermit<Vec<u8>>,
        payload: Vec<u8>,
    },
    Duplicate,
    Full,
    Conflict,
    Terminal,
}

#[derive(Default)]
struct ReceivedPayloadHistory {
    fingerprints: HashMap<String, [u8; 32]>,
    order: VecDeque<String>,
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
    offer_history: StdMutex<VecDeque<(String, OfferTerminal)>>,
    close_history: StdMutex<VecDeque<(String, bool)>>,
    cancel_history: StdMutex<VecDeque<(String, Option<bool>)>>,
    pipe_history: StdMutex<VecDeque<(String, String)>>,
    delivery_history: StdMutex<VecDeque<(String, String, DeliveryTerminal)>>,
    pipe_slots: AtomicUsize,
    pub(crate) dispatcher: StdMutex<Option<JoinHandle<()>>>,
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
    delivery: StdMutex<PipeDeliveryState>,
    received: StdMutex<ReceivedPayloadHistory>,
}

impl PipeState {
    pub(crate) fn begin_delivery(
        &self,
        payload_id: String,
    ) -> Result<oneshot::Receiver<Result<(), DeliveryError>>, SessionError> {
        let (response, receiver) = oneshot::channel();
        let mut delivery = self.delivery.lock().expect("delivery lock poisoned");
        if delivery.pending.is_some() {
            return Err(SessionError::Protocol(
                "multiple payload deliveries were admitted for one Pipe",
            ));
        }
        delivery.pending = Some(PendingDelivery {
            payload_id,
            response,
        });
        Ok(receiver)
    }

    pub(crate) fn finish_delivery(
        &self,
        payload_id: &str,
        terminal: DeliveryTerminal,
    ) -> Result<(), SessionError> {
        let mut delivery = self.delivery.lock().expect("delivery lock poisoned");
        if let Some(pending) = delivery.pending.take() {
            if pending.payload_id != payload_id {
                delivery.pending = Some(pending);
                return Err(SessionError::Protocol(
                    "payload delivery outcome identity conflict",
                ));
            }
            let result = delivery_result(payload_id, &terminal);
            delivery.last = Some((payload_id.to_owned(), terminal));
            let _ = pending.response.send(result);
            return Ok(());
        }
        let Some((known_payload, known_terminal)) = delivery.last.as_mut() else {
            return Err(SessionError::Protocol("foreign payload delivery outcome"));
        };
        if known_payload != payload_id {
            return Err(SessionError::Protocol(
                "payload delivery outcome identity conflict",
            ));
        }
        if *known_terminal == terminal {
            return Ok(());
        }
        if *known_terminal == DeliveryTerminal::Unknown {
            return Ok(());
        }
        Err(SessionError::Protocol(
            "conflicting payload delivery outcome",
        ))
    }

    fn last_delivery(&self) -> Option<(String, DeliveryTerminal)> {
        self.delivery
            .lock()
            .expect("delivery lock poisoned")
            .last
            .clone()
    }

    fn deliver(&self, payload_id: String, payload: Vec<u8>) -> IncomingPayload {
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(digest(&SHA256, &payload).as_ref());
        let mut received = self
            .received
            .lock()
            .expect("received payload lock poisoned");
        if let Some(known_fingerprint) = received.fingerprints.get(&payload_id) {
            return if known_fingerprint == &fingerprint {
                IncomingPayload::Duplicate
            } else {
                IncomingPayload::Conflict
            };
        }
        if self.terminal.borrow().is_some() {
            return IncomingPayload::Terminal;
        }
        match self.payload_tx.clone().try_reserve_owned() {
            Ok(permit) => {
                if received.order.len() == MAX_RECEIVED_PAYLOADS
                    && let Some(oldest) = received.order.pop_front()
                {
                    received.fingerprints.remove(&oldest);
                }
                received.order.push_back(payload_id.clone());
                received.fingerprints.insert(payload_id, fingerprint);
                IncomingPayload::Accepted { permit, payload }
            }
            Err(mpsc::error::TrySendError::Full(_)) => IncomingPayload::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => IncomingPayload::Terminal,
        }
    }

    fn terminate(&self, error: PipeError) {
        {
            let mut delivery = self.delivery.lock().expect("delivery lock poisoned");
            if let Some(pending) = delivery.pending.take() {
                let payload_id = pending.payload_id;
                delivery.last = Some((payload_id.clone(), DeliveryTerminal::Unknown));
                let session = match &error {
                    PipeError::Session(error) => Some(error.clone()),
                    _ => None,
                };
                let _ = pending
                    .response
                    .send(Err(DeliveryError::unknown(payload_id, session)));
            }
        }
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

fn delivery_result(payload_id: &str, terminal: &DeliveryTerminal) -> Result<(), DeliveryError> {
    match terminal {
        DeliveryTerminal::Received => Ok(()),
        DeliveryTerminal::NotSent => {
            Err(DeliveryError::not_sent(Some(payload_id.to_owned()), None))
        }
        DeliveryTerminal::Rejected(failure) => {
            Err(DeliveryError::rejected(payload_id.to_owned(), *failure))
        }
        DeliveryTerminal::Unknown => Err(DeliveryError::unknown(payload_id.to_owned(), None)),
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
                    shared.terminate(SessionError::Rpc {
                        code: status.code(),
                        message: status.message().to_owned(),
                    });
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
