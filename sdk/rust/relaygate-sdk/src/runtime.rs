mod state;
pub(crate) use state::*;

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
