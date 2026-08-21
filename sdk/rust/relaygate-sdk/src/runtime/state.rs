use super::*;

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
    pub(in crate::runtime) fn operation_id(&self) -> &str {
        match self {
            Self::Bind { operation_id, .. } | Self::Unbind { operation_id, .. } => operation_id,
        }
    }

    pub(in crate::runtime) fn fail(self, error: SessionError) {
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
    pub(in crate::runtime) offers_tx: StdMutex<Option<mpsc::Sender<Offer>>>,
    pub(crate) offers_rx: Mutex<mpsc::Receiver<Offer>>,
    pub(crate) active: AtomicBool,
}

impl ListenerState {
    pub(in crate::runtime) fn retire(&self) {
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
    pub(in crate::runtime) cleanup_started: AtomicBool,
    pub(crate) slot_reserved: AtomicBool,
    pub(crate) pipe_id: StdMutex<Option<String>>,
    pub(crate) events: watch::Sender<OfferEvent>,
    pub(in crate::runtime) shared: Weak<Shared>,
}

impl OfferState {
    pub(in crate::runtime) fn publish(&self, event: OfferEvent) {
        self.events.send_replace(event);
    }

    pub(in crate::runtime) fn release_slot(&self, shared: &Shared) {
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
    pub(in crate::runtime) fn complete(&self, result: Result<Pipe, OpenError>) -> bool {
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
    pub(in crate::runtime) payload_tx: mpsc::Sender<Vec<u8>>,
    pub(crate) terminal: watch::Sender<Option<PipeError>>,
    pub(crate) closing: AtomicBool,
    pub(crate) enqueue_gate: Mutex<()>,
    pub(in crate::runtime) slot_released: AtomicBool,
    pub(in crate::runtime) delivery: StdMutex<PipeDeliveryState>,
    pub(in crate::runtime) received: StdMutex<ReceivedPayloadHistory>,
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

    pub(in crate::runtime) fn last_delivery(&self) -> Option<(String, DeliveryTerminal)> {
        self.delivery
            .lock()
            .expect("delivery lock poisoned")
            .last
            .clone()
    }

    pub(in crate::runtime) fn deliver(
        &self,
        payload_id: String,
        payload: Vec<u8>,
    ) -> IncomingPayload {
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

    pub(in crate::runtime) fn terminate(&self, error: PipeError) {
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

    pub(in crate::runtime) fn release_slot(&self, shared: &Shared) {
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
