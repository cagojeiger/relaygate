use super::*;

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
            offer_history: StdMutex::new(VecDeque::new()),
            close_history: StdMutex::new(VecDeque::new()),
            cancel_history: StdMutex::new(VecDeque::new()),
            pipe_history: StdMutex::new(VecDeque::new()),
            delivery_history: StdMutex::new(VecDeque::new()),
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
        let _ = self.try_send_background(message);
    }

    pub(super) fn try_send_background(
        self: &Arc<Self>,
        message: wire::ConnectRequest,
    ) -> Result<(), SessionError> {
        if self.terminal().is_some() {
            return Err(self.terminal_or_transport());
        }
        if let Err(error) = self.outbound.try_send(message) {
            let detail = match error {
                mpsc::error::TrySendError::Full(_) => "outbound control queue exhausted",
                mpsc::error::TrySendError::Closed(_) => "request stream ended",
            };
            let error = SessionError::Transport(detail.into());
            self.terminate(error.clone());
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn spawn_task(
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
                if let Some((payload_id, terminal)) = pipe.last_delivery() {
                    self.remember_delivery(pipe.pipe_id.clone(), payload_id, terminal);
                }
            }
            for (_, close) in self.closes.lock().expect("closes lock poisoned").drain() {
                let _ = close.send(Err(CloseError::Session(error.clone())));
            }
        }
    }

    pub(super) fn clear_binding_pending_if(&self, operation_id: &str) {
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

    pub(super) fn record_retired_binding(&self, binding_id: &str) -> bool {
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

    pub(super) fn binding_is_retired(&self, binding_id: &str) -> bool {
        self.retired_bindings
            .lock()
            .expect("retired bindings lock poisoned")
            .iter()
            .any(|known| known == binding_id)
    }

    pub(super) fn forget_retired_binding(&self, binding_id: &str) {
        let mut retired = self
            .retired_bindings
            .lock()
            .expect("retired bindings lock poisoned");
        if let Some(index) = retired.iter().position(|known| known == binding_id) {
            retired.remove(index);
        }
    }

    pub(super) fn remember_binding_fingerprint(&self, binding: &wire::ListenerBinding) {
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

    pub(super) fn binding_fingerprint_matches(
        &self,
        binding: &wire::ListenerBinding,
    ) -> Option<bool> {
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

    pub(super) fn remove_open_if(&self, request_id: &str, expected: &Arc<PendingOpen>) -> bool {
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

    pub(super) fn open_terminal_matches(
        &self,
        request_id: &str,
        terminal: &OpenTerminal,
    ) -> Option<bool> {
        self.open_history
            .lock()
            .expect("open history lock poisoned")
            .iter()
            .find(|(known_id, _)| known_id == request_id)
            .map(|(_, known)| known == terminal)
    }

    pub(super) fn remember_open_terminal(&self, request_id: String, terminal: OpenTerminal) {
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

    pub(super) fn confirmation_matches(&self, attempt_id: &str, pipe_id: &str) -> Option<bool> {
        self.offer_history
            .lock()
            .expect("offer history lock poisoned")
            .iter()
            .find(|(known_attempt, _)| known_attempt == attempt_id)
            .map(|(_, terminal)| {
                matches!(
                    terminal,
                    OfferTerminal::Retired {
                        pipe_id: known_pipe,
                    } if known_pipe == pipe_id
                )
            })
    }

    pub(super) fn offer_terminal_exists(&self, attempt_id: &str) -> bool {
        self.offer_history
            .lock()
            .expect("offer history lock poisoned")
            .iter()
            .any(|(known_attempt, _)| known_attempt == attempt_id)
    }

    pub(super) fn decision_rejection_matches(
        &self,
        attempt_id: &str,
        failure: i32,
    ) -> Option<bool> {
        self.offer_history
            .lock()
            .expect("offer history lock poisoned")
            .iter()
            .find(|(known_attempt, _)| known_attempt == attempt_id)
            .map(|(_, terminal)| {
                matches!(
                    terminal,
                    OfferTerminal::DecisionRejected {
                        failure: known_failure,
                        ..
                    } if *known_failure == failure
                )
            })
    }

    pub(super) fn remember_offer_terminal(&self, attempt_id: String, terminal: OfferTerminal) {
        let mut history = self
            .offer_history
            .lock()
            .expect("offer history lock poisoned");
        if history
            .iter()
            .any(|(known_attempt, _)| known_attempt == &attempt_id)
        {
            return;
        }
        if history.len() == MAX_OFFERS {
            history.pop_front();
        }
        history.push_back((attempt_id, terminal));
    }

    pub(super) fn remember_confirmation(&self, attempt_id: String, pipe_id: String) {
        self.remember_offer_terminal(attempt_id, OfferTerminal::Retired { pipe_id });
    }

    pub(super) fn remember_decision_rejection(&self, attempt_id: String, failure: i32) {
        self.remember_offer_terminal(attempt_id, OfferTerminal::DecisionRejected { failure });
    }

    pub(crate) fn retire_offer_identity(&self, attempt_id: &str, pipe_id: &str) {
        self.remember_confirmation(attempt_id.to_owned(), pipe_id.to_owned());
    }

    pub(super) fn close_ack_matches(&self, pipe_id: &str, owned: bool) -> Option<bool> {
        self.close_history
            .lock()
            .expect("close history lock poisoned")
            .iter()
            .find(|(known_pipe, _)| known_pipe == pipe_id)
            .map(|(_, known_owned)| *known_owned == owned)
    }

    pub(super) fn remember_close_ack(&self, pipe_id: String, owned: bool) {
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

    pub(super) fn remember_cancel_request(&self, request_id: &str) {
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

    pub(super) fn acknowledge_cancel(&self, request_id: &str, was_pending: bool) -> Option<bool> {
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

    pub(super) fn pipe_was_retired(&self, pipe_id: &str) -> bool {
        self.pipe_history
            .lock()
            .expect("pipe history lock poisoned")
            .iter()
            .any(|(known_pipe, _)| known_pipe == pipe_id)
    }

    pub(super) fn remember_pipe(&self, pipe_id: String, attempt_id: String) {
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

    pub(super) fn remember_delivery(
        &self,
        pipe_id: String,
        payload_id: String,
        terminal: DeliveryTerminal,
    ) {
        let mut history = self
            .delivery_history
            .lock()
            .expect("delivery history lock poisoned");
        if let Some(index) = history
            .iter()
            .position(|(known_pipe, _, _)| known_pipe == &pipe_id)
        {
            history.remove(index);
        }
        if history.len() == MAX_PIPES {
            history.pop_front();
        }
        history.push_back((pipe_id, payload_id, terminal));
    }

    pub(super) fn complete_delivery(
        &self,
        pipe_id: &str,
        payload_id: &str,
        terminal: DeliveryTerminal,
    ) -> Result<(), SessionError> {
        let pipe = self
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .get(pipe_id)
            .cloned();
        if let Some(pipe) = pipe {
            return pipe.finish_delivery(payload_id, terminal);
        }
        let mut history = self
            .delivery_history
            .lock()
            .expect("delivery history lock poisoned");
        let Some((_, known_payload, known_terminal)) = history
            .iter_mut()
            .find(|(known_pipe, _, _)| known_pipe == pipe_id)
        else {
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

    pub(super) fn release_pipe_slot(&self) {
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
            delivery: StdMutex::new(PipeDeliveryState::default()),
            received: StdMutex::new(ReceivedPayloadHistory::default()),
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

    pub(super) fn register_open_pipe(
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

    pub(crate) fn terminalize_pipe(&self, pipe_id: &str, error: PipeError) -> bool {
        if let Some(pipe) = self
            .pipes
            .lock()
            .expect("pipes lock poisoned")
            .remove(pipe_id)
        {
            self.remember_pipe(pipe.pipe_id.clone(), pipe.attempt_id.clone());
            pipe.release_slot(self);
            pipe.terminate(error);
            if let Some((payload_id, terminal)) = pipe.last_delivery() {
                self.remember_delivery(pipe.pipe_id.clone(), payload_id, terminal);
            }
            true
        } else {
            false
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
