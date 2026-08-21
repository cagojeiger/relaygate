use super::*;

impl Shared {
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

    pub(in crate::runtime) fn release_pipe_slot(&self) {
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

    pub(in crate::runtime) fn register_open_pipe(
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
