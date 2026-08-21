use super::*;

mod history;
mod pipes;

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
