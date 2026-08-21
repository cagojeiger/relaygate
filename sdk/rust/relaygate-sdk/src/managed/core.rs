use super::*;

impl ManagedCore {
    pub(super) async fn run(self: Arc<Self>) {
        let mut delay = INITIAL_BACKOFF;
        loop {
            if self.cancelled() {
                self.finish(ManagedState::Closed, None).await;
                return;
            }
            self.set_state(ManagedState::Connecting, None);
            let result = tokio::time::timeout(
                CONNECT_TIMEOUT,
                self.connector.connect(self.config.reconnect_copy()),
            )
            .await;
            let client = match result {
                Ok(Ok(client)) => client,
                Ok(Err(error)) if permanent_connect_error(&error) => {
                    self.finish(ManagedState::Failed, Some(error.to_string()))
                        .await;
                    return;
                }
                Ok(Err(error)) => {
                    self.set_state(ManagedState::Backoff, Some(error.to_string()));
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
                Err(_) => {
                    self.set_state(
                        ManagedState::Backoff,
                        Some("connection attempt timed out".into()),
                    );
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
            };

            let ready_at = match self.install_and_rebind(&client).await {
                Ok(ready_at) => ready_at,
                Err(error) => {
                    client.shared.terminate(crate::SessionError::Closed);
                    if !retryable_managed_error(&error) {
                        self.finish(ManagedState::Failed, Some(error.to_string()))
                            .await;
                        return;
                    }
                    self.set_state(ManagedState::Backoff, Some(error.to_string()));
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                    continue;
                }
            };

            let mut cancel = self.cancel_tx.subscribe();
            tokio::select! {
                error = client.done() => {
                    if permanent_session_error(&error) {
                        self.finish(ManagedState::Failed, Some(error.to_string())).await;
                        return;
                    }
                    if ready_at.elapsed() >= STABLE_WINDOW {
                        delay = INITIAL_BACKOFF;
                    }
                    self.detach().await;
                    if !self.wait_backoff(delay).await {
                        self.finish(ManagedState::Closed, None).await;
                        return;
                    }
                    delay = next_backoff(delay);
                }
                _ = wait_cancelled(&mut cancel) => {
                    client.shared.terminate(crate::SessionError::Closed);
                    self.finish(ManagedState::Closed, None).await;
                    return;
                }
            }
        }
    }

    pub(super) async fn install_and_rebind(
        &self,
        client: &Client,
    ) -> Result<Instant, ManagedError> {
        let generation = {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.generation = data.generation.wrapping_add(1);
            data.current = Some(client.managed_handle());
            data.state = ManagedState::Rebinding;
            data.failure = None;
            data.generation
        };
        self.clear_current_listeners().await;
        self.publish_state();

        loop {
            let pending = {
                let data = self.data.lock().expect("managed data lock poisoned");
                data.bindings.values().cloned().collect::<Vec<_>>()
            };
            let mut rebound = 0usize;
            for binding in &pending {
                if !binding.active.load(Ordering::Acquire) {
                    continue;
                }
                if binding
                    .current
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|(bound_generation, _)| *bound_generation == generation)
                {
                    rebound += 1;
                    continue;
                }
                let listener = client
                    .bind(binding.endpoint.clone(), binding.target_id.clone())
                    .await
                    .map_err(ManagedError::Bind)?;
                if binding.active.load(Ordering::Acquire) {
                    *binding.current.lock().await = Some((generation, listener));
                    rebound += 1;
                }
            }
            let data = self.data.lock().expect("managed data lock poisoned");
            if data.generation != generation {
                return Err(ManagedError::NotReady);
            }
            let active = data
                .bindings
                .values()
                .filter(|binding| binding.active.load(Ordering::Acquire))
                .count();
            drop(data);
            if rebound == active {
                self.set_state(ManagedState::Ready, None);
                return Ok(Instant::now());
            }
        }
    }

    pub(super) async fn bind_declaration(
        &self,
        binding: &Arc<ManagedBinding>,
    ) -> Result<(), ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            if !binding.active.load(Ordering::Acquire) {
                return Err(ManagedError::Closed);
            }
            let (managed_state, generation, client, failure) = {
                let data = self.data.lock().expect("managed data lock poisoned");
                (
                    data.state,
                    data.generation,
                    data.current.as_ref().map(Client::managed_handle),
                    data.failure.clone(),
                )
            };
            match managed_state {
                ManagedState::Failed => {
                    return Err(ManagedError::Failed(
                        failure.unwrap_or_else(|| "managed connection failed".into()),
                    ));
                }
                ManagedState::Closed => return Err(ManagedError::Closed),
                ManagedState::Ready => {
                    let client = client.ok_or(ManagedError::NotReady)?;
                    match client
                        .bind(binding.endpoint.clone(), binding.target_id.clone())
                        .await
                    {
                        Ok(listener) => {
                            let current_generation = self
                                .data
                                .lock()
                                .expect("managed data lock poisoned")
                                .generation;
                            if current_generation == generation
                                && binding.active.load(Ordering::Acquire)
                            {
                                *binding.current.lock().await = Some((generation, listener));
                                self.publish_state();
                                return Ok(());
                            }
                        }
                        Err(BindError::Session(_)) => {}
                        Err(error) => return Err(ManagedError::Bind(error)),
                    }
                }
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    pub(super) async fn wait_binding(
        &self,
        binding: &Arc<ManagedBinding>,
        observed: u64,
    ) -> Result<u64, ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            if !binding.active.load(Ordering::Acquire) {
                return Err(ManagedError::Closed);
            }
            if let Some((generation, _)) = binding.current.lock().await.as_ref()
                && *generation > observed
            {
                return Ok(*generation);
            }
            match *state.borrow() {
                ManagedState::Failed => return Err(self.failure()),
                ManagedState::Closed => return Err(ManagedError::Closed),
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    pub(super) async fn wait_ready(&self) -> Result<(), ManagedError> {
        let mut state = self.state_tx.subscribe();
        loop {
            match *state.borrow() {
                ManagedState::Ready => return Ok(()),
                ManagedState::Failed => return Err(self.failure()),
                ManagedState::Closed => return Err(ManagedError::Closed),
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(ManagedError::Closed);
            }
        }
    }

    pub(super) async fn detach(&self) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.current = None;
            data.state = ManagedState::Backoff;
        }
        self.clear_current_listeners().await;
        self.publish_state();
    }

    pub(super) async fn clear_current_listeners(&self) {
        let bindings = self
            .data
            .lock()
            .expect("managed data lock poisoned")
            .bindings
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for binding in bindings {
            binding.current.lock().await.take();
        }
    }

    pub(super) async fn finish(&self, state: ManagedState, failure: Option<String>) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            if let Some(client) = data.current.take() {
                client.shared.terminate(crate::SessionError::Closed);
            }
            data.state = state;
            data.failure = failure;
        }
        self.clear_current_listeners().await;
        self.publish_state();
    }

    pub(super) fn set_state(&self, state: ManagedState, failure: Option<String>) {
        {
            let mut data = self.data.lock().expect("managed data lock poisoned");
            data.state = state;
            data.failure = failure;
        }
        self.publish_state();
    }

    pub(super) fn publish_state(&self) {
        let state = self.data.lock().expect("managed data lock poisoned").state;
        self.state_tx.send_replace(state);
    }

    pub(super) fn remove_binding(&self, binding: &Arc<ManagedBinding>) {
        let key = (binding.endpoint.clone(), binding.target_id.clone());
        let mut data = self.data.lock().expect("managed data lock poisoned");
        if data
            .bindings
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, binding))
        {
            data.bindings.remove(&key);
        }
        drop(data);
        self.publish_state();
    }

    pub(super) async fn wait_backoff(&self, delay: Duration) -> bool {
        let mut cancel = self.cancel_tx.subscribe();
        tokio::select! {
            _ = sleep(jitter(delay)) => true,
            _ = wait_cancelled(&mut cancel) => false,
        }
    }

    pub(super) fn failure(&self) -> ManagedError {
        ManagedError::Failed(
            self.data
                .lock()
                .expect("managed data lock poisoned")
                .failure
                .clone()
                .unwrap_or_else(|| "managed connection failed".into()),
        )
    }

    pub(super) fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    pub(super) fn cancelled(&self) -> bool {
        *self.cancel_tx.borrow()
    }
}
