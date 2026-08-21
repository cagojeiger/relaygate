use super::*;

impl ManagedListener {
    pub fn endpoint(&self) -> &str {
        &self.binding.endpoint
    }

    pub fn target_id(&self) -> &str {
        &self.binding.target_id
    }

    /// Waits across reconnects for an Offer on the current underlying
    /// Listener. Returned Offers remain bound to the session that created them.
    pub async fn next(&mut self) -> Result<Option<Offer>, ManagedError> {
        let mut observed = 0;
        loop {
            let generation = self.core.wait_binding(&self.binding, observed).await?;
            let mut current = self.binding.current.lock().await;
            let Some((current_generation, listener)) = current.as_mut() else {
                observed = generation;
                continue;
            };
            if *current_generation != generation {
                observed = generation;
                continue;
            }
            match listener.next().await {
                Ok(offer) => return Ok(offer),
                Err(error) => {
                    let session_ended = listener.shared.terminal().is_some();
                    observed = generation;
                    if !session_ended {
                        return Err(ManagedError::Session(error));
                    }
                }
            }
        }
    }

    /// Removes the desired declaration before current-session cleanup.
    pub async fn unbind(&self) -> Result<(), ManagedError> {
        if self.binding.active.swap(false, Ordering::AcqRel) {
            self.core.remove_binding(&self.binding);
        }
        let listener = self.binding.current.lock().await.take();
        if let Some((_, listener)) = listener {
            listener.unbind().await?;
        }
        Ok(())
    }
}

impl Drop for ManagedListener {
    fn drop(&mut self) {
        if self.binding.active.swap(false, Ordering::AcqRel) {
            self.core.remove_binding(&self.binding);
            if let Ok(mut current) = self.binding.current.try_lock() {
                current.take();
            }
        }
    }
}
