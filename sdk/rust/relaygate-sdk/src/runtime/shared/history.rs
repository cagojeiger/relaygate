use super::*;

impl Shared {
    pub(in crate::runtime) fn clear_binding_pending_if(&self, operation_id: &str) {
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

    pub(in crate::runtime) fn record_retired_binding(&self, binding_id: &str) -> bool {
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

    pub(in crate::runtime) fn binding_is_retired(&self, binding_id: &str) -> bool {
        self.retired_bindings
            .lock()
            .expect("retired bindings lock poisoned")
            .iter()
            .any(|known| known == binding_id)
    }

    pub(in crate::runtime) fn forget_retired_binding(&self, binding_id: &str) {
        let mut retired = self
            .retired_bindings
            .lock()
            .expect("retired bindings lock poisoned");
        if let Some(index) = retired.iter().position(|known| known == binding_id) {
            retired.remove(index);
        }
    }

    pub(in crate::runtime) fn remember_binding_fingerprint(&self, binding: &wire::ListenerBinding) {
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

    pub(in crate::runtime) fn binding_fingerprint_matches(
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

    pub(in crate::runtime) fn remove_open_if(
        &self,
        request_id: &str,
        expected: &Arc<PendingOpen>,
    ) -> bool {
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

    pub(in crate::runtime) fn open_terminal_matches(
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

    pub(in crate::runtime) fn remember_open_terminal(
        &self,
        request_id: String,
        terminal: OpenTerminal,
    ) {
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

    pub(in crate::runtime) fn confirmation_matches(
        &self,
        attempt_id: &str,
        pipe_id: &str,
    ) -> Option<bool> {
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

    pub(in crate::runtime) fn offer_terminal_exists(&self, attempt_id: &str) -> bool {
        self.offer_history
            .lock()
            .expect("offer history lock poisoned")
            .iter()
            .any(|(known_attempt, _)| known_attempt == attempt_id)
    }

    pub(in crate::runtime) fn decision_rejection_matches(
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

    pub(in crate::runtime) fn remember_offer_terminal(
        &self,
        attempt_id: String,
        terminal: OfferTerminal,
    ) {
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

    pub(in crate::runtime) fn remember_confirmation(&self, attempt_id: String, pipe_id: String) {
        self.remember_offer_terminal(attempt_id, OfferTerminal::Retired { pipe_id });
    }

    pub(in crate::runtime) fn remember_decision_rejection(&self, attempt_id: String, failure: i32) {
        self.remember_offer_terminal(attempt_id, OfferTerminal::DecisionRejected { failure });
    }

    pub(crate) fn retire_offer_identity(&self, attempt_id: &str, pipe_id: &str) {
        self.remember_confirmation(attempt_id.to_owned(), pipe_id.to_owned());
    }

    pub(in crate::runtime) fn close_ack_matches(&self, pipe_id: &str, owned: bool) -> Option<bool> {
        self.close_history
            .lock()
            .expect("close history lock poisoned")
            .iter()
            .find(|(known_pipe, _)| known_pipe == pipe_id)
            .map(|(_, known_owned)| *known_owned == owned)
    }

    pub(in crate::runtime) fn remember_close_ack(&self, pipe_id: String, owned: bool) {
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

    pub(in crate::runtime) fn remember_cancel_request(&self, request_id: &str) {
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

    pub(in crate::runtime) fn acknowledge_cancel(
        &self,
        request_id: &str,
        was_pending: bool,
    ) -> Option<bool> {
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

    pub(in crate::runtime) fn pipe_was_retired(&self, pipe_id: &str) -> bool {
        self.pipe_history
            .lock()
            .expect("pipe history lock poisoned")
            .iter()
            .any(|(known_pipe, _)| known_pipe == pipe_id)
    }

    pub(in crate::runtime) fn remember_pipe(&self, pipe_id: String, attempt_id: String) {
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

    pub(in crate::runtime) fn remember_delivery(
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

    pub(in crate::runtime) fn complete_delivery(
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
}
