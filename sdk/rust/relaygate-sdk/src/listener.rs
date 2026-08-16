use std::sync::{Arc, atomic::Ordering};

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    AcceptError, Pipe, RejectError, SessionError, UnbindError,
    runtime::{
        AcceptGuard, BindingOperationGuard, BindingPending, ListenerState, OfferState, Shared,
        request, wait_for_acknowledged, wait_for_established,
    },
    wire::{self, connect_request},
};

/// A committed listener binding and its bounded offer queue.
pub struct Listener {
    pub(crate) state: Arc<ListenerState>,
    pub(crate) shared: Arc<Shared>,
}

impl Listener {
    pub fn binding_id(&self) -> &str {
        &self.state.binding_id
    }

    pub fn endpoint_pattern(&self) -> &str {
        &self.state.endpoint_pattern
    }

    pub fn target_id(&self) -> &str {
        &self.state.target_id
    }

    /// Waits for the next provisional listener offer.
    pub async fn next(&mut self) -> Result<Option<Offer>, SessionError> {
        if let Some(error) = self.shared.terminal() {
            return Err(error);
        }
        let mut receiver = self.state.offers_rx.lock().await;
        tokio::select! {
            biased;
            error = self.shared.wait_done() => Err(error),
            offer = receiver.recv() => Ok(offer),
        }
    }

    /// Makes this binding immediately ineligible at the Gateway and waits for
    /// its serialized unbind response.
    pub async fn unbind(&self) -> Result<(), UnbindError> {
        let _lane = self.shared.binding_lane.lock().await;
        self.shared.ensure_active().map_err(UnbindError::Session)?;
        let (tx, rx) = oneshot::channel();
        let operation_id = Uuid::new_v4().to_string();
        {
            let mut pending = self
                .shared
                .binding_pending
                .lock()
                .expect("binding pending lock poisoned");
            if pending.is_some() {
                return Err(UnbindError::OperationPending);
            }
            *pending = Some(BindingPending::Unbind {
                operation_id: operation_id.clone(),
                binding_id: self.state.binding_id.clone(),
                response: tx,
            });
        }
        let mut guard = BindingOperationGuard {
            shared: Arc::downgrade(&self.shared),
            operation_id,
            sent: false,
            armed: true,
        };
        if let Err(error) = self
            .shared
            .send(request(connect_request::Message::UnbindListener(
                wire::UnbindListener {
                    listener_binding_id: self.state.binding_id.clone(),
                },
            )))
            .await
        {
            return Err(UnbindError::Session(error));
        }
        guard.sent = true;
        self.state.active.store(false, Ordering::Release);
        let result = rx
            .await
            .unwrap_or_else(|_| Err(self.shared.terminal_or_transport().into()));
        guard.armed = false;
        result
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if self.state.active.swap(false, Ordering::AcqRel) {
            self.shared.auto_unbind(self.state.binding_id.clone());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OfferMetadata {
    pub(crate) attempt_id: String,
    pub(crate) listener_binding_id: String,
    pub(crate) endpoint: String,
    pub(crate) target_id: String,
    pub(crate) caller_session_id: String,
}

impl OfferMetadata {
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub fn listener_binding_id(&self) -> &str {
        &self.listener_binding_id
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    pub fn caller_session_id(&self) -> &str {
        &self.caller_session_id
    }
}

/// A one-shot listener decision.
pub struct Offer {
    pub(crate) metadata: OfferMetadata,
    pub(crate) state: Option<Arc<OfferState>>,
    pub(crate) shared: Arc<Shared>,
}

struct RejectGuard {
    shared: Arc<Shared>,
    state: Arc<OfferState>,
    complete: bool,
}

impl Drop for RejectGuard {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        self.shared
            .send_background(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: self.state.attempt_id.clone(),
                },
            )));
        self.shared.remove_offer(&self.state.attempt_id);
    }
}

impl Offer {
    pub fn metadata(&self) -> &OfferMetadata {
        &self.metadata
    }

    /// Provisionally accepts, registers Pipe dispatch before confirming, and
    /// exposes the Pipe only after the exact confirmation acknowledgement.
    pub async fn accept(mut self) -> Result<Pipe, AcceptError> {
        let state = self.state.take().ok_or(AcceptError::NotPending)?;
        state
            .decision
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| AcceptError::NotPending)?;
        if !self.shared.reserve_pipe_slot() {
            state.decision.store(2, Ordering::Release);
            state.ended.store(true, Ordering::Release);
            self.shared.retire_offer_identity(&state.attempt_id, "");
            let mut reject = RejectGuard {
                shared: Arc::clone(&self.shared),
                state: Arc::clone(&state),
                complete: false,
            };
            self.shared
                .send(request(connect_request::Message::ListenerReject(
                    wire::ListenerReject {
                        attempt_id: state.attempt_id.clone(),
                    },
                )))
                .await?;
            self.shared.remove_offer(&state.attempt_id);
            reject.complete = true;
            return Err(AcceptError::CapacityReached);
        }
        state.slot_reserved.store(true, Ordering::Release);
        let mut guard = AcceptGuard {
            state: Arc::clone(&state),
            sent: false,
            armed: true,
        };
        self.shared
            .send(request(connect_request::Message::ListenerAccept(
                wire::ListenerAccept {
                    attempt_id: state.attempt_id.clone(),
                },
            )))
            .await?;
        guard.sent = true;

        let mut events = state.events.subscribe();
        let pipe_id = wait_for_established(&mut events).await?;
        let pipe =
            match self
                .shared
                .register_pipe(&pipe_id, &state.attempt_id, &state.slot_reserved)
            {
                Ok(pipe) => pipe,
                Err(error) => {
                    state.cancelled.store(true, Ordering::Release);
                    self.shared.start_accept_cleanup(Arc::clone(&state));
                    guard.armed = false;
                    return Err(error);
                }
            };
        *state.pipe_id.lock().expect("offer pipe lock poisoned") = Some(pipe_id.clone());
        if state.ended.load(Ordering::Acquire) || pipe.state.terminal.borrow().is_some() {
            self.shared
                .terminalize_pipe(&pipe_id, crate::PipeError::Terminal);
            guard.armed = false;
            return Err(AcceptError::NotPending);
        }
        self.shared
            .send(request(connect_request::Message::ListenerConfirmed(
                wire::ListenerConfirmed {
                    attempt_id: state.attempt_id.clone(),
                    pipe_id: pipe_id.clone(),
                },
            )))
            .await?;
        state.confirm_sent.store(true, Ordering::Release);
        wait_for_acknowledged(&state, &mut events, &pipe_id).await?;
        self.shared.remove_offer(&state.attempt_id);
        guard.armed = false;
        Ok(pipe)
    }

    pub async fn reject(mut self) -> Result<(), RejectError> {
        let state = self.state.take().ok_or(RejectError::NotPending)?;
        state
            .decision
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RejectError::NotPending)?;
        state.ended.store(true, Ordering::Release);
        self.shared.retire_offer_identity(&state.attempt_id, "");
        let mut guard = RejectGuard {
            shared: Arc::clone(&self.shared),
            state: Arc::clone(&state),
            complete: false,
        };
        self.shared
            .send(request(connect_request::Message::ListenerReject(
                wire::ListenerReject {
                    attempt_id: state.attempt_id.clone(),
                },
            )))
            .await?;
        self.shared.remove_offer(&state.attempt_id);
        guard.complete = true;
        Ok(())
    }
}

impl Drop for Offer {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        if state
            .decision
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            state.ended.store(true, Ordering::Release);
            self.shared.retire_offer_identity(&state.attempt_id, "");
            self.shared
                .send_background(request(connect_request::Message::ListenerReject(
                    wire::ListenerReject {
                        attempt_id: state.attempt_id.clone(),
                    },
                )));
            self.shared.remove_offer(&state.attempt_id);
        }
    }
}
