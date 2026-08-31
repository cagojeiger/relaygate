use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{ClientId as RouteClientId, GatewayId, GatewayLocator};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    peer::{OpenIdentity, PeerEvent, PeerFailure, PeerOpenRequest, PeerTarget},
    state::{GatewayAction, GatewayState, PeerDelivery},
};

use super::{Inner, route_resolver::RouteResolver};

#[cfg(test)]
mod tests;

pub(super) struct ControlEffects {
    slots: Arc<Semaphore>,
    route_resolver: Arc<dyn RouteResolver>,
    tasks: TaskTracker,
    results: mpsc::Sender<Vec<GatewayAction>>,
    shutdown: CancellationToken,
}

enum ControlAction {
    ResolveRoute {
        open_identity: OpenIdentity,
        client_id: RouteClientId,
    },
    OpenPeer {
        open_identity: OpenIdentity,
        gateway_id: GatewayId,
        gateway_locator: GatewayLocator,
        client_id: String,
        listener_session_id: SessionId,
        binding_id: BindingId,
    },
    CancelPeerOpen {
        open_identity: OpenIdentity,
    },
}

impl ControlEffects {
    pub(super) fn new(
        capacity: usize,
        route_resolver: Arc<dyn RouteResolver>,
        results: mpsc::Sender<Vec<GatewayAction>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(capacity)),
            route_resolver,
            tasks: TaskTracker::new(),
            results,
            shutdown,
        }
    }

    pub(super) async fn close_and_wait(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl Inner {
    fn transition(
        &self,
        apply: impl FnOnce(&mut GatewayState) -> Vec<GatewayAction>,
    ) -> Vec<GatewayAction> {
        let mut state = self.lock_state();
        let actions = apply(&mut state);
        self.commit_registration_actions(&actions);
        actions
    }

    pub(super) async fn execute_all(self: &Arc<Self>, actions: Vec<GatewayAction>) {
        let mut pending = VecDeque::from(actions);
        let mut cleaned = HashSet::new();
        while let Some(action) = pending.pop_front() {
            match action {
                GatewayAction::SendSdkFrame(delivery) => {
                    let Some(failed_session) = delivery.deliver() else {
                        continue;
                    };
                    if cleaned.insert(failed_session) {
                        let cleanup_actions = {
                            let mut state = self.lock_state();
                            let actions = state.remove_session(failed_session);
                            self.commit_registration_actions(&actions);
                            actions
                        };
                        pending.extend(cleanup_actions);
                    }
                }
                GatewayAction::PublishRegistration { .. } => {}
                GatewayAction::ResolveRoute {
                    open_identity,
                    client_id,
                } => pending.extend(self.spawn_control_effect(ControlAction::ResolveRoute {
                    open_identity,
                    client_id,
                })),
                GatewayAction::OpenPeer {
                    open_identity,
                    gateway_id,
                    gateway_locator,
                    client_id,
                    listener_session_id,
                    binding_id,
                } => pending.extend(self.spawn_control_effect(ControlAction::OpenPeer {
                    open_identity,
                    gateway_id,
                    gateway_locator,
                    client_id,
                    listener_session_id,
                    binding_id,
                })),
                GatewayAction::CancelPeerOpen { open_identity } => pending.extend(
                    self.spawn_control_effect(ControlAction::CancelPeerOpen { open_identity }),
                ),
                GatewayAction::SendPeerFrame(delivery) => {
                    pending.extend(self.send_peer_delivery(delivery).await);
                }
            }
        }
    }

    fn spawn_control_effect(self: &Arc<Self>, action: ControlAction) -> Vec<GatewayAction> {
        let Some(control) = &self.control_effects else {
            return self.reject_control_effect(action, "distributed control runtime is disabled");
        };
        let Ok(permit) = Arc::clone(&control.slots).try_acquire_owned() else {
            return self.reject_control_effect(action, "Gateway control effect limit reached");
        };
        let inner = Arc::clone(self);
        let results = control.results.clone();
        let shutdown = control.shutdown.clone();
        control.tasks.spawn(async move {
            let actions = inner.run_control_effect(action).await;
            drop(permit);
            if actions.is_empty() {
                return;
            }
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = results.send(actions) => {
                    if result.is_err() && !shutdown.is_cancelled() {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.control_result.dropped",
                            "distributed control result loop stopped"
                        );
                    }
                }
            }
        });
        Vec::new()
    }

    fn reject_control_effect(&self, action: ControlAction, message: &str) -> Vec<GatewayAction> {
        match action {
            ControlAction::ResolveRoute { open_identity, .. } => self.transition(|state| {
                state.route_failed(open_identity, ErrorCode::ResourceExhausted, message)
            }),
            ControlAction::OpenPeer { open_identity, .. } => self.transition(|state| {
                state.peer_open_commit_failed(
                    open_identity,
                    ErrorCode::ResourceExhausted,
                    PeerObservation::NotObserved,
                    message,
                )
            }),
            ControlAction::CancelPeerOpen { .. } => Vec::new(),
        }
    }

    async fn run_control_effect(&self, action: ControlAction) -> Vec<GatewayAction> {
        match action {
            ControlAction::ResolveRoute {
                open_identity,
                client_id,
            } => {
                let Some(control) = &self.control_effects else {
                    return self.transition(|state| {
                        state.route_failed(
                            open_identity,
                            ErrorCode::Internal,
                            "RouteTable routing is not configured",
                        )
                    });
                };
                match control.route_resolver.resolve(client_id).await {
                    Ok(bindings) => {
                        self.transition(|state| state.route_resolved(open_identity, bindings))
                    }
                    Err(error) => self.transition(|state| {
                        state.route_failed(open_identity, error.code(), error.message())
                    }),
                }
            }
            ControlAction::OpenPeer {
                open_identity,
                gateway_id,
                gateway_locator,
                client_id,
                listener_session_id,
                binding_id,
            } => {
                let Some(peer) = &self.peer else {
                    return self.transition(|state| {
                        state.peer_open_commit_failed(
                            open_identity,
                            ErrorCode::Internal,
                            PeerObservation::NotObserved,
                            "Gateway peer relay is not configured",
                        )
                    });
                };
                let request = match PeerOpenRequest::new(
                    PeerTarget::new(gateway_id, gateway_locator),
                    open_identity,
                    client_id,
                    listener_session_id,
                    binding_id,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        return self.transition(|state| {
                            state.peer_open_commit_failed(
                                open_identity,
                                error.code(),
                                error.observation(),
                                error.message(),
                            )
                        });
                    }
                };
                match peer.open(request).await {
                    Ok(key) => {
                        self.transition(|state| state.peer_open_committed(open_identity, key))
                    }
                    Err(error) => self.transition(|state| {
                        state.peer_open_commit_failed(
                            open_identity,
                            error.code(),
                            error.observation(),
                            error.message(),
                        )
                    }),
                }
            }
            ControlAction::CancelPeerOpen { open_identity } => {
                if let Some(peer) = &self.peer
                    && let Err(error) = peer.cancel_open(open_identity).await
                {
                    tracing::debug!(
                        component = "gateway",
                        event = "gateway.peer_open.cancel_failed",
                        error_code = ?error.code(),
                        observation = ?error.observation(),
                        "peer OPEN cancellation did not commit; a late result remains terminal locally"
                    );
                }
                Vec::new()
            }
        }
    }

    async fn send_peer_delivery(&self, delivery: PeerDelivery) -> Vec<GatewayAction> {
        let Some(peer) = &self.peer else {
            return self.cleanup_unsent_peer_delivery(
                delivery,
                ErrorCode::Internal,
                "Gateway peer relay is not configured",
            );
        };
        let (key, recover_stream, close_transport_on_failure, result) = match delivery {
            PeerDelivery::Opened { key } => (key, true, false, peer.send_opened(key).await),
            PeerDelivery::Failed {
                key,
                code,
                observation,
                message,
            } => (
                key,
                false,
                false,
                peer.send_failed(key, PeerFailure::new(code, observation, message))
                    .await,
            ),
            PeerDelivery::Data { key, payload } => {
                (key, true, false, peer.send_data(key, payload).await)
            }
            PeerDelivery::Fin { key } => (key, true, false, peer.send_fin(key).await),
            PeerDelivery::Close { key } => (key, false, false, peer.send_close(key).await),
            PeerDelivery::Reset { key, code, message } => {
                (key, false, true, peer.send_reset(key, code, message).await)
            }
        };
        let Err(error) = result else {
            return Vec::new();
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.peer_frame.commit_failed",
            peer_gateway_id = %key.peer_gateway_id(),
            error_code = ?error.code(),
            observation = ?error.observation(),
            "peer frame did not commit"
        );
        if close_transport_on_failure {
            peer.close_transport(key);
            return Vec::new();
        }
        if !recover_stream {
            return Vec::new();
        }

        let actions = self
            .transition(|state| state.peer_reset(key, error.code(), error.message().to_owned()));
        if peer
            .send_reset(key, error.code(), error.message().to_owned())
            .await
            .is_err()
        {
            peer.close_transport(key);
        }
        actions
    }

    fn cleanup_unsent_peer_delivery(
        &self,
        delivery: PeerDelivery,
        code: ErrorCode,
        message: &str,
    ) -> Vec<GatewayAction> {
        match delivery {
            PeerDelivery::Opened { key }
            | PeerDelivery::Data { key, .. }
            | PeerDelivery::Fin { key } => {
                self.transition(|state| state.peer_reset(key, code, message.to_owned()))
            }
            PeerDelivery::Failed { .. }
            | PeerDelivery::Close { .. }
            | PeerDelivery::Reset { .. } => Vec::new(),
        }
    }

    pub(super) async fn run_peer_events(
        self: Arc<Self>,
        mut events: crate::peer::PeerEvents,
        shutdown: CancellationToken,
    ) -> Result<(), crate::GatewayError> {
        loop {
            let event = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                event = events.recv() => event,
            };
            let Some(event) = event else {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                return Err(crate::GatewayError::Peer(
                    "peer event stream stopped unexpectedly".to_owned(),
                ));
            };
            let actions = self.handle_peer_event(event);
            self.execute_all(actions).await;
        }
    }

    fn handle_peer_event(&self, event: PeerEvent) -> Vec<GatewayAction> {
        self.transition(|state| match event {
            PeerEvent::IncomingOpen {
                key,
                open_identity,
                client_id,
                listener_session_id,
                binding_id,
            } => state.receive_peer_open(
                key,
                open_identity,
                client_id,
                listener_session_id,
                binding_id,
            ),
            PeerEvent::Opened { key, open_identity } => state.peer_opened(key, open_identity),
            PeerEvent::Failed {
                key,
                open_identity,
                failure,
            } => state.peer_open_failed(
                key,
                open_identity,
                failure.code(),
                failure.observation(),
                failure.message(),
            ),
            PeerEvent::Data { key, payload } => state.peer_data(key, payload),
            PeerEvent::Fin { key } => state.peer_fin(key),
            PeerEvent::Close { key } => state.peer_close(key),
            PeerEvent::Reset { key, code, message } => state.peer_reset(key, code, message),
            PeerEvent::TransportLost { streams, .. } => streams
                .into_iter()
                .flat_map(|stream| {
                    state.peer_transport_lost_stream(
                        stream.key,
                        stream.open_identity,
                        stream.progress.failure_observation(),
                    )
                })
                .collect(),
        })
    }

    pub(super) async fn run_control_results(
        self: Arc<Self>,
        mut results: mpsc::Receiver<Vec<GatewayAction>>,
        shutdown: CancellationToken,
    ) -> Result<(), crate::GatewayError> {
        loop {
            let actions = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                actions = results.recv() => actions,
            };
            let Some(actions) = actions else {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                return Err(crate::GatewayError::Peer(
                    "distributed control result stream stopped unexpectedly".to_owned(),
                ));
            };
            self.execute_all(actions).await;
        }
    }

    pub(super) async fn wait_control_effects(&self) {
        if let Some(control) = &self.control_effects {
            control.close_and_wait().await;
        }
    }
}
