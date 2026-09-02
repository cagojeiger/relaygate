use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    auth::ClientKeyStore,
    peer::PeerHandle,
    routing::RoutingHandle,
    state::GatewayState,
    state::{GatewayAction, GatewayLimits},
};

mod distributed;
mod effects;
mod heartbeat;
mod route_resolver;
mod sdk_server;
mod session;
mod snapshot;
#[cfg(test)]
mod tests;

use distributed::DistributedRuntime;
use effects::ControlEffects;
pub use sdk_server::check;

#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<GatewayState>,
    writer_queue_capacity: usize,
    max_frame_len: usize,
    offer_timeout: Duration,
    heartbeat_idle_interval: Duration,
    heartbeat_response_timeout: Duration,
    session_slots: Arc<Semaphore>,
    routing: Option<RoutingHandle>,
    peer: Option<PeerHandle>,
    control_effects: Option<ControlEffects>,
    distributed_runtime: Mutex<Option<DistributedRuntime>>,
}

impl fmt::Debug for Gateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Gateway")
            .field("distributed", &self.inner.distributed_runtime())
            .finish_non_exhaustive()
    }
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Result<Self, GatewayError> {
        Self::build(config, None)
    }

    /// Creates a distributed Gateway with RouteTable discovery and one-hop
    /// Gateway peer relay.
    ///
    /// RouteTable availability is not a startup prerequisite. The manager
    /// reconnects in the background while local SDK sessions remain usable.
    /// Construction must occur inside a running Tokio runtime.
    pub fn new_distributed(
        config: GatewayConfig,
        routing_config: GatewayRoutingConfig,
        peer_config: GatewayPeerConfig,
        shutdown: CancellationToken,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            GatewayError::Routing(format!(
                "distributed Gateway must be constructed inside a Tokio runtime: {error}"
            ))
        })?;
        let gateway_id = relaygate_route_table::GatewayId::new();
        let action_result_capacity = config.max_pending_offers;
        let distributed = DistributedRuntime::start(
            gateway_id,
            routing_config,
            peer_config,
            action_result_capacity,
            shutdown,
        )?;
        Self::build(config, Some(distributed))
    }

    fn build(
        config: GatewayConfig,
        distributed: Option<DistributedRuntime>,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let limits = GatewayLimits {
            max_sessions: config.max_sessions,
            max_bindings: config.max_bindings,
            max_pending_offers: config.max_pending_offers,
            max_live_pipes: config.max_live_pipes,
            offer_timeout: config.offer_timeout,
        };
        let gateway_id = distributed.as_ref().map(DistributedRuntime::gateway_id);
        let routing = distributed.as_ref().map(DistributedRuntime::routing);
        let peer = distributed.as_ref().map(DistributedRuntime::peer);
        let control_effects = distributed.as_ref().map(|runtime| {
            ControlEffects::new(
                config.max_pending_offers,
                Arc::new(runtime.routing()),
                runtime.action_results(),
                runtime.shutdown(),
            )
        });
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(match gateway_id {
                    Some(gateway_id) => GatewayState::new_distributed(
                        ClientKeyStore::new(config.client_keys),
                        limits,
                        gateway_id,
                    ),
                    None => GatewayState::new(ClientKeyStore::new(config.client_keys), limits),
                }),
                writer_queue_capacity: config.writer_queue_capacity,
                max_frame_len: config.max_frame_len,
                offer_timeout: config.offer_timeout,
                heartbeat_idle_interval: config.heartbeat_idle_interval,
                heartbeat_response_timeout: config.heartbeat_response_timeout,
                session_slots: Arc::new(Semaphore::new(config.max_sessions)),
                routing,
                peer,
                control_effects,
                distributed_runtime: Mutex::new(distributed),
            }),
        })
    }

    /// Serves SDK sessions until `shutdown` is cancelled.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        if self.inner.distributed_runtime() {
            return Err(GatewayError::InvalidConfig(
                "a distributed Gateway must be served with serve_distributed".to_owned(),
            ));
        }
        self.serve_sdk(listener, shutdown).await
    }
}

impl Inner {
    /// Commits the latest complete snapshot while the Gateway state lock still
    /// orders the corresponding local mutation. The manager wake is bounded
    /// and synchronous; no network I/O occurs under this lock. This prevents a
    /// delayed action from publishing an older snapshot after session cleanup.
    fn commit_registration_actions(&self, actions: &[GatewayAction]) {
        let Some(routing) = &self.routing else {
            return;
        };
        for action in actions {
            let GatewayAction::PublishRegistration {
                session_id,
                bindings,
            } = action
            else {
                continue;
            };
            if let Err(error) = routing.publish_session(*session_id, bindings.clone()) {
                tracing::warn!(
                    component = "gateway",
                    event = "gateway.registration.publish_failed",
                    listener_session_id = %session_id.as_uuid(),
                    %error,
                    "local registration remains active while RouteTable publication is unavailable"
                );
            }
        }
    }

    fn distributed_runtime(&self) -> bool {
        match self.distributed_runtime.lock() {
            Ok(runtime) => runtime.is_some(),
            Err(poisoned) => poisoned.into_inner().is_some(),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, GatewayState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.state.lock_poisoned",
                    "recovering poisoned Gateway state lock"
                );
                poisoned.into_inner()
            }
        }
    }
}
