use std::time::Duration;

use relaygate_route_table::{GatewayLocator, ShardDirectory};
use relaygate_route_table_transport::{GatewayName, InternalGatewayKey, RouteTableClientConfig};
use relaygate_transport::ClientTlsConfig;
use tokio::{sync::Semaphore, time::Instant};

use super::RoutingError;

const DEFAULT_ROUTING_QUEUE_CAPACITY: usize = 128;
const DEFAULT_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_DESIRED_SCAN_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_ROUTING_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Immutable RouteTable configuration for one Gateway runtime incarnation.
#[derive(Clone)]
pub struct GatewayRoutingConfig {
    pub(super) directory: ShardDirectory,
    pub(super) gateway_name: GatewayName,
    pub(super) internal_gateway_key: InternalGatewayKey,
    pub(super) gateway_locator: GatewayLocator,
    pub(super) client: RouteTableClientConfig,
    pub(super) tls: Option<ClientTlsConfig>,
    pub(super) command_queue_capacity: usize,
    pub(super) reconnect_initial_backoff: Duration,
    pub(super) reconnect_max_backoff: Duration,
    pub(super) desired_scan_interval: Duration,
    pub(super) shutdown_timeout: Duration,
}

impl GatewayRoutingConfig {
    pub fn new(
        directory: ShardDirectory,
        gateway_name: GatewayName,
        internal_gateway_key: InternalGatewayKey,
        gateway_locator: GatewayLocator,
        client: RouteTableClientConfig,
    ) -> Self {
        Self {
            directory,
            gateway_name,
            internal_gateway_key,
            gateway_locator,
            client,
            tls: None,
            command_queue_capacity: DEFAULT_ROUTING_QUEUE_CAPACITY,
            reconnect_initial_backoff: DEFAULT_RECONNECT_INITIAL_BACKOFF,
            reconnect_max_backoff: DEFAULT_RECONNECT_MAX_BACKOFF,
            desired_scan_interval: DEFAULT_DESIRED_SCAN_INTERVAL,
            shutdown_timeout: DEFAULT_ROUTING_SHUTDOWN_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    #[must_use]
    pub const fn with_command_queue_capacity(mut self, capacity: usize) -> Self {
        self.command_queue_capacity = capacity;
        self
    }

    #[must_use]
    pub const fn with_reconnect_backoff(mut self, initial: Duration, maximum: Duration) -> Self {
        self.reconnect_initial_backoff = initial;
        self.reconnect_max_backoff = maximum;
        self
    }

    #[must_use]
    pub const fn with_desired_scan_interval(mut self, interval: Duration) -> Self {
        self.desired_scan_interval = interval;
        self
    }

    #[must_use]
    pub const fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub(super) fn validate(&self) -> Result<(), RoutingError> {
        if self.command_queue_capacity == 0 {
            return Err(RoutingError::InvalidConfig(
                "routing command queue capacity must be greater than zero".to_owned(),
            ));
        }
        if self.command_queue_capacity > Semaphore::MAX_PERMITS {
            return Err(RoutingError::InvalidConfig(
                "routing command queue capacity exceeds the runtime limit".to_owned(),
            ));
        }
        if self.reconnect_initial_backoff.is_zero() || self.reconnect_max_backoff.is_zero() {
            return Err(RoutingError::InvalidConfig(
                "routing reconnect backoff must be greater than zero".to_owned(),
            ));
        }
        if self.reconnect_initial_backoff > self.reconnect_max_backoff {
            return Err(RoutingError::InvalidConfig(
                "routing reconnect initial backoff must not exceed its maximum".to_owned(),
            ));
        }
        if Instant::now()
            .checked_add(self.reconnect_max_backoff)
            .is_none()
        {
            return Err(RoutingError::InvalidConfig(
                "routing reconnect backoff cannot be represented".to_owned(),
            ));
        }
        if self.desired_scan_interval.is_zero() || self.shutdown_timeout.is_zero() {
            return Err(RoutingError::InvalidConfig(
                "routing scan interval and shutdown timeout must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}
