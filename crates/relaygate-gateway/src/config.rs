use std::time::Duration;

use relaygate_protocol::DEFAULT_MAX_FRAME_LEN;
use relaygate_transport::ServerTlsConfig;
use tokio::time::Instant;

use crate::GatewayError;

pub const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_MAX_SESSIONS: usize = 10_000;
pub const DEFAULT_MAX_BINDINGS: usize = 100_000;
pub const DEFAULT_MAX_PENDING_OFFERS: usize = 10_000;
pub const DEFAULT_MAX_LIVE_PIPES: usize = 100_000;
pub const DEFAULT_OFFER_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_HEARTBEAT_IDLE_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_HEARTBEAT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(120);

/// Immutable runtime configuration for one Gateway process.
#[derive(Clone)]
pub struct GatewayConfig {
    pub(crate) cluster_token: String,
    pub(crate) next_cluster_token: Option<String>,
    pub(crate) sdk_tls: Option<ServerTlsConfig>,
    pub(crate) writer_queue_capacity: usize,
    pub(crate) max_frame_len: usize,
    pub(crate) max_sessions: usize,
    pub(crate) max_bindings: usize,
    pub(crate) max_pending_offers: usize,
    pub(crate) max_live_pipes: usize,
    pub(crate) offer_timeout: Duration,
    pub(crate) heartbeat_idle_interval: Duration,
    pub(crate) heartbeat_response_timeout: Duration,
    pub(crate) drain_timeout: Duration,
}

impl std::fmt::Debug for GatewayConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayConfig")
            .field("cluster_token", &"[REDACTED]")
            .field(
                "next_cluster_token",
                &self.next_cluster_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("writer_queue_capacity", &self.writer_queue_capacity)
            .field("max_frame_len", &self.max_frame_len)
            .field("max_sessions", &self.max_sessions)
            .field("max_bindings", &self.max_bindings)
            .field("max_pending_offers", &self.max_pending_offers)
            .field("max_live_pipes", &self.max_live_pipes)
            .field("offer_timeout", &self.offer_timeout)
            .field("heartbeat_idle_interval", &self.heartbeat_idle_interval)
            .field(
                "heartbeat_response_timeout",
                &self.heartbeat_response_timeout,
            )
            .field("drain_timeout", &self.drain_timeout)
            .finish()
    }
}

impl GatewayConfig {
    #[must_use]
    pub fn new(cluster_token: impl Into<String>) -> Self {
        Self {
            cluster_token: cluster_token.into(),
            next_cluster_token: None,
            sdk_tls: None,
            writer_queue_capacity: DEFAULT_WRITER_QUEUE_CAPACITY,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_bindings: DEFAULT_MAX_BINDINGS,
            max_pending_offers: DEFAULT_MAX_PENDING_OFFERS,
            max_live_pipes: DEFAULT_MAX_LIVE_PIPES,
            offer_timeout: DEFAULT_OFFER_TIMEOUT,
            heartbeat_idle_interval: DEFAULT_HEARTBEAT_IDLE_INTERVAL,
            heartbeat_response_timeout: DEFAULT_HEARTBEAT_RESPONSE_TIMEOUT,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_sdk_tls(mut self, tls: ServerTlsConfig) -> Self {
        self.sdk_tls = Some(tls);
        self
    }

    #[must_use]
    pub fn with_next_cluster_token(mut self, token: impl Into<String>) -> Self {
        self.next_cluster_token = Some(token.into());
        self
    }

    #[must_use]
    pub const fn with_writer_queue_capacity(mut self, capacity: usize) -> Self {
        self.writer_queue_capacity = capacity;
        self
    }

    #[must_use]
    pub const fn with_max_frame_len(mut self, maximum: usize) -> Self {
        self.max_frame_len = maximum;
        self
    }

    #[must_use]
    pub const fn with_max_sessions(mut self, maximum: usize) -> Self {
        self.max_sessions = maximum;
        self
    }

    #[must_use]
    pub const fn with_max_bindings(mut self, maximum: usize) -> Self {
        self.max_bindings = maximum;
        self
    }

    #[must_use]
    pub const fn with_max_pending_offers(mut self, maximum: usize) -> Self {
        self.max_pending_offers = maximum;
        self
    }

    #[must_use]
    pub const fn with_max_live_pipes(mut self, maximum: usize) -> Self {
        self.max_live_pipes = maximum;
        self
    }

    #[must_use]
    pub const fn with_offer_timeout(mut self, timeout: Duration) -> Self {
        self.offer_timeout = timeout;
        self
    }

    #[must_use]
    pub const fn with_heartbeat(
        mut self,
        idle_interval: Duration,
        response_timeout: Duration,
    ) -> Self {
        self.heartbeat_idle_interval = idle_interval;
        self.heartbeat_response_timeout = response_timeout;
        self
    }

    #[must_use]
    pub const fn heartbeat_idle_interval(&self) -> Duration {
        self.heartbeat_idle_interval
    }

    #[must_use]
    pub const fn heartbeat_response_timeout(&self) -> Duration {
        self.heartbeat_response_timeout
    }

    #[must_use]
    pub const fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), GatewayError> {
        if self.writer_queue_capacity == 0 {
            return Err(GatewayError::InvalidConfig(
                "writer queue capacity must be greater than zero".to_owned(),
            ));
        }
        if self.max_frame_len == 0 {
            return Err(GatewayError::InvalidConfig(
                "maximum frame length must be greater than zero".to_owned(),
            ));
        }
        if self.max_sessions == 0
            || self.max_bindings == 0
            || self.max_pending_offers == 0
            || self.max_live_pipes == 0
        {
            return Err(GatewayError::InvalidConfig(
                "Gateway resource limits must be greater than zero".to_owned(),
            ));
        }
        if self.offer_timeout.is_zero()
            || self.heartbeat_idle_interval.is_zero()
            || self.heartbeat_response_timeout.is_zero()
            || self.drain_timeout.is_zero()
        {
            return Err(GatewayError::InvalidConfig(
                "Gateway timeouts must be greater than zero".to_owned(),
            ));
        }
        validate_deadline_timeout("offer_timeout", self.offer_timeout)?;
        validate_deadline_timeout(
            "heartbeat_idle_interval",
            jitter_upper_bound(self.heartbeat_idle_interval).ok_or_else(|| {
                GatewayError::InvalidConfig(
                    "heartbeat_idle_interval is too large after heartbeat jitter".to_owned(),
                )
            })?,
        )?;
        validate_deadline_timeout(
            "heartbeat_response_timeout",
            self.heartbeat_response_timeout,
        )?;
        validate_deadline_timeout("drain_timeout", self.drain_timeout)?;
        if self.cluster_token.is_empty() {
            return Err(GatewayError::InvalidConfig(
                "current ClusterToken must not be empty".to_owned(),
            ));
        }
        if let Some(next) = &self.next_cluster_token {
            if next.is_empty() {
                return Err(GatewayError::InvalidConfig(
                    "next ClusterToken must not be empty".to_owned(),
                ));
            }
            if next == &self.cluster_token {
                return Err(GatewayError::InvalidConfig(
                    "current and next ClusterToken must be different".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_deadline_timeout(name: &str, timeout: Duration) -> Result<(), GatewayError> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        GatewayError::InvalidConfig(format!("{name} is too large to form a monotonic deadline"))
    })?;
    Ok(())
}

fn jitter_upper_bound(duration: Duration) -> Option<Duration> {
    duration_from_nanos(duration.as_nanos().checked_mul(1_100)?.checked_div(1_000)?)
}

fn duration_from_nanos(nanos: u128) -> Option<Duration> {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = nanos % NANOS_PER_SECOND;
    Some(Duration::new(
        seconds.try_into().ok()?,
        subsecond_nanos.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::GatewayConfig;

    #[test]
    fn unrepresentable_deadline_configuration_is_rejected() {
        let valid = Duration::from_secs(1);
        for config in [
            GatewayConfig::new("test-token").with_offer_timeout(Duration::MAX),
            GatewayConfig::new("test-token").with_heartbeat(Duration::MAX, valid),
            GatewayConfig::new("test-token").with_heartbeat(valid, Duration::MAX),
            GatewayConfig::new("test-token").with_drain_timeout(Duration::MAX),
        ] {
            assert!(config.validate().is_err());
        }
    }
}
