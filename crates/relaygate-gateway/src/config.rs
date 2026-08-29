use std::{collections::HashMap, time::Duration};

use relaygate_protocol::DEFAULT_MAX_FRAME_LEN;

use crate::GatewayError;

pub const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_MAX_SESSIONS: usize = 10_000;
pub const DEFAULT_MAX_BINDINGS: usize = 100_000;
pub const DEFAULT_MAX_PENDING_OFFERS: usize = 10_000;
pub const DEFAULT_MAX_LIVE_PIPES: usize = 100_000;
pub const DEFAULT_OFFER_TIMEOUT: Duration = Duration::from_secs(5);

/// Immutable runtime configuration for one Gateway process.
#[derive(Clone)]
pub struct GatewayConfig {
    pub(crate) client_keys: HashMap<String, String>,
    pub(crate) writer_queue_capacity: usize,
    pub(crate) max_frame_len: usize,
    pub(crate) max_sessions: usize,
    pub(crate) max_bindings: usize,
    pub(crate) max_pending_offers: usize,
    pub(crate) max_live_pipes: usize,
    pub(crate) offer_timeout: Duration,
}

impl std::fmt::Debug for GatewayConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayConfig")
            .field("client_count", &self.client_keys.len())
            .field("writer_queue_capacity", &self.writer_queue_capacity)
            .field("max_frame_len", &self.max_frame_len)
            .field("max_sessions", &self.max_sessions)
            .field("max_bindings", &self.max_bindings)
            .field("max_pending_offers", &self.max_pending_offers)
            .field("max_live_pipes", &self.max_live_pipes)
            .field("offer_timeout", &self.offer_timeout)
            .finish()
    }
}

impl GatewayConfig {
    #[must_use]
    pub fn new(client_keys: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            client_keys: client_keys.into_iter().collect(),
            writer_queue_capacity: DEFAULT_WRITER_QUEUE_CAPACITY,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_bindings: DEFAULT_MAX_BINDINGS,
            max_pending_offers: DEFAULT_MAX_PENDING_OFFERS,
            max_live_pipes: DEFAULT_MAX_LIVE_PIPES,
            offer_timeout: DEFAULT_OFFER_TIMEOUT,
        }
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
        if self.offer_timeout.is_zero() {
            return Err(GatewayError::InvalidConfig(
                "offer timeout must be greater than zero".to_owned(),
            ));
        }
        if self.client_keys.keys().any(String::is_empty) {
            return Err(GatewayError::InvalidConfig(
                "ClientId must not be empty".to_owned(),
            ));
        }
        if self.client_keys.values().any(String::is_empty) {
            return Err(GatewayError::InvalidConfig(
                "ClientKey must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self::new([])
    }
}
