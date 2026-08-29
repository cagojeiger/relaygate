use std::time::Duration;

use relaygate_protocol::DEFAULT_MAX_FRAME_LEN;

use crate::{Error, ErrorCode, PeerObservation, Result};

/// Runtime limits and reconnect policy shared by Connector and Listener roles.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) gateway_addr: String,
    pub(crate) connect_timeout: Duration,
    pub(crate) operation_timeout: Duration,
    pub(crate) reconnect_initial: Duration,
    pub(crate) reconnect_maximum: Duration,
    pub(crate) offer_timeout: Duration,
    pub(crate) outbound_capacity: usize,
    pub(crate) listener_queue_capacity: usize,
    pub(crate) pipe_inbound_capacity: usize,
    pub(crate) max_frame_len: usize,
}

impl Config {
    /// Creates a configuration for a Gateway TCP address such as
    /// `127.0.0.1:27420`.
    #[must_use]
    pub fn new(gateway_addr: impl Into<String>) -> Self {
        Self {
            gateway_addr: gateway_addr.into(),
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            reconnect_initial: Duration::from_millis(100),
            reconnect_maximum: Duration::from_secs(5),
            offer_timeout: Duration::from_millis(250),
            outbound_capacity: 256,
            listener_queue_capacity: 64,
            pipe_inbound_capacity: 64,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        }
    }

    #[must_use]
    pub const fn with_connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    #[must_use]
    pub const fn with_operation_timeout(mut self, value: Duration) -> Self {
        self.operation_timeout = value;
        self
    }

    #[must_use]
    pub const fn with_reconnect_backoff(mut self, initial: Duration, maximum: Duration) -> Self {
        self.reconnect_initial = initial;
        self.reconnect_maximum = maximum;
        self
    }

    #[must_use]
    pub const fn with_offer_timeout(mut self, value: Duration) -> Self {
        self.offer_timeout = value;
        self
    }

    #[must_use]
    pub const fn with_outbound_capacity(mut self, value: usize) -> Self {
        self.outbound_capacity = value;
        self
    }

    #[must_use]
    pub const fn with_listener_queue_capacity(mut self, value: usize) -> Self {
        self.listener_queue_capacity = value;
        self
    }

    #[must_use]
    pub const fn with_pipe_inbound_capacity(mut self, value: usize) -> Self {
        self.pipe_inbound_capacity = value;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.gateway_addr.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "gateway address must not be empty",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.offer_timeout.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_maximum < self.reconnect_initial
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "timeouts and reconnect backoff must be positive and ordered",
            ));
        }
        if self.outbound_capacity == 0
            || self.listener_queue_capacity == 0
            || self.pipe_inbound_capacity == 0
            || self.max_frame_len < 1024
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "queue capacities must be positive and max_frame_len must be at least 1024",
            ));
        }
        Ok(())
    }
}
