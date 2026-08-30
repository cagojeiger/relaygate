use std::time::Duration;

use tokio::sync::Semaphore;

use crate::GatewayError;

use super::{
    error::PeerError,
    identity::{PeerGatewayKey, PeerGatewayName},
};

const DEFAULT_MANAGER_QUEUE_CAPACITY: usize = 256;
const DEFAULT_EVENT_QUEUE_CAPACITY: usize = 256;
const DEFAULT_TRANSPORT_QUEUE_CAPACITY: usize = 256;
const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 256;
const DEFAULT_STREAM_QUEUE_CAPACITY: usize = 32;
const DEFAULT_MAX_STREAMS_PER_TRANSPORT: usize = 1_024;
const DEFAULT_MAX_PENDING_OPENS: usize = 1_024;
const DEFAULT_MAX_HANDSHAKES: usize = 128;
const DEFAULT_MAX_FRAME_LEN: usize = 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_OPEN_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// One trusted stable peer entry for the local/CI plain-TCP adapter.
#[derive(Clone)]
pub struct TrustedPeerConfig {
    pub(super) gateway_name: PeerGatewayName,
    pub(super) internal_gateway_key: PeerGatewayKey,
}

impl TrustedPeerConfig {
    pub fn new(
        gateway_name: impl Into<String>,
        internal_gateway_key: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            gateway_name: PeerGatewayName::new(gateway_name).map_err(config_error)?,
            internal_gateway_key: PeerGatewayKey::new(internal_gateway_key)
                .map_err(config_error)?,
        })
    }
}

impl std::fmt::Debug for TrustedPeerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TrustedPeerConfig")
            .field("gateway_name", &self.gateway_name)
            .field("internal_gateway_key", &self.internal_gateway_key)
            .finish()
    }
}

/// Immutable bounds, deadlines, local identity, and trusted allowlist for one
/// Gateway peer runtime incarnation.
#[derive(Clone)]
pub struct GatewayPeerConfig {
    pub(super) local_gateway_name: PeerGatewayName,
    pub(super) local_gateway_key: PeerGatewayKey,
    pub(super) trusted_peers: Vec<TrustedPeerConfig>,
    pub(super) manager_queue_capacity: usize,
    pub(super) event_queue_capacity: usize,
    pub(super) transport_queue_capacity: usize,
    pub(super) writer_queue_capacity: usize,
    pub(super) stream_queue_capacity: usize,
    pub(super) max_streams_per_transport: usize,
    pub(super) max_pending_opens: usize,
    pub(super) max_handshakes: usize,
    pub(super) max_frame_len: usize,
    pub(super) connect_timeout: Duration,
    pub(super) handshake_timeout: Duration,
    pub(super) open_response_timeout: Duration,
}

impl GatewayPeerConfig {
    pub fn new(
        local_gateway_name: impl Into<String>,
        local_gateway_key: impl Into<String>,
        trusted_peers: impl IntoIterator<Item = TrustedPeerConfig>,
    ) -> Result<Self, GatewayError> {
        let config = Self {
            local_gateway_name: PeerGatewayName::new(local_gateway_name).map_err(config_error)?,
            local_gateway_key: PeerGatewayKey::new(local_gateway_key).map_err(config_error)?,
            trusted_peers: trusted_peers.into_iter().collect(),
            manager_queue_capacity: DEFAULT_MANAGER_QUEUE_CAPACITY,
            event_queue_capacity: DEFAULT_EVENT_QUEUE_CAPACITY,
            transport_queue_capacity: DEFAULT_TRANSPORT_QUEUE_CAPACITY,
            writer_queue_capacity: DEFAULT_WRITER_QUEUE_CAPACITY,
            stream_queue_capacity: DEFAULT_STREAM_QUEUE_CAPACITY,
            max_streams_per_transport: DEFAULT_MAX_STREAMS_PER_TRANSPORT,
            max_pending_opens: DEFAULT_MAX_PENDING_OPENS,
            max_handshakes: DEFAULT_MAX_HANDSHAKES,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            open_response_timeout: DEFAULT_OPEN_RESPONSE_TIMEOUT,
        };
        config.validate().map_err(config_error)?;
        Ok(config)
    }

    #[must_use]
    pub const fn with_queue_bounds(
        mut self,
        manager: usize,
        events: usize,
        transport: usize,
        writer: usize,
        per_stream: usize,
    ) -> Self {
        self.manager_queue_capacity = manager;
        self.event_queue_capacity = events;
        self.transport_queue_capacity = transport;
        self.writer_queue_capacity = writer;
        self.stream_queue_capacity = per_stream;
        self
    }

    #[must_use]
    pub const fn with_resource_limits(
        mut self,
        max_streams_per_transport: usize,
        max_pending_opens: usize,
        max_handshakes: usize,
        max_frame_len: usize,
    ) -> Self {
        self.max_streams_per_transport = max_streams_per_transport;
        self.max_pending_opens = max_pending_opens;
        self.max_handshakes = max_handshakes;
        self.max_frame_len = max_frame_len;
        self
    }

    #[must_use]
    pub const fn with_timeouts(
        mut self,
        connect: Duration,
        handshake: Duration,
        open_response: Duration,
    ) -> Self {
        self.connect_timeout = connect;
        self.handshake_timeout = handshake;
        self.open_response_timeout = open_response;
        self
    }

    pub(super) fn validate(&self) -> Result<(), PeerError> {
        for capacity in [
            self.manager_queue_capacity,
            self.event_queue_capacity,
            self.transport_queue_capacity,
            self.writer_queue_capacity,
            self.stream_queue_capacity,
            self.max_streams_per_transport,
            self.max_pending_opens,
            self.max_handshakes,
            self.max_frame_len,
        ] {
            if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
                return Err(PeerError::InvalidArgument(
                    "peer runtime capacities must be within the Tokio runtime limit",
                ));
            }
        }
        if self.connect_timeout.is_zero()
            || self.handshake_timeout.is_zero()
            || self.open_response_timeout.is_zero()
        {
            return Err(PeerError::InvalidArgument(
                "peer runtime timeouts must be greater than zero",
            ));
        }

        for (index, peer) in self.trusted_peers.iter().enumerate() {
            if peer.gateway_name == self.local_gateway_name {
                return Err(PeerError::InvalidArgument(
                    "trusted peer Gateway name must differ from the local Gateway name",
                ));
            }
            if self.trusted_peers[..index]
                .iter()
                .any(|candidate| candidate.gateway_name == peer.gateway_name)
            {
                return Err(PeerError::InvalidArgument(
                    "trusted peer Gateway names must be unique",
                ));
            }
        }
        Ok(())
    }
}

fn config_error(error: PeerError) -> GatewayError {
    GatewayError::InvalidConfig(error.to_string())
}

impl std::fmt::Debug for GatewayPeerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayPeerConfig")
            .field("local_gateway_name", &self.local_gateway_name)
            .field("local_gateway_key", &self.local_gateway_key)
            .field("trusted_peers", &self.trusted_peers)
            .field("manager_queue_capacity", &self.manager_queue_capacity)
            .field("event_queue_capacity", &self.event_queue_capacity)
            .field("transport_queue_capacity", &self.transport_queue_capacity)
            .field("writer_queue_capacity", &self.writer_queue_capacity)
            .field("stream_queue_capacity", &self.stream_queue_capacity)
            .field("max_streams_per_transport", &self.max_streams_per_transport)
            .field("max_pending_opens", &self.max_pending_opens)
            .field("max_handshakes", &self.max_handshakes)
            .field("max_frame_len", &self.max_frame_len)
            .field("connect_timeout", &self.connect_timeout)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("open_response_timeout", &self.open_response_timeout)
            .finish()
    }
}
