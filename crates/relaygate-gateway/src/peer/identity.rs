use std::fmt;

use relaygate_protocol::{PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use uuid::Uuid;

use super::error::PeerError;

/// Stable configuration name presented by one Gateway during peer handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerGatewayName(String);

impl PeerGatewayName {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, PeerError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PeerError::InvalidArgument(
                "peer Gateway name must not be empty",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Local/CI credential carried only during peer handshake.
///
/// The cleartext value is deliberately absent from `Debug`. It must not be
/// retained in stream state after the handshake completes.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PeerGatewayKey(String);

impl PeerGatewayKey {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, PeerError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PeerError::InvalidArgument(
                "peer internal Gateway key must not be empty",
            ));
        }
        Ok(Self(value))
    }

    pub(super) fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PeerGatewayKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PeerGatewayKey([REDACTED])")
    }
}

/// Identity claims exchanged by both sides of a peer handshake.
///
/// Carrying the same tuple in `HELLO` and `WELCOME` lets each endpoint verify
/// the configured peer name/key, the expected runtime incarnation, and the
/// direction-specific transport identity before admitting any stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerHandshake {
    pub(crate) gateway_name: PeerGatewayName,
    pub(crate) internal_gateway_key: PeerGatewayKey,
    pub(crate) gateway_id: GatewayId,
    pub(crate) expected_peer_gateway_id: GatewayId,
    pub(crate) dialer_gateway_id: GatewayId,
    pub(crate) peer_transport_id: PeerTransportId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PeerTransportId(Uuid);

impl PeerTransportId {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub(crate) const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StreamId(u64);

impl StreamId {
    #[must_use]
    pub(crate) const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    pub(super) const fn initiator_bit(self) -> u64 {
        self.0 & 1
    }

    pub(super) const fn counter(self) -> u64 {
        self.0 >> 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEndpoint {
    Dialer,
    Acceptor,
}

impl StreamEndpoint {
    pub(super) const fn bit(self) -> u64 {
        match self {
            Self::Dialer => 0,
            Self::Acceptor => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamIdAllocator {
    endpoint: StreamEndpoint,
    next_counter: u64,
}

impl StreamIdAllocator {
    #[must_use]
    pub(crate) const fn new(endpoint: StreamEndpoint) -> Self {
        Self {
            endpoint,
            next_counter: 0,
        }
    }

    #[cfg(test)]
    const fn with_next_counter(endpoint: StreamEndpoint, next_counter: u64) -> Self {
        Self {
            endpoint,
            next_counter,
        }
    }

    pub(crate) fn allocate(&mut self) -> Result<StreamId, PeerError> {
        if self.next_counter > i64::MAX as u64 {
            return Err(PeerError::ResourceExhausted(
                "PeerTransport StreamId counter is exhausted",
            ));
        }
        let stream_id = StreamId((self.next_counter << 1) | self.endpoint.bit());
        self.next_counter += 1;
        Ok(stream_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RemoteStreamGuard {
    remote_endpoint: StreamEndpoint,
    highest_counter: Option<u64>,
}

impl RemoteStreamGuard {
    #[must_use]
    pub(crate) const fn new(remote_endpoint: StreamEndpoint) -> Self {
        Self {
            remote_endpoint,
            highest_counter: None,
        }
    }

    pub(crate) fn accept_open(&mut self, stream_id: StreamId) -> Result<(), PeerError> {
        if stream_id.initiator_bit() != self.remote_endpoint.bit() {
            return Err(PeerError::Protocol("remote StreamId role bit is invalid"));
        }
        let counter = stream_id.counter();
        if self
            .highest_counter
            .is_some_and(|highest| counter <= highest)
        {
            return Err(PeerError::Protocol(
                "remote StreamId counter is not strictly increasing",
            ));
        }
        self.highest_counter = Some(counter);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OpenIdentity {
    entry_gateway: GatewayId,
    connector_session: SessionId,
    connection_id: u64,
}

impl OpenIdentity {
    #[must_use]
    pub(crate) const fn new(
        entry_gateway: GatewayId,
        connector_session: SessionId,
        connection_id: u64,
    ) -> Self {
        Self {
            entry_gateway,
            connector_session,
            connection_id,
        }
    }

    #[must_use]
    pub(crate) const fn entry_gateway(self) -> GatewayId {
        self.entry_gateway
    }

    #[must_use]
    pub(crate) const fn connector_session(self) -> SessionId {
        self.connector_session
    }

    #[must_use]
    pub(crate) const fn connection_id(self) -> u64 {
        self.connection_id
    }
}

/// Stream state uses the same active correlation identity as peer `OPEN`.
pub(crate) type StreamOwner = OpenIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerOpenProgress {
    #[cfg(test)]
    BeforeOpenCommit,
    AfterOpenCommit,
    Opened,
}

impl PeerOpenProgress {
    #[must_use]
    pub(crate) const fn failure_observation(self) -> PeerObservation {
        match self {
            #[cfg(test)]
            Self::BeforeOpenCommit => PeerObservation::NotObserved,
            Self::AfterOpenCommit | Self::Opened => PeerObservation::MaybeObserved,
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn allocator_exhausts_without_wrapping_or_reusing() -> Result<(), PeerError> {
        let mut allocator =
            StreamIdAllocator::with_next_counter(StreamEndpoint::Acceptor, i64::MAX as u64);

        assert_eq!(allocator.allocate()?.raw(), u64::MAX);
        assert!(matches!(
            allocator.allocate(),
            Err(PeerError::ResourceExhausted(_))
        ));
        assert!(matches!(
            allocator.allocate(),
            Err(PeerError::ResourceExhausted(_))
        ));
        Ok(())
    }
}
