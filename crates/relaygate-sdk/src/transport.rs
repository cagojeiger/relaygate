use std::{fmt, time::Duration};

#[cfg(any(test, feature = "insecure-test-transport"))]
use relaygate_transport::insecure_boxed;
use relaygate_transport::{BoxedIo, ClientTlsConfig};
use tokio::{net::TcpStream, time::timeout};

use crate::{Error, ErrorCode, PeerObservation, Result};

/// Connection settings for the SDK-facing Gateway transport.
///
/// Relay's public `listen`, `dial`, `accept`, and `Pipe` API is independent of
/// this choice. RelayGate 0.2 provides TLS over TCP; future transports can be
/// added through constructors without changing the Relay API.
#[derive(Clone)]
pub struct GatewayTransportConfig {
    kind: GatewayTransport,
}

#[derive(Clone)]
enum GatewayTransport {
    TlsTcp {
        gateway_addr: String,
        tls: ClientTlsConfig,
    },
    #[cfg(any(test, feature = "insecure-test-transport"))]
    InsecureTcp { gateway_addr: String },
}

impl GatewayTransportConfig {
    /// Uses RelayGate framing over a server-authenticated TLS/TCP connection.
    #[must_use]
    pub fn tls_tcp(gateway_addr: impl Into<String>, tls: ClientTlsConfig) -> Self {
        Self {
            kind: GatewayTransport::TlsTcp {
                gateway_addr: gateway_addr.into(),
                tls,
            },
        }
    }

    #[cfg(any(test, feature = "insecure-test-transport"))]
    pub(crate) fn insecure_tcp(gateway_addr: impl Into<String>) -> Self {
        Self {
            kind: GatewayTransport::InsecureTcp {
                gateway_addr: gateway_addr.into(),
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.gateway_addr().trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "Gateway address must not be empty",
            ));
        }
        Ok(())
    }

    pub(crate) async fn connect(&self, connect_timeout: Duration) -> Result<BoxedIo> {
        let stream = timeout(connect_timeout, TcpStream::connect(self.gateway_addr()))
            .await
            .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
            .map_err(|error| Error::unavailable(format!("Gateway connection failed: {error}")))?;
        let _ = stream.set_nodelay(true);

        timeout(connect_timeout, async {
            match &self.kind {
                GatewayTransport::TlsTcp { tls, .. } => tls.connect_boxed(stream).await,
                #[cfg(any(test, feature = "insecure-test-transport"))]
                GatewayTransport::InsecureTcp { .. } => Ok(insecure_boxed(stream)),
            }
        })
        .await
        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?
        .map_err(|error| Error::unavailable(format!("Gateway TLS handshake failed: {error}")))
    }

    fn gateway_addr(&self) -> &str {
        match &self.kind {
            GatewayTransport::TlsTcp { gateway_addr, .. } => gateway_addr,
            #[cfg(any(test, feature = "insecure-test-transport"))]
            GatewayTransport::InsecureTcp { gateway_addr } => gateway_addr,
        }
    }
}

impl fmt::Debug for GatewayTransportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            GatewayTransport::TlsTcp { gateway_addr, .. } => formatter
                .debug_struct("TlsTcp")
                .field("gateway_addr", gateway_addr)
                .finish(),
            #[cfg(any(test, feature = "insecure-test-transport"))]
            GatewayTransport::InsecureTcp { gateway_addr } => formatter
                .debug_struct("InsecureTcp")
                .field("gateway_addr", gateway_addr)
                .finish(),
        }
    }
}
