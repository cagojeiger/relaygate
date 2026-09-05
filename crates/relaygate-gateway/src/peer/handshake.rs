use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::ErrorCode;
use relaygate_route_table::GatewayId;
use relaygate_transport::{BoxedIo, insecure_boxed};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::{
    auth::TrustedPeers,
    codec::PeerFrameCodec,
    config::GatewayPeerConfig,
    event::{PeerFailure, PeerTarget},
    frame::PeerFrame,
    identity::{PeerHandshake, PeerTransportId, StreamEndpoint},
};

pub(super) type PeerFramed = Framed<BoxedIo, PeerFrameCodec>;

pub(super) struct EstablishedPeer {
    pub(super) framed: PeerFramed,
    pub(super) remote_gateway_id: GatewayId,
    pub(super) peer_transport_id: PeerTransportId,
    pub(super) local_endpoint: StreamEndpoint,
}

pub(super) struct InboundHello {
    pub(super) framed: PeerFramed,
    pub(super) remote_gateway_id: GatewayId,
    pub(super) peer_transport_id: PeerTransportId,
}

pub(super) async fn dial_and_handshake(
    config: GatewayPeerConfig,
    trusted: TrustedPeers,
    local_gateway_id: GatewayId,
    target: PeerTarget,
    peer_transport_id: PeerTransportId,
) -> Result<EstablishedPeer, PeerFailure> {
    let stream = tokio::time::timeout(config.connect_timeout, async {
        #[cfg(test)]
        if let Some(gate) = &config.connect_gate {
            gate.wait().await;
        }
        TcpStream::connect(target.gateway_locator().as_str()).await
    })
    .await
    .map_err(|_| {
        PeerFailure::not_observed(
            ErrorCode::DeadlineExceeded,
            "peer transport connect timed out",
        )
    })?
    .map_err(|_| {
        PeerFailure::not_observed(ErrorCode::Unavailable, "peer transport connect failed")
    })?;
    stream.set_nodelay(true).map_err(|_| {
        PeerFailure::not_observed(
            ErrorCode::Unavailable,
            "peer transport TCP_NODELAY setup failed",
        )
    })?;
    let stream = match &config.client_tls {
        Some(tls) => tokio::time::timeout(config.handshake_timeout, tls.connect_boxed(stream))
            .await
            .map_err(|_| {
                PeerFailure::not_observed(
                    ErrorCode::DeadlineExceeded,
                    "peer TLS handshake timed out",
                )
            })?
            .map_err(|_| {
                PeerFailure::not_observed(
                    ErrorCode::Unauthenticated,
                    "peer TLS server authentication failed",
                )
            })?,
        None => insecure_boxed(stream),
    };
    let mut framed = PeerFramed::new(stream, PeerFrameCodec::new(config.max_frame_len));
    let hello = PeerHandshake {
        gateway_name: config.local_gateway_name.clone(),
        internal_gateway_key: config.local_gateway_key.clone(),
        gateway_id: local_gateway_id,
        expected_peer_gateway_id: target.gateway_id(),
        dialer_gateway_id: local_gateway_id,
        peer_transport_id,
    };

    let response = tokio::time::timeout(config.handshake_timeout, async {
        framed.send(PeerFrame::Hello(hello)).await.map_err(|_| {
            PeerFailure::not_observed(ErrorCode::Unavailable, "peer HELLO could not be sent")
        })?;
        framed
            .next()
            .await
            .ok_or_else(|| {
                PeerFailure::not_observed(
                    ErrorCode::Unavailable,
                    "peer transport closed during handshake",
                )
            })?
            .map_err(|_| {
                PeerFailure::not_observed(ErrorCode::ProtocolError, "invalid peer handshake frame")
            })
    })
    .await
    .map_err(|_| {
        PeerFailure::not_observed(
            ErrorCode::DeadlineExceeded,
            "peer transport handshake timed out",
        )
    })??;

    match response {
        PeerFrame::Welcome(welcome) => {
            if !trusted.authenticate(&welcome.gateway_name, &welcome.internal_gateway_key) {
                return Err(PeerFailure::not_observed(
                    ErrorCode::Unauthenticated,
                    "peer Gateway credential was rejected",
                ));
            }
            if welcome.gateway_id != target.gateway_id()
                || welcome.expected_peer_gateway_id != local_gateway_id
                || welcome.dialer_gateway_id != local_gateway_id
                || welcome.peer_transport_id != peer_transport_id
            {
                return Err(PeerFailure::not_observed(
                    ErrorCode::PermissionDenied,
                    "authenticated peer handshake claims do not match the selected pair",
                ));
            }
        }
        PeerFrame::HandshakeRejected { code, message } => {
            return Err(PeerFailure::not_observed(code, message));
        }
        _ => {
            return Err(PeerFailure::not_observed(
                ErrorCode::ProtocolError,
                "unexpected peer handshake response",
            ));
        }
    }

    Ok(EstablishedPeer {
        framed,
        remote_gateway_id: target.gateway_id(),
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
    })
}

pub(super) async fn receive_inbound_hello(
    stream: TcpStream,
    config: GatewayPeerConfig,
    trusted: TrustedPeers,
    local_gateway_id: GatewayId,
) -> Result<InboundHello, PeerFailure> {
    stream.set_nodelay(true).map_err(|_| {
        PeerFailure::not_observed(
            ErrorCode::Unavailable,
            "inbound peer TCP_NODELAY setup failed",
        )
    })?;
    let stream = match &config.server_tls {
        Some(tls) => tokio::time::timeout(config.handshake_timeout, tls.accept_boxed(stream))
            .await
            .map_err(|_| {
                PeerFailure::not_observed(
                    ErrorCode::DeadlineExceeded,
                    "inbound peer TLS handshake timed out",
                )
            })?
            .map_err(|_| {
                PeerFailure::not_observed(
                    ErrorCode::Unauthenticated,
                    "inbound peer mTLS authentication failed",
                )
            })?,
        None => insecure_boxed(stream),
    };
    let mut framed = PeerFramed::new(stream, PeerFrameCodec::new(config.max_frame_len));
    let frame = tokio::time::timeout(config.handshake_timeout, framed.next())
        .await
        .map_err(|_| {
            PeerFailure::not_observed(
                ErrorCode::DeadlineExceeded,
                "inbound peer handshake timed out",
            )
        })?
        .ok_or_else(|| {
            PeerFailure::not_observed(ErrorCode::Unavailable, "inbound peer closed before HELLO")
        })?
        .map_err(|_| {
            PeerFailure::not_observed(ErrorCode::ProtocolError, "invalid inbound peer HELLO")
        })?;

    let PeerFrame::Hello(hello) = frame else {
        reject_handshake(
            &mut framed,
            ErrorCode::ProtocolError,
            "expected peer HELLO",
            config.handshake_timeout,
        )
        .await;
        return Err(PeerFailure::not_observed(
            ErrorCode::ProtocolError,
            "expected peer HELLO",
        ));
    };

    if !trusted.authenticate(&hello.gateway_name, &hello.internal_gateway_key) {
        reject_handshake(
            &mut framed,
            ErrorCode::Unauthenticated,
            "peer Gateway credential was rejected",
            config.handshake_timeout,
        )
        .await;
        return Err(PeerFailure::not_observed(
            ErrorCode::Unauthenticated,
            "peer Gateway credential was rejected",
        ));
    }
    if hello.gateway_id == local_gateway_id
        || hello.expected_peer_gateway_id != local_gateway_id
        || hello.dialer_gateway_id != hello.gateway_id
    {
        reject_handshake(
            &mut framed,
            ErrorCode::PermissionDenied,
            "authenticated peer handshake claims do not match the selected pair",
            config.handshake_timeout,
        )
        .await;
        return Err(PeerFailure::not_observed(
            ErrorCode::PermissionDenied,
            "authenticated peer handshake claims do not match the selected pair",
        ));
    }

    Ok(InboundHello {
        framed,
        remote_gateway_id: hello.gateway_id,
        peer_transport_id: hello.peer_transport_id,
    })
}

pub(super) async fn complete_inbound_handshake(
    mut hello: InboundHello,
    config: GatewayPeerConfig,
    local_gateway_id: GatewayId,
) -> Result<EstablishedPeer, PeerFailure> {
    let welcome = PeerHandshake {
        gateway_name: config.local_gateway_name,
        internal_gateway_key: config.local_gateway_key,
        gateway_id: local_gateway_id,
        expected_peer_gateway_id: hello.remote_gateway_id,
        dialer_gateway_id: hello.remote_gateway_id,
        peer_transport_id: hello.peer_transport_id,
    };
    tokio::time::timeout(
        config.handshake_timeout,
        hello.framed.send(PeerFrame::Welcome(welcome)),
    )
    .await
    .map_err(|_| {
        PeerFailure::not_observed(
            ErrorCode::DeadlineExceeded,
            "inbound peer WELCOME timed out",
        )
    })?
    .map_err(|_| {
        PeerFailure::not_observed(ErrorCode::Unavailable, "inbound peer WELCOME failed")
    })?;

    Ok(EstablishedPeer {
        framed: hello.framed,
        remote_gateway_id: hello.remote_gateway_id,
        peer_transport_id: hello.peer_transport_id,
        local_endpoint: StreamEndpoint::Acceptor,
    })
}

pub(super) async fn reject_duplicate(mut hello: InboundHello, timeout: std::time::Duration) {
    reject_handshake(
        &mut hello.framed,
        ErrorCode::AlreadyExists,
        "same-direction PeerTransport candidate already exists",
        timeout,
    )
    .await;
}

async fn reject_handshake(
    framed: &mut PeerFramed,
    code: ErrorCode,
    message: &str,
    timeout: std::time::Duration,
) {
    let _ = tokio::time::timeout(
        timeout,
        framed.send(PeerFrame::HandshakeRejected {
            code,
            message: message.to_owned(),
        }),
    )
    .await;
}
