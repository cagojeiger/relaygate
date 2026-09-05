use std::{fmt, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_route_table::{
    BindingSet, DestinationId, GatewayId, LeaseId, MappingSnapshot, RegistrationAck,
    RegistrationKey, RegistrationRevision, ShardDirectoryGeneration,
};
use relaygate_transport::{BoxedIo, ClientTlsConfig, insecure_boxed};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{Semaphore, mpsc, oneshot},
    time::Instant,
};
use tokio_util::codec::Framed;

use crate::{
    ErrorCode, GatewayName, InternalGatewayKey, TransportError,
    codec::{CodecError, FrameCodec},
    dto::{
        WireRequest, WireResponse, response_bindings, response_deregistered,
        response_registration_ack,
    },
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame, WireResult},
};

/// Bounds and deadlines for one persistent RouteTable client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTableClientConfig {
    command_queue_capacity: usize,
    max_frame_len: usize,
    connect_timeout: Duration,
    handshake_timeout: Duration,
    request_timeout: Duration,
}

impl RouteTableClientConfig {
    pub fn new(
        command_queue_capacity: usize,
        max_frame_len: usize,
        connect_timeout: Duration,
        handshake_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, TransportError> {
        validate_capacity("client command queue capacity", command_queue_capacity)?;
        validate_frame_len(max_frame_len)?;
        validate_duration("connect timeout", connect_timeout)?;
        validate_duration("handshake timeout", handshake_timeout)?;
        validate_duration("request timeout", request_timeout)?;
        Ok(Self {
            command_queue_capacity,
            max_frame_len,
            connect_timeout,
            handshake_timeout,
            request_timeout,
        })
    }
}

/// Cloneable, bounded handle to one persistent RouteTable TCP connection.
///
/// The connection actor is strictly sequential. It never reconnects, replays,
/// or pipelines requests.
#[derive(Clone)]
pub struct RouteTableClient {
    commands: mpsc::Sender<ClientCommand>,
    request_timeout: Duration,
}

impl fmt::Debug for RouteTableClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteTableClient")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl RouteTableClient {
    pub async fn connect(
        endpoint: impl ToSocketAddrs,
        gateway_name: GatewayName,
        gateway_id: GatewayId,
        internal_gateway_key: InternalGatewayKey,
        config: RouteTableClientConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_transport(
            endpoint,
            gateway_name,
            gateway_id,
            internal_gateway_key,
            config,
            None,
        )
        .await
    }

    pub async fn connect_secure(
        endpoint: impl ToSocketAddrs,
        gateway_name: GatewayName,
        gateway_id: GatewayId,
        internal_gateway_key: InternalGatewayKey,
        config: RouteTableClientConfig,
        tls: ClientTlsConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_transport(
            endpoint,
            gateway_name,
            gateway_id,
            internal_gateway_key,
            config,
            Some(tls),
        )
        .await
    }

    async fn connect_with_transport(
        endpoint: impl ToSocketAddrs,
        gateway_name: GatewayName,
        gateway_id: GatewayId,
        internal_gateway_key: InternalGatewayKey,
        config: RouteTableClientConfig,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self, TransportError> {
        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| TransportError::deadline_exceeded("RouteTable connect timed out"))?
            .map_err(|error| {
                TransportError::unavailable(format!("RouteTable connect failed: {error}"))
            })?;
        let stream = match tls {
            Some(tls) => tokio::time::timeout(config.handshake_timeout, tls.connect_boxed(stream))
                .await
                .map_err(|_| {
                    TransportError::deadline_exceeded("RouteTable TLS handshake timed out")
                })?
                .map_err(|error| {
                    TransportError::unavailable(format!("RouteTable TLS handshake failed: {error}"))
                })?,
            None => insecure_boxed(stream),
        };
        let mut framed = Framed::new(stream, FrameCodec::new(config.max_frame_len));
        let handshake = async {
            framed
                .send(WireFrame::Hello {
                    role: GATEWAY_ROLE.to_owned(),
                    gateway_name: gateway_name.as_str().to_owned(),
                    gateway_id: gateway_id.to_string(),
                    internal_gateway_key: internal_gateway_key.expose_secret().to_owned(),
                })
                .await
                .map_err(map_send_codec_error)?;

            let frame = framed
                .next()
                .await
                .ok_or_else(|| {
                    TransportError::unavailable(
                        "RouteTable connection closed during authentication",
                    )
                })?
                .map_err(map_receive_codec_error)?;
            match frame {
                WireFrame::Welcome { role } if role == ROUTE_TABLE_ROLE => Ok(()),
                WireFrame::HandshakeRejected {
                    role,
                    code,
                    message,
                } if role == ROUTE_TABLE_ROLE => Err(TransportError::new(code, message)),
                WireFrame::Welcome { .. } | WireFrame::HandshakeRejected { .. } => Err(
                    TransportError::protocol("RouteTable handshake response has an invalid role"),
                ),
                _ => Err(TransportError::protocol(
                    "unexpected RouteTable handshake response",
                )),
            }
        };
        tokio::time::timeout(config.handshake_timeout, handshake)
            .await
            .map_err(|_| TransportError::deadline_exceeded("RouteTable handshake timed out"))??;

        let (commands, receiver) = mpsc::channel(config.command_queue_capacity);
        tokio::spawn(run_client_actor(framed, receiver));
        Ok(Self {
            commands,
            request_timeout: config.request_timeout,
        })
    }

    pub async fn register(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
    ) -> Result<RegistrationAck, TransportError> {
        let response = self.request(WireRequest::register(generation, key)).await?;
        response_registration_ack(response, "REGISTER", None, None)
    }

    pub async fn update(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: &MappingSnapshot,
    ) -> Result<RegistrationAck, TransportError> {
        let response = self
            .request(WireRequest::update(
                generation, key, lease_id, revision, snapshot,
            ))
            .await?;
        response_registration_ack(response, "UPDATE", Some(lease_id), Some(revision))
    }

    pub async fn keep_alive(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<RegistrationAck, TransportError> {
        let response = self
            .request(WireRequest::keep_alive(generation, key, lease_id))
            .await?;
        response_registration_ack(response, "KEEP_ALIVE", Some(lease_id), None)
    }

    pub async fn deregister(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<(), TransportError> {
        let response = self
            .request(WireRequest::deregister(generation, key, lease_id))
            .await?;
        response_deregistered(response)
    }

    pub async fn resolve(
        &self,
        generation: ShardDirectoryGeneration,
        destination_id: &DestinationId,
    ) -> Result<BindingSet, TransportError> {
        let response = self
            .request(WireRequest::resolve(generation, destination_id))
            .await?;
        response_bindings(response, destination_id)
    }

    async fn request(&self, request: WireRequest) -> Result<WireResponse, TransportError> {
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or_else(|| TransportError::internal("RouteTable request deadline overflow"))?;
        let (reply, response) = oneshot::channel();
        let command = ClientCommand {
            request,
            deadline,
            reply,
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(TransportError::resource_exhausted(
                    "RouteTable client command queue is full",
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(TransportError::unavailable(
                    "RouteTable client connection is closed",
                ));
            }
        }
        response.await.map_err(|_| {
            TransportError::unavailable("RouteTable client connection actor stopped")
        })?
    }
}

struct ClientCommand {
    request: WireRequest,
    deadline: Instant,
    reply: oneshot::Sender<Result<WireResponse, TransportError>>,
}

async fn run_client_actor(
    mut framed: Framed<BoxedIo, FrameCodec>,
    mut commands: mpsc::Receiver<ClientCommand>,
) {
    let mut next_request_id = 1_u64;

    while let Some(command) = commands.recv().await {
        let request_id = next_request_id;
        let Some(next) = next_request_id.checked_add(1) else {
            let error = TransportError::internal("RouteTable request identifier exhausted");
            let _ = command.reply.send(Err(error));
            break;
        };
        next_request_id = next;

        let result = tokio::time::timeout_at(command.deadline, async {
            framed
                .send(WireFrame::Request {
                    role: GATEWAY_ROLE.to_owned(),
                    request_id,
                    request: command.request,
                })
                .await
                .map_err(map_send_codec_error)?;
            let frame = framed
                .next()
                .await
                .ok_or_else(|| TransportError::unavailable("RouteTable service closed"))?
                .map_err(map_receive_codec_error)?;
            decode_response(frame, request_id)
        })
        .await
        .unwrap_or_else(|_| {
            Err(TransportError::deadline_exceeded(
                "RouteTable request timed out",
            ))
        });

        let connection_terminal = result.as_ref().err().is_some_and(|error| {
            matches!(
                error.code(),
                ErrorCode::Unavailable
                    | ErrorCode::DeadlineExceeded
                    | ErrorCode::ProtocolError
                    | ErrorCode::Internal
            )
        });
        let _ = command.reply.send(result);
        if connection_terminal {
            break;
        }
    }

    let error = TransportError::unavailable(
        "RouteTable client connection closed before the queued request was sent",
    );
    while let Ok(command) = commands.try_recv() {
        let _ = command.reply.send(Err(error.clone()));
    }
}

fn decode_response(
    frame: WireFrame,
    expected_request_id: u64,
) -> Result<WireResponse, TransportError> {
    match frame {
        WireFrame::Response {
            role,
            request_id,
            result,
        } => {
            if role != ROUTE_TABLE_ROLE {
                return Err(TransportError::protocol(
                    "RouteTable response has an invalid role",
                ));
            }
            if request_id != expected_request_id {
                return Err(TransportError::protocol(format!(
                    "RouteTable response request_id mismatch: expected {expected_request_id}, got {request_id}"
                )));
            }
            match result {
                WireResult::Ok { response } => Ok(response),
                WireResult::Error { code, message } => Err(TransportError::new(code, message)),
            }
        }
        WireFrame::ProtocolFault {
            role,
            code,
            message,
        } if role == ROUTE_TABLE_ROLE && code == ErrorCode::ProtocolError => {
            Err(TransportError::new(code, message))
        }
        WireFrame::ProtocolFault { .. } => Err(TransportError::protocol(
            "RouteTable protocol fault has an invalid role or code",
        )),
        _ => Err(TransportError::protocol(
            "unexpected frame from RouteTable service",
        )),
    }
}

fn map_send_codec_error(error: CodecError) -> TransportError {
    match error {
        CodecError::FrameTooLarge { .. } | CodecError::LengthOverflow => {
            TransportError::resource_exhausted(error.to_string())
        }
        _ if error.is_io() => TransportError::unavailable(error.to_string()),
        _ => TransportError::protocol(error.to_string()),
    }
}

fn map_receive_codec_error(error: CodecError) -> TransportError {
    if error.is_io() {
        TransportError::unavailable(error.to_string())
    } else {
        TransportError::protocol(error.to_string())
    }
}

fn validate_nonzero(name: &'static str, value: usize) -> Result<(), TransportError> {
    if value == 0 {
        Err(TransportError::invalid_argument(format!(
            "{name} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

fn validate_capacity(name: &'static str, value: usize) -> Result<(), TransportError> {
    validate_nonzero(name, value)?;
    if value > Semaphore::MAX_PERMITS {
        return Err(TransportError::invalid_argument(format!(
            "{name} exceeds the runtime limit"
        )));
    }
    Ok(())
}

fn validate_frame_len(value: usize) -> Result<(), TransportError> {
    validate_nonzero("maximum frame length", value)?;
    if value > u32::MAX as usize {
        return Err(TransportError::invalid_argument(
            "maximum frame length exceeds the wire limit",
        ));
    }
    Ok(())
}

fn validate_duration(name: &'static str, value: Duration) -> Result<(), TransportError> {
    if value.is_zero() {
        Err(TransportError::invalid_argument(format!(
            "{name} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_zero_bounds_and_deadlines() {
        let second = Duration::from_secs(1);
        assert!(RouteTableClientConfig::new(0, 1024, second, second, second).is_err());
        assert!(RouteTableClientConfig::new(1, 0, second, second, second).is_err());
        assert!(RouteTableClientConfig::new(1, 1024, Duration::ZERO, second, second).is_err());
        assert!(RouteTableClientConfig::new(1, 1024, second, Duration::ZERO, second).is_err());
        assert!(RouteTableClientConfig::new(1, 1024, second, second, Duration::ZERO).is_err());
        assert!(RouteTableClientConfig::new(usize::MAX, 1024, second, second, second).is_err());
    }
}
