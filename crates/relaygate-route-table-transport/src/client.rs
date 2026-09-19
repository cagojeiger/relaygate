use std::{fmt, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_route_table::{
    BindingSet, BindingSnapshot, Destination, GatewayId, LeaseId, RegistrationAck, RegistrationKey,
    RegistrationRevision, ShardDirectoryGeneration,
};
use relaygate_transport::{ClientTlsConfig, insecure_boxed};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::codec::Framed;

use crate::{
    GatewayName, TransportError,
    bounds::{validate_capacity, validate_duration, validate_frame_len},
    codec::{FrameCodec, map_receive_codec_error, map_send_codec_error},
    dto::{
        RegistrationRequest, WireRequest, WireResponse, response_bindings, response_deregistered,
        response_registration_ack,
    },
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame},
};

use self::actor::{ClientCommand, run_client_actor};

mod actor;

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
        config: RouteTableClientConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_transport(endpoint, gateway_name, gateway_id, config, None).await
    }

    pub async fn connect_secure(
        endpoint: impl ToSocketAddrs,
        gateway_name: GatewayName,
        gateway_id: GatewayId,
        config: RouteTableClientConfig,
        tls: ClientTlsConfig,
    ) -> Result<Self, TransportError> {
        Self::connect_with_transport(endpoint, gateway_name, gateway_id, config, Some(tls)).await
    }

    async fn connect_with_transport(
        endpoint: impl ToSocketAddrs,
        gateway_name: GatewayName,
        gateway_id: GatewayId,
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
        let started_at = Instant::now();
        let result = async {
            let response = self.request(WireRequest::register(generation, key)).await?;
            response_registration_ack(response, RegistrationRequest::Register, None, None)
        }
        .await;
        observe_request("register", started_at, &result);
        result
    }

    pub async fn update(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
        revision: RegistrationRevision,
        snapshot: &BindingSnapshot,
    ) -> Result<RegistrationAck, TransportError> {
        let started_at = Instant::now();
        let result = async {
            let response = self
                .request(WireRequest::update(
                    generation, key, lease_id, revision, snapshot,
                ))
                .await?;
            response_registration_ack(
                response,
                RegistrationRequest::Update,
                Some(lease_id),
                Some(revision),
            )
        }
        .await;
        observe_request("update", started_at, &result);
        result
    }

    pub async fn keep_alive(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<RegistrationAck, TransportError> {
        let started_at = Instant::now();
        let result = async {
            let response = self
                .request(WireRequest::keep_alive(generation, key, lease_id))
                .await?;
            response_registration_ack(
                response,
                RegistrationRequest::KeepAlive,
                Some(lease_id),
                None,
            )
        }
        .await;
        observe_request("keep_alive", started_at, &result);
        result
    }

    pub async fn deregister(
        &self,
        generation: ShardDirectoryGeneration,
        key: &RegistrationKey,
        lease_id: LeaseId,
    ) -> Result<(), TransportError> {
        let started_at = Instant::now();
        let result = async {
            let response = self
                .request(WireRequest::deregister(generation, key, lease_id))
                .await?;
            response_deregistered(response)
        }
        .await;
        observe_request("deregister", started_at, &result);
        result
    }

    pub async fn resolve(
        &self,
        generation: ShardDirectoryGeneration,
        destination: &Destination,
    ) -> Result<BindingSet, TransportError> {
        let started_at = Instant::now();
        let result = async {
            let response = self
                .request(WireRequest::resolve(generation, destination))
                .await?;
            response_bindings(response, destination)
        }
        .await;
        observe_request("resolve", started_at, &result);
        result
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

fn observe_request<T>(
    operation: &'static str,
    started_at: Instant,
    result: &Result<T, TransportError>,
) {
    let (outcome, code) = match result {
        Ok(_) => ("success", "ok"),
        Err(error) => ("error", error.code().metric_name()),
    };
    metrics::counter!(
        "relaygate_gateway_route_table_requests_total",
        "operation" => operation,
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
    metrics::histogram!(
        "relaygate_gateway_route_table_request_duration_seconds",
        "operation" => operation,
        "outcome" => outcome
    )
    .record(started_at.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use metrics_util::{CompositeKey, debugging::DebugValue, debugging::DebuggingRecorder};

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

    #[test]
    fn client_request_metrics_separate_operation_outcome_and_code() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            observe_request("resolve", Instant::now(), &Ok(()));
            observe_request(
                "resolve",
                Instant::now(),
                &Err::<(), _>(TransportError::unavailable("test failure")),
            );
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_table_requests_total"
                && has_label(key, "operation", "resolve")
                && has_label(key, "outcome", "success")
                && has_label(key, "code", "ok")
                && matches!(value, DebugValue::Counter(1))
        }));
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_table_requests_total"
                && has_label(key, "operation", "resolve")
                && has_label(key, "outcome", "error")
                && has_label(key, "code", "unavailable")
                && matches!(value, DebugValue::Counter(1))
        }));
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_table_request_duration_seconds"
                && has_label(key, "operation", "resolve")
                && has_label(key, "outcome", "success")
                && matches!(value, DebugValue::Histogram(values) if values.len() == 1)
        }));
    }

    fn has_label(key: &CompositeKey, expected_key: &str, expected_value: &str) -> bool {
        key.key()
            .labels()
            .any(|label| label.key() == expected_key && label.value() == expected_value)
    }
}
