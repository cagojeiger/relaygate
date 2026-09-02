use std::{fmt, future::pending, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_route_table::{AuthenticatedGatewayId, GatewayId, RequestContext, RouteTableShard};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_util::{codec::Framed, sync::CancellationToken};
use uuid::Uuid;

use crate::{
    ErrorCode, GatewayName, InternalGatewayKey, TransportError, TrustedGatewayKeys,
    codec::{CodecError, FrameCodec},
    dto::{DomainRequest, WireRequest, WireResponse},
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame, WireResult},
};

/// Bounds and handshake deadline for one RouteTable service runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteTableServiceConfig {
    request_queue_capacity: usize,
    writer_queue_capacity: usize,
    max_connections: usize,
    max_frame_len: usize,
    handshake_timeout: Duration,
}

impl RouteTableServiceConfig {
    pub fn new(
        request_queue_capacity: usize,
        writer_queue_capacity: usize,
        max_connections: usize,
        max_frame_len: usize,
        handshake_timeout: Duration,
    ) -> Result<Self, TransportError> {
        validate_capacity("service request queue capacity", request_queue_capacity)?;
        validate_capacity("connection writer queue capacity", writer_queue_capacity)?;
        validate_capacity("maximum connection count", max_connections)?;
        validate_service_frame_len(max_frame_len)?;
        validate_duration("handshake timeout", handshake_timeout)?;
        Ok(Self {
            request_queue_capacity,
            writer_queue_capacity,
            max_connections,
            max_frame_len,
            handshake_timeout,
        })
    }
}

/// A bounded TCP service that single-owns one memory-only RouteTable shard.
pub struct RouteTableService {
    shard: RouteTableShard,
    trusted_gateway_keys: TrustedGatewayKeys,
    config: RouteTableServiceConfig,
}

impl RouteTableService {
    #[must_use]
    pub const fn new(
        shard: RouteTableShard,
        trusted_gateway_keys: TrustedGatewayKeys,
        config: RouteTableServiceConfig,
    ) -> Self {
        Self {
            shard,
            trusted_gateway_keys,
            config,
        }
    }

    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), TransportError> {
        self.serve_with_actor(listener, shutdown, spawn_shard_actor)
            .await
    }

    async fn serve_with_actor<F>(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
        spawn_actor: F,
    ) -> Result<(), TransportError>
    where
        F: FnOnce(
                RouteTableShard,
                mpsc::Receiver<ServiceCommand>,
                CancellationToken,
            ) -> JoinHandle<RouteTableShard>
            + Send,
    {
        let Self {
            shard,
            trusted_gateway_keys,
            config,
        } = self;
        let keys = Arc::new(trusted_gateway_keys);
        let permits = Arc::new(Semaphore::new(config.max_connections));
        let (requests, request_receiver) = mpsc::channel(config.request_queue_capacity);
        let runtime_shutdown = shutdown.child_token();
        let actor_shutdown = runtime_shutdown.child_token();
        let mut actor = spawn_actor(shard, request_receiver, actor_shutdown.clone());
        let mut actor_completed = false;
        let mut connections = JoinSet::new();
        let mut service_error = None;

        loop {
            while let Some(completed) = connections.try_join_next() {
                if let Err(join_error) = completed {
                    service_error = Some(TransportError::internal(format!(
                        "RouteTable connection task failed: {join_error}"
                    )));
                    break;
                }
            }
            if service_error.is_some() {
                break;
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                completed = &mut actor => {
                    actor_completed = true;
                    if !shutdown.is_cancelled() {
                        service_error = Some(unexpected_actor_exit(completed));
                    }
                    break;
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(join_error)) = completed {
                        service_error = Some(TransportError::internal(format!(
                            "RouteTable connection task failed: {join_error}"
                        )));
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            service_error = Some(TransportError::unavailable(format!(
                                "RouteTable listener failed: {error}"
                            )));
                            break;
                        }
                    };
                    match Arc::clone(&permits).try_acquire_owned() {
                        Ok(permit) => {
                            let keys = Arc::clone(&keys);
                            let requests = requests.clone();
                            let connection_shutdown = runtime_shutdown.child_token();
                            connections.spawn(async move {
                                let _permit = permit;
                                handle_connection(
                                    stream,
                                    keys,
                                    requests,
                                    config,
                                    connection_shutdown,
                                ).await;
                            });
                        }
                        Err(_) => {
                            reject_over_capacity(stream, config, &runtime_shutdown).await;
                        }
                    }
                }
            }
        }

        runtime_shutdown.cancel();
        actor_shutdown.cancel();
        drop(requests);
        while let Some(completed) = connections.join_next().await {
            if let Err(join_error) = completed
                && service_error.is_none()
            {
                service_error = Some(TransportError::internal(format!(
                    "RouteTable connection task failed: {join_error}"
                )));
            }
        }
        if !actor_completed {
            match actor.await {
                Ok(_) => {}
                Err(join_error) if service_error.is_none() => {
                    service_error = Some(TransportError::internal(format!(
                        "RouteTable state actor failed: {join_error}"
                    )));
                }
                Err(_) => {}
            }
        }
        if let Some(error) = service_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}

fn spawn_shard_actor(
    shard: RouteTableShard,
    requests: mpsc::Receiver<ServiceCommand>,
    shutdown: CancellationToken,
) -> JoinHandle<RouteTableShard> {
    tokio::spawn(run_shard_actor(shard, requests, shutdown))
}

fn unexpected_actor_exit<T>(result: Result<T, JoinError>) -> TransportError {
    match result {
        Ok(_) => TransportError::internal("RouteTable state actor stopped unexpectedly"),
        Err(error) => TransportError::internal(format!("RouteTable state actor failed: {error}")),
    }
}

impl fmt::Debug for RouteTableService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteTableService")
            .field("shard_id", &self.shard.shard_id())
            .field("trusted_gateway_keys", &self.trusted_gateway_keys)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

struct ServiceCommand {
    context: RequestContext,
    request: WireRequest,
    reply: oneshot::Sender<Result<WireResponse, TransportError>>,
}

async fn run_shard_actor(
    mut shard: RouteTableShard,
    mut requests: mpsc::Receiver<ServiceCommand>,
    shutdown: CancellationToken,
) -> RouteTableShard {
    loop {
        let next_expiry = shard.next_expiry_deadline();
        tokio::select! {
            _ = shutdown.cancelled() => break,
            request = requests.recv() => {
                let Some(request) = request else {
                    break;
                };
                let now = tokio::time::Instant::now().into_std();
                let response = request.request
                    .validate_preconditions(request.context, shard.generation())
                    .and_then(|()| request.request.into_domain())
                    .and_then(|operation| execute(&mut shard, request.context, operation, now));
                let _ = request.reply.send(response);
            }
            () = wait_until(next_expiry) => {
                shard.expire_due(tokio::time::Instant::now().into_std());
            }
        }
    }
    shard
}

fn execute(
    shard: &mut RouteTableShard,
    context: RequestContext,
    request: DomainRequest,
    now: std::time::Instant,
) -> Result<WireResponse, TransportError> {
    match request {
        DomainRequest::Register { generation, key } => shard
            .register(context, generation, key, now)
            .map(WireResponse::registered)
            .map_err(TransportError::from),
        DomainRequest::Update {
            generation,
            key,
            lease_id,
            revision,
            snapshot,
        } => shard
            .update(context, generation, &key, lease_id, revision, snapshot, now)
            .map(WireResponse::updated)
            .map_err(TransportError::from),
        DomainRequest::KeepAlive {
            generation,
            key,
            lease_id,
        } => shard
            .keep_alive(context, generation, &key, lease_id, now)
            .map(WireResponse::kept_alive)
            .map_err(TransportError::from),
        DomainRequest::Deregister {
            generation,
            key,
            lease_id,
        } => shard
            .deregister(context, generation, &key, lease_id, now)
            .map(|()| WireResponse::Deregistered)
            .map_err(TransportError::from),
        DomainRequest::Resolve {
            generation,
            client_id,
        } => shard
            .resolve(context, generation, &client_id, now)
            .map(|bindings| WireResponse::resolved(&bindings))
            .map_err(TransportError::from),
    }
}

async fn wait_until(deadline: Option<std::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    } else {
        pending::<()>().await;
    }
}

async fn handle_connection(
    stream: TcpStream,
    keys: Arc<TrustedGatewayKeys>,
    requests: mpsc::Sender<ServiceCommand>,
    config: RouteTableServiceConfig,
    shutdown: CancellationToken,
) {
    let mut framed = Framed::new(stream, FrameCodec::new(config.max_frame_len));
    let context = match authenticate(&mut framed, &keys, config.handshake_timeout, &shutdown).await
    {
        Ok(Some(context)) => context,
        Ok(None) | Err(_) => return,
    };
    let (sink, stream) = framed.split();
    let (writer, writer_receiver) = mpsc::channel(config.writer_queue_capacity);
    let writer_shutdown = shutdown.child_token();
    let mut writer_task = tokio::spawn(run_writer(sink, writer_receiver, writer_shutdown.clone()));
    run_reader(
        stream,
        writer.clone(),
        requests,
        context,
        shutdown.clone(),
        config.max_frame_len,
    )
    .await;
    drop(writer);
    tokio::select! {
        _ = shutdown.cancelled() => {
            writer_shutdown.cancel();
            let _ = writer_task.await;
        }
        completed = tokio::time::timeout(config.handshake_timeout, &mut writer_task) => {
            if completed.is_err() {
                writer_shutdown.cancel();
                let _ = writer_task.await;
            }
        }
    }
}

async fn authenticate(
    framed: &mut Framed<TcpStream, FrameCodec>,
    keys: &TrustedGatewayKeys,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<Option<RequestContext>, TransportError> {
    let handshake = async {
        let frame = framed
            .next()
            .await
            .ok_or_else(|| TransportError::unavailable("Gateway closed during authentication"))?
            .map_err(map_receive_codec_error)?;
        let WireFrame::Hello {
            role,
            gateway_name,
            gateway_id,
            internal_gateway_key,
        } = frame
        else {
            send_protocol_fault(framed, "expected Gateway HELLO").await;
            return Ok(None);
        };
        if role != GATEWAY_ROLE {
            send_protocol_fault(framed, "Gateway HELLO has an invalid role").await;
            return Ok(None);
        }

        let name = GatewayName::new(gateway_name).ok();
        let gateway_id = Uuid::parse_str(&gateway_id).ok().map(GatewayId::from_uuid);
        let key = InternalGatewayKey::from_wire(internal_gateway_key);
        let authenticated = name
            .as_ref()
            .is_some_and(|name| keys.authenticate(name, &key));
        if !authenticated || gateway_id.is_none() {
            let error = TransportError::unauthenticated();
            let _ = framed
                .send(WireFrame::HandshakeRejected {
                    role: ROUTE_TABLE_ROLE.to_owned(),
                    code: error.code(),
                    message: error.message().to_owned(),
                })
                .await;
            return Ok(None);
        }
        let Some(gateway_id) = gateway_id else {
            return Ok(None);
        };
        framed
            .send(WireFrame::Welcome {
                role: ROUTE_TABLE_ROLE.to_owned(),
            })
            .await
            .map_err(map_send_codec_error)?;
        Ok(Some(RequestContext::new(
            AuthenticatedGatewayId::from_verified_transport(gateway_id),
        )))
    };

    tokio::select! {
        _ = shutdown.cancelled() => Ok(None),
        result = tokio::time::timeout(timeout, handshake) => {
            result
                .map_err(|_| TransportError::deadline_exceeded("Gateway handshake timed out"))?
        }
    }
}

async fn run_reader(
    mut stream: futures_util::stream::SplitStream<Framed<TcpStream, FrameCodec>>,
    writer: mpsc::Sender<WireFrame>,
    requests: mpsc::Sender<ServiceCommand>,
    context: RequestContext,
    shutdown: CancellationToken,
    max_frame_len: usize,
) {
    let mut last_request_id = 0_u64;
    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = stream.next() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                let _ = try_write_protocol_fault(&writer, error.to_string());
                break;
            }
        };
        let WireFrame::Request {
            role,
            request_id,
            request,
        } = frame
        else {
            let _ = try_write_protocol_fault(&writer, "expected Gateway request frame");
            break;
        };
        if role != GATEWAY_ROLE || request_id == 0 || request_id <= last_request_id {
            let _ = try_write_protocol_fault(
                &writer,
                "Gateway request has an invalid role or request_id",
            );
            break;
        }
        last_request_id = request_id;

        let response = match submit_service_request(&requests, context, request) {
            Ok(response) => response,
            Err(error) if error.code() == ErrorCode::ResourceExhausted => {
                if try_write_response(&writer, request_id, Err(error), max_frame_len).is_err() {
                    break;
                }
                continue;
            }
            Err(error) => {
                let _ = try_write_response(&writer, request_id, Err(error), max_frame_len);
                break;
            }
        };

        let result = tokio::select! {
            _ = shutdown.cancelled() => break,
            result = response => result.unwrap_or_else(|_| {
                Err(TransportError::internal("RouteTable state actor dropped a response"))
            }),
        };
        if try_write_response(&writer, request_id, result, max_frame_len).is_err() {
            break;
        }
    }
}

fn submit_service_request(
    requests: &mpsc::Sender<ServiceCommand>,
    context: RequestContext,
    request: WireRequest,
) -> Result<oneshot::Receiver<Result<WireResponse, TransportError>>, TransportError> {
    let (reply, response) = oneshot::channel();
    match requests.try_send(ServiceCommand {
        context,
        request,
        reply,
    }) {
        Ok(()) => Ok(response),
        Err(mpsc::error::TrySendError::Full(_)) => Err(TransportError::resource_exhausted(
            "RouteTable service request queue is full",
        )),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::unavailable(
            "RouteTable state actor is unavailable",
        )),
    }
}

async fn run_writer(
    mut sink: futures_util::stream::SplitSink<Framed<TcpStream, FrameCodec>, WireFrame>,
    mut frames: mpsc::Receiver<WireFrame>,
    shutdown: CancellationToken,
) {
    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = frames.recv() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let sent = tokio::select! {
            _ = shutdown.cancelled() => break,
            sent = sink.send(frame) => sent,
        };
        if sent.is_err() {
            break;
        }
    }
}

fn try_write_response(
    writer: &mpsc::Sender<WireFrame>,
    request_id: u64,
    result: Result<WireResponse, TransportError>,
    max_frame_len: usize,
) -> Result<(), BoundedSendError> {
    let frame = bounded_response_frame(request_id, result, max_frame_len)?;
    try_send_frame(writer, frame)
}

fn bounded_response_frame(
    request_id: u64,
    result: Result<WireResponse, TransportError>,
    max_frame_len: usize,
) -> Result<WireFrame, BoundedSendError> {
    let result = match result {
        Ok(response) => WireResult::Ok { response },
        Err(error) => WireResult::Error {
            code: error.code(),
            message: error.message().to_owned(),
        },
    };
    let frame = WireFrame::Response {
        role: ROUTE_TABLE_ROLE.to_owned(),
        request_id,
        result,
    };
    let codec = FrameCodec::new(max_frame_len);
    match codec.validate(&frame) {
        Ok(()) => return Ok(frame),
        Err(CodecError::FrameTooLarge { .. }) => {}
        Err(_) => {
            return bounded_error_response_frame(
                request_id,
                ErrorCode::Internal,
                "response encoding failed",
                max_frame_len,
            );
        }
    }
    bounded_error_response_frame(
        request_id,
        ErrorCode::ResourceExhausted,
        "response exceeds frame limit",
        max_frame_len,
    )
}

fn bounded_error_response_frame(
    request_id: u64,
    code: ErrorCode,
    message: &'static str,
    max_frame_len: usize,
) -> Result<WireFrame, BoundedSendError> {
    let codec = FrameCodec::new(max_frame_len);
    let frame = error_response_frame(request_id, code, message);
    if codec.validate(&frame).is_ok() {
        return Ok(frame);
    }
    let minimum = error_response_frame(request_id, code, "");
    if codec.validate(&minimum).is_ok() {
        Ok(minimum)
    } else {
        Err(BoundedSendError::FrameCannotBeEncoded)
    }
}

fn error_response_frame(request_id: u64, code: ErrorCode, message: &str) -> WireFrame {
    WireFrame::Response {
        role: ROUTE_TABLE_ROLE.to_owned(),
        request_id,
        result: WireResult::Error {
            code,
            message: message.to_owned(),
        },
    }
}

fn try_write_protocol_fault(
    writer: &mpsc::Sender<WireFrame>,
    message: impl Into<String>,
) -> Result<(), BoundedSendError> {
    try_send_frame(
        writer,
        WireFrame::ProtocolFault {
            role: ROUTE_TABLE_ROLE.to_owned(),
            code: ErrorCode::ProtocolError,
            message: message.into(),
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedSendError {
    Full,
    Closed,
    FrameCannotBeEncoded,
}

fn try_send_frame(
    writer: &mpsc::Sender<WireFrame>,
    frame: WireFrame,
) -> Result<(), BoundedSendError> {
    writer.try_send(frame).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => BoundedSendError::Full,
        mpsc::error::TrySendError::Closed(_) => BoundedSendError::Closed,
    })
}

async fn send_protocol_fault(framed: &mut Framed<TcpStream, FrameCodec>, message: &str) {
    let _ = framed
        .send(WireFrame::ProtocolFault {
            role: ROUTE_TABLE_ROLE.to_owned(),
            code: ErrorCode::ProtocolError,
            message: message.to_owned(),
        })
        .await;
}

async fn reject_over_capacity(
    stream: TcpStream,
    config: RouteTableServiceConfig,
    shutdown: &CancellationToken,
) {
    let mut framed = Framed::new(stream, FrameCodec::new(config.max_frame_len));
    let error = TransportError::resource_exhausted("RouteTable connection limit reached");
    let send = framed.send(WireFrame::HandshakeRejected {
        role: ROUTE_TABLE_ROLE.to_owned(),
        code: error.code(),
        message: error.message().to_owned(),
    });
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = tokio::time::timeout(config.handshake_timeout, send) => {}
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

fn validate_service_frame_len(value: usize) -> Result<(), TransportError> {
    validate_frame_len(value)?;
    let minimum = error_response_frame(u64::MAX, ErrorCode::ResourceExhausted, "");
    if FrameCodec::new(value).validate(&minimum).is_err() {
        return Err(TransportError::invalid_argument(
            "maximum frame length cannot encode the minimum RouteTable error response",
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
    use futures_util::{SinkExt, StreamExt};
    use relaygate_route_table::{
        AuthenticatedGatewayId, BindingId, ClientId, GatewayLocator, ListenerSessionId,
        MappingEntry, MappingSnapshot, RegistrationKey, RegistrationRevision, RouteTableConfig,
        ShardDirectory, ShardDirectoryGeneration, ShardId,
    };

    use super::*;

    const DIRECTORY: &[u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"rt-0:27430"}]}"#;

    #[test]
    fn config_rejects_zero_bounds_and_deadline() {
        let second = Duration::from_secs(1);
        assert!(RouteTableServiceConfig::new(0, 1, 1, 1024, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 0, 1, 1024, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 1, 0, 1024, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 1, 1, 0, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 1, 1, 1, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 1, 1, 1024, Duration::ZERO).is_err());
        assert!(RouteTableServiceConfig::new(usize::MAX, 1, 1, 1024, second).is_err());
        assert!(RouteTableServiceConfig::new(1, usize::MAX, 1, 1024, second).is_err());
        assert!(RouteTableServiceConfig::new(1, 1, usize::MAX, 1024, second).is_err());
    }

    #[test]
    fn service_frame_limit_covers_the_minimum_resource_exhausted_response() {
        let minimum = error_response_frame(u64::MAX, ErrorCode::ResourceExhausted, "");
        let mut minimum_len = 1;
        while FrameCodec::new(minimum_len).validate(&minimum).is_err() {
            minimum_len += 1;
        }
        let second = Duration::from_secs(1);
        assert!(
            RouteTableServiceConfig::new(1, 1, 1, minimum_len.saturating_sub(1), second).is_err()
        );
        assert!(RouteTableServiceConfig::new(1, 1, 1, minimum_len, second).is_ok());
    }

    #[tokio::test]
    async fn unexpected_actor_exit_is_a_terminal_internal_failure() {
        let completed = tokio::spawn(async {}).await;
        assert_eq!(unexpected_actor_exit(completed).code(), ErrorCode::Internal);

        let aborted = tokio::spawn(pending::<()>());
        aborted.abort();
        let aborted = aborted.await;
        assert_eq!(unexpected_actor_exit(aborted).code(), ErrorCode::Internal);
    }

    #[tokio::test]
    async fn unexpected_actor_exit_terminates_service_and_live_connection()
    -> Result<(), TransportError> {
        let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
        let shard = RouteTableShard::new(
            directory,
            ShardId::new("rt-0")?,
            RouteTableConfig::new(Duration::from_secs(10))?,
        )?;
        let trusted = TrustedGatewayKeys::new([(
            GatewayName::new("gw-a")?,
            InternalGatewayKey::new("key-a")?,
        )])?;
        let config = RouteTableServiceConfig::new(4, 2, 2, 64 * 1024, Duration::from_secs(1))?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let shutdown = CancellationToken::new();
        let (stop_actor, actor_stopped) = oneshot::channel();
        let service_task = tokio::spawn(
            RouteTableService::new(shard, trusted, config).serve_with_actor(
                listener,
                shutdown.clone(),
                move |shard, requests, _actor_shutdown| {
                    tokio::spawn(async move {
                        let _requests = requests;
                        let _ = actor_stopped.await;
                        shard
                    })
                },
            ),
        );

        let stream = TcpStream::connect(endpoint)
            .await
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let mut framed = Framed::new(stream, FrameCodec::new(64 * 1024));
        framed
            .send(WireFrame::Hello {
                role: GATEWAY_ROLE.to_owned(),
                gateway_name: "gw-a".to_owned(),
                gateway_id: Uuid::from_u128(1).to_string(),
                internal_gateway_key: "key-a".to_owned(),
            })
            .await
            .map_err(map_send_codec_error)?;
        let welcome = framed
            .next()
            .await
            .ok_or_else(|| TransportError::unavailable("test handshake closed"))?
            .map_err(map_receive_codec_error)?;
        assert!(matches!(
            welcome,
            WireFrame::Welcome { role } if role == ROUTE_TABLE_ROLE
        ));

        stop_actor
            .send(())
            .map_err(|()| TransportError::internal("test actor trigger dropped"))?;
        let service_result = tokio::time::timeout(Duration::from_secs(1), service_task)
            .await
            .map_err(|_| TransportError::deadline_exceeded("test service shutdown timed out"))?
            .map_err(|error| TransportError::internal(error.to_string()))?;
        let service_error = service_result
            .err()
            .ok_or_else(|| TransportError::internal("test service exited without INTERNAL"))?;
        assert_eq!(service_error.code(), ErrorCode::Internal);
        assert!(!shutdown.is_cancelled());

        let closed = tokio::time::timeout(Duration::from_secs(1), framed.next())
            .await
            .map_err(|_| TransportError::deadline_exceeded("test connection close timed out"))?;
        assert!(matches!(closed, None | Some(Err(_))));
        Ok(())
    }

    #[tokio::test]
    async fn bounded_request_and_writer_queues_report_full_without_waiting()
    -> Result<(), TransportError> {
        let gateway_id = GatewayId::from_uuid(Uuid::from_u128(1));
        let context =
            RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id));
        let request = WireRequest::resolve(
            ShardDirectoryGeneration::from_bytes([1; 32]),
            &ClientId::new("echo.a")?,
        );
        let (requests, _request_receiver) = mpsc::channel(1);
        let _pending = submit_service_request(&requests, context, request)?;
        let full = submit_service_request(
            &requests,
            context,
            WireRequest::resolve(
                ShardDirectoryGeneration::from_bytes([1; 32]),
                &ClientId::new("echo.b")?,
            ),
        )
        .err();
        assert_eq!(
            full.map(|error| error.code()),
            Some(ErrorCode::ResourceExhausted)
        );

        let (writer, _frame_receiver) = mpsc::channel(1);
        assert!(
            try_send_frame(
                &writer,
                WireFrame::Welcome {
                    role: ROUTE_TABLE_ROLE.to_owned(),
                },
            )
            .is_ok()
        );
        assert_eq!(
            try_write_protocol_fault(&writer, "must not wait"),
            Err(BoundedSendError::Full)
        );
        Ok(())
    }

    #[tokio::test]
    async fn authenticated_owner_mismatch_leaves_actor_state_empty() -> Result<(), TransportError> {
        let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
        let generation = directory.generation();
        let shard = RouteTableShard::new(
            directory,
            ShardId::new("rt-0")?,
            RouteTableConfig::new(Duration::from_secs(10))?,
        )?;
        let authenticated_gateway = GatewayId::from_uuid(Uuid::from_u128(1));
        let claimed_gateway = GatewayId::from_uuid(Uuid::from_u128(2));
        let context = RequestContext::new(AuthenticatedGatewayId::from_verified_transport(
            authenticated_gateway,
        ));
        let key = RegistrationKey::new(
            claimed_gateway,
            ListenerSessionId::from_uuid(Uuid::from_u128(3)),
            ShardId::new("rt-0")?,
        );
        let shutdown = CancellationToken::new();
        let (requests, receiver) = mpsc::channel(1);
        let actor = tokio::spawn(run_shard_actor(shard, receiver, shutdown.clone()));

        let denied = issue(&requests, context, WireRequest::register(generation, &key))
            .await
            .err();
        assert_eq!(
            denied.map(|error| error.code()),
            Some(ErrorCode::PermissionDenied)
        );

        shutdown.cancel();
        drop(requests);
        let shard = actor.await.map_err(|error| {
            TransportError::internal(format!("test RouteTable actor failed: {error}"))
        })?;
        let stats = shard.stats();
        assert_eq!(stats.registration_count, 0);
        assert_eq!(stats.mapping_count, 0);
        assert_eq!(stats.route_count, 0);
        assert_eq!(stats.expiry_record_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_protocol_fault_is_flushed_before_connection_close()
    -> Result<(), TransportError> {
        let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
        let shard = RouteTableShard::new(
            directory,
            ShardId::new("rt-0")?,
            RouteTableConfig::new(Duration::from_secs(10))?,
        )?;
        let trusted = TrustedGatewayKeys::new([(
            GatewayName::new("gw-a")?,
            InternalGatewayKey::new("key-a")?,
        )])?;
        let config = RouteTableServiceConfig::new(4, 2, 2, 64 * 1024, Duration::from_secs(1))?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let shutdown = CancellationToken::new();
        let service_task = tokio::spawn(
            RouteTableService::new(shard, trusted, config).serve(listener, shutdown.clone()),
        );

        let stream = TcpStream::connect(endpoint)
            .await
            .map_err(|error| TransportError::unavailable(error.to_string()))?;
        let mut framed = Framed::new(stream, FrameCodec::new(64 * 1024));
        framed
            .send(WireFrame::Hello {
                role: GATEWAY_ROLE.to_owned(),
                gateway_name: "gw-a".to_owned(),
                gateway_id: Uuid::from_u128(1).to_string(),
                internal_gateway_key: "key-a".to_owned(),
            })
            .await
            .map_err(map_send_codec_error)?;
        let welcome = framed
            .next()
            .await
            .ok_or_else(|| TransportError::unavailable("test handshake closed"))?
            .map_err(map_receive_codec_error)?;
        assert!(matches!(
            welcome,
            WireFrame::Welcome { role } if role == ROUTE_TABLE_ROLE
        ));

        framed
            .send(WireFrame::Welcome {
                role: GATEWAY_ROLE.to_owned(),
            })
            .await
            .map_err(map_send_codec_error)?;
        let fault = tokio::time::timeout(Duration::from_secs(1), framed.next())
            .await
            .map_err(|_| TransportError::deadline_exceeded("test protocol fault timed out"))?
            .ok_or_else(|| TransportError::unavailable("test protocol connection closed"))?
            .map_err(map_receive_codec_error)?;
        assert!(matches!(
            fault,
            WireFrame::ProtocolFault { role, code: ErrorCode::ProtocolError, .. }
                if role == ROUTE_TABLE_ROLE
        ));

        shutdown.cancel();
        service_task
            .await
            .map_err(|error| TransportError::internal(error.to_string()))??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_driver_removes_state_without_an_intervening_request()
    -> Result<(), TransportError> {
        let ttl = Duration::from_secs(10);
        let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
        let generation = directory.generation();
        let shard = RouteTableShard::new(
            directory,
            ShardId::new("rt-0")?,
            RouteTableConfig::new(ttl)?,
        )?;
        let gateway_id = GatewayId::from_uuid(Uuid::from_u128(1));
        let listener_session_id = ListenerSessionId::from_uuid(Uuid::from_u128(2));
        let context =
            RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id));
        let key = RegistrationKey::new(gateway_id, listener_session_id, ShardId::new("rt-0")?);
        let snapshot = MappingSnapshot::new([MappingEntry::new(
            ClientId::new("echo.a")?,
            gateway_id,
            listener_session_id,
            BindingId::from_uuid(Uuid::from_u128(3)),
            GatewayLocator::new("gw-a:27431")?,
        )])?;

        let shutdown = CancellationToken::new();
        let (requests, receiver) = mpsc::channel(4);
        let actor = tokio::spawn(run_shard_actor(shard, receiver, shutdown.clone()));
        let registered = issue(&requests, context, WireRequest::register(generation, &key)).await?;
        let WireResponse::Registered { ack } = registered else {
            return Err(TransportError::internal(
                "test actor returned the wrong register response",
            ));
        };
        let ack = ack.into_domain()?;
        let _ = issue(
            &requests,
            context,
            WireRequest::update(
                generation,
                &key,
                ack.lease_id(),
                RegistrationRevision::FIRST,
                &snapshot,
            ),
        )
        .await?;

        tokio::time::advance(ttl + Duration::from_nanos(1)).await;
        tokio::task::yield_now().await;
        shutdown.cancel();
        drop(requests);
        let shard = actor.await.map_err(|error| {
            TransportError::internal(format!("test RouteTable actor failed: {error}"))
        })?;
        let stats = shard.stats();
        assert_eq!(stats.registration_count, 0);
        assert_eq!(stats.mapping_count, 0);
        assert_eq!(stats.expiry_record_count, 0);
        Ok(())
    }

    async fn issue(
        requests: &mpsc::Sender<ServiceCommand>,
        context: RequestContext,
        request: WireRequest,
    ) -> Result<WireResponse, TransportError> {
        let (reply, response) = oneshot::channel();
        requests
            .send(ServiceCommand {
                context,
                request,
                reply,
            })
            .await
            .map_err(|_| TransportError::internal("test RouteTable actor stopped"))?;
        response
            .await
            .map_err(|_| TransportError::internal("test RouteTable actor dropped response"))?
    }
}
