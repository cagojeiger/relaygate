mod actor;
mod connection;
mod response;

use std::{fmt, sync::Arc, time::Duration};

use relaygate_route_table::RouteTableShard;
use relaygate_transport::{ServerTlsConfig, insecure_boxed};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc},
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{ErrorCode, TransportError, TrustedGatewayKeys, codec::FrameCodec};

use actor::{ServiceCommand, spawn_shard_actor};
use connection::{handle_connection, reject_over_capacity};
use response::error_response_frame;

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
    tls: Option<ServerTlsConfig>,
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
            tls: None,
        }
    }

    #[must_use]
    pub fn with_tls(mut self, tls: ServerTlsConfig) -> Self {
        self.tls = Some(tls);
        self
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
            tls,
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
                            let tls = tls.clone();
                            connections.spawn(async move {
                                let _permit = permit;
                                let stream = match tls {
                                    Some(tls) => match tokio::time::timeout(
                                        config.handshake_timeout,
                                        tls.accept_boxed(stream),
                                    )
                                    .await
                                    {
                                        Ok(Ok(stream)) => stream,
                                        Ok(Err(error)) => {
                                            tracing::debug!(
                                                event = "route_table.tls.rejected",
                                                %error,
                                                "RouteTable TLS handshake failed"
                                            );
                                            return;
                                        }
                                        Err(_) => return,
                                    },
                                    None => insecure_boxed(stream),
                                };
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
                            if tls.is_some() {
                                drop(stream);
                            } else {
                                reject_over_capacity(stream, config, &runtime_shutdown).await;
                            }
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
mod tests;
