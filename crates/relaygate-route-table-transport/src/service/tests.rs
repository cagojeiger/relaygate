use std::future::pending;

use futures_util::{SinkExt, StreamExt};
use relaygate_route_table::{
    AuthenticatedGatewayId, BindingId, DestinationId, GatewayId, GatewayLocator, MappingEntry,
    MappingSnapshot, RegistrationKey, RegistrationRevision, RelaySessionId, RequestContext,
    RouteTableConfig, ShardDirectory, ShardDirectoryGeneration, ShardId,
};
use tokio::{net::TcpStream, sync::oneshot};
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::{
    GatewayName, InternalGatewayKey,
    dto::{WireRequest, WireResponse},
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame},
};

use super::{
    actor::{ServiceCommand, run_shard_actor},
    connection::{map_receive_codec_error, map_send_codec_error, submit_service_request},
    response::{BoundedSendError, try_send_frame, try_write_protocol_fault},
    *,
};

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
    assert!(RouteTableServiceConfig::new(1, 1, 1, minimum_len.saturating_sub(1), second).is_err());
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
async fn unexpected_actor_exit_terminates_service_and_live_connection() -> Result<(), TransportError>
{
    let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
    let shard = RouteTableShard::new(
        directory,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_secs(10))?,
    )?;
    let trusted =
        TrustedGatewayKeys::new([(GatewayName::new("gw-a")?, InternalGatewayKey::new("key-a")?)])?;
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
    let context = RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id));
    let request = WireRequest::resolve(
        ShardDirectoryGeneration::from_bytes([1; 32]),
        &DestinationId::new("11111111-1111-4111-8111-111111111111")?,
    );
    let (requests, _request_receiver) = mpsc::channel(1);
    let _pending = submit_service_request(&requests, context, request)?;
    let full = submit_service_request(
        &requests,
        context,
        WireRequest::resolve(
            ShardDirectoryGeneration::from_bytes([1; 32]),
            &DestinationId::new("22222222-2222-4222-8222-222222222222")?,
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
        RelaySessionId::from_uuid(Uuid::from_u128(3)),
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
async fn terminal_protocol_fault_is_flushed_before_connection_close() -> Result<(), TransportError>
{
    let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
    let shard = RouteTableShard::new(
        directory,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_secs(10))?,
    )?;
    let trusted =
        TrustedGatewayKeys::new([(GatewayName::new("gw-a")?, InternalGatewayKey::new("key-a")?)])?;
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
async fn expiry_driver_removes_state_without_an_intervening_request() -> Result<(), TransportError>
{
    let ttl = Duration::from_secs(10);
    let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
    let generation = directory.generation();
    let shard = RouteTableShard::new(
        directory,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(ttl)?,
    )?;
    let gateway_id = GatewayId::from_uuid(Uuid::from_u128(1));
    let relay_session_id = RelaySessionId::from_uuid(Uuid::from_u128(2));
    let context = RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id));
    let key = RegistrationKey::new(gateway_id, relay_session_id, ShardId::new("rt-0")?);
    let snapshot = MappingSnapshot::new([MappingEntry::new(
        DestinationId::new("11111111-1111-4111-8111-111111111111")?,
        gateway_id,
        relay_session_id,
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
