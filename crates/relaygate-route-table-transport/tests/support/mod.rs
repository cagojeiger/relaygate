#![allow(dead_code)]

use std::{error::Error, net::SocketAddr, time::Duration};

use relaygate_route_table::{
    BindingId, DestinationId, GatewayId, GatewayLocator, MappingEntry, MappingSnapshot,
    RegistrationKey, RelaySessionId, RouteTableConfig, RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, InternalGatewayKey, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError, TrustedGatewayKeys,
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub const ONE_SHARD_DIRECTORY: &[u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"rt-0:27430"}]}"#;

pub struct RunningService {
    pub endpoint: SocketAddr,
    pub generation: relaygate_route_table::ShardDirectoryGeneration,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), TransportError>>,
}

impl RunningService {
    pub async fn start(
        ttl: Duration,
        keys: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> TestResult<Self> {
        Self::start_with_max_connections(ttl, keys, 8).await
    }

    pub async fn start_with_max_connections(
        ttl: Duration,
        keys: impl IntoIterator<Item = (&'static str, &'static str)>,
        max_connections: usize,
    ) -> TestResult<Self> {
        Self::start_with_limits(ttl, keys, max_connections, 256 * 1024).await
    }

    pub async fn start_with_limits(
        ttl: Duration,
        keys: impl IntoIterator<Item = (&'static str, &'static str)>,
        max_connections: usize,
        max_frame_len: usize,
    ) -> TestResult<Self> {
        let directory = ShardDirectory::from_json_bytes(ONE_SHARD_DIRECTORY)?;
        let generation = directory.generation();
        let shard = RouteTableShard::new(
            directory,
            ShardId::new("rt-0")?,
            RouteTableConfig::new(ttl)?,
        )?;
        let trusted = TrustedGatewayKeys::new(
            keys.into_iter()
                .map(|(name, key)| Ok((GatewayName::new(name)?, InternalGatewayKey::new(key)?)))
                .collect::<Result<Vec<_>, TransportError>>()?,
        )?;
        let service = RouteTableService::new(
            shard,
            trusted,
            RouteTableServiceConfig::new(
                16,
                8,
                max_connections,
                max_frame_len,
                Duration::from_secs(1),
            )?,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(service.serve(listener, shutdown.clone()));
        Ok(Self {
            endpoint,
            generation,
            shutdown,
            task,
        })
    }

    pub async fn connect(
        &self,
        name: &str,
        gateway_id: GatewayId,
        key: &str,
    ) -> Result<RouteTableClient, TransportError> {
        RouteTableClient::connect(
            self.endpoint,
            GatewayName::new(name)?,
            gateway_id,
            InternalGatewayKey::new(key)?,
            RouteTableClientConfig::new(
                16,
                256 * 1024,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )?,
        )
        .await
    }

    pub async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        self.task.await??;
        Ok(())
    }
}

pub const fn gateway(value: u128) -> GatewayId {
    GatewayId::from_uuid(Uuid::from_u128(value))
}

pub const fn session(value: u128) -> RelaySessionId {
    RelaySessionId::from_uuid(Uuid::from_u128(value))
}

pub const fn binding(value: u128) -> BindingId {
    BindingId::from_uuid(Uuid::from_u128(value))
}

pub fn registration_key(
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
) -> TestResult<RegistrationKey> {
    Ok(RegistrationKey::new(
        gateway_id,
        relay_session_id,
        ShardId::new("rt-0")?,
    ))
}

pub fn mapping_snapshot(
    destination_id: &str,
    gateway_id: GatewayId,
    relay_session_id: RelaySessionId,
    binding_id: BindingId,
) -> TestResult<MappingSnapshot> {
    Ok(MappingSnapshot::new([MappingEntry::new(
        DestinationId::new(destination_id)?,
        gateway_id,
        relay_session_id,
        binding_id,
        GatewayLocator::new(format!("gw-{gateway_id}:27431"))?,
    )])?)
}
