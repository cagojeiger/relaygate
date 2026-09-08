use anyhow::{Context, Result};
use relaygate_route_table_transport::RouteTableService;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::RouteTableRuntimeConfig;

pub(crate) async fn serve(
    config: RouteTableRuntimeConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let shard_id = config.shard.shard_id().to_string();
    let generation = config.shard.generation().to_string();
    let insecure = config.tls.is_none();
    let mut service = RouteTableService::new(config.shard, config.service);
    if let Some(tls) = config.tls {
        service = service.with_tls(tls);
    }
    let listener = TcpListener::bind(&config.bind_address)
        .await
        .with_context(|| format!("failed to bind RouteTable at {}", config.bind_address))?;
    let local_address = listener.local_addr()?;

    if insecure {
        tracing::warn!(
            component = "route_table",
            event = "route_table.transport.plaintext_enabled",
            role = "route_table",
            transport = "plain_tcp",
            authentication = "none",
            "internal plaintext transport selected; traffic is unauthenticated and unencrypted"
        );
    }

    tracing::info!(
        component = "server",
        event = "server.started",
        role = "route_table",
        address = %local_address,
        shard_id,
        directory_generation = generation,
        "RelayGate RouteTable started"
    );
    service.serve(listener, shutdown).await?;
    tracing::info!(
        component = "server",
        event = "server.stopped",
        role = "route_table",
        shard_id,
        "RelayGate RouteTable stopped"
    );
    Ok(())
}
