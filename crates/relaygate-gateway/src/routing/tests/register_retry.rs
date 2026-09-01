use std::time::Duration;

use relaygate_route_table::{
    AuthenticatedGatewayId, ClientId, RegistrationKey, RegistrationRevision, RequestContext,
    RouteTableConfig, RouteTableShard, ShardId,
};
use tokio::time::Instant;

use crate::routing::lifecycle::{RegistrationAction, RegistrationState};

use super::{TestResult, gateway, listener_session, one_shard_directory, snapshot};

#[test]
fn lost_register_response_retry_uses_only_the_current_attempt() -> TestResult {
    let retry = Duration::from_millis(10);
    let lifecycle_start = Instant::now();
    let route_table_start = std::time::Instant::now();
    let gateway_id = gateway(1);
    let session_id = listener_session(2);
    let key = RegistrationKey::new(gateway_id, session_id, ShardId::new("rt-0")?);
    let context = RequestContext::new(AuthenticatedGatewayId::from_verified_transport(gateway_id));
    let directory = one_shard_directory("rt-0:27430")?;
    let mut shard = RouteTableShard::new(
        directory,
        ShardId::new("rt-0")?,
        RouteTableConfig::new(Duration::from_secs(30))?,
    )?;
    let generation = shard.generation();
    let mut state = RegistrationState::new(
        key.clone(),
        1,
        Some(snapshot("client-a")?),
        lifecycle_start,
        retry,
        Duration::from_secs(1),
    );

    let first = state
        .begin_next(lifecycle_start)?
        .ok_or("missing first REGISTER")?;
    let first_ack = shard.register(context, generation, key.clone(), route_table_start)?;

    // The RT committed the lease, but the first response was not observed by
    // the Gateway. Retrying the same intent is a distinct local attempt.
    state.transient_failure(&first, lifecycle_start);
    let current = state
        .begin_next(lifecycle_start + retry)?
        .ok_or("missing retry REGISTER")?;
    assert_eq!(current.action, first.action);
    assert_ne!(current, first);

    let current_ack = shard.register(
        context,
        generation,
        key.clone(),
        route_table_start + Duration::from_secs(5),
    )?;
    assert_eq!(current_ack.lease_id(), first_ack.lease_id());
    assert_eq!(current_ack.accepted_revision(), None);
    assert_eq!(current_ack.expires_in(), Duration::from_secs(25));
    assert_eq!(shard.stats().mapping_count, 0);

    // A delayed terminal result from the first attempt cannot consume the
    // retry's pending slot or install its acknowledgement.
    state.register_succeeded(&first, first_ack, lifecycle_start + retry);
    assert!(state.active_lease().is_none());

    state.register_succeeded(&current, current_ack, lifecycle_start + retry);
    assert_eq!(
        state.active_lease().map(|(_, lease_id)| lease_id),
        Some(current_ack.lease_id())
    );

    let update = state
        .begin_next(lifecycle_start + retry)?
        .ok_or("missing first UPDATE")?;
    let RegistrationAction::Update {
        lease_id,
        revision,
        snapshot,
        ..
    } = &update.action
    else {
        return Err("expected first UPDATE".into());
    };
    assert_eq!(*revision, RegistrationRevision::FIRST);
    let updated = shard.update(
        context,
        generation,
        &key,
        *lease_id,
        *revision,
        snapshot.clone(),
        route_table_start + Duration::from_secs(6),
    )?;
    state.update_succeeded(&update, updated, lifecycle_start + retry);
    assert!(state.is_synced());
    assert_eq!(
        shard
            .resolve(
                context,
                generation,
                &ClientId::new("client-a")?,
                route_table_start + Duration::from_secs(6),
            )?
            .len(),
        1
    );
    Ok(())
}
