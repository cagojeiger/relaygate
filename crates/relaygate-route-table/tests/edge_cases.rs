mod support;

use std::time::{Duration, Instant};

use relaygate_route_table::{
    BindingId, BindingSet, ClientId, ErrorCode, GatewayLocator, MappingEntry, MappingSnapshot,
    RegistrationKey, RegistrationRevision, RequestContext, RouteTableConfig, RouteTableError,
    RouteTableShard, ShardDirectory, ShardDirectoryGeneration, ShardId,
};
use uuid::Uuid;

use support::{binding, client, context, gateway, key, mapping, session, shard, snapshot};

#[test]
fn one_registration_expiry_preserves_sibling_bindings() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let ttl = Duration::from_secs(10);
    let mut shard = shard(ttl)?;
    let generation = shard.generation();
    let client_id = client("shared")?;

    let gateway_one = gateway(11);
    let gateway_two = gateway(12);
    let gateway_three = gateway(13);
    let session_one = session(111);
    let session_two = session(112);
    let session_three = session(113);
    let key_one = key(gateway_one, session_one)?;
    let key_two = key(gateway_two, session_two)?;
    let key_three = key(gateway_three, session_three)?;

    let lease_one = shard
        .register(context(gateway_one), generation, key_one.clone(), start)?
        .lease_id();
    let lease_two = shard
        .register(context(gateway_two), generation, key_two.clone(), start)?
        .lease_id();
    let lease_three = shard
        .register(context(gateway_three), generation, key_three.clone(), start)?
        .lease_id();
    shard.update(
        context(gateway_one),
        generation,
        &key_one,
        lease_one,
        RegistrationRevision::FIRST,
        snapshot([mapping("shared", gateway_one, session_one, binding(1111))?])?,
        start,
    )?;
    shard.update(
        context(gateway_two),
        generation,
        &key_two,
        lease_two,
        RegistrationRevision::FIRST,
        snapshot([mapping("shared", gateway_two, session_two, binding(1112))?])?,
        start,
    )?;
    shard.update(
        context(gateway_three),
        generation,
        &key_three,
        lease_three,
        RegistrationRevision::FIRST,
        snapshot([mapping(
            "shared",
            gateway_three,
            session_three,
            binding(1113),
        )?])?,
        start,
    )?;

    shard.keep_alive(
        context(gateway_two),
        generation,
        &key_two,
        lease_two,
        start + Duration::from_secs(5),
    )?;
    shard.keep_alive(
        context(gateway_three),
        generation,
        &key_three,
        lease_three,
        start + Duration::from_secs(5),
    )?;

    assert_eq!(shard.expire_due(start + ttl), 1);
    let remaining = shard.resolve(context(gateway_two), generation, &client_id, start + ttl)?;
    assert_eq!(remaining.len(), 2);
    assert!(
        remaining
            .entries()
            .iter()
            .all(|entry| entry.identity().gateway_id() != gateway_one)
    );
    assert_eq!(shard.stats().registration_count, 2);
    assert_eq!(shard.stats().expiry_record_count, 2);
    Ok(())
}

#[test]
fn ended_lease_operations_cannot_change_a_new_lease() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(21);
    let session_id = session(210);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(60))?;
    let generation = shard.generation();

    let lease_one = shard
        .register(context, generation, key.clone(), start)?
        .lease_id();
    shard.update(
        context,
        generation,
        &key,
        lease_one,
        RegistrationRevision::FIRST,
        snapshot([mapping("old", gateway_id, session_id, binding(2101))?])?,
        start,
    )?;
    shard.deregister(context, generation, &key, lease_one, start)?;

    let lease_two = shard
        .register(context, generation, key.clone(), start)?
        .lease_id();
    assert_ne!(lease_one, lease_two);
    let new_mapping = mapping("new", gateway_id, session_id, binding(2102))?;
    shard.update(
        context,
        generation,
        &key,
        lease_two,
        RegistrationRevision::FIRST,
        snapshot([new_mapping.clone()])?,
        start,
    )?;

    let stale_update = shard.update(
        context,
        generation,
        &key,
        lease_one,
        RegistrationRevision::new(2)?,
        snapshot([mapping("old", gateway_id, session_id, binding(2103))?])?,
        start,
    );
    let stale_keepalive = shard.keep_alive(context, generation, &key, lease_one, start);
    let stale_deregister = shard.deregister(context, generation, &key, lease_one, start);
    assert!(matches!(
        stale_update,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert!(matches!(
        stale_keepalive,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert!(matches!(
        stale_deregister,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(
        shard
            .resolve(context, generation, &client("new")?, start)?
            .entries(),
        &[new_mapping]
    );
    assert!(matches!(
        shard.resolve(context, generation, &client("old")?, start),
        Err(RouteTableError::NotFound)
    ));
    Ok(())
}

#[test]
fn auth_generation_and_scope_failures_do_not_expire_or_mutate_state() -> Result<(), RouteTableError>
{
    let start = Instant::now();
    let gateway_id = gateway(31);
    let session_id = session(310);
    let request_context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(5))?;
    let generation = shard.generation();
    let lease = shard
        .register(request_context, generation, key.clone(), start)?
        .lease_id();
    let original = mapping("alpha", gateway_id, session_id, binding(3101))?;
    shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([original.clone()])?,
        start,
    )?;
    let after_deadline = start + Duration::from_secs(6);
    let mismatched_generation = ShardDirectoryGeneration::from_bytes([9; 32]);
    let wrong_gateway = gateway(32);

    let auth_error = shard.keep_alive(
        context(wrong_gateway),
        mismatched_generation,
        &key,
        lease,
        after_deadline,
    );
    assert!(matches!(auth_error, Err(RouteTableError::PermissionDenied)));
    assert_eq!(shard.stats().registration_count, 1);

    let generation_error = shard.keep_alive(
        request_context,
        mismatched_generation,
        &key,
        lease,
        after_deadline,
    );
    assert!(matches!(
        generation_error,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(shard.stats().registration_count, 1);

    let wrong_shard_key = relaygate_route_table::RegistrationKey::new(
        gateway_id,
        session_id,
        ShardId::new("rt-other")?,
    );
    let scope_error = shard.keep_alive(
        request_context,
        generation,
        &wrong_shard_key,
        lease,
        after_deadline,
    );
    assert!(matches!(
        scope_error,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert_eq!(shard.stats().registration_count, 1);

    assert_eq!(shard.expire_due(after_deadline), 1);
    assert_eq!(shard.stats().registration_count, 0);
    Ok(())
}

#[test]
fn invalid_snapshot_is_rejected_before_existing_state_changes() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(41);
    let session_id = session(410);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(60))?;
    let generation = shard.generation();
    let lease = shard
        .register(context, generation, key.clone(), start)?
        .lease_id();
    let original = mapping("alpha", gateway_id, session_id, binding(4101))?;
    shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([original.clone()])?,
        start,
    )?;

    let wrong_session = session(411);
    let out_of_scope = MappingSnapshot::new([MappingEntry::new(
        ClientId::new("beta")?,
        gateway_id,
        wrong_session,
        BindingId::from_uuid(Uuid::from_u128(4102)),
        GatewayLocator::new("gw")?,
    )])?;
    let result = shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        out_of_scope,
        start,
    );
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert_eq!(shard.stats().mapping_count, 1);
    assert_eq!(
        shard
            .resolve(context, generation, &client("alpha")?, start)?
            .entries(),
        &[original]
    );
    assert!(matches!(
        shard.resolve(context, generation, &client("beta")?, start),
        Err(RouteTableError::NotFound)
    ));
    Ok(())
}

#[test]
fn expiry_memory_is_bounded_by_live_leases_not_keepalive_count() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(51);
    let session_id = session(510);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(1_000_000))?;
    let generation = shard.generation();
    let lease = shard
        .register(context, generation, key.clone(), start)?
        .lease_id();
    assert_eq!(
        shard.next_expiry_deadline(),
        Some(start + Duration::from_secs(1_000_000))
    );

    for second in 1..=10_000 {
        shard.keep_alive(
            context,
            generation,
            &key,
            lease,
            start + Duration::from_secs(second),
        )?;
    }
    assert_eq!(shard.stats().registration_count, 1);
    assert_eq!(shard.stats().expiry_record_count, 1);
    assert_eq!(shard.stats().mapping_count, 0);
    assert_eq!(
        shard.next_expiry_deadline(),
        Some(start + Duration::from_secs(1_010_000))
    );
    Ok(())
}

#[test]
fn empty_and_duplicate_snapshot_shapes_are_rejected() -> Result<(), RouteTableError> {
    assert!(matches!(
        MappingSnapshot::new([]),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));

    let gateway_id = gateway(61);
    let session_id = session(610);
    let client_id = ClientId::new("duplicate")?;
    let first = MappingEntry::new(
        client_id.clone(),
        gateway_id,
        session_id,
        binding(6101),
        GatewayLocator::new("gw")?,
    );
    let second = MappingEntry::new(
        client_id,
        gateway_id,
        session_id,
        binding(6102),
        GatewayLocator::new("gw")?,
    );
    assert!(matches!(
        MappingSnapshot::new([first, second]),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    Ok(())
}

#[test]
fn binding_set_transport_reconstruction_rejects_invalid_shapes() -> Result<(), RouteTableError> {
    assert!(matches!(
        BindingSet::from_entries(Vec::new()),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));

    let gateway_id = gateway(62);
    let session_id = session(620);
    let alpha = mapping("alpha", gateway_id, session_id, binding(6201))?;
    let duplicate = alpha.clone();
    let beta = mapping("beta", gateway_id, session_id, binding(6202))?;

    assert!(matches!(
        BindingSet::from_entries(vec![alpha.clone(), duplicate]),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert!(matches!(
        BindingSet::from_entries(vec![alpha.clone(), beta]),
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert_eq!(BindingSet::from_entries(vec![alpha])?.len(), 1);
    Ok(())
}

#[test]
fn a_new_shard_instance_starts_ready_and_empty() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(71);
    let session_id = session(710);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut before_restart = shard(Duration::from_secs(60))?;
    let generation = before_restart.generation();
    let old_lease = before_restart
        .register(context, generation, key.clone(), start)?
        .lease_id();
    before_restart.update(
        context,
        generation,
        &key,
        old_lease,
        RegistrationRevision::FIRST,
        snapshot([mapping("alpha", gateway_id, session_id, binding(7101))?])?,
        start,
    )?;

    let mut after_restart = shard(Duration::from_secs(60))?;
    assert_eq!(after_restart.generation(), generation);
    assert_eq!(after_restart.stats().registration_count, 0);
    assert!(matches!(
        after_restart.resolve(context, generation, &client("alpha")?, start),
        Err(RouteTableError::NotFound)
    ));
    let new_lease = after_restart
        .register(context, generation, key, start)?
        .lease_id();
    assert_ne!(old_lease, new_lease);
    Ok(())
}

#[test]
fn expired_lease_update_and_keepalive_fail_without_recreating_state() -> Result<(), RouteTableError>
{
    let start = Instant::now();
    let gateway_id = gateway(81);
    let session_id = session(810);
    let request_context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(5))?;
    let generation = shard.generation();
    let lease = shard
        .register(request_context, generation, key.clone(), start)?
        .lease_id();
    shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([mapping("alpha", gateway_id, session_id, binding(8101))?])?,
        start,
    )?;

    let expired_update = shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        snapshot([mapping("alpha", gateway_id, session_id, binding(8102))?])?,
        start + Duration::from_secs(5),
    );
    let expired_keepalive = shard.keep_alive(
        request_context,
        generation,
        &key,
        lease,
        start + Duration::from_secs(5),
    );
    assert!(matches!(
        expired_update,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert!(matches!(
        expired_keepalive,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(shard.stats().registration_count, 0);
    assert_eq!(shard.stats().mapping_count, 0);
    assert_eq!(shard.stats().expiry_record_count, 0);
    Ok(())
}

#[test]
fn wrong_authority_snapshot_and_resolve_are_invalid_and_atomic() -> Result<(), RouteTableError> {
    const DIRECTORY: &[u8] = br#"{"format_version":1,"authority_hash":"sha256-modulo-v1","shards":[{"id":"rt-0","endpoint":"rt-0"},{"id":"rt-1","endpoint":"rt-1"},{"id":"rt-2","endpoint":"rt-2"}]}"#;

    let start = Instant::now();
    let gateway_id = gateway(91);
    let session_id = session(910);
    let request_context: RequestContext = context(gateway_id);
    let directory = ShardDirectory::from_json_bytes(DIRECTORY)?;
    let generation = directory.generation();
    let shard_id = ShardId::new("rt-0")?;
    let key = RegistrationKey::new(gateway_id, session_id, shard_id.clone());
    let mut shard = RouteTableShard::new(
        directory,
        shard_id,
        RouteTableConfig::new(Duration::from_secs(30))?,
    )?;
    let lease = shard
        .register(request_context, generation, key.clone(), start)?
        .lease_id();

    // SHA-256("alpha") modulo 3 selects rt-2, not this rt-0 shard.
    let wrong_authority_mapping = MappingEntry::new(
        ClientId::new("alpha")?,
        gateway_id,
        session_id,
        binding(9101),
        GatewayLocator::new("gw-91")?,
    );
    let update = shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([wrong_authority_mapping])?,
        start,
    );
    let resolve = shard.resolve(request_context, generation, &ClientId::new("alpha")?, start);
    assert!(matches!(
        update,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert!(matches!(
        resolve,
        Err(ref error) if error.code() == ErrorCode::InvalidArgument
    ));
    assert_eq!(shard.stats().registration_count, 1);
    assert_eq!(shard.stats().mapping_count, 0);
    assert_eq!(shard.stats().expiry_record_count, 1);
    Ok(())
}

#[test]
fn active_mapping_identity_cannot_change_destination_or_locator() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(101);
    let session_id = session(1010);
    let request_context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(60))?;
    let generation = shard.generation();
    let lease = shard
        .register(request_context, generation, key.clone(), start)?
        .lease_id();
    let binding_id = binding(10101);
    let original = mapping("alpha", gateway_id, session_id, binding_id)?;
    shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([original.clone()])?,
        start,
    )?;

    let changed_destination = MappingEntry::new(
        ClientId::new("beta")?,
        gateway_id,
        session_id,
        binding_id,
        original.gateway_locator().clone(),
    );
    let destination_result = shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        snapshot([changed_destination])?,
        start + Duration::from_secs(1),
    );
    assert!(matches!(
        destination_result,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));

    let changed_locator = MappingEntry::new(
        ClientId::new("alpha")?,
        gateway_id,
        session_id,
        binding_id,
        GatewayLocator::new("different-gateway-locator")?,
    );
    let locator_result = shard.update(
        request_context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        snapshot([changed_locator])?,
        start + Duration::from_secs(2),
    );
    assert!(matches!(
        locator_result,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));

    assert_eq!(
        shard
            .resolve(
                request_context,
                generation,
                &ClientId::new("alpha")?,
                start + Duration::from_secs(2),
            )?
            .entries(),
        &[original]
    );
    assert!(matches!(
        shard.resolve(
            request_context,
            generation,
            &ClientId::new("beta")?,
            start + Duration::from_secs(2),
        ),
        Err(RouteTableError::NotFound)
    ));
    assert_eq!(shard.stats().mapping_count, 1);
    Ok(())
}
