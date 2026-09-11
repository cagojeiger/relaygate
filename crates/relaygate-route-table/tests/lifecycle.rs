mod support;

use std::time::{Duration, Instant};

use relaygate_route_table::{
    BindingId, ErrorCode, GatewayLocator, MappingEntry, RegistrationRevision, RouteAddress,
    RouteTableError,
};
use uuid::Uuid;

use support::{address, binding, context, gateway, key, mapping, session, shard, snapshot};

#[test]
fn equal_destination_names_in_different_namespaces_are_isolated() -> Result<(), RouteTableError> {
    let now = Instant::now();
    let gateway_id = gateway(900);
    let relay_session_id = session(901);
    let registration_key = key(gateway_id, relay_session_id)?;
    let mut shard = shard(Duration::from_secs(60))?;
    let generation = shard.generation();
    let lease_id = shard
        .register(
            context(gateway_id),
            generation,
            registration_key.clone(),
            now,
        )?
        .lease_id();
    let red = "red/shared".parse::<RouteAddress>()?;
    let blue = "blue/shared".parse::<RouteAddress>()?;
    let red_mapping = MappingEntry::new(
        red.clone(),
        gateway_id,
        relay_session_id,
        BindingId::from_uuid(Uuid::from_u128(902)),
        GatewayLocator::new("gw-red")?,
    );
    let blue_mapping = MappingEntry::new(
        blue.clone(),
        gateway_id,
        relay_session_id,
        BindingId::from_uuid(Uuid::from_u128(903)),
        GatewayLocator::new("gw-blue")?,
    );

    shard.update(
        context(gateway_id),
        generation,
        &registration_key,
        lease_id,
        RegistrationRevision::FIRST,
        snapshot([red_mapping.clone(), blue_mapping.clone()])?,
        now,
    )?;

    assert_eq!(
        shard
            .resolve(context(gateway_id), generation, &red, now)?
            .entries(),
        &[red_mapping]
    );
    assert_eq!(
        shard
            .resolve(context(gateway_id), generation, &blue, now)?
            .entries(),
        &[blue_mapping]
    );
    assert_eq!(shard.stats().route_count, 2);
    Ok(())
}

#[test]
fn register_update_resolve_keepalive_and_deregister_form_a_closed_lifecycle()
-> Result<(), RouteTableError> {
    let ttl = Duration::from_secs(30);
    let start = Instant::now();
    let gateway_id = gateway(1);
    let session_id = session(10);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(ttl)?;
    let generation = shard.generation();

    let registered = shard.register(context, generation, key.clone(), start)?;
    assert_eq!(registered.accepted_revision(), None);
    assert_eq!(registered.expires_in(), ttl);
    assert_eq!(
        shard.stats(),
        relaygate_route_table::RouteTableStats {
            registration_count: 1,
            mapping_count: 0,
            route_count: 0,
            expiry_record_count: 1,
        }
    );

    let duplicate = shard.register(
        context,
        generation,
        key.clone(),
        start + Duration::from_secs(5),
    )?;
    assert_eq!(duplicate.lease_id(), registered.lease_id());
    assert_eq!(duplicate.accepted_revision(), None);
    assert_eq!(duplicate.expires_in(), Duration::from_secs(25));

    let alpha = mapping("alpha", gateway_id, session_id, binding(100))?;
    let beta = mapping("beta", gateway_id, session_id, binding(101))?;
    let first_update = shard.update(
        context,
        generation,
        &key,
        registered.lease_id(),
        RegistrationRevision::FIRST,
        snapshot([alpha.clone(), beta.clone()])?,
        start + Duration::from_secs(6),
    )?;
    assert_eq!(
        first_update.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );
    assert_eq!(first_update.expires_in(), Duration::from_secs(24));

    let alpha_bindings = shard.resolve(
        context,
        generation,
        &address("alpha")?,
        start + Duration::from_secs(7),
    )?;
    assert_eq!(alpha_bindings.entries(), std::slice::from_ref(&alpha));

    let revision_two = RegistrationRevision::new(2)?;
    shard.update(
        context,
        generation,
        &key,
        registered.lease_id(),
        revision_two,
        snapshot([beta.clone()])?,
        start + Duration::from_secs(8),
    )?;
    assert!(matches!(
        shard.resolve(
            context,
            generation,
            &address("alpha")?,
            start + Duration::from_secs(8),
        ),
        Err(RouteTableError::NotFound)
    ));
    assert_eq!(
        shard
            .resolve(
                context,
                generation,
                &address("beta")?,
                start + Duration::from_secs(8),
            )?
            .entries(),
        &[beta]
    );

    let kept_alive = shard.keep_alive(
        context,
        generation,
        &key,
        registered.lease_id(),
        start + Duration::from_secs(20),
    )?;
    assert_eq!(kept_alive.accepted_revision(), Some(revision_two));
    assert_eq!(kept_alive.expires_in(), ttl);
    assert_eq!(shard.stats().expiry_record_count, 1);

    shard.deregister(
        context,
        generation,
        &key,
        registered.lease_id(),
        start + Duration::from_secs(21),
    )?;
    shard.deregister(
        context,
        generation,
        &key,
        registered.lease_id(),
        start + Duration::from_secs(21),
    )?;
    assert!(matches!(
        shard.resolve(
            context,
            generation,
            &address("beta")?,
            start + Duration::from_secs(21),
        ),
        Err(RouteTableError::NotFound)
    ));
    assert_eq!(shard.stats().registration_count, 0);
    assert_eq!(shard.stats().mapping_count, 0);
    assert_eq!(shard.stats().expiry_record_count, 0);
    Ok(())
}

#[test]
fn revision_rules_are_monotonic_atomic_and_idempotent() -> Result<(), RouteTableError> {
    let start = Instant::now();
    let gateway_id = gateway(2);
    let session_id = session(20);
    let context = context(gateway_id);
    let key = key(gateway_id, session_id)?;
    let mut shard = shard(Duration::from_secs(60))?;
    let generation = shard.generation();
    let lease = shard
        .register(context, generation, key.clone(), start)?
        .lease_id();
    let alpha = mapping("alpha", gateway_id, session_id, binding(200))?;
    let beta = mapping("beta", gateway_id, session_id, binding(201))?;

    let invalid_first = shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(2)?,
        snapshot([alpha.clone()])?,
        start,
    );
    assert!(matches!(
        invalid_first,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(shard.stats().mapping_count, 0);

    shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([alpha.clone(), beta.clone()])?,
        start,
    )?;
    let same_revision_reordered = shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([beta.clone(), alpha.clone()])?,
        start + Duration::from_secs(1),
    )?;
    assert_eq!(
        same_revision_reordered.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );
    assert_eq!(
        same_revision_reordered.expires_in(),
        Duration::from_secs(59)
    );

    let conflicting_same_revision = shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::FIRST,
        snapshot([alpha.clone()])?,
        start + Duration::from_secs(2),
    );
    assert!(matches!(
        conflicting_same_revision,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(shard.stats().mapping_count, 2);

    let revision_eight = RegistrationRevision::new(8)?;
    shard.update(
        context,
        generation,
        &key,
        lease,
        revision_eight,
        snapshot([beta.clone()])?,
        start + Duration::from_secs(3),
    )?;
    let delayed_revision = shard.update(
        context,
        generation,
        &key,
        lease,
        RegistrationRevision::new(7)?,
        snapshot([alpha])?,
        start + Duration::from_secs(4),
    );
    assert!(matches!(
        delayed_revision,
        Err(ref error) if error.code() == ErrorCode::FailedPrecondition
    ));
    assert_eq!(
        shard
            .resolve(
                context,
                generation,
                &address("beta")?,
                start + Duration::from_secs(4),
            )?
            .entries(),
        &[beta]
    );
    assert_eq!(shard.expire_due(start + Duration::from_secs(60)), 1);
    assert_eq!(shard.stats().registration_count, 0);
    Ok(())
}
