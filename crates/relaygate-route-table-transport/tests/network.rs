mod support;

use std::time::Duration;

use relaygate_route_table::{DestinationId, LeaseId, RegistrationRevision};
use relaygate_route_table_transport::ErrorCode;
use tokio::net::TcpStream;
use uuid::Uuid;

use support::{
    RunningService, TestResult, binding, gateway, mapping_snapshot, registration_key, session,
};

#[tokio::test]
async fn full_registration_lifecycle_and_ready_empty_not_found() -> TestResult {
    let service = RunningService::start(Duration::from_secs(5), [("gw-a", "key-a")]).await?;
    let gateway_id = gateway(1);
    let relay_session_id = session(11);
    let key = registration_key(gateway_id, relay_session_id)?;
    let client = service.connect("gw-a", gateway_id, "key-a").await?;
    let destination_id = DestinationId::new("11111111-1111-4111-8111-111111111111")?;

    let empty = client
        .resolve(service.generation, &destination_id)
        .await
        .err();
    assert_eq!(empty.map(|error| error.code()), Some(ErrorCode::NotFound));

    let registered = client.register(service.generation, &key).await?;
    assert_eq!(registered.accepted_revision(), None);
    assert!(registered.expires_in() > Duration::ZERO);

    let snapshot = mapping_snapshot(
        destination_id.as_str(),
        gateway_id,
        relay_session_id,
        binding(111),
    )?;
    let updated = client
        .update(
            service.generation,
            &key,
            registered.lease_id(),
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    assert_eq!(
        updated.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    let bindings = client.resolve(service.generation, &destination_id).await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings.entries()[0].destination_id(), &destination_id);

    let kept_alive = client
        .keep_alive(service.generation, &key, registered.lease_id())
        .await?;
    assert_eq!(
        kept_alive.accepted_revision(),
        Some(RegistrationRevision::FIRST)
    );

    client
        .deregister(service.generation, &key, registered.lease_id())
        .await?;
    let removed = client
        .resolve(service.generation, &destination_id)
        .await
        .err();
    assert_eq!(removed.map(|error| error.code()), Some(ErrorCode::NotFound));

    service.stop().await
}

#[tokio::test]
async fn authentication_and_owner_mismatch_are_terminal_and_do_not_create_state() -> TestResult {
    let service = RunningService::start(
        Duration::from_secs(5),
        [("gw-a", "key-a"), ("gw-b", "key-b")],
    )
    .await?;

    let wrong = service
        .connect("gw-a", gateway(1), "wrong-secret")
        .await
        .err();
    assert_eq!(
        wrong.as_ref().map(|error| error.code()),
        Some(ErrorCode::Unauthenticated)
    );
    assert!(
        !wrong
            .as_ref()
            .is_some_and(|error| error.to_string().contains("wrong-secret"))
    );

    let unknown = service
        .connect("gw-unknown", gateway(1), "key-a")
        .await
        .err();
    assert_eq!(
        unknown.map(|error| error.code()),
        Some(ErrorCode::Unauthenticated)
    );

    let client_a = service.connect("gw-a", gateway(1), "key-a").await?;
    let key_b = registration_key(gateway(2), session(22))?;
    let denied = client_a.register(service.generation, &key_b).await.err();
    assert_eq!(
        denied.map(|error| error.code()),
        Some(ErrorCode::PermissionDenied)
    );

    let client_b = service.connect("gw-b", gateway(2), "key-b").await?;
    client_b
        .deregister(
            service.generation,
            &key_b,
            LeaseId::from_uuid(Uuid::from_u128(999)),
        )
        .await?;

    service.stop().await
}

#[tokio::test]
async fn service_loss_is_reported_as_unavailable_without_reconnect() -> TestResult {
    let service = RunningService::start(Duration::from_secs(5), [("gw-a", "key-a")]).await?;
    let generation = service.generation;
    let client = service.connect("gw-a", gateway(1), "key-a").await?;
    service.stop().await?;

    let error = client
        .resolve(
            generation,
            &DestinationId::new("11111111-1111-4111-8111-111111111111")?,
        )
        .await
        .err();
    assert_eq!(
        error.map(|error| error.code()),
        Some(ErrorCode::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn restart_starts_empty_and_recovers_only_from_a_new_lease_snapshot() -> TestResult {
    let gateway_id = gateway(1);
    let relay_session_id = session(11);
    let key = registration_key(gateway_id, relay_session_id)?;
    let destination_id = DestinationId::new("22222222-2222-4222-8222-222222222222")?;
    let snapshot = mapping_snapshot(
        destination_id.as_str(),
        gateway_id,
        relay_session_id,
        binding(111),
    )?;

    let service_a = RunningService::start(Duration::from_secs(5), [("gw-a", "key-a")]).await?;
    let generation = service_a.generation;
    let client_a = service_a.connect("gw-a", gateway_id, "key-a").await?;
    let registered = client_a.register(generation, &key).await?;
    let old_lease = registered.lease_id();
    client_a
        .update(
            generation,
            &key,
            old_lease,
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    assert_eq!(
        client_a.resolve(generation, &destination_id).await?.len(),
        1
    );
    service_a.stop().await?;

    let service_b = RunningService::start(Duration::from_secs(5), [("gw-a", "key-a")]).await?;
    assert_eq!(service_b.generation, generation);
    let client_b = service_b.connect("gw-a", gateway_id, "key-a").await?;
    let empty = client_b.resolve(generation, &destination_id).await.err();
    assert_eq!(empty.map(|error| error.code()), Some(ErrorCode::NotFound));

    let stale_keep_alive = client_b.keep_alive(generation, &key, old_lease).await.err();
    assert_eq!(
        stale_keep_alive.map(|error| error.code()),
        Some(ErrorCode::FailedPrecondition)
    );
    let stale_update = client_b
        .update(
            generation,
            &key,
            old_lease,
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await
        .err();
    assert_eq!(
        stale_update.map(|error| error.code()),
        Some(ErrorCode::FailedPrecondition)
    );

    let fresh = client_b.register(generation, &key).await?;
    assert_ne!(fresh.lease_id(), old_lease);
    client_b
        .update(
            generation,
            &key,
            fresh.lease_id(),
            RegistrationRevision::FIRST,
            &snapshot,
        )
        .await?;
    let restored = client_b.resolve(generation, &destination_id).await?;
    assert_eq!(restored.len(), 1);
    assert_eq!(restored.entries()[0].destination_id(), &destination_id);
    service_b.stop().await
}

#[tokio::test]
async fn connection_limit_rejects_the_next_handshake_as_resource_exhausted() -> TestResult {
    let service =
        RunningService::start_with_max_connections(Duration::from_secs(5), [("gw-a", "key-a")], 1)
            .await?;
    let first = service.connect("gw-a", gateway(1), "key-a").await?;

    let rejected = service.connect("gw-a", gateway(2), "key-a").await.err();
    assert_eq!(
        rejected.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );

    drop(first);
    service.stop().await
}

#[tokio::test]
async fn shutdown_completes_with_an_unread_handshake_stalled_connection() -> TestResult {
    let service =
        RunningService::start_with_max_connections(Duration::from_secs(5), [("gw-a", "key-a")], 1)
            .await?;
    let _stalled = TcpStream::connect(service.endpoint).await?;

    let rejected = service.connect("gw-a", gateway(2), "key-a").await.err();
    assert_eq!(
        rejected.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );
    tokio::time::timeout(Duration::from_secs(1), service.stop())
        .await
        .map_err(|_| std::io::Error::other("RouteTable shutdown timed out"))??;
    Ok(())
}

#[tokio::test]
async fn oversized_binding_set_returns_resource_exhausted_and_connection_stays_usable() -> TestResult
{
    let service =
        RunningService::start_with_limits(Duration::from_secs(30), [("gw-a", "key-a")], 8, 1024)
            .await?;
    let gateway_id = gateway(1);
    let client = service.connect("gw-a", gateway_id, "key-a").await?;
    let generation = service.generation;
    let large_destination_id = DestinationId::new("33333333-3333-4333-8333-333333333333")?;

    for index in 0_u128..16 {
        let relay_session_id = session(1_000 + index);
        let key = registration_key(gateway_id, relay_session_id)?;
        let registered = client.register(generation, &key).await?;
        let snapshot = mapping_snapshot(
            large_destination_id.as_str(),
            gateway_id,
            relay_session_id,
            binding(2_000 + index),
        )?;
        client
            .update(
                generation,
                &key,
                registered.lease_id(),
                RegistrationRevision::FIRST,
                &snapshot,
            )
            .await?;
    }

    let small_destination_id = DestinationId::new("44444444-4444-4444-8444-444444444444")?;
    let small_session = session(9_000);
    let small_key = registration_key(gateway_id, small_session)?;
    let small_registration = client.register(generation, &small_key).await?;
    let small_snapshot = mapping_snapshot(
        small_destination_id.as_str(),
        gateway_id,
        small_session,
        binding(9_001),
    )?;
    client
        .update(
            generation,
            &small_key,
            small_registration.lease_id(),
            RegistrationRevision::FIRST,
            &small_snapshot,
        )
        .await?;

    let oversized = client
        .resolve(generation, &large_destination_id)
        .await
        .err();
    assert_eq!(
        oversized.map(|error| error.code()),
        Some(ErrorCode::ResourceExhausted)
    );
    let small = client.resolve(generation, &small_destination_id).await?;
    assert_eq!(small.len(), 1);
    assert_eq!(small.entries()[0].destination_id(), &small_destination_id);

    service.stop().await
}
