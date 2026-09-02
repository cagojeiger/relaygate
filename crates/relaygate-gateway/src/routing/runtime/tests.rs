use std::{collections::BTreeMap, error::Error, time::Duration};

use relaygate_route_table::{
    BindingId, ClientId, GatewayId, GatewayLocator, LeaseId, ListenerSessionId, MappingEntry,
    MappingSnapshot, RegistrationAck, RegistrationKey, RegistrationRevision, RouteTableError,
    ShardId,
};
use relaygate_route_table_transport::{ErrorCode, TransportError};
use tokio::time::Instant;
use uuid::Uuid;

use super::{OperationCompletion, OperationResult, apply_epoch_scoped_operation_completion};
use crate::routing::lifecycle::{RegistrationAction, RegistrationState};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn old_epoch_terminal_completion_retries_current_registration() -> TestResult {
    let now = Instant::now();
    let retry = Duration::from_millis(10);
    let session_id = ListenerSessionId::from_uuid(Uuid::from_u128(2));
    let mut state = RegistrationState::new(
        RegistrationKey::new(
            GatewayId::from_uuid(Uuid::from_u128(1)),
            session_id,
            ShardId::new("rt-0")?,
        ),
        1,
        Some(snapshot()?),
        now,
        retry,
        Duration::from_secs(1),
    );

    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    state.register_succeeded(&register, registration_ack(None), now);
    let update = state.begin_next(now)?.ok_or("missing UPDATE")?;
    state.update_succeeded(
        &update,
        registration_ack(Some(RegistrationRevision::FIRST)),
        now,
    );
    assert!(state.is_synced());

    let keep_alive_at = now + Duration::from_secs(3);
    let old_keep_alive = state
        .begin_next(keep_alive_at)?
        .ok_or("missing KEEP_ALIVE")?;
    assert!(matches!(
        old_keep_alive.action,
        RegistrationAction::KeepAlive { .. }
    ));
    state.connection_lost(keep_alive_at);

    let completion = OperationCompletion {
        epoch: 1,
        ticket: old_keep_alive,
        result: OperationResult::Registration(Err(TransportError::from(
            RouteTableError::PermissionDenied,
        ))),
    };
    let mut registrations = BTreeMap::from([(session_id, state)]);

    let propagated = apply_epoch_scoped_operation_completion(
        &mut registrations,
        &completion,
        Some(2),
        keep_alive_at,
    );
    assert!(propagated.is_none());

    let state = registrations
        .get_mut(&session_id)
        .ok_or("missing registration")?;
    assert_eq!(
        state.active_lease().map(|(_, lease_id)| lease_id),
        Some(LeaseId::from_uuid(Uuid::from_u128(10)))
    );
    assert!(!state.is_synced());
    assert!(state.begin_next(keep_alive_at)?.is_none());

    let retry_at = keep_alive_at + retry;
    let validation = state
        .begin_next(retry_at)?
        .ok_or("missing current KEEP_ALIVE")?;
    assert!(matches!(
        validation.action,
        RegistrationAction::KeepAlive { .. }
    ));
    state.keep_alive_succeeded(
        &validation,
        registration_ack(Some(RegistrationRevision::FIRST)),
        retry_at,
    );

    let update = state
        .begin_next(retry_at)?
        .ok_or("missing current UPDATE")?;
    assert!(matches!(update.action, RegistrationAction::Update { .. }));
    state.update_succeeded(
        &update,
        registration_ack(Some(RegistrationRevision::new(2)?)),
        retry_at,
    );
    assert!(state.is_synced());
    Ok(())
}

#[test]
fn current_epoch_terminal_completion_keeps_existing_error_policy() -> TestResult {
    let now = Instant::now();
    let session_id = ListenerSessionId::from_uuid(Uuid::from_u128(2));
    let mut state = RegistrationState::new(
        RegistrationKey::new(
            GatewayId::from_uuid(Uuid::from_u128(1)),
            session_id,
            ShardId::new("rt-0")?,
        ),
        1,
        Some(snapshot()?),
        now,
        Duration::from_millis(10),
        Duration::from_secs(1),
    );
    let register = state.begin_next(now)?.ok_or("missing REGISTER")?;
    let completion = OperationCompletion {
        epoch: 2,
        ticket: register,
        result: OperationResult::Registration(Err(TransportError::from(
            RouteTableError::PermissionDenied,
        ))),
    };
    let mut registrations = BTreeMap::from([(session_id, state)]);

    let propagated =
        apply_epoch_scoped_operation_completion(&mut registrations, &completion, Some(2), now)
            .ok_or("missing terminal error")?;
    assert_eq!(propagated.code(), ErrorCode::PermissionDenied);
    let state = registrations
        .get_mut(&session_id)
        .ok_or("missing registration")?;
    assert!(state.begin_next(now + Duration::from_secs(60))?.is_none());
    Ok(())
}

fn snapshot() -> TestResult<MappingSnapshot> {
    Ok(MappingSnapshot::new([MappingEntry::new(
        ClientId::new("client-a")?,
        GatewayId::from_uuid(Uuid::from_u128(1)),
        ListenerSessionId::from_uuid(Uuid::from_u128(2)),
        BindingId::from_uuid(Uuid::from_u128(3)),
        GatewayLocator::new("gw-a.internal:27431")?,
    )])?)
}

fn registration_ack(revision: Option<RegistrationRevision>) -> RegistrationAck {
    RegistrationAck::from_parts(
        LeaseId::from_uuid(Uuid::from_u128(10)),
        revision,
        Duration::from_secs(5),
    )
}
