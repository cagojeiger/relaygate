use relaygate_protocol::{DestinationId, PipeId, SessionId};
use uuid::Uuid;

#[test]
fn same_connection_id_is_distinct_across_origin_sessions() {
    let first_session = SessionId::from_uuid(Uuid::from_u128(1));
    let second_session = SessionId::from_uuid(Uuid::from_u128(2));

    let first = PipeId::new(first_session, 1);
    let second = PipeId::new(second_session, 1);

    assert_ne!(first, second);
    assert_eq!(first.origin_session_id(), first_session);
    assert_eq!(second.origin_session_id(), second_session);
    assert_eq!(first.connection_id(), 1);
    assert_eq!(second.connection_id(), 1);
}

#[test]
fn destination_id_preserves_application_uuid() -> Result<(), &'static str> {
    let application_id = Uuid::new_v4();
    let destination_id =
        DestinationId::try_from_uuid(application_id).ok_or("generated UUIDv4 was rejected")?;

    assert_eq!(destination_id.as_uuid(), application_id);
    Ok(())
}
