use relaygate_protocol::{PipeId, SessionId};
use uuid::Uuid;

#[test]
fn same_connection_id_is_distinct_across_connector_sessions() {
    let first_session = SessionId::from_uuid(Uuid::from_u128(1));
    let second_session = SessionId::from_uuid(Uuid::from_u128(2));

    let first = PipeId::new(first_session, 1);
    let second = PipeId::new(second_session, 1);

    assert_ne!(first, second);
    assert_eq!(first.connector_session_id(), first_session);
    assert_eq!(second.connector_session_id(), second_session);
    assert_eq!(first.connection_id(), 1);
    assert_eq!(second.connection_id(), 1);
}
