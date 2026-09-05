use std::collections::BTreeMap;

use relaygate_route_table::{RelaySessionId, ShardDirectoryGeneration};
use relaygate_route_table_transport::{ErrorCode, RouteTableClient, TransportError};
use tokio::time::Instant;

use super::{
    super::lifecycle::{OperationTicket, RegistrationAction, RegistrationState},
    BoxFuture,
};

pub(super) struct OperationCompletion {
    pub(super) epoch: u64,
    pub(super) ticket: OperationTicket,
    pub(super) result: OperationResult,
}

pub(super) enum OperationResult {
    Registration(Result<relaygate_route_table::RegistrationAck, TransportError>),
    Deregister(Result<(), TransportError>),
}

pub(super) fn execute_operation(
    epoch: u64,
    client: RouteTableClient,
    generation: ShardDirectoryGeneration,
    ticket: OperationTicket,
) -> BoxFuture<OperationCompletion> {
    Box::pin(async move {
        let result = match &ticket.action {
            RegistrationAction::Register { key } => {
                OperationResult::Registration(client.register(generation, key).await)
            }
            RegistrationAction::Update {
                key,
                lease_id,
                revision,
                snapshot,
            } => OperationResult::Registration(
                client
                    .update(generation, key, *lease_id, *revision, snapshot)
                    .await,
            ),
            RegistrationAction::KeepAlive { key, lease_id } => {
                OperationResult::Registration(client.keep_alive(generation, key, *lease_id).await)
            }
            RegistrationAction::Deregister { key, lease_id } => {
                OperationResult::Deregister(client.deregister(generation, key, *lease_id).await)
            }
        };
        OperationCompletion {
            epoch,
            ticket,
            result,
        }
    })
}

pub(super) fn apply_epoch_scoped_operation_completion(
    registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
    completion: &OperationCompletion,
    current_epoch: Option<u64>,
    now: Instant,
) -> Option<TransportError> {
    if completion.epoch != current_epoch.unwrap_or_default() {
        if let Some(state) = registration_for_ticket(registrations, &completion.ticket) {
            state.transient_failure(&completion.ticket, now);
        }
        return None;
    }
    apply_operation_completion(registrations, completion, now)
}

/// Applies a completion only to the RegistrationKey captured in its ticket.
/// Returns the transport error so the worker can update shared connection state.
fn apply_operation_completion(
    registrations: &mut BTreeMap<RelaySessionId, RegistrationState>,
    completion: &OperationCompletion,
    now: Instant,
) -> Option<TransportError> {
    let state = registration_for_ticket(registrations, &completion.ticket)?;
    match (&completion.ticket.action, &completion.result) {
        (RegistrationAction::Deregister { .. }, OperationResult::Deregister(result)) => {
            state.finish_deregister(&completion.ticket);
            result.clone().err()
        }
        (RegistrationAction::Register { .. }, OperationResult::Registration(Ok(ack))) => {
            state.register_succeeded(&completion.ticket, *ack, now);
            None
        }
        (RegistrationAction::Update { .. }, OperationResult::Registration(Ok(ack))) => {
            state.update_succeeded(&completion.ticket, *ack, now);
            None
        }
        (RegistrationAction::KeepAlive { .. }, OperationResult::Registration(Ok(ack))) => {
            state.keep_alive_succeeded(&completion.ticket, *ack, now);
            None
        }
        (_, OperationResult::Registration(Err(error))) => {
            match error.code() {
                ErrorCode::FailedPrecondition => {
                    state.precondition_failed(&completion.ticket, now);
                }
                ErrorCode::Unauthenticated
                | ErrorCode::PermissionDenied
                | ErrorCode::InvalidArgument
                | ErrorCode::NotFound => state.terminal_failure(&completion.ticket),
                ErrorCode::Unavailable
                | ErrorCode::DeadlineExceeded
                | ErrorCode::ResourceExhausted
                | ErrorCode::ProtocolError
                | ErrorCode::Internal => state.transient_failure(&completion.ticket, now),
            }
            Some(error.clone())
        }
        _ => {
            state.mark_terminal();
            None
        }
    }
}

fn registration_for_ticket<'a>(
    registrations: &'a mut BTreeMap<RelaySessionId, RegistrationState>,
    ticket: &OperationTicket,
) -> Option<&'a mut RegistrationState> {
    let key = match &ticket.action {
        RegistrationAction::Register { key }
        | RegistrationAction::Update { key, .. }
        | RegistrationAction::KeepAlive { key, .. }
        | RegistrationAction::Deregister { key, .. } => key,
    };
    registrations.get_mut(&key.relay_session_id())
}

pub(in crate::routing) const fn is_connection_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unavailable
            | ErrorCode::DeadlineExceeded
            | ErrorCode::ProtocolError
            | ErrorCode::Internal
    )
}

pub(in crate::routing) const fn is_terminal_control_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied | ErrorCode::FailedPrecondition
    )
}
