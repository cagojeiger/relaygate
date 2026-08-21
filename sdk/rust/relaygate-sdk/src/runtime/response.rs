use super::*;

mod binding;
mod listener;
mod open;
mod pipe;

use binding::*;
use listener::*;
use open::*;
use pipe::*;

pub(crate) async fn dispatch_response(
    shared: &Arc<Shared>,
    response: wire::ConnectResponse,
) -> Result<(), SessionError> {
    let message = response
        .message
        .ok_or(SessionError::Protocol("ConnectResponse omitted message"))?;
    match message {
        connect_response::Message::ClientSessionOpened(_) => {
            return Err(SessionError::Protocol("duplicate ClientSessionOpened"));
        }
        connect_response::Message::ListenerBound(bound) => listener_bound(shared, bound)?,
        connect_response::Message::ListenerBindFailed(failed) => {
            listener_bind_failed(shared, failed)?;
        }
        connect_response::Message::ListenerUnbound(unbound) => listener_unbound(shared, unbound)?,
        connect_response::Message::ListenerUnbindFailed(failed) => {
            listener_unbind_failed(shared, failed)?;
        }
        connect_response::Message::ListenerOffer(offer) => listener_offer(shared, offer).await?,
        connect_response::Message::ListenerEstablished(established) => {
            listener_established(shared, established)?;
        }
        connect_response::Message::ListenerConfirmationAcknowledged(acknowledged) => {
            listener_confirmation_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::ListenerTerminated(terminated) => {
            listener_terminated(shared, terminated)?;
        }
        connect_response::Message::PipeOpened(opened) => pipe_opened(shared, opened).await?,
        connect_response::Message::PipeOpenFailed(failed) => pipe_open_failed(shared, failed)?,
        connect_response::Message::PipeOpenUnknown(unknown) => pipe_open_unknown(shared, unknown)?,
        connect_response::Message::ListenerDecisionRejected(rejected) => {
            listener_decision_rejected(shared, rejected)?;
        }
        connect_response::Message::OpenCancelAcknowledged(acknowledged) => {
            open_cancel_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::PipeCloseAcknowledged(acknowledged) => {
            pipe_close_acknowledged(shared, acknowledged)?;
        }
        connect_response::Message::OpenRequestRejected(rejected) => {
            open_request_rejected(shared, rejected)?;
        }
        connect_response::Message::PipePayload(payload) => pipe_payload(shared, payload)?,
        connect_response::Message::PipePayloadReceived(received) => {
            pipe_payload_received(shared, received)?;
        }
        connect_response::Message::PipeTerminated(terminated) => {
            pipe_terminated(shared, terminated)?;
        }
        connect_response::Message::PipePayloadRejected(rejected) => {
            pipe_payload_rejected(shared, rejected)?;
        }
    }
    Ok(())
}
