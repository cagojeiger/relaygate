use relaygate_protocol::{ErrorCode, Frame, PeerObservation, SessionId};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::state::GatewayAction;

use super::SessionError;

pub(super) fn is_local_rejection(actions: &[GatewayAction], session_id: SessionId) -> bool {
    let [GatewayAction::SendSdkFrame(delivery)] = actions else {
        return false;
    };
    delivery.target == session_id
        && matches!(
            delivery.frame,
            Frame::DialFailed {
                code: ErrorCode::ResourceExhausted,
                observation: PeerObservation::NotObserved,
                ..
            } | Frame::PublishFailed {
                code: ErrorCode::ResourceExhausted,
                ..
            }
        )
}

// Only the requesting session waits; no state lock or shared effects loop is held.
pub(super) async fn send_rejection(
    sender: &mpsc::Sender<Frame>,
    frame: Frame,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SessionError> {
    let reason = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(SessionError::AdmissionResponseUnavailable),
        _ = tokio::time::sleep_until(deadline) => "timeout",
        result = sender.send(frame) => {
            match result {
                Ok(()) => return Ok(()),
                Err(_) => "closed",
            }
        }
    };
    metrics::counter!(
        "relaygate_gateway_writer_queue_rejections_total",
        "reason" => reason
    )
    .increment(1);
    tracing::warn!(
        component = "gateway",
        event = "gateway.session.admission_response_failed",
        reason,
        "SDK admission response delivery failed"
    );
    Err(SessionError::AdmissionResponseUnavailable)
}

#[cfg(test)]
mod tests;
