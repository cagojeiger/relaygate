//! Outbound SDK delivery: the bounded writer-queue handoff for one session and
//! the failure it reports back to the runtime, which feeds it into the state
//! core through `transition`. `GatewayState` decides which frame goes to which
//! session; this module owns how that handoff can fail (queue full or closed),
//! the predicate for which frames may coalesce into one terminal batch (the
//! coalescing pass itself lives in `gateway::effects`) and the rejection
//! observation.

use relaygate_protocol::{ErrorCode, Frame, PipeId, SessionId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) target: SessionId,
    pub(crate) frame: Frame,
    pub(super) sender: mpsc::Sender<SdkWriterItem>,
    pub(super) cancellation: CancellationToken,
}

impl Delivery {
    pub(crate) fn deliver(self) -> Option<DeliveryFailure> {
        let offer_pipe_id = match &self.frame {
            Frame::Offer { pipe_id, .. } => Some(*pipe_id),
            _ => None,
        };
        if let Err(error) = self.sender.try_send(SdkWriterItem::Single(self.frame)) {
            let queue_state = match error {
                mpsc::error::TrySendError::Full(_) => "full",
                mpsc::error::TrySendError::Closed(_) => "closed",
            };
            observe_writer_queue_rejection(
                self.target,
                queue_state,
                if offer_pipe_id.is_some() {
                    "offer"
                } else {
                    "other"
                },
                1,
            );
            if queue_state == "full"
                && let Some(pipe_id) = offer_pipe_id
            {
                return Some(DeliveryFailure::OfferQueueFull {
                    acceptor: self.target,
                    pipe_id,
                });
            }
            self.cancellation.cancel();
            return Some(DeliveryFailure::SessionUnavailable(self.target));
        }
        None
    }

    pub(crate) fn into_terminal_batch(self) -> Result<TerminalBatchDelivery, Self> {
        if !is_transport_terminal_frame(&self.frame) {
            return Err(self);
        }
        Ok(TerminalBatchDelivery {
            target: self.target,
            frames: vec![self.frame],
            sender: self.sender,
            cancellation: self.cancellation,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TerminalBatchDelivery {
    pub(crate) target: SessionId,
    frames: Vec<Frame>,
    sender: mpsc::Sender<SdkWriterItem>,
    cancellation: CancellationToken,
}

impl TerminalBatchDelivery {
    pub(crate) fn push(&mut self, delivery: Delivery) -> Result<(), Delivery> {
        if delivery.target != self.target || !is_transport_terminal_frame(&delivery.frame) {
            return Err(delivery);
        }
        self.frames.push(delivery.frame);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn frames(&self) -> &[Frame] {
        &self.frames
    }

    pub(crate) fn deliver(self) -> Option<DeliveryFailure> {
        let rejected_frames = self.frames.len() as u64;
        if let Err(error) = self
            .sender
            .try_send(SdkWriterItem::TerminalBatch(self.frames))
        {
            let queue_state = match error {
                mpsc::error::TrySendError::Full(_) => "full",
                mpsc::error::TrySendError::Closed(_) => "closed",
            };
            observe_writer_queue_rejection(
                self.target,
                queue_state,
                "terminal_batch",
                rejected_frames,
            );
            self.cancellation.cancel();
            return Some(DeliveryFailure::SessionUnavailable(self.target));
        }
        None
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SdkWriterItem {
    Single(Frame),
    TerminalBatch(Vec<Frame>),
}

fn is_transport_terminal_frame(frame: &Frame) -> bool {
    matches!(frame, Frame::Reset { .. } | Frame::DialFailed { .. })
}

fn observe_writer_queue_rejection(
    session_id: SessionId,
    queue_state: &'static str,
    frame: &'static str,
    rejected_frames: u64,
) {
    tracing::warn!(
        component = "gateway",
        event = "gateway.session.writer_queue_rejected",
        session_id = %session_id.as_uuid(),
        queue_state,
        frame,
        rejected_frames,
        error_code = ?ErrorCode::ResourceExhausted,
        "bounded SDK writer queue could not accept outbound frames"
    );
    metrics::counter!(
        "relaygate_gateway_writer_queue_rejections_total",
        "reason" => queue_state
    )
    .increment(rejected_frames);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryFailure {
    OfferQueueFull {
        acceptor: SessionId,
        pipe_id: PipeId,
    },
    SessionUnavailable(SessionId),
}
