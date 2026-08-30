//! Ordered transport-event forwarding and scoped loss reconciliation.

use relaygate_protocol::ErrorCode;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::Manager;
use crate::peer::{
    event::{PeerEvent, PeerFailure},
    identity::OpenIdentity,
    transport::TransportNotice,
};

impl Manager {
    pub(super) fn handle_transport_notice(
        &mut self,
        notice: TransportNotice,
        shutting_down: bool,
    ) -> Result<(), PeerFailure> {
        match notice {
            TransportNotice::Event(event) => {
                if !shutting_down {
                    self.emit_event(event)?;
                }
            }
            TransportNotice::StreamEnded { key, open_identity } => {
                if !self.active_opens.contains(open_identity)
                    && self.assignments.get(&open_identity) == Some(&key.peer_transport_id())
                {
                    self.assignments.remove(&open_identity);
                }
            }
            TransportNotice::AttemptEnded { open_identity } => {
                if !self.active_opens.contains(open_identity) {
                    self.assignments.remove(&open_identity);
                }
            }
            TransportNotice::TransportLost {
                peer_gateway_id,
                peer_transport_id,
                streams,
            } => {
                self.pool.remove_transport(peer_transport_id);
                self.transports.remove(&peer_transport_id);
                if let Ok(mut shared) = self.shared_transports.write() {
                    shared.remove(&peer_transport_id);
                }
                let lost_assignments: Vec<OpenIdentity> = self
                    .assignments
                    .iter()
                    .filter_map(|(identity, assigned)| {
                        (*assigned == peer_transport_id).then_some(*identity)
                    })
                    .collect();
                for identity in lost_assignments {
                    self.assignments.remove(&identity);
                    // This is idempotent with actor-side release for current
                    // streams and also covers OPEN commands accepted by the
                    // bounded transport queue but not processed before loss.
                    self.active_opens.release(identity);
                }
                self.counts.update_pool(&self.pool);
                if !shutting_down {
                    self.emit_event(PeerEvent::TransportLost {
                        peer_gateway_id,
                        peer_transport_id,
                        streams,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn emit_event(&self, event: PeerEvent) -> Result<(), PeerFailure> {
        try_emit_event(&self.events, &self.shutdown, event)
    }
}

fn try_emit_event(
    events: &mpsc::Sender<PeerEvent>,
    shutdown: &CancellationToken,
    event: PeerEvent,
) -> Result<(), PeerFailure> {
    if shutdown.is_cancelled() {
        return Ok(());
    }
    match events.try_send(event) {
        Ok(()) => Ok(()),
        Err(error) if shutdown.is_cancelled() => {
            // RunningGateway can cancel the runtime and drop the receiver
            // before the manager selects its shutdown branch. Rejection in
            // that window is expected shutdown cleanup.
            drop(error);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(_)) => Err(PeerFailure::not_observed(
            ErrorCode::ResourceExhausted,
            "peer event queue is full",
        )),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(PeerFailure::not_observed(
            ErrorCode::Unavailable,
            "peer event receiver is closed",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use relaygate_route_table::GatewayId;

    use super::*;
    use crate::peer::identity::PeerTransportId;

    fn loss_event() -> PeerEvent {
        PeerEvent::TransportLost {
            peer_gateway_id: GatewayId::new(),
            peer_transport_id: PeerTransportId::new(),
            streams: Vec::new(),
        }
    }

    #[test]
    fn cancelled_shutdown_ignores_closed_or_full_event_queue() -> Result<(), Box<dyn Error>> {
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let (closed_sender, closed_receiver) = mpsc::channel(1);
        drop(closed_receiver);
        assert!(try_emit_event(&closed_sender, &shutdown, loss_event()).is_ok());

        let (full_sender, _full_receiver) = mpsc::channel(1);
        full_sender.try_send(loss_event())?;
        assert!(try_emit_event(&full_sender, &shutdown, loss_event()).is_ok());
        Ok(())
    }

    #[test]
    fn live_runtime_still_fails_closed_on_closed_or_full_event_queue() -> Result<(), Box<dyn Error>>
    {
        let shutdown = CancellationToken::new();

        let (closed_sender, closed_receiver) = mpsc::channel(1);
        drop(closed_receiver);
        let closed = try_emit_event(&closed_sender, &shutdown, loss_event())
            .err()
            .ok_or("live closed event receiver must fail")?;
        assert_eq!(closed.code(), ErrorCode::Unavailable);

        let (full_sender, _full_receiver) = mpsc::channel(1);
        full_sender.try_send(loss_event())?;
        let full = try_emit_event(&full_sender, &shutdown, loss_event())
            .err()
            .ok_or("live full event queue must fail")?;
        assert_eq!(full.code(), ErrorCode::ResourceExhausted);
        Ok(())
    }
}
