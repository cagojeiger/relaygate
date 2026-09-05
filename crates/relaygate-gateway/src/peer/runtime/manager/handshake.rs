//! Bounded mutual handshake tasks and direction-slot admission.

use std::sync::Arc;

use relaygate_protocol::ErrorCode;
use tokio::net::TcpStream;

use super::{HandshakeNotice, Manager};
use crate::peer::{
    error::PeerError,
    event::{PeerFailure, PeerTarget},
    handshake::{
        EstablishedPeer, complete_inbound_handshake, dial_and_handshake, receive_inbound_hello,
        reject_duplicate,
    },
    identity::PeerTransportId,
    transport::spawn_transport,
};

impl Manager {
    pub(super) fn start_outbound_handshake(
        &mut self,
        target: PeerTarget,
        transport_id: PeerTransportId,
    ) {
        self.handshakes_inflight += 1;
        let sender = self.handshake_notice_sender.clone();
        let config = self.config.clone();
        let trusted = self.trusted.clone();
        let local_gateway_id = self.local_gateway_id;
        let remote_gateway_id = target.gateway_id();
        self.tasks.spawn(async move {
            let result =
                dial_and_handshake(config, trusted, local_gateway_id, target, transport_id).await;
            let _ = sender
                .send(HandshakeNotice::Outbound {
                    remote_gateway_id,
                    peer_transport_id: transport_id,
                    result,
                })
                .await;
        });
    }

    pub(super) fn start_inbound_handshake(&mut self, stream: TcpStream) {
        if self.handshakes_inflight >= self.config.max_handshakes {
            observe_handshake("inbound", "error", "resource_exhausted");
            tracing::warn!(
                component = "gateway",
                event = "gateway.peer.handshake.rejected",
                direction = "inbound",
                error_code = "resource_exhausted",
                "Gateway peer handshake capacity is exhausted"
            );
            return;
        }
        self.handshakes_inflight += 1;
        let sender = self.handshake_notice_sender.clone();
        let config = self.config.clone();
        let trusted = self.trusted.clone();
        let local_gateway_id = self.local_gateway_id;
        self.tasks.spawn(async move {
            let result = receive_inbound_hello(stream, config, trusted, local_gateway_id).await;
            let _ = sender.send(HandshakeNotice::InboundHello(result)).await;
        });
    }

    pub(super) fn handle_handshake_notice(&mut self, notice: HandshakeNotice) {
        self.handshakes_inflight = self.handshakes_inflight.saturating_sub(1);
        match notice {
            HandshakeNotice::Outbound {
                remote_gateway_id,
                peer_transport_id,
                result,
            } => match result {
                Ok(established) => {
                    observe_handshake("outbound", "success", "ok");
                    self.admit_established(established);
                }
                Err(error) => {
                    observe_handshake("outbound", "error", error.metric_code());
                    tracing::debug!(
                        component = "gateway",
                        event = "gateway.peer.handshake.failed",
                        direction = "outbound",
                        error_code = error.metric_code(),
                        "Outbound Gateway peer handshake failed"
                    );
                    self.pool.remove_transport(peer_transport_id);
                    self.counts.update_pool(&self.pool);
                    self.fail_pending_for(remote_gateway_id, error);
                }
            },
            HandshakeNotice::InboundHello(result) => {
                let hello = match result {
                    Ok(hello) => hello,
                    Err(error) => {
                        observe_handshake("inbound", "error", error.metric_code());
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.peer.handshake.failed",
                            direction = "inbound",
                            error_code = error.metric_code(),
                            "Inbound Gateway peer HELLO failed"
                        );
                        return;
                    }
                };
                match self.pool.connect(
                    hello.remote_gateway_id,
                    self.local_gateway_id,
                    hello.peer_transport_id,
                ) {
                    Ok(()) => {
                        self.counts.update_pool(&self.pool);
                        self.handshakes_inflight += 1;
                        let sender = self.handshake_notice_sender.clone();
                        let config = self.config.clone();
                        let local_gateway_id = self.local_gateway_id;
                        let remote_gateway_id = hello.remote_gateway_id;
                        let peer_transport_id = hello.peer_transport_id;
                        self.tasks.spawn(async move {
                            let result =
                                complete_inbound_handshake(hello, config, local_gateway_id).await;
                            let _ = sender
                                .send(HandshakeNotice::InboundComplete {
                                    remote_gateway_id,
                                    peer_transport_id,
                                    result,
                                })
                                .await;
                        });
                    }
                    Err(PeerError::AlreadyExists(_)) => {
                        observe_handshake("inbound", "error", "already_exists");
                        self.handshakes_inflight += 1;
                        let sender = self.handshake_notice_sender.clone();
                        let timeout = self.config.handshake_timeout;
                        self.tasks.spawn(async move {
                            reject_duplicate(hello, timeout).await;
                            let _ = sender.send(HandshakeNotice::Rejected).await;
                        });
                    }
                    Err(error) => {
                        observe_handshake("inbound", "error", error.metric_code());
                    }
                }
            }
            HandshakeNotice::InboundComplete {
                remote_gateway_id,
                peer_transport_id,
                result,
            } => match result {
                Ok(established) => {
                    observe_handshake("inbound", "success", "ok");
                    self.admit_established(established);
                }
                Err(error) => {
                    observe_handshake("inbound", "error", error.metric_code());
                    tracing::debug!(
                        component = "gateway",
                        event = "gateway.peer.handshake.failed",
                        direction = "inbound",
                        error_code = error.metric_code(),
                        "Inbound Gateway peer handshake failed"
                    );
                    self.pool.remove_transport(peer_transport_id);
                    self.counts.update_pool(&self.pool);
                    self.fail_pending_for(
                        remote_gateway_id,
                        PeerFailure::not_observed(
                            ErrorCode::Unavailable,
                            "inbound peer handshake did not become ready",
                        ),
                    );
                }
            },
            HandshakeNotice::Rejected => {}
        }
    }

    fn admit_established(&mut self, established: EstablishedPeer) {
        let remote_gateway_id = established.remote_gateway_id;
        let transport_id = established.peer_transport_id;
        if self
            .pool
            .ready(self.local_gateway_id, remote_gateway_id, transport_id)
            .is_err()
            && self
                .pool
                .ready(remote_gateway_id, self.local_gateway_id, transport_id)
                .is_err()
        {
            self.pool.remove_transport(transport_id);
            self.counts.update_pool(&self.pool);
            self.fail_pending_for(
                remote_gateway_id,
                PeerFailure::not_observed(
                    ErrorCode::FailedPrecondition,
                    "peer candidate no longer owns its direction slot",
                ),
            );
            return;
        }
        let transport = spawn_transport(
            established,
            &self.config,
            self.transport_notice_sender.clone(),
            Arc::clone(&self.active_opens),
            Arc::clone(&self.counts.streams),
            &self.shutdown,
            &mut self.tasks,
        );
        self.transports.insert(transport_id, transport.clone());
        if let Ok(mut shared) = self.shared_transports.write() {
            shared.insert(transport_id, transport);
        }
        self.counts.update_pool(&self.pool);
        self.flush_pending_for(remote_gateway_id, transport_id);
    }
}

fn observe_handshake(direction: &'static str, outcome: &'static str, code: &'static str) {
    metrics::counter!(
        "relaygate_gateway_peer_handshakes_total",
        "direction" => direction,
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::observe_handshake;

    #[test]
    fn peer_handshake_metrics_separate_direction_outcome_and_code() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            observe_handshake("outbound", "success", "ok");
            observe_handshake("inbound", "error", "unauthenticated");
        });

        let values = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "relaygate_gateway_peer_handshakes_total")
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        assert!(
            values
                .iter()
                .all(|(_, _, _, value)| matches!(value, DebugValue::Counter(1)))
        );
    }
}
