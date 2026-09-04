//! Single owner of peer-pair slots, handshakes, transports, and events.

mod command;
mod handshake;
mod notice;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use relaygate_protocol::ErrorCode;
use relaygate_route_table::GatewayId;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

use super::{ManagerCommand, PeerRuntime, SharedCounts, TransportRegistry};
use crate::peer::{
    auth::TrustedPeers,
    config::GatewayPeerConfig,
    event::{PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey},
    handshake::{EstablishedPeer, InboundHello},
    identity::{OpenIdentity, PeerTransportId},
    pool::PeerPool,
    transport::{ActiveOpenSet, TransportHandle, TransportNotice},
};

pub(super) enum HandshakeNotice {
    Outbound {
        remote_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        result: Result<EstablishedPeer, PeerFailure>,
    },
    InboundHello(Result<InboundHello, PeerFailure>),
    InboundComplete {
        remote_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        result: Result<EstablishedPeer, PeerFailure>,
    },
    Rejected,
}

struct PendingOpen {
    request: PeerOpenRequest,
    reply: oneshot::Sender<Result<PeerStreamKey, PeerFailure>>,
}

pub(super) struct Manager {
    config: GatewayPeerConfig,
    trusted: TrustedPeers,
    local_gateway_id: GatewayId,
    commands: mpsc::Receiver<ManagerCommand>,
    transport_notices: mpsc::Receiver<TransportNotice>,
    transport_notice_sender: mpsc::Sender<TransportNotice>,
    handshake_notices: mpsc::Receiver<HandshakeNotice>,
    handshake_notice_sender: mpsc::Sender<HandshakeNotice>,
    events: mpsc::Sender<PeerEvent>,
    shared_transports: TransportRegistry,
    active_opens: Arc<ActiveOpenSet>,
    counts: Arc<SharedCounts>,
    shutdown: CancellationToken,
    pool: PeerPool,
    transports: BTreeMap<PeerTransportId, TransportHandle>,
    pending: Vec<PendingOpen>,
    assignments: HashMap<OpenIdentity, PeerTransportId>,
    handshakes_inflight: usize,
    tasks: JoinSet<()>,
}

impl Manager {
    pub(super) fn new(runtime: PeerRuntime) -> Self {
        Self {
            trusted: TrustedPeers::from_config(&runtime.config),
            config: runtime.config,
            local_gateway_id: runtime.local_gateway_id,
            commands: runtime.commands,
            transport_notices: runtime.transport_notices,
            transport_notice_sender: runtime.transport_notice_sender,
            handshake_notices: runtime.handshake_notices,
            handshake_notice_sender: runtime.handshake_notice_sender,
            events: runtime.events,
            shared_transports: runtime.transports,
            active_opens: runtime.active_opens,
            counts: runtime.counts,
            shutdown: runtime.shutdown,
            pool: PeerPool::default(),
            transports: BTreeMap::new(),
            pending: Vec::new(),
            assignments: HashMap::new(),
            handshakes_inflight: 0,
            tasks: JoinSet::new(),
        }
    }

    pub(super) async fn run(mut self, listener: TcpListener) -> Result<(), PeerFailure> {
        let mut shutting_down = false;
        let mut shutdown_deadline = None;
        let mut terminal_error = None;
        loop {
            if shutting_down && self.transports.is_empty() && self.handshakes_inflight == 0 {
                break;
            }
            tokio::select! {
                () = self.shutdown.cancelled(), if !shutting_down => {
                    shutting_down = true;
                    shutdown_deadline = Instant::now().checked_add(self.config.handshake_timeout);
                    self.begin_shutdown();
                }
                accepted = listener.accept(), if !shutting_down => {
                    match accepted {
                        Ok((stream, _)) => self.start_inbound_handshake(stream),
                        Err(_) => {
                            terminal_error = Some(PeerFailure::not_observed(
                                ErrorCode::Unavailable,
                                "peer listener accept failed",
                            ));
                            shutting_down = true;
                            shutdown_deadline = Instant::now().checked_add(self.config.handshake_timeout);
                            self.begin_shutdown();
                        }
                    }
                }
                command = self.commands.recv(), if !shutting_down => {
                    if let Some(command) = command {
                        self.handle_command(command);
                    }
                }
                notice = self.handshake_notices.recv() => {
                    if let Some(notice) = notice {
                        self.handle_handshake_notice(notice);
                    }
                }
                notice = self.transport_notices.recv() => {
                    if let Some(notice) = notice
                        && let Err(error) = self.handle_transport_notice(notice, shutting_down)
                    {
                        terminal_error.get_or_insert(error);
                        shutting_down = true;
                        shutdown_deadline = Instant::now().checked_add(self.config.handshake_timeout);
                        self.begin_shutdown();
                    }
                }
                task = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(Err(_)) = task {
                        terminal_error.get_or_insert_with(|| PeerFailure::not_observed(
                            ErrorCode::Internal,
                            "peer runtime task failed",
                        ));
                        shutting_down = true;
                        shutdown_deadline = Instant::now().checked_add(self.config.handshake_timeout);
                        self.begin_shutdown();
                    }
                }
                () = wait_shutdown_deadline(shutdown_deadline), if shutdown_deadline.is_some() => {
                    break;
                }
            }
        }
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        if let Ok(mut shared) = self.shared_transports.write() {
            shared.clear();
        }
        self.counts.clear();
        terminal_error.map_or(Ok(()), Err)
    }

    fn begin_shutdown(&mut self) {
        self.shutdown.cancel();
        for pending in std::mem::take(&mut self.pending) {
            self.active_opens.release(pending.request.open_identity());
            let _ = pending.reply.send(Err(PeerFailure::not_observed(
                ErrorCode::Unavailable,
                "peer runtime is shutting down",
            )));
        }
        for transport in self.transports.values() {
            transport.close();
        }
    }
}

async fn wait_shutdown_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}
