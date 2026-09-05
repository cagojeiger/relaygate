//! Central bounded Gateway peer runtime.
//!
//! The facade owns lifecycle wiring and shared counters. `handle` is the
//! Gateway-facing command surface; `manager` owns pair arbitration,
//! handshakes, transport admission, and ordered event delivery.

mod handle;
mod manager;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use relaygate_protocol::ErrorCode;
use relaygate_route_table::GatewayId;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use super::{
    config::GatewayPeerConfig,
    error::PeerError,
    event::{PeerCounts, PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey},
    identity::{OpenIdentity, PeerTransportId},
    pool::PeerPool,
    transport::{ActiveOpenSet, TransportHandle, TransportNotice},
};

pub(crate) use handle::PeerHandle;
use manager::{HandshakeNotice, Manager};

pub(super) type CommandReply = oneshot::Sender<Result<(), PeerFailure>>;

#[derive(Debug)]
pub(super) enum ManagerCommand {
    Open {
        request: PeerOpenRequest,
        reply: oneshot::Sender<Result<PeerStreamKey, PeerFailure>>,
    },
    Cancel {
        open_identity: OpenIdentity,
        reply: CommandReply,
    },
    Opened {
        key: PeerStreamKey,
        reply: CommandReply,
    },
    Failed {
        key: PeerStreamKey,
        failure: PeerFailure,
        reply: CommandReply,
    },
    Data {
        key: PeerStreamKey,
        payload: Bytes,
        reply: CommandReply,
    },
    Fin {
        key: PeerStreamKey,
        reply: CommandReply,
    },
    Close {
        key: PeerStreamKey,
        reply: CommandReply,
    },
    Reset {
        key: PeerStreamKey,
        code: ErrorCode,
        message: String,
        reply: CommandReply,
    },
}

#[derive(Debug, Default)]
struct SharedCounts {
    connecting: AtomicUsize,
    ready: AtomicUsize,
    streams: Arc<AtomicUsize>,
}

impl SharedCounts {
    fn update_pool(&self, pool: &PeerPool) {
        let (connecting, ready) = pool.state_counts();
        self.connecting.store(connecting, Ordering::Relaxed);
        self.ready.store(ready, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PeerCounts {
        PeerCounts {
            connecting: self.connecting.load(Ordering::Relaxed),
            ready: self.ready.load(Ordering::Relaxed),
            streams: self.streams.load(Ordering::Relaxed),
        }
    }

    fn clear(&self) {
        self.connecting.store(0, Ordering::Relaxed);
        self.ready.store(0, Ordering::Relaxed);
        self.streams.store(0, Ordering::Relaxed);
    }
}

type TransportRegistry = Arc<RwLock<BTreeMap<PeerTransportId, TransportHandle>>>;

pub(crate) struct PeerEvents {
    receiver: mpsc::Receiver<PeerEvent>,
}

impl PeerEvents {
    pub(crate) async fn recv(&mut self) -> Option<PeerEvent> {
        self.receiver.recv().await
    }
}

/// Owns the central pair manager and accepts inbound peer TCP connections on a
/// caller-supplied listener.
pub(crate) struct PeerRuntime {
    config: GatewayPeerConfig,
    local_gateway_id: GatewayId,
    commands: mpsc::Receiver<ManagerCommand>,
    transport_notices: mpsc::Receiver<TransportNotice>,
    transport_notice_sender: mpsc::Sender<TransportNotice>,
    handshake_notices: mpsc::Receiver<HandshakeNotice>,
    handshake_notice_sender: mpsc::Sender<HandshakeNotice>,
    events: mpsc::Sender<PeerEvent>,
    transports: TransportRegistry,
    active_opens: Arc<ActiveOpenSet>,
    counts: Arc<SharedCounts>,
    shutdown: CancellationToken,
}

impl PeerRuntime {
    pub(crate) fn start(
        config: GatewayPeerConfig,
        local_gateway_id: GatewayId,
        shutdown: CancellationToken,
    ) -> Result<(PeerHandle, PeerEvents, Self), PeerError> {
        config.validate()?;
        let (command_sender, commands) = mpsc::channel(config.manager_queue_capacity);
        let (transport_notice_sender, transport_notices) =
            mpsc::channel(config.manager_queue_capacity);
        let (handshake_notice_sender, handshake_notices) =
            mpsc::channel(config.manager_queue_capacity);
        let (events, event_receiver) = mpsc::channel(config.event_queue_capacity);
        let transports = Arc::new(RwLock::new(BTreeMap::new()));
        let counts = Arc::new(SharedCounts::default());
        let handle = PeerHandle::new(command_sender, Arc::clone(&transports), Arc::clone(&counts));
        let runtime = Self {
            config,
            local_gateway_id,
            commands,
            transport_notices,
            transport_notice_sender,
            handshake_notices,
            handshake_notice_sender,
            events,
            transports,
            active_opens: Arc::new(ActiveOpenSet::default()),
            counts,
            shutdown,
        };
        Ok((
            handle,
            PeerEvents {
                receiver: event_receiver,
            },
            runtime,
        ))
    }

    pub(crate) async fn serve(self, listener: TcpListener) -> Result<(), PeerFailure> {
        Manager::new(self).run(listener).await
    }
}
