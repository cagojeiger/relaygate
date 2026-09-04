use std::{
    error::Error,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use relaygate_route_table::GatewayId;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use super::super::{ActiveOpenSet, TransportNotice, actor::run_transport_actor};
use crate::peer::{
    GatewayPeerConfig,
    codec::PeerFrameCodec,
    handshake::EstablishedPeer,
    identity::{PeerTransportId, StreamEndpoint},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "current_thread")]
async fn competing_local_cancel_and_peer_eof_emit_exactly_one_transport_loss() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let connect = tokio::spawn(TcpStream::connect(listener.local_addr()?));
    let (peer_stream, _) = listener.accept().await?;
    let actor_stream = connect.await??;

    let config = GatewayPeerConfig::new("gateway-a", "key-a", [])?.with_queue_bounds(8, 8, 8, 8, 2);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let established = EstablishedPeer {
        framed: Framed::new(actor_stream, PeerFrameCodec::new(64 * 1024)),
        remote_gateway_id: peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
    };
    let (command_sender, command_receiver) = mpsc::channel(8);
    let (notice_sender, mut notice_receiver) = mpsc::channel(8);
    let stream_count = Arc::new(AtomicUsize::new(0));
    let close = CancellationToken::new();
    let actor = tokio::spawn(run_transport_actor(
        established,
        config,
        command_receiver,
        notice_sender,
        Arc::new(ActiveOpenSet::default()),
        Arc::clone(&stream_count),
        close.clone(),
    ));
    tokio::task::yield_now().await;

    // The current-thread runtime cannot select either terminal branch until
    // both OS threads cross the barrier and make local cancel and peer EOF
    // eligible together.
    let barrier = Arc::new(Barrier::new(3));
    let cancel = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            close.cancel();
        })
    };
    let disconnect = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            drop(peer_stream);
        })
    };
    barrier.wait();
    cancel.join().map_err(|_| "local cancel thread panicked")?;
    disconnect
        .join()
        .map_err(|_| "peer disconnect thread panicked")?;

    tokio::time::timeout(Duration::from_secs(2), actor).await??;
    drop(command_sender);
    let notices = tokio::time::timeout(Duration::from_secs(2), async {
        let mut notices = Vec::new();
        while let Some(notice) = notice_receiver.recv().await {
            notices.push(notice);
        }
        notices
    })
    .await?;

    assert_eq!(notices.len(), 1, "actor emitted duplicate terminal notices");
    let TransportNotice::TransportLost {
        peer_gateway_id: actual_peer_gateway_id,
        peer_transport_id: actual_transport_id,
        reason: _,
        streams,
    } = &notices[0]
    else {
        return Err(format!("expected TransportLost, got {:?}", notices[0]).into());
    };
    assert_eq!(*actual_peer_gateway_id, peer_gateway_id);
    assert_eq!(*actual_transport_id, peer_transport_id);
    assert!(streams.is_empty());
    assert_eq!(stream_count.load(Ordering::SeqCst), 0);
    Ok(())
}
