use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    ActiveOpenSet, TransportNotice,
    state::{RuntimeStream, StreamOrigin, TransportActor},
};
use crate::peer::{
    GatewayPeerConfig,
    event::PeerEvent,
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerOpenProgress, PeerTransportId, RemoteStreamGuard, StreamEndpoint,
        StreamId, StreamIdAllocator,
    },
    stream::RelayStream,
};

#[tokio::test]
async fn saturated_cleanup_reset_closes_containing_transport() -> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a", "key-a", [])?.with_queue_bounds(4, 4, 4, 4, 1);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let stream_id = StreamId::from_raw(0);
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 1);
    let active_opens = Arc::new(ActiveOpenSet::default());
    assert!(active_opens.reserve(open_identity)?);
    let mut relay = RelayStream::owned_opening(open_identity);
    relay.opened()?;
    let mut queued_frames = VecDeque::new();
    queued_frames.push_back(PeerFrame::Data {
        stream_id,
        payload: Bytes::from_static(b"already queued"),
    });
    let streams = BTreeMap::from([(
        stream_id,
        RuntimeStream {
            relay,
            queued_frames,
            terminal_queued: false,
            progress: PeerOpenProgress::Opened,
            origin: StreamOrigin::Local,
            open_deadline: None,
        },
    )]);
    let (aggregate_writer, _aggregate_receiver) = mpsc::channel(1);
    let (notices, _notice_receiver) = mpsc::channel(4);
    let close = CancellationToken::new();
    let mut actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams,
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::new(AtomicUsize::new(1)),
        close: close.clone(),
        config,
    };

    let failure = actor
        .send_reset(
            stream_id,
            ErrorCode::Cancelled,
            "connector session closed".to_owned(),
        )
        .await
        .err()
        .ok_or("expected saturated RESET queue failure")?;
    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert!(close.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn invalid_frame_during_local_opening_emits_failed_not_reset() -> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a", "key-a", [])?.with_queue_bounds(4, 4, 4, 4, 2);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let stream_id = StreamId::from_raw(0);
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 9);
    let active_opens = Arc::new(ActiveOpenSet::default());
    assert!(active_opens.reserve(open_identity)?);
    let streams = BTreeMap::from([(
        stream_id,
        RuntimeStream {
            relay: RelayStream::owned_opening(open_identity),
            queued_frames: VecDeque::new(),
            terminal_queued: false,
            progress: PeerOpenProgress::AfterOpenCommit,
            origin: StreamOrigin::Local,
            open_deadline: None,
        },
    )]);
    let (aggregate_writer, _aggregate_receiver) = mpsc::channel(2);
    let (notices, mut notice_receiver) = mpsc::channel(4);
    let close = CancellationToken::new();
    let mut actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams,
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::new(AtomicUsize::new(1)),
        close,
        config,
    };

    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id,
                payload: Bytes::from_static(b"invalid before OPENED"),
            })
            .await
    );
    let notice = notice_receiver
        .recv()
        .await
        .ok_or("expected local OPEN terminal event")?;
    assert!(matches!(
        notice,
        TransportNotice::Event(PeerEvent::Failed {
            key: _,
            open_identity: identity,
            failure,
        }) if identity == open_identity
            && failure.code() == ErrorCode::ProtocolError
            && failure.observation() == PeerObservation::MaybeObserved
    ));
    Ok(())
}
