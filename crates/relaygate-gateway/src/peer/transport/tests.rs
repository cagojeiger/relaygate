use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    sync::{Arc, atomic::AtomicUsize},
};

use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    ActiveOpenSet, TransportCommand, TransportNotice,
    state::{RuntimeStream, StreamOrigin, TransportActor},
};
use crate::peer::{
    GatewayPeerConfig,
    event::{PeerEvent, PeerOpenRequest, PeerTarget},
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerOpenProgress, PeerTransportId, RemoteStreamGuard, StreamEndpoint,
        StreamId, StreamIdAllocator,
    },
    stream::RelayStream,
};

type TestActor = (
    TransportActor,
    mpsc::Receiver<PeerFrame>,
    mpsc::Receiver<TransportNotice>,
    OpenIdentity,
    PeerOpenRequest,
);

fn actor_for_open(writer_capacity: usize) -> Result<TestActor, Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a", "key-a", [])?
        .with_queue_bounds(4, 4, 4, writer_capacity, 2)
        .with_timeouts(
            std::time::Duration::from_millis(500),
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(1),
        );
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 1);
    let request = PeerOpenRequest::new(
        PeerTarget::new(
            peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        open_identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(writer_capacity);
    let (notices, notice_receiver) = mpsc::channel(4);
    let active_opens = Arc::new(ActiveOpenSet::default());
    let actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams: BTreeMap::new(),
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::new(AtomicUsize::new(0)),
        close: CancellationToken::new(),
        config,
    };
    Ok((
        actor,
        aggregate_receiver,
        notice_receiver,
        open_identity,
        request,
    ))
}

#[tokio::test]
async fn open_rejected_before_writer_commit_leaves_no_stream_state() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, _notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    actor.aggregate_writer.try_send(PeerFrame::Close {
        stream_id: StreamId::from_raw(99),
    })?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let failure = actor
        .open(request)
        .err()
        .ok_or("expected OPEN pre-commit rejection")?;

    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert!(actor.streams.is_empty());
    assert_eq!(
        actor
            .stream_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!actor.active_opens.contains(open_identity));
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Close { stream_id }) if stream_id == StreamId::from_raw(99)
    ));
    Ok(())
}

#[tokio::test]
async fn closed_writer_rejects_open_before_commit() -> Result<(), Box<dyn Error>> {
    let (mut actor, aggregate_receiver, _notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    drop(aggregate_receiver);
    assert!(actor.active_opens.reserve(open_identity)?);

    let failure = actor
        .open(request)
        .err()
        .ok_or("expected closed writer OPEN rejection")?;

    assert_eq!(failure.code(), ErrorCode::Unavailable);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert!(actor.streams.is_empty());
    assert!(!actor.active_opens.contains(open_identity));
    Ok(())
}

#[tokio::test]
async fn committed_open_timeout_is_reported_as_maybe_observed() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(4)?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let key = actor.open(request)?;
    let committed = aggregate_receiver
        .recv()
        .await
        .ok_or("expected committed OPEN frame")?;
    assert!(matches!(
        committed,
        PeerFrame::Open {
            stream_id,
            open_identity: identity,
            ..
        } if stream_id == key.stream_id() && identity == open_identity
    ));
    assert!(actor.active_opens.contains(open_identity));
    let timeout_at = actor
        .next_open_deadline()
        .ok_or("expected OPEN response deadline")?;

    actor.expire_open_deadlines_at(timeout_at).await;
    let failure_notice = notice_receiver
        .recv()
        .await
        .ok_or("expected timeout failure event")?;
    assert!(matches!(
        failure_notice,
        TransportNotice::Event(PeerEvent::Failed {
            key: failed_key,
            open_identity: identity,
            failure,
        }) if failed_key == key
            && identity == open_identity
            && failure.code() == ErrorCode::DeadlineExceeded
            && failure.observation() == PeerObservation::MaybeObserved
    ));

    actor.flush_stream_queues().await;
    let reset = aggregate_receiver
        .recv()
        .await
        .ok_or("expected timeout RESET frame")?;
    assert!(matches!(
        reset,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::DeadlineExceeded,
            ..
        } if stream_id == key.stream_id()
    ));
    let ended = notice_receiver
        .recv()
        .await
        .ok_or("expected stream cleanup notice")?;
    assert!(matches!(
        ended,
        TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        } if ended_key == key && identity == open_identity
    ));
    assert!(actor.streams.is_empty());
    assert!(!actor.active_opens.contains(open_identity));
    let (cancel_reply, cancel_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Cancel {
            open_identity,
            reply: cancel_reply,
        })
        .await;
    cancel_result.await??;
    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    actor.flush_stream_queues().await;
    assert!(aggregate_receiver.try_recv().is_err());
    assert!(notice_receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cancel_before_open_deadline_keeps_cancelled_as_the_only_terminal_cause()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let key = actor.open(request)?;
    let timeout_at = actor
        .next_open_deadline()
        .ok_or("expected OPEN response deadline")?;
    let (cancel_reply, cancel_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Cancel {
            open_identity,
            reply: cancel_reply,
        })
        .await;
    cancel_result.await??;
    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    actor.expire_open_deadlines_at(timeout_at).await;
    assert!(
        notice_receiver.try_recv().is_err(),
        "cancelled OPEN emitted a second deadline terminal event"
    );

    let committed = aggregate_receiver
        .recv()
        .await
        .ok_or("expected committed OPEN frame")?;
    assert!(matches!(
        committed,
        PeerFrame::Open {
            stream_id,
            open_identity: identity,
            ..
        } if stream_id == key.stream_id() && identity == open_identity
    ));
    actor.flush_stream_queues().await;
    let reset = aggregate_receiver
        .recv()
        .await
        .ok_or("expected cancellation RESET frame")?;
    assert!(matches!(
        reset,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            ..
        } if stream_id == key.stream_id()
    ));
    let ended = notice_receiver
        .recv()
        .await
        .ok_or("expected stream cleanup notice")?;
    assert!(matches!(
        ended,
        TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        } if ended_key == key && identity == open_identity
    ));
    assert!(aggregate_receiver.try_recv().is_err());
    assert!(notice_receiver.try_recv().is_err());
    assert!(actor.streams.is_empty());
    assert_eq!(
        actor
            .stream_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!actor.active_opens.contains(open_identity));
    Ok(())
}

#[tokio::test]
async fn committed_open_preserves_order_while_writer_is_stalled() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request)?;

    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    let _ = notice_receiver
        .recv()
        .await
        .ok_or("expected OPENED event")?;
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Data {
            stream_id: key.stream_id(),
            payload: Bytes::from_static(b"after-opened"),
            reply,
        })
        .await;
    result.await??;

    actor.flush_stream_queues().await;
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Open { stream_id, .. }) if stream_id == key.stream_id()
    ));
    assert!(aggregate_receiver.try_recv().is_err());

    actor.flush_stream_queues().await;
    let data = aggregate_receiver
        .recv()
        .await
        .ok_or("expected DATA after committed OPEN")?;
    assert!(matches!(
        data,
        PeerFrame::Data { stream_id, payload }
            if stream_id == key.stream_id() && payload == Bytes::from_static(b"after-opened")
    ));
    Ok(())
}

#[tokio::test]
async fn writer_pressure_orders_multiple_opens_and_never_reuses_failed_counter()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, _notice_receiver, first_identity, first_request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(first_identity)?);
    let first_key = actor.open(first_request)?;
    assert_eq!(first_key.stream_id().raw(), 0);

    let second_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 2);
    let second_request = PeerOpenRequest::new(
        PeerTarget::new(
            actor.peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        second_identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    assert!(actor.active_opens.reserve(second_identity)?);
    let second_failure = actor
        .open(second_request)
        .err()
        .ok_or("expected the second OPEN to fail while the first commit fills the writer")?;
    assert_eq!(second_failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(second_failure.observation(), PeerObservation::NotObserved);
    assert!(!actor.active_opens.contains(second_identity));

    let first_frame = aggregate_receiver
        .recv()
        .await
        .ok_or("expected the first committed OPEN")?;
    assert!(matches!(
        first_frame,
        PeerFrame::Open {
            stream_id,
            open_identity,
            ..
        } if stream_id == first_key.stream_id() && open_identity == first_identity
    ));

    let third_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 3);
    let third_request = PeerOpenRequest::new(
        PeerTarget::new(
            actor.peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        third_identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    assert!(actor.active_opens.reserve(third_identity)?);
    let third_key = actor.open(third_request)?;
    assert_eq!(
        third_key.stream_id().raw(),
        4,
        "the failed second OPEN must consume counter 1 instead of reusing it"
    );
    let third_frame = aggregate_receiver
        .recv()
        .await
        .ok_or("expected the third committed OPEN")?;
    assert!(matches!(
        third_frame,
        PeerFrame::Open {
            stream_id,
            open_identity,
            ..
        } if stream_id == third_key.stream_id() && open_identity == third_identity
    ));
    assert!(aggregate_receiver.try_recv().is_err());
    Ok(())
}

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
