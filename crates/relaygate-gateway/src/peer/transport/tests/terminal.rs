use std::{error::Error, sync::atomic::Ordering};

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation};
use tokio::sync::{mpsc, oneshot};

use crate::peer::{
    event::{PeerEvent, PeerOpenRequest, PeerStreamKey},
    frame::PeerFrame,
    identity::{OpenIdentity, StreamId},
    transport::{TransportCommand, TransportNotice, state::TransportActor},
};

mod fin;
mod reordering;

async fn send_data(
    actor: &mut TransportActor,
    stream_id: StreamId,
    payload: Bytes,
) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Data {
            stream_id,
            payload,
            reply,
        })
        .await;
    result.await??;
    Ok(())
}

async fn send_fin(actor: &mut TransportActor, stream_id: StreamId) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Fin { stream_id, reply })
        .await;
    result.await??;
    Ok(())
}

async fn commit_open(
    actor: &mut TransportActor,
    frames: &mut mpsc::Receiver<PeerFrame>,
    open_identity: OpenIdentity,
    request: PeerOpenRequest,
) -> Result<PeerStreamKey, Box<dyn Error>> {
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request).await?;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Open {
            stream_id,
            open_identity: identity,
            ..
        }) if stream_id == key.stream_id() && identity == open_identity
    ));
    Ok(key)
}

async fn confirm_opened(
    actor: &mut TransportActor,
    notices: &mut mpsc::Receiver<TransportNotice>,
    open_identity: OpenIdentity,
    key: PeerStreamKey,
) -> Result<(), Box<dyn Error>> {
    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id(),
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Opened {
            key: opened_key,
            open_identity: identity,
        })) if opened_key == key && identity == open_identity
    ));
    Ok(())
}

fn late_stream_frames(stream_id: StreamId) -> [PeerFrame; 6] {
    [
        PeerFrame::Opened { stream_id },
        PeerFrame::Failed {
            stream_id,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "late FAILED".to_owned(),
        },
        PeerFrame::Data {
            stream_id,
            payload: Bytes::from_static(b"late DATA"),
        },
        PeerFrame::Fin { stream_id },
        PeerFrame::Close { stream_id },
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            message: "late RESET".to_owned(),
        },
    ]
}

fn assert_stream_removed(actor: &TransportActor, open_identity: OpenIdentity) {
    assert!(actor.streams.is_empty());
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), 0);
    assert!(!actor.active_opens.contains(open_identity));
}
