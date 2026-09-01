use std::error::Error;

use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};

use super::super::actor_for_open;
use super::{assert_stream_removed, commit_open, confirm_opened, late_stream_frames};
use crate::peer::{
    event::{PeerEvent, PeerOpenRequest, PeerTarget},
    frame::PeerFrame,
    identity::{OpenIdentity, StreamId},
    transport::{TransportNotice, state::TransportActor},
};

fn request_for(
    actor: &TransportActor,
    connection_id: u64,
) -> Result<(OpenIdentity, PeerOpenRequest), Box<dyn Error>> {
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), connection_id);
    let request = PeerOpenRequest::new(
        PeerTarget::new(
            actor.peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        open_identity,
        "echo.b",
        SessionId::new(),
        BindingId::new(),
    )?;
    Ok((open_identity, request))
}

#[derive(Debug, Clone, Copy)]
enum OpeningTerminalCase {
    Failed,
    Reset,
    Data,
    Fin,
    Close,
}

impl OpeningTerminalCase {
    fn frame(self, stream_id: StreamId) -> PeerFrame {
        match self {
            Self::Failed => PeerFrame::Failed {
                stream_id,
                code: ErrorCode::Unavailable,
                observation: PeerObservation::MaybeObserved,
                message: "peer rejected OPEN".to_owned(),
            },
            Self::Reset => PeerFrame::Reset {
                stream_id,
                code: ErrorCode::Cancelled,
                message: "peer cancelled OPEN".to_owned(),
            },
            Self::Data => PeerFrame::Data {
                stream_id,
                payload: Bytes::from_static(b"invalid opening DATA"),
            },
            Self::Fin => PeerFrame::Fin { stream_id },
            Self::Close => PeerFrame::Close { stream_id },
        }
    }

    const fn expected_code(self) -> ErrorCode {
        match self {
            Self::Failed => ErrorCode::Unavailable,
            Self::Reset => ErrorCode::Cancelled,
            Self::Data | Self::Fin | Self::Close => ErrorCode::ProtocolError,
        }
    }

    const fn sends_reset(self) -> bool {
        matches!(self, Self::Data | Self::Fin | Self::Close)
    }
}

#[tokio::test]
async fn opening_terminal_reordering_converges_once() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, first_identity, first_request) = actor_for_open(4)?;
    let mut first = Some((first_identity, first_request));
    let mut last_stream_id = None;
    for (connection_id, case) in (1_u64..).zip([
        OpeningTerminalCase::Failed,
        OpeningTerminalCase::Reset,
        OpeningTerminalCase::Data,
        OpeningTerminalCase::Fin,
        OpeningTerminalCase::Close,
    ]) {
        let (open_identity, request) = match first.take() {
            Some(first) => first,
            None => request_for(&actor, connection_id)?,
        };
        let key = commit_open(&mut actor, &mut frames, open_identity, request).await?;
        let stream_id = key.stream_id();
        if let Some(previous) = last_stream_id {
            assert!(stream_id.raw() > previous);
        }
        last_stream_id = Some(stream_id.raw());
        assert!(actor.handle_frame(case.frame(stream_id)).await);
        for late in late_stream_frames(stream_id) {
            assert!(actor.handle_frame(late).await);
        }
        actor.flush_stream_queues().await;

        assert!(matches!(
            notices.recv().await,
            Some(TransportNotice::Event(PeerEvent::Failed {
                key: failed_key,
                open_identity: identity,
                failure,
            })) if failed_key == key
                && identity == open_identity
                && failure.code() == case.expected_code()
                && failure.observation() == PeerObservation::MaybeObserved
        ));
        assert!(matches!(
            notices.recv().await,
            Some(TransportNotice::StreamEnded {
                key: ended_key,
                open_identity: identity,
            }) if ended_key == key && identity == open_identity
        ));
        if case.sends_reset() {
            assert!(matches!(
                frames.recv().await,
                Some(PeerFrame::Reset {
                    stream_id: reset_stream,
                    code: ErrorCode::ProtocolError,
                    ..
                }) if reset_stream == stream_id
            ));
        }
        assert!(frames.try_recv().is_err());
        assert!(notices.try_recv().is_err());
        assert_stream_removed(&actor, open_identity);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum OpenedTerminalCase {
    Failed,
    Close,
    Reset,
}

impl OpenedTerminalCase {
    fn frame(self, stream_id: StreamId) -> PeerFrame {
        match self {
            Self::Failed => PeerFrame::Failed {
                stream_id,
                code: ErrorCode::Unavailable,
                observation: PeerObservation::MaybeObserved,
                message: "invalid FAILED after OPENED".to_owned(),
            },
            Self::Close => PeerFrame::Close { stream_id },
            Self::Reset => PeerFrame::Reset {
                stream_id,
                code: ErrorCode::Cancelled,
                message: "peer reset open stream".to_owned(),
            },
        }
    }

    const fn sends_reset(self) -> bool {
        matches!(self, Self::Failed)
    }
}

#[tokio::test]
async fn opened_terminal_reordering_converges_once() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, first_identity, first_request) = actor_for_open(4)?;
    let mut first = Some((first_identity, first_request));
    let mut last_stream_id = None;
    for (connection_id, case) in (1_u64..).zip([
        OpenedTerminalCase::Failed,
        OpenedTerminalCase::Close,
        OpenedTerminalCase::Reset,
    ]) {
        let (open_identity, request) = match first.take() {
            Some(first) => first,
            None => request_for(&actor, connection_id)?,
        };
        let key = commit_open(&mut actor, &mut frames, open_identity, request).await?;
        let stream_id = key.stream_id();
        if let Some(previous) = last_stream_id {
            assert!(stream_id.raw() > previous);
        }
        last_stream_id = Some(stream_id.raw());
        confirm_opened(&mut actor, &mut notices, open_identity, key).await?;
        assert!(actor.handle_frame(case.frame(stream_id)).await);
        for late in late_stream_frames(stream_id) {
            assert!(actor.handle_frame(late).await);
        }
        actor.flush_stream_queues().await;

        let terminal = notices
            .recv()
            .await
            .ok_or("missing established terminal event")?;
        match case {
            OpenedTerminalCase::Failed => assert!(matches!(
                terminal,
                TransportNotice::Event(PeerEvent::Reset {
                    key: reset_key,
                    code: ErrorCode::ProtocolError,
                    ..
                }) if reset_key == key
            )),
            OpenedTerminalCase::Close => assert!(matches!(
                terminal,
                TransportNotice::Event(PeerEvent::Close { key: close_key })
                    if close_key == key
            )),
            OpenedTerminalCase::Reset => assert!(matches!(
                terminal,
                TransportNotice::Event(PeerEvent::Reset {
                    key: reset_key,
                    code: ErrorCode::Cancelled,
                    ..
                }) if reset_key == key
            )),
        }
        assert!(matches!(
            notices.recv().await,
            Some(TransportNotice::StreamEnded {
                key: ended_key,
                open_identity: identity,
            }) if ended_key == key && identity == open_identity
        ));
        if case.sends_reset() {
            assert!(matches!(
                frames.recv().await,
                Some(PeerFrame::Reset {
                    stream_id: reset_stream,
                    code: ErrorCode::ProtocolError,
                    ..
                }) if reset_stream == stream_id
            ));
        }
        assert!(frames.try_recv().is_err());
        assert!(notices.try_recv().is_err());
        assert_stream_removed(&actor, open_identity);
    }
    Ok(())
}
