use bytes::{Bytes, BytesMut};
use relaygate_protocol::{
    BindingId, ClientKey, ErrorCode, Frame, FrameCodec, PeerObservation, PipeId, SessionId,
    SessionRole,
};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn every_frame_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let session_id = SessionId::new();
    let binding_id = BindingId::new();
    let pipe_id = PipeId::new(session_id, 42);
    let frames = vec![
        Frame::Hello {
            role: SessionRole::Listener,
        },
        Frame::Welcome { session_id },
        Frame::Register {
            request_id: 1,
            client_id: "echo.alpha".to_owned(),
            client_key: ClientKey::new("secret"),
        },
        Frame::Registered {
            request_id: 1,
            binding_id,
        },
        Frame::RegisterFailed {
            request_id: 2,
            code: ErrorCode::Unauthenticated,
            message: "bad key".to_owned(),
        },
        Frame::Unregister {
            request_id: 3,
            binding_id,
        },
        Frame::Unregistered { request_id: 3 },
        Frame::Open {
            connection_id: 42,
            client_id: "echo.alpha".to_owned(),
        },
        Frame::Offer {
            pipe_id,
            binding_id,
            client_id: "echo.alpha".to_owned(),
        },
        Frame::OfferAccepted { pipe_id },
        Frame::OfferRejected {
            pipe_id,
            code: ErrorCode::ResourceExhausted,
            message: "full".to_owned(),
        },
        Frame::Opened { pipe_id },
        Frame::OpenFailed {
            connection_id: 42,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "lost".to_owned(),
        },
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"\0binary\xff"),
        },
        Frame::Fin { pipe_id },
        Frame::Close { pipe_id },
        Frame::Reset {
            pipe_id,
            code: ErrorCode::ProtocolError,
            message: "bad state".to_owned(),
        },
        Frame::Ping { nonce: 9 },
        Frame::Pong { nonce: 9 },
        Frame::Cancel { pipe_id },
    ];

    for expected in frames {
        let mut encoded = BytesMut::new();
        FrameCodec::default().encode(expected.clone(), &mut encoded)?;
        let actual = FrameCodec::default().decode(&mut encoded)?;
        assert_eq!(actual, Some(expected));
        assert!(encoded.is_empty());
    }
    Ok(())
}

#[test]
fn fragmented_frame_waits_for_complete_payload() -> Result<(), Box<dyn std::error::Error>> {
    let expected = Frame::Open {
        connection_id: 7,
        client_id: "echo.alpha".to_owned(),
    };
    let mut encoded = BytesMut::new();
    FrameCodec::default().encode(expected.clone(), &mut encoded)?;
    let split_at = encoded.len().saturating_sub(1);
    let tail = encoded.split_off(split_at);
    let mut codec = FrameCodec::default();
    assert_eq!(codec.decode(&mut encoded)?, None);
    encoded.extend_from_slice(&tail);
    assert_eq!(codec.decode(&mut encoded)?, Some(expected));
    Ok(())
}

#[test]
fn oversized_frame_is_rejected_before_allocation() {
    let mut input = BytesMut::from(&b"RG\x01\x0e\x00\x10\x00\x00"[..]);
    let error = FrameCodec::new(1024).decode(&mut input);
    assert!(error.is_err());
}

#[test]
fn client_key_debug_is_redacted() {
    let key = ClientKey::new("must-not-appear");
    let rendered = format!("{key:?}");
    assert!(!rendered.contains("must-not-appear"));
    assert!(rendered.contains("REDACTED"));
}
