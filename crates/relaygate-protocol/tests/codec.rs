use bytes::{Bytes, BytesMut};
use relaygate_protocol::{
    BearerToken, BindingId, ErrorCode, Frame, FrameCodec, MAX_BEARER_TOKEN_BYTES, PeerObservation,
    PipeId, ProtocolError, RouteAddress, SessionId,
};
use tokio_util::codec::{Decoder, Encoder};

fn address() -> Result<RouteAddress, Box<dyn std::error::Error>> {
    Ok("inference/stt.seoul".parse()?)
}

#[test]
fn every_frame_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let session_id = SessionId::new();
    let address = address()?;
    let access_token = BearerToken::new("secret")?;
    let binding_id = BindingId::new();
    let pipe_id = PipeId::new(session_id, 42);
    let frames = vec![
        Frame::Hello,
        Frame::Welcome { session_id },
        Frame::SessionRejected {
            code: ErrorCode::Unauthenticated,
            message: "bad token".to_owned(),
        },
        Frame::Publish {
            request_id: 1,
            address: address.clone(),
            access_token: access_token.clone(),
        },
        Frame::Published {
            request_id: 1,
            binding_id,
        },
        Frame::PublishFailed {
            request_id: 2,
            code: ErrorCode::Unavailable,
            message: "draining".to_owned(),
        },
        Frame::Unpublish {
            request_id: 3,
            binding_id,
        },
        Frame::Unpublished { request_id: 3 },
        Frame::Dial {
            connection_id: 42,
            address: address.clone(),
            access_token,
        },
        Frame::Offer {
            pipe_id,
            binding_id,
            address,
        },
        Frame::OfferAccepted { pipe_id },
        Frame::OfferRejected {
            pipe_id,
            code: ErrorCode::ResourceExhausted,
            message: "full".to_owned(),
        },
        Frame::Opened { pipe_id },
        Frame::DialFailed {
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
    let expected = Frame::Dial {
        connection_id: 7,
        address: address()?,
        access_token: BearerToken::new("grant")?,
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
fn version_two_is_rejected_before_payload_decode() {
    let mut input = BytesMut::from(&b"RG\x02\x01\x00\x00\x00\x00"[..]);

    let error = FrameCodec::default().decode(&mut input);

    assert!(matches!(error, Err(ProtocolError::UnsupportedVersion(2))));
}

#[test]
fn oversized_frame_is_rejected_before_allocation() {
    let mut input = BytesMut::from(&b"RG\x03\x0f\x00\x10\x00\x00"[..]);
    let error = FrameCodec::new(1024).decode(&mut input);
    assert!(error.is_err());
}

#[test]
fn bearer_token_is_bounded_and_redacted() -> Result<(), ProtocolError> {
    let token = BearerToken::new("must-not-appear")?;
    let rendered = format!("{token:?}");
    assert!(!rendered.contains("must-not-appear"));
    assert!(rendered.contains("REDACTED"));
    let frame = Frame::Dial {
        connection_id: 1,
        address: "inference/stt.seoul"
            .parse()
            .map_err(|_| ProtocolError::InvalidRouteAddress)?,
        access_token: token,
    };
    assert!(!format!("{frame:?}").contains("must-not-appear"));
    assert!(matches!(
        BearerToken::new("x".repeat(MAX_BEARER_TOKEN_BYTES + 1)),
        Err(ProtocolError::FieldTooLong {
            field: "access_token",
            maximum: MAX_BEARER_TOKEN_BYTES,
            ..
        })
    ));
    Ok(())
}
