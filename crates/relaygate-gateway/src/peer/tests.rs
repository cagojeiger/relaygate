use bytes::{Bytes, BytesMut};
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use tokio_util::codec::{Decoder, Encoder};

use super::{
    codec::{PeerCodecError, PeerFrameCodec},
    error::PeerError,
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerGatewayKey, PeerGatewayName, PeerHandshake, PeerOpenProgress,
        PeerTransportId, RemoteStreamGuard, StreamEndpoint, StreamId, StreamIdAllocator,
    },
    pool::PeerPool,
    stream::RelayStream,
};

#[test]
fn stream_id_allocator_uses_endpoint_bit_and_never_reuses_failed_counter() -> Result<(), PeerError>
{
    let mut dialer = StreamIdAllocator::new(StreamEndpoint::Dialer);
    let first = dialer.allocate()?;
    let second = dialer.allocate()?;

    assert_eq!(first.raw(), 0);
    assert_eq!(second.raw(), 2);

    let mut acceptor = StreamIdAllocator::new(StreamEndpoint::Acceptor);
    let acceptor_stream = acceptor.allocate()?;
    assert_eq!(acceptor_stream.raw(), 1);
    Ok(())
}

#[test]
fn remote_stream_guard_rejects_reused_or_wrong_role_stream_id() -> Result<(), PeerError> {
    let mut guard = RemoteStreamGuard::new(StreamEndpoint::Acceptor);
    let mut remote = StreamIdAllocator::new(StreamEndpoint::Acceptor);
    let first = remote.allocate()?;
    let second = remote.allocate()?;

    guard.accept_open(first)?;
    guard.accept_open(second)?;

    assert!(matches!(
        guard.accept_open(first),
        Err(PeerError::Protocol(_))
    ));

    let mut local_role = StreamIdAllocator::new(StreamEndpoint::Dialer);
    let wrong_role = local_role.allocate()?;
    assert!(matches!(
        guard.accept_open(wrong_role),
        Err(PeerError::Protocol(_))
    ));
    Ok(())
}

#[test]
fn peer_pool_keeps_at_most_one_transport_per_direction() -> Result<(), PeerError> {
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let first = PeerTransportId::new();
    let duplicate = PeerTransportId::new();
    let reverse = PeerTransportId::new();

    let mut pool = PeerPool::default();
    pool.connect(gateway_a, gateway_b, first)?;
    assert!(matches!(
        pool.connect(gateway_a, gateway_b, duplicate),
        Err(PeerError::AlreadyExists(_))
    ));

    pool.ready(gateway_a, gateway_b, first)?;
    pool.connect(gateway_b, gateway_a, reverse)?;
    pool.ready(gateway_b, gateway_a, reverse)?;

    assert_eq!(pool.ready_count_for_pair(gateway_a, gateway_b), 2);
    Ok(())
}

#[test]
fn removing_one_peer_transport_keeps_opposite_direction_ready() -> Result<(), PeerError> {
    let gateway_a = GatewayId::new();
    let gateway_b = GatewayId::new();
    let outbound = PeerTransportId::new();
    let inbound = PeerTransportId::new();

    let mut pool = PeerPool::default();
    pool.connect(gateway_a, gateway_b, outbound)?;
    pool.ready(gateway_a, gateway_b, outbound)?;
    pool.connect(gateway_b, gateway_a, inbound)?;
    pool.ready(gateway_b, gateway_a, inbound)?;

    pool.remove_transport(outbound);

    assert_eq!(pool.ready_count_for_pair(gateway_a, gateway_b), 1);
    assert_eq!(pool.slot_count(), 1);
    Ok(())
}

#[test]
fn ended_gateway_incarnations_leave_no_idle_peer_slots() -> Result<(), PeerError> {
    let local = GatewayId::new();
    let mut pool = PeerPool::default();

    for _ in 0..100 {
        let remote = GatewayId::new();
        let transport = PeerTransportId::new();
        pool.connect(local, remote, transport)?;
        pool.ready(local, remote, transport)?;
        pool.remove_transport(transport);
    }

    assert_eq!(pool.slot_count(), 0);
    Ok(())
}

#[test]
fn relay_stream_fin_closes_only_after_both_directions_finish() -> Result<(), PeerError> {
    let mut stream = RelayStream::opening();
    stream.opened()?;

    assert!(stream.fin(StreamEndpoint::Dialer)?);
    assert!(!stream.fin(StreamEndpoint::Dialer)?);
    assert!(!stream.is_closed());
    stream.data(StreamEndpoint::Acceptor)?;

    assert!(stream.fin(StreamEndpoint::Acceptor)?);
    assert!(stream.is_closed());
    Ok(())
}

#[test]
fn relay_stream_rejects_data_after_same_direction_fin() -> Result<(), PeerError> {
    let mut stream = RelayStream::opening();
    stream.opened()?;
    assert!(stream.fin(StreamEndpoint::Dialer)?);

    assert!(matches!(
        stream.data(StreamEndpoint::Dialer),
        Err(PeerError::Protocol(_))
    ));
    Ok(())
}

#[test]
fn relay_stream_reset_is_idempotent_terminal_cleanup() -> Result<(), PeerError> {
    let mut stream = RelayStream::opening();
    stream.opened()?;

    stream.reset(ErrorCode::Unavailable);
    stream.reset(ErrorCode::Cancelled);
    stream.close();

    assert!(stream.is_closed());
    Ok(())
}

#[test]
fn peer_open_failure_observation_depends_on_writer_commit_point() {
    assert_eq!(
        PeerOpenProgress::BeforeOpenCommit.failure_observation(),
        relaygate_protocol::PeerObservation::NotObserved
    );
    assert_eq!(
        PeerOpenProgress::AfterOpenCommit.failure_observation(),
        relaygate_protocol::PeerObservation::MaybeObserved
    );
    assert_eq!(
        PeerOpenProgress::Opened.failure_observation(),
        relaygate_protocol::PeerObservation::MaybeObserved
    );
}

#[test]
fn peer_frame_codec_round_trips_every_frame_kind() -> Result<(), Box<dyn std::error::Error>> {
    let dialer_handshake = handshake("gw-a", "key-a")?;
    let acceptor_handshake = handshake("gw-b", "key-b")?;
    let stream_id = StreamId::from_raw(2);
    let frames = vec![
        PeerFrame::Hello(dialer_handshake),
        PeerFrame::Welcome(acceptor_handshake),
        PeerFrame::HandshakeRejected {
            code: ErrorCode::Unauthenticated,
            message: "credential rejected".to_owned(),
        },
        PeerFrame::Open {
            stream_id,
            open_identity: OpenIdentity::new(GatewayId::new(), SessionId::new(), 7),
            destination_id: "echo.a".to_owned(),
            relay_session_id: SessionId::new(),
            binding_id: BindingId::new(),
        },
        PeerFrame::Opened { stream_id },
        PeerFrame::Failed {
            stream_id,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "peer unavailable".to_owned(),
        },
        PeerFrame::Data {
            stream_id,
            payload: Bytes::from_static(&[0, 1, 2, 3, 255]),
        },
        PeerFrame::Fin { stream_id },
        PeerFrame::Close { stream_id },
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            message: "cancelled".to_owned(),
        },
        PeerFrame::Ping { nonce: 11 },
        PeerFrame::Pong { nonce: 11 },
    ];
    for frame in frames {
        let mut codec = PeerFrameCodec::new(1024);
        let mut bytes = BytesMut::new();
        codec.encode(frame.clone(), &mut bytes)?;

        let decoded = codec.decode(&mut bytes)?;

        assert_eq!(decoded, Some(frame));
    }
    Ok(())
}

#[test]
fn peer_data_encoding_has_constant_overhead_without_text_expansion() -> Result<(), PeerCodecError> {
    let payload = Bytes::from_static(&[0, 1, 2, 3, 254, 255]);
    let mut codec = PeerFrameCodec::new(1024);
    let mut encoded = BytesMut::new();

    codec.encode(
        PeerFrame::Data {
            stream_id: StreamId::from_raw(2),
            payload: payload.clone(),
        },
        &mut encoded,
    )?;

    // Eight-byte transport header plus the eight-byte StreamId. Payload bytes
    // appear exactly once and are not rendered as JSON numbers or base64.
    assert_eq!(encoded.len(), 16 + payload.len());
    assert_eq!(&encoded[16..], payload.as_ref());
    Ok(())
}

#[test]
fn peer_frame_codec_rejects_oversized_payload_before_writing() -> Result<(), PeerCodecError> {
    let frame = PeerFrame::Failed {
        stream_id: StreamId::from_raw(1),
        code: ErrorCode::ResourceExhausted,
        observation: PeerObservation::NotObserved,
        message: "x".repeat(128),
    };
    let mut codec = PeerFrameCodec::new(32);
    let mut destination = BytesMut::from(&b"prefix"[..]);
    let original = destination.clone();
    let error = codec.encode(frame, &mut destination).err();

    assert!(matches!(
        error,
        Some(PeerCodecError::FrameTooLarge {
            actual: _,
            maximum: 32
        })
    ));
    assert_eq!(destination, original);
    Ok(())
}

#[test]
fn peer_frame_codec_rejects_invalid_magic_and_version() -> Result<(), PeerCodecError> {
    let mut codec = PeerFrameCodec::new(1024);
    let mut bytes = BytesMut::new();
    codec.encode(
        PeerFrame::Fin {
            stream_id: StreamId::from_raw(1),
        },
        &mut bytes,
    )?;

    let mut invalid_magic = bytes.clone();
    invalid_magic[0] = b'X';
    assert!(matches!(
        codec.decode(&mut invalid_magic),
        Err(PeerCodecError::InvalidMagic)
    ));

    let mut invalid_version = bytes;
    invalid_version[2] = 2;
    assert!(matches!(
        codec.decode(&mut invalid_version),
        Err(PeerCodecError::UnsupportedVersion(2))
    ));
    Ok(())
}

#[test]
fn peer_frame_codec_rejects_unknown_kind_enum_and_trailing_bytes() -> Result<(), PeerCodecError> {
    let mut codec = PeerFrameCodec::new(1024);
    let mut encoded = BytesMut::new();
    codec.encode(
        PeerFrame::Reset {
            stream_id: StreamId::from_raw(1),
            code: ErrorCode::Cancelled,
            message: String::new(),
        },
        &mut encoded,
    )?;

    let mut unknown_kind = encoded.clone();
    unknown_kind[3] = 255;
    assert!(matches!(
        codec.decode(&mut unknown_kind),
        Err(PeerCodecError::UnknownFrameKind(255))
    ));

    let mut unknown_code = encoded.clone();
    unknown_code[16] = 255;
    assert!(matches!(
        codec.decode(&mut unknown_code),
        Err(PeerCodecError::UnknownEnum {
            name: "ErrorCode",
            value: 255
        })
    ));

    let mut trailing = encoded;
    trailing[7] += 1;
    trailing.extend_from_slice(&[0]);
    assert!(matches!(
        codec.decode(&mut trailing),
        Err(PeerCodecError::TrailingBytes(1))
    ));
    Ok(())
}

#[test]
fn peer_frame_codec_rejects_truncated_fields_and_invalid_utf8() {
    let mut codec = PeerFrameCodec::new(1024);
    let mut truncated = raw_frame(5, &[0; 7]);
    assert!(matches!(
        codec.decode(&mut truncated),
        Err(PeerCodecError::Truncated("stream_id"))
    ));

    let mut invalid_utf8 = raw_frame(3, &[ErrorCode::Unauthenticated as u8, 0, 1, 255]);
    assert!(matches!(
        codec.decode(&mut invalid_utf8),
        Err(PeerCodecError::InvalidUtf8("message"))
    ));
}

#[test]
fn peer_frame_codec_rejects_oversized_declared_frame_without_waiting_for_payload() {
    let mut codec = PeerFrameCodec::new(8);
    let mut bytes = BytesMut::from(&[b'G', b'P', 1, 8, 0, 0, 0, 9][..]);

    assert!(matches!(
        codec.decode(&mut bytes),
        Err(PeerCodecError::FrameTooLarge {
            actual: 9,
            maximum: 8
        })
    ));
}

#[test]
fn peer_frame_codec_bounds_strings_and_rejects_empty_open_destination() {
    let codec = PeerFrameCodec::new(usize::MAX);
    let too_long = PeerFrame::HandshakeRejected {
        code: ErrorCode::Unauthenticated,
        message: "x".repeat(u16::MAX as usize + 1),
    };
    assert!(matches!(
        codec.validate(&too_long),
        Err(PeerCodecError::FieldTooLong {
            field: "message",
            actual: 65_536,
            maximum: 65_535
        })
    ));

    let empty_client = PeerFrame::Open {
        stream_id: StreamId::from_raw(0),
        open_identity: OpenIdentity::new(GatewayId::new(), SessionId::new(), 1),
        destination_id: String::new(),
        relay_session_id: SessionId::new(),
        binding_id: BindingId::new(),
    };
    assert!(matches!(
        codec.validate(&empty_client),
        Err(PeerCodecError::InvalidField("destination_id"))
    ));
}

#[test]
fn peer_handshake_round_trip_preserves_uuid_claims_and_redacts_key()
-> Result<(), Box<dyn std::error::Error>> {
    let handshake = handshake("gw-a", "do-not-log")?;
    let expected = handshake.clone();
    let debug = format!("{:?}", PeerFrame::Hello(handshake.clone()));
    assert!(!debug.contains("do-not-log"));
    assert!(debug.contains("[REDACTED]"));

    let mut codec = PeerFrameCodec::new(1024);
    let mut encoded = BytesMut::new();
    codec.encode(PeerFrame::Hello(handshake), &mut encoded)?;
    let decoded = codec.decode(&mut encoded)?;
    assert_eq!(decoded, Some(PeerFrame::Hello(expected)));
    Ok(())
}

#[test]
fn peer_data_debug_reports_only_length() {
    let frame = PeerFrame::Data {
        stream_id: StreamId::from_raw(2),
        payload: Bytes::from_static(b"payload-must-not-be-logged"),
    };

    let debug = format!("{frame:?}");
    assert!(debug.contains("payload_len"));
    assert!(!debug.contains("payload-must-not-be-logged"));
}

fn handshake(name: &str, key: &str) -> Result<PeerHandshake, Box<dyn std::error::Error>> {
    Ok(PeerHandshake {
        gateway_name: PeerGatewayName::new(name)?,
        internal_gateway_key: PeerGatewayKey::new(key)?,
        gateway_id: GatewayId::new(),
        expected_peer_gateway_id: GatewayId::new(),
        dialer_gateway_id: GatewayId::new(),
        peer_transport_id: PeerTransportId::new(),
    })
}

fn raw_frame(kind: u8, payload: &[u8]) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(b"GP");
    bytes.extend_from_slice(&[1, kind]);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}
