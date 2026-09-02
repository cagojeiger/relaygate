use futures_util::SinkExt;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::{
    ErrorCode, TransportError,
    codec::{CodecError, FrameCodec},
    dto::WireResponse,
    frame::{ROUTE_TABLE_ROLE, WireFrame, WireResult},
};

pub(super) fn try_write_response(
    writer: &mpsc::Sender<WireFrame>,
    request_id: u64,
    result: Result<WireResponse, TransportError>,
    max_frame_len: usize,
) -> Result<(), BoundedSendError> {
    let frame = bounded_response_frame(request_id, result, max_frame_len)?;
    try_send_frame(writer, frame)
}

fn bounded_response_frame(
    request_id: u64,
    result: Result<WireResponse, TransportError>,
    max_frame_len: usize,
) -> Result<WireFrame, BoundedSendError> {
    let result = match result {
        Ok(response) => WireResult::Ok { response },
        Err(error) => WireResult::Error {
            code: error.code(),
            message: error.message().to_owned(),
        },
    };
    let frame = WireFrame::Response {
        role: ROUTE_TABLE_ROLE.to_owned(),
        request_id,
        result,
    };
    let codec = FrameCodec::new(max_frame_len);
    match codec.validate(&frame) {
        Ok(()) => return Ok(frame),
        Err(CodecError::FrameTooLarge { .. }) => {}
        Err(_) => {
            return bounded_error_response_frame(
                request_id,
                ErrorCode::Internal,
                "response encoding failed",
                max_frame_len,
            );
        }
    }
    bounded_error_response_frame(
        request_id,
        ErrorCode::ResourceExhausted,
        "response exceeds frame limit",
        max_frame_len,
    )
}

fn bounded_error_response_frame(
    request_id: u64,
    code: ErrorCode,
    message: &'static str,
    max_frame_len: usize,
) -> Result<WireFrame, BoundedSendError> {
    let codec = FrameCodec::new(max_frame_len);
    let frame = error_response_frame(request_id, code, message);
    if codec.validate(&frame).is_ok() {
        return Ok(frame);
    }
    let minimum = error_response_frame(request_id, code, "");
    if codec.validate(&minimum).is_ok() {
        Ok(minimum)
    } else {
        Err(BoundedSendError::FrameCannotBeEncoded)
    }
}

pub(super) fn error_response_frame(request_id: u64, code: ErrorCode, message: &str) -> WireFrame {
    WireFrame::Response {
        role: ROUTE_TABLE_ROLE.to_owned(),
        request_id,
        result: WireResult::Error {
            code,
            message: message.to_owned(),
        },
    }
}

pub(super) fn try_write_protocol_fault(
    writer: &mpsc::Sender<WireFrame>,
    message: impl Into<String>,
) -> Result<(), BoundedSendError> {
    try_send_frame(
        writer,
        WireFrame::ProtocolFault {
            role: ROUTE_TABLE_ROLE.to_owned(),
            code: ErrorCode::ProtocolError,
            message: message.into(),
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundedSendError {
    Full,
    Closed,
    FrameCannotBeEncoded,
}

pub(super) fn try_send_frame(
    writer: &mpsc::Sender<WireFrame>,
    frame: WireFrame,
) -> Result<(), BoundedSendError> {
    writer.try_send(frame).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => BoundedSendError::Full,
        mpsc::error::TrySendError::Closed(_) => BoundedSendError::Closed,
    })
}

pub(super) async fn send_protocol_fault(framed: &mut Framed<TcpStream, FrameCodec>, message: &str) {
    let _ = framed
        .send(WireFrame::ProtocolFault {
            role: ROUTE_TABLE_ROLE.to_owned(),
            code: ErrorCode::ProtocolError,
            message: message.to_owned(),
        })
        .await;
}
