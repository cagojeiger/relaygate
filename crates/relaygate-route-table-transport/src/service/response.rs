use futures_util::SinkExt;
use relaygate_transport::BoxedIo;
use tokio::sync::mpsc;
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
    operation: &'static str,
    result: Result<WireResponse, TransportError>,
    max_frame_len: usize,
) -> Result<(), BoundedSendError> {
    let frame = bounded_response_frame(request_id, result, max_frame_len)?;
    let observation = response_observation(&frame);
    try_send_frame(writer, frame)?;
    if let Some((outcome, code)) = observation {
        observe_response(operation, outcome, code);
    }
    Ok(())
}

fn response_observation(frame: &WireFrame) -> Option<(&'static str, &'static str)> {
    match frame {
        WireFrame::Response {
            result: WireResult::Ok { .. },
            ..
        } => Some(("success", "ok")),
        WireFrame::Response {
            result: WireResult::Error { code, .. },
            ..
        } => Some(("error", code.metric_name())),
        _ => None,
    }
}

fn observe_response(operation: &'static str, outcome: &'static str, code: &'static str) {
    metrics::counter!(
        "relaygate_route_table_requests_total",
        "operation" => operation,
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
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

pub(super) async fn send_protocol_fault(framed: &mut Framed<BoxedIo, FrameCodec>, message: &str) {
    let _ = framed
        .send(WireFrame::ProtocolFault {
            role: ROUTE_TABLE_ROLE.to_owned(),
            code: ErrorCode::ProtocolError,
            message: message.to_owned(),
        })
        .await;
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn bounded_response_metric_uses_terminal_wire_code() -> Result<(), Box<dyn std::error::Error>> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (writer, mut receiver) = mpsc::channel(1);
        let result = metrics::with_local_recorder(&recorder, || {
            try_write_response(
                &writer,
                1,
                "resolve",
                Err(TransportError::unavailable("x".repeat(1024))),
                256,
            )
        });
        assert!(result.is_ok(), "bounded error response should fit");

        let frame = receiver.try_recv()?;
        assert!(matches!(
            frame,
            WireFrame::Response {
                result: WireResult::Error {
                    code: ErrorCode::ResourceExhausted,
                    ..
                },
                ..
            }
        ));
        assert!(
            snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .any(|(key, _, _, value)| {
                    key.key().name() == "relaygate_route_table_requests_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == "error")
                        && key.key().labels().any(|label| {
                            label.key() == "code" && label.value() == "resource_exhausted"
                        })
                        && matches!(value, DebugValue::Counter(1))
                })
        );
        Ok(())
    }

    #[test]
    fn rejected_writer_queue_does_not_count_a_response() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (writer, _receiver) = mpsc::channel(1);
        assert!(
            writer
                .try_send(error_response_frame(1, ErrorCode::Unavailable, "occupied"))
                .is_ok()
        );

        let result = metrics::with_local_recorder(&recorder, || {
            try_write_response(
                &writer,
                2,
                "deregister",
                Ok(WireResponse::Deregistered),
                256,
            )
        });
        assert_eq!(result, Err(BoundedSendError::Full));
        assert!(
            snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .all(|(key, _, _, _)| key.key().name() != "relaygate_route_table_requests_total")
        );
    }
}
