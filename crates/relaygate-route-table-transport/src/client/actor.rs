//! Sequential request execution for one established RouteTable connection.

use futures_util::{SinkExt, StreamExt};
use relaygate_transport::BoxedIo;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::codec::Framed;

use crate::{
    ErrorCode, TransportError,
    codec::{FrameCodec, map_receive_codec_error, map_send_codec_error},
    dto::{WireRequest, WireResponse},
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame, WireResult},
};

pub(super) struct ClientCommand {
    pub(super) request: WireRequest,
    pub(super) deadline: Instant,
    pub(super) reply: oneshot::Sender<Result<WireResponse, TransportError>>,
}

pub(super) async fn run_client_actor(
    mut framed: Framed<BoxedIo, FrameCodec>,
    mut commands: mpsc::Receiver<ClientCommand>,
) {
    let mut next_request_id = 1_u64;

    while let Some(command) = commands.recv().await {
        let request_id = next_request_id;
        let Some(next) = next_request_id.checked_add(1) else {
            let error = TransportError::internal("RouteTable request identifier exhausted");
            let _ = command.reply.send(Err(error));
            break;
        };
        next_request_id = next;

        let result = tokio::time::timeout_at(command.deadline, async {
            framed
                .send(WireFrame::Request {
                    role: GATEWAY_ROLE.to_owned(),
                    request_id,
                    request: command.request,
                })
                .await
                .map_err(map_send_codec_error)?;
            let frame = framed
                .next()
                .await
                .ok_or_else(|| TransportError::unavailable("RouteTable service closed"))?
                .map_err(map_receive_codec_error)?;
            decode_response(frame, request_id)
        })
        .await
        .unwrap_or_else(|_| {
            Err(TransportError::deadline_exceeded(
                "RouteTable request timed out",
            ))
        });

        let connection_terminal = result.as_ref().err().is_some_and(|error| {
            matches!(
                error.code(),
                ErrorCode::Unavailable
                    | ErrorCode::DeadlineExceeded
                    | ErrorCode::ProtocolError
                    | ErrorCode::Internal
            )
        });
        let _ = command.reply.send(result);
        if connection_terminal {
            break;
        }
    }

    let error = TransportError::unavailable(
        "RouteTable client connection closed before the queued request was sent",
    );
    while let Ok(command) = commands.try_recv() {
        let _ = command.reply.send(Err(error.clone()));
    }
}

fn decode_response(
    frame: WireFrame,
    expected_request_id: u64,
) -> Result<WireResponse, TransportError> {
    match frame {
        WireFrame::Response {
            role,
            request_id,
            result,
        } => {
            if role != ROUTE_TABLE_ROLE {
                return Err(TransportError::protocol(
                    "RouteTable response has an invalid role",
                ));
            }
            if request_id != expected_request_id {
                return Err(TransportError::protocol(format!(
                    "RouteTable response request_id mismatch: expected {expected_request_id}, got {request_id}"
                )));
            }
            match result {
                WireResult::Ok { response } => Ok(response),
                WireResult::Error { code, message } => Err(TransportError::new(code, message)),
            }
        }
        WireFrame::ProtocolFault {
            role,
            code,
            message,
        } if role == ROUTE_TABLE_ROLE && code == ErrorCode::ProtocolError => {
            Err(TransportError::new(code, message))
        }
        WireFrame::ProtocolFault { .. } => Err(TransportError::protocol(
            "RouteTable protocol fault has an invalid role or code",
        )),
        _ => Err(TransportError::protocol(
            "unexpected frame from RouteTable service",
        )),
    }
}
