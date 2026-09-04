use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_route_table::{AuthenticatedGatewayId, GatewayId, RequestContext};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::Framed, sync::CancellationToken};
use uuid::Uuid;

use crate::{
    ErrorCode, GatewayName, InternalGatewayKey, TransportError, TrustedGatewayKeys,
    codec::{CodecError, FrameCodec},
    dto::WireRequest,
    frame::{GATEWAY_ROLE, ROUTE_TABLE_ROLE, WireFrame},
};

use super::{
    RouteTableServiceConfig,
    actor::ServiceCommand,
    response::{send_protocol_fault, try_write_protocol_fault, try_write_response},
};

pub(super) async fn handle_connection(
    stream: TcpStream,
    keys: Arc<TrustedGatewayKeys>,
    requests: mpsc::Sender<ServiceCommand>,
    config: RouteTableServiceConfig,
    shutdown: CancellationToken,
) {
    let mut framed = Framed::new(stream, FrameCodec::new(config.max_frame_len));
    let context = match authenticate(&mut framed, &keys, config.handshake_timeout, &shutdown).await
    {
        Ok(Some(context)) => {
            observe_handshake("success", "ok");
            context
        }
        Ok(None) => return,
        Err(error) => {
            observe_handshake("error", error.code().metric_name());
            tracing::debug!(
                event = "route_table.handshake.failed",
                error_code = error.code().metric_name(),
                error = %error,
                "RouteTable rejected or lost a Gateway handshake"
            );
            return;
        }
    };
    let (sink, stream) = framed.split();
    let (writer, writer_receiver) = mpsc::channel(config.writer_queue_capacity);
    let writer_shutdown = shutdown.child_token();
    let mut writer_task = tokio::spawn(run_writer(sink, writer_receiver, writer_shutdown.clone()));
    run_reader(
        stream,
        writer.clone(),
        requests,
        context,
        shutdown.clone(),
        config.max_frame_len,
    )
    .await;
    drop(writer);
    tokio::select! {
        _ = shutdown.cancelled() => {
            writer_shutdown.cancel();
            let _ = writer_task.await;
        }
        completed = tokio::time::timeout(config.handshake_timeout, &mut writer_task) => {
            if completed.is_err() {
                writer_shutdown.cancel();
                let _ = writer_task.await;
            }
        }
    }
}

async fn authenticate(
    framed: &mut Framed<TcpStream, FrameCodec>,
    keys: &TrustedGatewayKeys,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<Option<RequestContext>, TransportError> {
    let handshake = async {
        let frame = framed
            .next()
            .await
            .ok_or_else(|| TransportError::unavailable("Gateway closed during authentication"))?
            .map_err(map_receive_codec_error)?;
        let WireFrame::Hello {
            role,
            gateway_name,
            gateway_id,
            internal_gateway_key,
        } = frame
        else {
            observe_handshake("error", "protocol_error");
            send_protocol_fault(framed, "expected Gateway HELLO").await;
            return Ok(None);
        };
        if role != GATEWAY_ROLE {
            observe_handshake("error", "protocol_error");
            send_protocol_fault(framed, "Gateway HELLO has an invalid role").await;
            return Ok(None);
        }

        let name = GatewayName::new(gateway_name).ok();
        let gateway_id = Uuid::parse_str(&gateway_id).ok().map(GatewayId::from_uuid);
        let key = InternalGatewayKey::from_wire(internal_gateway_key);
        let authenticated = name
            .as_ref()
            .is_some_and(|name| keys.authenticate(name, &key));
        if !authenticated || gateway_id.is_none() {
            let error = TransportError::unauthenticated();
            observe_handshake("error", error.code().metric_name());
            tracing::debug!(
                event = "route_table.handshake.rejected",
                error_code = error.code().metric_name(),
                "RouteTable rejected Gateway authentication"
            );
            let _ = framed
                .send(WireFrame::HandshakeRejected {
                    role: ROUTE_TABLE_ROLE.to_owned(),
                    code: error.code(),
                    message: error.message().to_owned(),
                })
                .await;
            return Ok(None);
        }
        let Some(gateway_id) = gateway_id else {
            return Ok(None);
        };
        framed
            .send(WireFrame::Welcome {
                role: ROUTE_TABLE_ROLE.to_owned(),
            })
            .await
            .map_err(map_send_codec_error)?;
        Ok(Some(RequestContext::new(
            AuthenticatedGatewayId::from_verified_transport(gateway_id),
        )))
    };

    tokio::select! {
        _ = shutdown.cancelled() => Ok(None),
        result = tokio::time::timeout(timeout, handshake) => {
            result
                .map_err(|_| TransportError::deadline_exceeded("Gateway handshake timed out"))?
        }
    }
}

fn observe_handshake(outcome: &'static str, code: &'static str) {
    metrics::counter!(
        "relaygate_route_table_handshakes_total",
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
}

async fn run_reader(
    mut stream: futures_util::stream::SplitStream<Framed<TcpStream, FrameCodec>>,
    writer: mpsc::Sender<WireFrame>,
    requests: mpsc::Sender<ServiceCommand>,
    context: RequestContext,
    shutdown: CancellationToken,
    max_frame_len: usize,
) {
    let mut last_request_id = 0_u64;
    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = stream.next() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                let _ = try_write_protocol_fault(&writer, error.to_string());
                break;
            }
        };
        let WireFrame::Request {
            role,
            request_id,
            request,
        } = frame
        else {
            let _ = try_write_protocol_fault(&writer, "expected Gateway request frame");
            break;
        };
        if role != GATEWAY_ROLE || request_id == 0 || request_id <= last_request_id {
            let _ = try_write_protocol_fault(
                &writer,
                "Gateway request has an invalid role or request_id",
            );
            break;
        }
        last_request_id = request_id;

        let response = match submit_service_request(&requests, context, request) {
            Ok(response) => response,
            Err(error) if error.code() == ErrorCode::ResourceExhausted => {
                if try_write_response(&writer, request_id, Err(error), max_frame_len).is_err() {
                    break;
                }
                continue;
            }
            Err(error) => {
                let _ = try_write_response(&writer, request_id, Err(error), max_frame_len);
                break;
            }
        };

        let result = tokio::select! {
            _ = shutdown.cancelled() => break,
            result = response => result.unwrap_or_else(|_| {
                Err(TransportError::internal("RouteTable state actor dropped a response"))
            }),
        };
        if try_write_response(&writer, request_id, result, max_frame_len).is_err() {
            break;
        }
    }
}

pub(super) fn submit_service_request(
    requests: &mpsc::Sender<ServiceCommand>,
    context: RequestContext,
    request: WireRequest,
) -> Result<oneshot::Receiver<Result<crate::dto::WireResponse, TransportError>>, TransportError> {
    let (reply, response) = oneshot::channel();
    match requests.try_send(ServiceCommand {
        context,
        request,
        reply,
    }) {
        Ok(()) => Ok(response),
        Err(mpsc::error::TrySendError::Full(_)) => Err(TransportError::resource_exhausted(
            "RouteTable service request queue is full",
        )),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::unavailable(
            "RouteTable state actor is unavailable",
        )),
    }
}

async fn run_writer(
    mut sink: futures_util::stream::SplitSink<Framed<TcpStream, FrameCodec>, WireFrame>,
    mut frames: mpsc::Receiver<WireFrame>,
    shutdown: CancellationToken,
) {
    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = frames.recv() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let sent = tokio::select! {
            _ = shutdown.cancelled() => break,
            sent = sink.send(frame) => sent,
        };
        if sent.is_err() {
            break;
        }
    }
}

pub(super) async fn reject_over_capacity(
    stream: TcpStream,
    config: RouteTableServiceConfig,
    shutdown: &CancellationToken,
) {
    let mut framed = Framed::new(stream, FrameCodec::new(config.max_frame_len));
    let error = TransportError::resource_exhausted("RouteTable connection limit reached");
    observe_handshake("error", error.code().metric_name());
    let send = framed.send(WireFrame::HandshakeRejected {
        role: ROUTE_TABLE_ROLE.to_owned(),
        code: error.code(),
        message: error.message().to_owned(),
    });
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = tokio::time::timeout(config.handshake_timeout, send) => {}
    }
}

pub(super) fn map_send_codec_error(error: CodecError) -> TransportError {
    match error {
        CodecError::FrameTooLarge { .. } | CodecError::LengthOverflow => {
            TransportError::resource_exhausted(error.to_string())
        }
        _ if error.is_io() => TransportError::unavailable(error.to_string()),
        _ => TransportError::protocol(error.to_string()),
    }
}

pub(super) fn map_receive_codec_error(error: CodecError) -> TransportError {
    if error.is_io() {
        TransportError::unavailable(error.to_string())
    } else {
        TransportError::protocol(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::observe_handshake;

    #[test]
    fn handshake_metrics_include_connection_capacity_rejection() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            observe_handshake("error", "resource_exhausted");
        });

        let values = snapshotter.snapshot().into_vec();
        assert!(values.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_route_table_handshakes_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "error")
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "code" && label.value() == "resource_exhausted")
                && matches!(value, DebugValue::Counter(1))
        }));
    }
}
