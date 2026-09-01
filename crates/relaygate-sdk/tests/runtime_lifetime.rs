mod support;

use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{BindingId, Frame, PipeId, SessionId, SessionRole};
use relaygate_sdk::{Config, Connector, ErrorCode, ListenerRuntime, ListenerStatus};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

use support::{
    TestResult, TestTransport, accept_session, bind_gateway, next_application_frame, unexpected,
};

const RECONNECT_INITIAL: Duration = Duration::from_millis(10);
const RECONNECT_MAXIMUM: Duration = Duration::from_millis(20);
const NO_RECONNECT_WINDOW: Duration = Duration::from_millis(120);

#[tokio::test]
async fn connector_clone_drop_and_explicit_close_terminate_runtime() -> TestResult {
    timeout(
        Duration::from_secs(3),
        connector_clone_drop_and_explicit_close_terminate_runtime_case(),
    )
    .await??;
    Ok(())
}

async fn connector_clone_drop_and_explicit_close_terminate_runtime_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, session_id) = accept_session(&gateway, SessionRole::Connector).await?;
        let open = next_application_frame(&mut transport).await?;
        let connection_id = match open {
            Frame::Open {
                connection_id,
                client_id,
            } if client_id == "lifetime.connector" => connection_id,
            other => return Err(unexpected(other).into()),
        };
        let pipe_id = PipeId::new(session_id, connection_id);
        transport.send(Frame::Opened { pipe_id }).await?;

        let data = next_application_frame(&mut transport).await?;
        match data {
            Frame::Data {
                pipe_id: received,
                payload,
            } if received == pipe_id && payload == Bytes::from_static(b"request") => {}
            other => return Err(unexpected(other).into()),
        }
        transport
            .send(Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"reply"),
            })
            .await?;

        assert_clean_session_end_and_no_reconnect(&gateway, &mut transport).await
    });

    let connector_a = Connector::connect(
        Config::new(address).with_reconnect_backoff(RECONNECT_INITIAL, RECONNECT_MAXIMUM),
    )
    .await?;
    let connector_b = connector_a.clone();
    drop(connector_a);

    let mut pipe = connector_b.open("lifetime.connector").await?;
    pipe.write_all_bytes(b"request").await?;
    let mut reply = [0_u8; 5];
    assert_eq!(pipe.read_into(&mut reply).await?, reply.len());
    assert_eq!(&reply, b"reply");

    connector_b.close();
    let pipe_error = timeout(Duration::from_secs(1), pipe.read_into(&mut reply))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("runtime close did not terminate the Connector Pipe"))?;
    assert_eq!(pipe_error.code(), ErrorCode::Unavailable);
    let session_error = connector_b
        .open("lifetime.after-close")
        .await
        .err()
        .ok_or_else(|| io::Error::other("closed Connector runtime accepted a new OPEN"))?;
    assert_eq!(session_error.code(), ErrorCode::Cancelled);

    server.await??;
    Ok(())
}

#[tokio::test]
async fn listener_runtime_clone_drop_and_explicit_close_terminate_runtime() -> TestResult {
    timeout(
        Duration::from_secs(3),
        listener_runtime_clone_drop_and_explicit_close_terminate_runtime_case(),
    )
    .await??;
    Ok(())
}

async fn listener_runtime_clone_drop_and_explicit_close_terminate_runtime_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let server = tokio::spawn(async move {
        let (mut transport, _, pipe_id) =
            accept_listener_pipe(&gateway, "lifetime.listener", 1).await?;

        let data = next_application_frame(&mut transport).await?;
        match data {
            Frame::Data {
                pipe_id: received,
                payload,
            } if received == pipe_id && payload == Bytes::from_static(b"response") => {}
            other => return Err(unexpected(other).into()),
        }
        transport
            .send(Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"request"),
            })
            .await?;

        assert_clean_session_end_and_no_reconnect(&gateway, &mut transport).await
    });

    let runtime_a = ListenerRuntime::connect(
        Config::new(address).with_reconnect_backoff(RECONNECT_INITIAL, RECONNECT_MAXIMUM),
    )
    .await?;
    let runtime_b = runtime_a.clone();
    drop(runtime_a);

    let listener = runtime_b.listen("lifetime.listener", "dev-key").await?;
    let mut pipe = listener.accept().await?;
    pipe.write_all_bytes(b"response").await?;
    let mut request = [0_u8; 7];
    assert_eq!(pipe.read_into(&mut request).await?, request.len());
    assert_eq!(&request, b"request");

    runtime_b.close();
    assert_eq!(listener.status(), ListenerStatus::Closed);
    let accept_error = listener
        .accept()
        .await
        .err()
        .ok_or_else(|| io::Error::other("closed Listener runtime accepted another Pipe"))?;
    assert_eq!(accept_error.code(), ErrorCode::Cancelled);
    let pipe_error = timeout(Duration::from_secs(1), pipe.read_into(&mut request))
        .await?
        .err()
        .ok_or_else(|| io::Error::other("runtime close did not terminate the Listener Pipe"))?;
    assert_eq!(pipe_error.code(), ErrorCode::Unavailable);

    server.await??;
    Ok(())
}

#[tokio::test]
async fn accepted_listener_pipe_is_the_last_runtime_owner() -> TestResult {
    timeout(
        Duration::from_secs(3),
        accepted_listener_pipe_is_the_last_runtime_owner_case(),
    )
    .await??;
    Ok(())
}

async fn accepted_listener_pipe_is_the_last_runtime_owner_case() -> TestResult {
    let (gateway, address) = bind_gateway().await?;
    let (detached_tx, detached_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut transport, binding_id, pipe_id) =
            accept_listener_pipe(&gateway, "lifetime.pipe-only", 2).await?;

        let unregister = next_application_frame(&mut transport).await?;
        let request_id = match unregister {
            Frame::Unregister {
                request_id,
                binding_id: received,
            } if received == binding_id => request_id,
            other => return Err(unexpected(other).into()),
        };
        transport.send(Frame::Unregistered { request_id }).await?;
        let _ = detached_tx.send(());

        let data = next_application_frame(&mut transport).await?;
        match data {
            Frame::Data {
                pipe_id: received,
                payload,
            } if received == pipe_id && payload == Bytes::from_static(b"still-alive") => {}
            other => return Err(unexpected(other).into()),
        }
        transport
            .send(Frame::Data {
                pipe_id,
                payload: Bytes::from_static(b"pipe-only"),
            })
            .await?;

        assert_clean_session_end_and_no_reconnect(&gateway, &mut transport).await
    });

    let runtime = ListenerRuntime::connect(
        Config::new(address).with_reconnect_backoff(RECONNECT_INITIAL, RECONNECT_MAXIMUM),
    )
    .await?;
    let listener = runtime.listen("lifetime.pipe-only", "dev-key").await?;
    let mut pipe = listener.accept().await?;

    drop(runtime);
    drop(listener);
    detached_rx.await?;

    pipe.write_all_bytes(b"still-alive").await?;
    let mut reply = [0_u8; 9];
    assert_eq!(pipe.read_into(&mut reply).await?, reply.len());
    assert_eq!(&reply, b"pipe-only");
    drop(pipe);

    server.await??;
    Ok(())
}

async fn accept_listener_pipe(
    gateway: &TcpListener,
    client_id: &str,
    connection_id: u64,
) -> TestResult<(TestTransport, BindingId, PipeId)> {
    let (mut transport, _) = accept_session(gateway, SessionRole::Listener).await?;
    let register = next_application_frame(&mut transport).await?;
    let request_id = match register {
        Frame::Register {
            request_id,
            client_id: received,
            ..
        } if received == client_id => request_id,
        other => return Err(unexpected(other).into()),
    };
    let binding_id = BindingId::new();
    transport
        .send(Frame::Registered {
            request_id,
            binding_id,
        })
        .await?;

    let pipe_id = PipeId::new(SessionId::new(), connection_id);
    transport
        .send(Frame::Offer {
            pipe_id,
            binding_id,
            client_id: client_id.to_owned(),
        })
        .await?;
    let accepted = next_application_frame(&mut transport).await?;
    if !matches!(accepted, Frame::OfferAccepted { pipe_id: received } if received == pipe_id) {
        return Err(unexpected(accepted).into());
    }
    Ok((transport, binding_id, pipe_id))
}

async fn assert_clean_session_end_and_no_reconnect(
    gateway: &TcpListener,
    transport: &mut TestTransport,
) -> TestResult {
    match timeout(Duration::from_secs(1), transport.next()).await? {
        None => {}
        Some(Ok(frame)) => {
            return Err(io::Error::other(format!(
                "SDK session emitted a frame after runtime termination: {frame:?}"
            ))
            .into());
        }
        Some(Err(error)) => {
            return Err(io::Error::other(format!(
                "SDK session did not close with clean TCP EOF: {error}"
            ))
            .into());
        }
    }

    if timeout(NO_RECONNECT_WINDOW, gateway.accept()).await.is_ok() {
        return Err(io::Error::other("SDK reconnected after runtime termination").into());
    }
    Ok(())
}
