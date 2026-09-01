use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec, PipeId, SessionRole};
use tokio::{net::TcpStream, time::timeout};
use tokio_util::codec::Framed;

use super::*;

type RawConnectorSession = Framed<TcpStream, FrameCodec>;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn same_connection_id_isolated_across_entry_gateways_and_connector_sessions() -> TestResult {
    timeout(Duration::from_secs(10), open_identity_case()).await??;
    Ok(())
}

async fn open_identity_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let mut gateway_a = RunningGateway::start(GATEWAY_A, GATEWAY_A_KEY, directory.clone()).await?;
    let mut gateway_b = RunningGateway::start(GATEWAY_B, GATEWAY_B_KEY, directory.clone()).await?;
    let mut gateway_c = RunningGateway::start(GATEWAY_C, GATEWAY_C_KEY, directory).await?;

    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway_c.sdk_address)).await?;
    let listener = listener_runtime.listen(CLIENT_C, CLIENT_KEY).await?;
    wait_until("owner registration synced", Duration::from_secs(2), || {
        gateway_c.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;

    let mut connector_a = raw_connector_session(gateway_a.sdk_address).await?;
    let mut connector_b = raw_connector_session(gateway_b.sdk_address).await?;
    let (pipe_id_a, mut listener_pipe_a) =
        open_raw_pipe(&mut connector_a, &listener, CLIENT_C, 1).await?;
    let (pipe_id_b, mut listener_pipe_b) =
        open_raw_pipe(&mut connector_b, &listener, CLIENT_C, 1).await?;

    assert_eq!(pipe_id_a.connection_id(), 1);
    assert_eq!(pipe_id_b.connection_id(), 1);
    assert_ne!(
        pipe_id_a.connector_session_id(),
        pipe_id_b.connector_session_id()
    );
    assert_raw_bidirectional(&mut connector_a, pipe_id_a, &mut listener_pipe_a, "entry-a").await?;
    assert_raw_bidirectional(&mut connector_b, pipe_id_b, &mut listener_pipe_b, "entry-b").await?;
    wait_for_two_entry_pipes(&gateway_a, &gateway_b, &gateway_c).await?;

    close_raw_session(connector_a).await?;
    assert_pipe_cancelled(&mut listener_pipe_a).await?;
    wait_until(
        "entry A session cleanup is isolated from entry B",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.connector_sessions == 0
                && a.remote_open_attempts == 0
                && a.live_pipes == 0
                && a.peer_streams == 0
                && b.connector_sessions == 1
                && b.remote_open_attempts == 0
                && b.live_pipes == 1
                && b.peer_streams == 1
                && c.live_pipes == 1
                && c.peer_streams == 1
        },
    )
    .await?;
    assert_raw_bidirectional(
        &mut connector_b,
        pipe_id_b,
        &mut listener_pipe_b,
        "entry-b-after-a-close",
    )
    .await?;

    let mut connector_a_restarted = raw_connector_session(gateway_a.sdk_address).await?;
    let (pipe_id_a_restarted, mut listener_pipe_a_restarted) =
        open_raw_pipe(&mut connector_a_restarted, &listener, CLIENT_C, 1).await?;
    assert_eq!(pipe_id_a_restarted.connection_id(), 1);
    assert_ne!(
        pipe_id_a_restarted.connector_session_id(),
        pipe_id_a.connector_session_id()
    );
    assert_ne!(
        pipe_id_a_restarted.connector_session_id(),
        pipe_id_b.connector_session_id()
    );
    assert_raw_bidirectional(
        &mut connector_a_restarted,
        pipe_id_a_restarted,
        &mut listener_pipe_a_restarted,
        "entry-a-new-session",
    )
    .await?;
    assert_raw_bidirectional(
        &mut connector_b,
        pipe_id_b,
        &mut listener_pipe_b,
        "entry-b-during-a-reopen",
    )
    .await?;
    wait_for_two_entry_pipes(&gateway_a, &gateway_b, &gateway_c).await?;

    close_raw_session(connector_a_restarted).await?;
    assert_pipe_cancelled(&mut listener_pipe_a_restarted).await?;
    assert_raw_bidirectional(
        &mut connector_b,
        pipe_id_b,
        &mut listener_pipe_b,
        "entry-b-after-new-a-close",
    )
    .await?;
    close_raw_session(connector_b).await?;
    assert_pipe_cancelled(&mut listener_pipe_b).await?;

    wait_until(
        "all connector-owned state returns to zero",
        Duration::from_secs(2),
        || {
            [&gateway_a, &gateway_b, &gateway_c]
                .into_iter()
                .all(|gateway| {
                    let snapshot = gateway.gateway.snapshot();
                    snapshot.connector_sessions == 0
                        && snapshot.pending_offers == 0
                        && snapshot.remote_open_attempts == 0
                        && snapshot.live_pipes == 0
                        && snapshot.peer_streams == 0
                })
        },
    )
    .await?;

    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    gateway_c.assert_running().await?;
    listener_runtime.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    gateway_c.stop().await?;
    route_table.stop().await?;
    Ok(())
}

async fn raw_connector_session(endpoint: SocketAddr) -> TestResult<RawConnectorSession> {
    let stream = TcpStream::connect(endpoint).await?;
    let mut session = Framed::new(stream, FrameCodec::default());
    session
        .send(Frame::Hello {
            role: SessionRole::Connector,
        })
        .await?;
    match next_raw_frame(&mut session).await? {
        Frame::Welcome { .. } => Ok(session),
        frame => Err(format!("expected Connector WELCOME, got {frame:?}").into()),
    }
}

async fn open_raw_pipe(
    connector: &mut RawConnectorSession,
    listener: &Listener,
    client_id: &str,
    connection_id: u64,
) -> TestResult<(PipeId, Pipe)> {
    connector
        .send(Frame::Open {
            connection_id,
            client_id: client_id.to_owned(),
        })
        .await?;
    let listener_pipe = timeout(Duration::from_secs(2), listener.accept()).await??;
    match next_raw_frame(connector).await? {
        Frame::Opened { pipe_id } if pipe_id.connection_id() == connection_id => {
            Ok((pipe_id, listener_pipe))
        }
        frame => {
            Err(format!("expected OPENED for connection {connection_id}, got {frame:?}").into())
        }
    }
}

async fn assert_raw_bidirectional(
    connector: &mut RawConnectorSession,
    pipe_id: PipeId,
    listener: &mut Pipe,
    marker: &str,
) -> TestResult {
    let toward_listener = Bytes::from(format!("connector:{marker}"));
    connector
        .send(Frame::Data {
            pipe_id,
            payload: toward_listener.clone(),
        })
        .await?;
    let mut received = vec![0_u8; toward_listener.len()];
    listener.read_exact(&mut received).await?;
    assert_eq!(received, toward_listener);

    let toward_connector = Bytes::from(format!("listener:{marker}"));
    listener.write_all(&toward_connector).await?;
    match next_raw_frame(connector).await? {
        Frame::Data {
            pipe_id: received_pipe,
            payload,
        } if received_pipe == pipe_id && payload == toward_connector => Ok(()),
        frame => Err(format!("expected DATA for {pipe_id:?}, got {frame:?}").into()),
    }
}

async fn assert_pipe_cancelled(pipe: &mut Pipe) -> TestResult {
    let mut byte = [0_u8; 1];
    let error = timeout(Duration::from_secs(2), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or("Listener Pipe survived its ConnectorSession closure")?;
    assert_eq!(error.code(), SdkErrorCode::Cancelled);
    assert_eq!(error.observation(), SdkPeerObservation::Observed);
    Ok(())
}

async fn close_raw_session(mut session: RawConnectorSession) -> TestResult {
    session.close().await?;
    Ok(())
}

async fn next_raw_frame(session: &mut RawConnectorSession) -> TestResult<Frame> {
    loop {
        let frame = match timeout(Duration::from_secs(2), session.next()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(None) => return Err("Gateway closed the raw ConnectorSession".into()),
            Err(error) => return Err(error.into()),
        };
        match frame {
            Frame::Ping { nonce } => session.send(Frame::Pong { nonce }).await?,
            frame => return Ok(frame),
        }
    }
}

async fn wait_for_two_entry_pipes(
    gateway_a: &RunningGateway,
    gateway_b: &RunningGateway,
    gateway_c: &RunningGateway,
) -> TestResult {
    wait_until(
        "two Entry Gateway sessions remain identity-isolated",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.connector_sessions == 1
                && a.remote_open_attempts == 0
                && a.live_pipes == 1
                && a.peer_streams == 1
                && b.connector_sessions == 1
                && b.remote_open_attempts == 0
                && b.live_pipes == 1
                && b.peer_streams == 1
                && c.live_pipes == 2
                && c.peer_streams == 2
        },
    )
    .await
}
