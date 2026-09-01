use std::{error::Error, net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec};
use relaygate_route_table::ShardDirectory;
use relaygate_sdk::{
    Config as SdkConfig, Connector, ErrorCode as SdkErrorCode, Listener, ListenerRuntime,
    PeerObservation as SdkPeerObservation,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

use super::{
    CLIENT_ID, CLIENT_KEY, GATEWAY_A, GATEWAY_A_KEY, GATEWAY_B, GATEWAY_B_KEY,
    OpenBlockingPeerProxy, RunningGateway, RunningRouteTable, TestResult, one_shard_directory,
    sdk_config, wait_until,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_open_committed_but_not_received_by_entry_returns_maybe_observed_without_gateway_state()
-> TestResult {
    timeout(Duration::from_secs(10), sdk_open_before_entry_case()).await??;
    Ok(())
}

async fn sdk_open_before_entry_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;
    let gateway_a = RunningGateway::start(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory.clone(),
    )
    .await?;
    let gateway_b = RunningGateway::start(
        GATEWAY_B,
        GATEWAY_B_KEY,
        GATEWAY_A,
        GATEWAY_A_KEY,
        directory,
    )
    .await?;

    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(2), || {
        gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;

    let mut proxy = SdkOpenDroppingProxy::start(gateway_a.sdk_address).await?;
    let connector = Connector::connect(
        SdkConfig::new(proxy.address.to_string())
            .with_connect_timeout(Duration::from_millis(200))
            .with_operation_timeout(Duration::from_millis(200))
            .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await?;
    let opening = tokio::spawn({
        let connector = connector.clone();
        async move { connector.open(CLIENT_ID).await }
    });
    proxy.wait_until_open_dropped().await?;

    wait_until(Duration::from_secs(1), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.connector_sessions == 1
            && entry.remote_open_attempts == 0
            && entry.live_pipes == 0
            && entry.peer_streams == 0
            && owner.pending_offers == 0
            && owner.live_pipes == 0
            && owner.peer_streams == 0
    })
    .await?;

    let error = opening
        .await?
        .err()
        .ok_or("OPEN unexpectedly succeeded without reaching the Entry Gateway")?;
    assert_eq!(error.code(), SdkErrorCode::DeadlineExceeded);
    assert_eq!(error.observation(), SdkPeerObservation::MaybeObserved);
    connector.close();
    proxy.finish().await?;

    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.connector_sessions == 0
            && entry.remote_open_attempts == 0
            && entry.live_pipes == 0
            && entry.peer_streams == 0
            && owner.listener_sessions == 1
            && owner.listener_bindings == 1
            && owner.pending_offers == 0
            && owner.live_pipes == 0
            && owner.peer_streams == 0
    })
    .await?;

    let fresh_connector = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;
    let (mut connector_pipe, mut listener_pipe) =
        tokio::try_join!(fresh_connector.open(CLIENT_ID), listener.accept())?;
    let payload = b"fresh after dropped open";
    connector_pipe.write_all(payload).await?;
    let mut received = vec![0; payload.len()];
    listener_pipe.read_exact(&mut received).await?;
    assert_eq!(received, payload);
    connector_pipe.close().await?;
    listener_pipe.close().await?;

    fresh_connector.close();
    listener_runtime.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    route_table.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_remote_open_after_peer_commit_does_not_survive_late_peer_open() -> TestResult {
    timeout(Duration::from_secs(10), cancelled_peer_open_case()).await??;
    Ok(())
}

async fn cancelled_peer_open_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let owner_peer_listener = TcpListener::bind("127.0.0.1:0").await?;
    let owner_peer_address = owner_peer_listener.local_addr()?;
    let mut proxy = OpenBlockingPeerProxy::start(owner_peer_address).await?;
    let gateway_b = RunningGateway::start_with_peer_listener(
        GATEWAY_B,
        GATEWAY_B_KEY,
        GATEWAY_A,
        GATEWAY_A_KEY,
        directory.clone(),
        owner_peer_listener,
        proxy.address,
        Duration::from_secs(5),
    )
    .await?;
    let gateway_a = RunningGateway::start_with_open_response_timeout(
        GATEWAY_A,
        GATEWAY_A_KEY,
        GATEWAY_B,
        GATEWAY_B_KEY,
        directory,
        Duration::from_secs(5),
    )
    .await?;

    let listener_runtime = ListenerRuntime::connect(sdk_config(gateway_b.sdk_address)).await?;
    let listener = listener_runtime.listen(CLIENT_ID, CLIENT_KEY).await?;
    wait_until(Duration::from_secs(2), || {
        gateway_b.gateway.snapshot().route_registrations_synced == 1
    })
    .await?;
    let connector = Connector::connect(sdk_config(gateway_a.sdk_address)).await?;

    let opening = tokio::spawn({
        let connector = connector.clone();
        async move { connector.open(CLIENT_ID).await }
    });
    proxy.wait_until_open_blocked().await?;
    assert_eq!(gateway_a.gateway.snapshot().remote_open_attempts, 1);
    assert_eq!(gateway_a.gateway.snapshot().peer_streams, 1);
    assert_eq!(gateway_b.gateway.snapshot().peer_streams, 0);
    assert_eq!(gateway_b.gateway.snapshot().pending_offers, 0);

    opening.abort();
    assert!(opening.await.is_err());
    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        entry.remote_open_attempts == 0 && entry.live_pipes == 0
    })
    .await?;

    proxy.release_open()?;
    wait_until(Duration::from_secs(2), || {
        let entry = gateway_a.gateway.snapshot();
        let owner = gateway_b.gateway.snapshot();
        entry.remote_open_attempts == 0
            && entry.live_pipes == 0
            && entry.peer_streams == 0
            && owner.listener_sessions == 1
            && owner.listener_bindings == 1
            && owner.pending_offers == 0
            && owner.live_pipes == 0
            && owner.peer_streams == 0
    })
    .await?;

    observe_cancelled_offer_if_admitted(&listener).await?;
    let (mut connector_pipe, mut listener_pipe) =
        tokio::try_join!(connector.open(CLIENT_ID), listener.accept())?;
    let payload = b"fresh after peer cancel";
    connector_pipe.write_all(payload).await?;
    let mut received = vec![0; payload.len()];
    listener_pipe.read_exact(&mut received).await?;
    assert_eq!(received, payload);
    connector_pipe.close().await?;
    listener_pipe.close().await?;
    wait_until(Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 0
            && gateway_b.gateway.snapshot().peer_streams == 0
    })
    .await?;

    connector.close();
    listener_runtime.close();
    gateway_a.stop().await?;
    gateway_b.stop().await?;
    proxy.stop().await;
    route_table.stop().await?;
    Ok(())
}

async fn observe_cancelled_offer_if_admitted(listener: &Listener) -> TestResult {
    let Ok(result) = timeout(Duration::from_millis(200), listener.accept()).await else {
        return Ok(());
    };
    let mut pipe = result?;
    let mut byte = [0_u8; 1];
    let error = timeout(Duration::from_secs(1), pipe.read_into(&mut byte))
        .await?
        .err()
        .ok_or("late admitted Listener Pipe survived Connector cancellation")?;
    assert_eq!(error.code(), SdkErrorCode::Cancelled);
    assert_eq!(error.observation(), SdkPeerObservation::Observed);
    Ok(())
}

struct SdkOpenDroppingProxy {
    address: SocketAddr,
    open_dropped: Option<oneshot::Receiver<()>>,
    task: JoinHandle<TestResult>,
}

impl SdkOpenDroppingProxy {
    async fn start(upstream: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (open_dropped_tx, open_dropped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (sdk_stream, _) = listener.accept().await?;
            let gateway_stream = TcpStream::connect(upstream).await?;
            let (mut sdk_sink, mut sdk_source) =
                tokio_util::codec::Framed::new(sdk_stream, FrameCodec::default()).split();
            let (mut gateway_sink, mut gateway_source) =
                tokio_util::codec::Framed::new(gateway_stream, FrameCodec::default()).split();

            let sdk_to_gateway = async {
                let mut open_dropped_tx = Some(open_dropped_tx);
                while let Some(frame) = sdk_source.next().await {
                    let frame = frame?;
                    if matches!(&frame, Frame::Open { .. })
                        && let Some(sender) = open_dropped_tx.take()
                    {
                        let _ = sender.send(());
                        continue;
                    }
                    gateway_sink.send(frame).await?;
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            let gateway_to_sdk = async {
                while let Some(frame) = gateway_source.next().await {
                    sdk_sink.send(frame?).await?;
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            };
            tokio::select! {
                result = sdk_to_gateway => result?,
                result = gateway_to_sdk => result?,
            }
            Ok(())
        });
        Ok(Self {
            address,
            open_dropped: Some(open_dropped),
            task,
        })
    }

    async fn wait_until_open_dropped(&mut self) -> TestResult {
        let receiver = self
            .open_dropped
            .take()
            .ok_or("SDK OPEN drop was already observed")?;
        timeout(Duration::from_secs(2), receiver)
            .await
            .map_err(|_| "SDK OPEN was not intercepted before the deadline")??;
        Ok(())
    }

    async fn finish(self) -> TestResult {
        timeout(Duration::from_secs(2), self.task)
            .await
            .map_err(|_| "SDK OPEN proxy did not stop after session cleanup")???;
        Ok(())
    }
}
