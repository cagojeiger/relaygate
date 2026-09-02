use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::TestResult;

const HEADER_LEN: usize = 7;
const MAX_FRAME_LEN: usize = 256 * 1024;
const UPDATE_OPERATION: &[u8] = br#""operation":"UPDATE""#;
const KEEP_ALIVE_OPERATION: &[u8] = br#""operation":"KEEP_ALIVE""#;

pub(super) struct UpdateGateProxy {
    endpoint: SocketAddr,
    gate: UpdateGate,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

#[derive(Clone, Default)]
struct UpdateGate {
    armed: Arc<AtomicBool>,
    blocked: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    blocked_notify: Arc<Notify>,
    release_notify: Arc<Notify>,
    post_release_keep_alive: Arc<AtomicBool>,
    post_release_keep_alive_notify: Arc<Notify>,
}

impl UpdateGateProxy {
    pub(super) async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let gate = UpdateGate::default();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(serve_proxy(
            listener,
            target,
            gate.clone(),
            shutdown.clone(),
        ));
        Ok(Self {
            endpoint,
            gate,
            shutdown,
            task,
        })
    }

    pub(super) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(super) fn arm(&self) {
        self.gate.blocked.store(false, Ordering::Release);
        self.gate.released.store(false, Ordering::Release);
        self.gate
            .post_release_keep_alive
            .store(false, Ordering::Release);
        self.gate.armed.store(true, Ordering::Release);
    }

    pub(super) async fn wait_until_update_blocked(&self) -> TestResult {
        timeout(Duration::from_secs(2), async {
            while !self.gate.blocked.load(Ordering::Acquire) {
                let notified = self.gate.blocked_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.gate.blocked.load(Ordering::Acquire) {
                    notified.await;
                }
            }
        })
        .await
        .map_err(|_| "RouteTable UPDATE was not blocked")?;
        Ok(())
    }

    pub(super) fn release(&self) {
        self.gate.released.store(true, Ordering::Release);
        self.gate.release_notify.notify_waiters();
    }

    pub(super) async fn wait_until_post_release_keep_alive(&self) -> TestResult {
        timeout(Duration::from_secs(1), async {
            while !self.gate.post_release_keep_alive.load(Ordering::Acquire) {
                let notified = self.gate.post_release_keep_alive_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.gate.post_release_keep_alive.load(Ordering::Acquire) {
                    notified.await;
                }
            }
        })
        .await
        .map_err(|_| "current epoch did not validate its lease after the old UPDATE completed")?;
        Ok(())
    }

    pub(super) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "UPDATE gate proxy shutdown timed out")???;
        Ok(())
    }
}

async fn serve_proxy(
    listener: TcpListener,
    target: SocketAddr,
    gate: UpdateGate,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, _) = accepted?;
                let connection_shutdown = shutdown.clone();
                connections.spawn(forward_connection(
                    client,
                    target,
                    gate.clone(),
                    connection_shutdown,
                ));
            }
            result = connections.join_next(), if !connections.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) if !shutdown.is_cancelled() => return Err(error),
                    Some(Err(error)) if !shutdown.is_cancelled() => {
                        return Err(io::Error::other(format!(
                            "proxy connection task failed: {error}"
                        )));
                    }
                    Some(Ok(Err(_))) | Some(Err(_)) => {}
                }
            }
            () = shutdown.cancelled() => break,
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn forward_connection(
    client: TcpStream,
    target: SocketAddr,
    gate: UpdateGate,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let server = tokio::select! {
        connected = TcpStream::connect(target) => connected?,
        () = shutdown.cancelled() => return Ok(()),
    };
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut server_reader, mut server_writer) = server.into_split();

    loop {
        tokio::select! {
            frame = read_frame(&mut client_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                if gate.released.load(Ordering::Acquire)
                    && frame_contains(&frame, KEEP_ALIVE_OPERATION)
                {
                    gate.post_release_keep_alive.store(true, Ordering::Release);
                    gate.post_release_keep_alive_notify.notify_waiters();
                }
                if frame_contains(&frame, UPDATE_OPERATION)
                    && gate.armed.compare_exchange(
                        true,
                        false,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ).is_ok()
                {
                    gate.blocked.store(true, Ordering::Release);
                    gate.blocked_notify.notify_waiters();
                    while !gate.released.load(Ordering::Acquire) {
                        let notified = gate.release_notify.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        if gate.released.load(Ordering::Acquire) {
                            break;
                        }
                        tokio::select! {
                            () = notified => {}
                            () = shutdown.cancelled() => return Ok(()),
                        }
                    }
                }
                server_writer.write_all(&frame).await?;
            }
            frame = read_frame(&mut server_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                client_writer.write_all(&frame).await?;
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; HEADER_LEN];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    if &header[..2] != b"RT" || header[2] != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RouteTable frame header",
        ));
    }
    let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    if payload_len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RouteTable frame exceeds test proxy limit",
        ));
    }
    let mut frame = header.to_vec();
    frame.resize(HEADER_LEN + payload_len, 0);
    reader.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(Some(frame))
}

fn frame_contains(frame: &[u8], needle: &[u8]) -> bool {
    frame.windows(needle.len()).any(|window| window == needle)
}
