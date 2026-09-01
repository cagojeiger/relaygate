use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

pub(super) struct UpdateGateProxy {
    endpoint: SocketAddr,
    armed: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    blocked_updates: Arc<AtomicUsize>,
    blocked: Arc<Notify>,
    release: Arc<Notify>,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl UpdateGateProxy {
    pub(super) async fn start(target: SocketAddr, gateway_name: &str) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let armed = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let blocked_updates = Arc::new(AtomicUsize::new(0));
        let blocked = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let target_gateway = format!(r#""gateway_name":"{gateway_name}""#).into_bytes();
        let task = tokio::spawn(serve_proxy(
            listener,
            target,
            target_gateway,
            Arc::clone(&armed),
            Arc::clone(&released),
            Arc::clone(&blocked_updates),
            Arc::clone(&blocked),
            Arc::clone(&release),
            shutdown.clone(),
        ));
        Ok(Self {
            endpoint,
            armed,
            released,
            blocked_updates,
            blocked,
            release,
            shutdown,
            task,
        })
    }

    pub(super) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(super) fn arm(&self) {
        self.released.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }

    pub(super) async fn wait_until_update_blocked(&self) -> TestResult {
        timeout(Duration::from_secs(2), async {
            while self.blocked_updates() == 0 {
                self.blocked.notified().await;
            }
        })
        .await
        .map_err(|_| "Gateway B UPDATE was not blocked")?;
        Ok(())
    }

    pub(super) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }

    pub(super) fn blocked_updates(&self) -> usize {
        self.blocked_updates.load(Ordering::Acquire)
    }

    pub(super) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "UPDATE gate proxy shutdown timed out")???;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_proxy(
    listener: TcpListener,
    target: SocketAddr,
    target_gateway: Vec<u8>,
    armed: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    blocked_updates: Arc<AtomicUsize>,
    blocked: Arc<Notify>,
    release: Arc<Notify>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, _) = accepted?;
                let target_gateway = target_gateway.clone();
                let armed = Arc::clone(&armed);
                let released = Arc::clone(&released);
                let blocked_updates = Arc::clone(&blocked_updates);
                let blocked = Arc::clone(&blocked);
                let release = Arc::clone(&release);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let result = forward_connection(
                        client,
                        target,
                        &target_gateway,
                        &armed,
                        &released,
                        &blocked_updates,
                        &blocked,
                        &release,
                        &connection_shutdown,
                    )
                    .await;
                    if let Err(error) = result
                        && !connection_shutdown.is_cancelled()
                    {
                        return Err(error);
                    }
                    Ok(())
                });
            }
            result = connections.join_next(), if !connections.is_empty() => {
                let _ = result;
            }
            () = shutdown.cancelled() => break,
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn forward_connection(
    client: TcpStream,
    target: SocketAddr,
    target_gateway: &[u8],
    armed: &AtomicBool,
    released: &AtomicBool,
    blocked_updates: &AtomicUsize,
    blocked: &Notify,
    release: &Notify,
    shutdown: &CancellationToken,
) -> io::Result<()> {
    let server = tokio::select! {
        connected = TcpStream::connect(target) => connected?,
        () = shutdown.cancelled() => return Ok(()),
    };
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut server_reader, mut server_writer) = server.into_split();
    let mut is_target_gateway = false;

    loop {
        tokio::select! {
            frame = read_frame(&mut client_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                if !is_target_gateway && frame_contains(&frame, target_gateway) {
                    is_target_gateway = true;
                }
                if is_target_gateway
                    && frame_contains(&frame, UPDATE_OPERATION)
                    && armed.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_ok()
                {
                    blocked_updates.fetch_add(1, Ordering::AcqRel);
                    blocked.notify_one();
                    while !released.load(Ordering::Acquire) {
                        tokio::select! {
                            () = release.notified() => {}
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
