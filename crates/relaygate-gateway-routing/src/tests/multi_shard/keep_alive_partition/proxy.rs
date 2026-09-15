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
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::TestResult;

const KEEP_ALIVE_OPERATION: &[u8] = br#""operation":"KEEP_ALIVE""#;

pub(super) struct KeepAliveResponsePartition {
    endpoint: SocketAddr,
    armed: Arc<AtomicBool>,
    holding: Arc<AtomicBool>,
    dropped_responses: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl KeepAliveResponsePartition {
    pub(super) async fn start(target: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let armed = Arc::new(AtomicBool::new(false));
        let holding = Arc::new(AtomicBool::new(false));
        let dropped_responses = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(serve_proxy(
            listener,
            target,
            Arc::clone(&armed),
            Arc::clone(&holding),
            Arc::clone(&dropped_responses),
            Arc::clone(&dropped),
            shutdown.clone(),
        ));
        Ok(Self {
            endpoint,
            armed,
            holding,
            dropped_responses,
            dropped,
            shutdown,
            task,
        })
    }

    pub(super) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    pub(super) async fn wait_until_response_dropped(&self) -> TestResult {
        timeout(Duration::from_secs(2), async {
            while self.dropped_responses() == 0 {
                self.dropped.notified().await;
            }
        })
        .await
        .map_err(|_| "KEEP_ALIVE response was not dropped")?;
        Ok(())
    }

    pub(super) fn release(&self) {
        self.holding.store(false, Ordering::Release);
    }

    pub(super) fn dropped_responses(&self) -> usize {
        self.dropped_responses.load(Ordering::Acquire)
    }

    pub(super) async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "KEEP_ALIVE partition proxy shutdown timed out")???;
        Ok(())
    }
}

async fn serve_proxy(
    listener: TcpListener,
    target: SocketAddr,
    armed: Arc<AtomicBool>,
    holding: Arc<AtomicBool>,
    dropped_responses: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    loop {
        let (client, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            () = shutdown.cancelled() => return Ok(()),
        };
        if holding.load(Ordering::Acquire) {
            drop(client);
            continue;
        }
        let server = TcpStream::connect(target).await?;
        forward_connection(
            client,
            server,
            &armed,
            &holding,
            &dropped_responses,
            &dropped,
            &shutdown,
        )
        .await?;
    }
}

async fn forward_connection(
    client: TcpStream,
    server: TcpStream,
    armed: &AtomicBool,
    holding: &AtomicBool,
    dropped_responses: &AtomicUsize,
    dropped: &Notify,
    shutdown: &CancellationToken,
) -> io::Result<()> {
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut server_reader, mut server_writer) = server.into_split();
    let mut drop_next_response = false;

    loop {
        tokio::select! {
            frame = read_frame(&mut client_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                if frame_contains(&frame, KEEP_ALIVE_OPERATION)
                    && armed.swap(false, Ordering::AcqRel)
                {
                    drop_next_response = true;
                }
                server_writer.write_all(&frame).await?;
            }
            frame = read_frame(&mut server_reader) => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                if drop_next_response {
                    holding.store(true, Ordering::Release);
                    dropped_responses.fetch_add(1, Ordering::AcqRel);
                    dropped.notify_one();
                    return Ok(());
                }
                client_writer.write_all(&frame).await?;
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
    const HEADER_LEN: usize = 7;

    let mut header = [0_u8; HEADER_LEN];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let payload_len = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;
    let mut frame = header.to_vec();
    frame.resize(HEADER_LEN + payload_len, 0);
    reader.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(Some(frame))
}

fn frame_contains(frame: &[u8], needle: &[u8]) -> bool {
    frame.windows(needle.len()).any(|window| window == needle)
}
