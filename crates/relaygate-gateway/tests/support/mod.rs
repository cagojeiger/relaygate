use std::{error::Error, net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_gateway::{Gateway, GatewayConfig, GatewayError, check};
use relaygate_protocol::{Frame, FrameCodec, SessionRole};
use tokio::{net::TcpStream, task::JoinHandle, time::timeout};
use tokio_util::{codec::Framed, sync::CancellationToken};

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

pub struct TestGateway {
    pub address: SocketAddr,
    shutdown: CancellationToken,
    server: JoinHandle<Result<(), GatewayError>>,
}

impl TestGateway {
    pub async fn start(client_keys: &[(&str, &str)]) -> TestResult<Self> {
        let config =
            GatewayConfig::new(client_keys.iter().map(|(client_id, client_key)| {
                ((*client_id).to_owned(), (*client_key).to_owned())
            }));
        Self::start_with_config(config).await
    }

    pub async fn start_with_config(config: GatewayConfig) -> TestResult<Self> {
        let gateway = Self::start_without_check(config).await?;
        check(gateway.address, Duration::from_secs(1)).await?;
        Ok(gateway)
    }

    pub async fn start_without_check(config: GatewayConfig) -> TestResult<Self> {
        let gateway = Gateway::new(config)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let server = tokio::spawn(async move { gateway.serve(listener, serve_shutdown).await });
        Ok(Self {
            address,
            shutdown,
            server,
        })
    }

    pub async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        self.server.await??;
        Ok(())
    }
}

pub async fn sdk_session(
    address: SocketAddr,
    role: SessionRole,
) -> TestResult<Framed<TcpStream, FrameCodec>> {
    let stream = TcpStream::connect(address).await?;
    let mut framed = Framed::new(stream, FrameCodec::default());
    framed.send(Frame::Hello { role }).await?;
    if !matches!(next_frame(&mut framed).await?, Frame::Welcome { .. }) {
        return Err("Gateway did not welcome the SDK session".into());
    }
    Ok(framed)
}

pub async fn next_frame(framed: &mut Framed<TcpStream, FrameCodec>) -> TestResult<Frame> {
    match timeout(Duration::from_secs(2), framed.next()).await {
        Ok(Some(Ok(frame))) => Ok(frame),
        Ok(Some(Err(error))) => Err(error.into()),
        Ok(None) => Err("Gateway closed the session".into()),
        Err(error) => Err(error.into()),
    }
}
