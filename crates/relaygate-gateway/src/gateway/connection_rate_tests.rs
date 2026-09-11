use std::{error::Error, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use relaygate_protocol::{Frame, FrameCodec};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use super::{Gateway, GatewayConfig};
use crate::test_support::authorization_config;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn connection_rate_rejects_before_tls_preserves_sessions_and_recovers() -> TestResult {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let tls = ServerTlsConfig::server_authenticated(
        certificate.as_bytes(),
        signing_key.serialize_pem().as_bytes(),
    )?;
    let client_tls =
        ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config())
            .with_sdk_tls(tls)
            .with_sdk_connection_rate_limit(1, 2)
            .with_max_sessions(8)
            .with_max_pending_handshakes(8)
            .with_drain_timeout(Duration::from_millis(50)),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let runtime = gateway.clone();
    let stop = shutdown.clone();
    let server = tokio::spawn(async move { runtime.serve(listener, stop).await });
    let result: TestResult = async {
        let connect = || async {
            let socket = TcpStream::connect(address).await?;
            let mut client = Framed::new(
                client_tls.connect_boxed(socket).await?,
                FrameCodec::default(),
            );
            client.send(Frame::Hello).await?;
            assert!(matches!(
                client.next().await,
                Some(Ok(Frame::Welcome { .. }))
            ));
            TestResult::Ok(client)
        };
        let mut active = timeout(Duration::from_secs(2), connect()).await??;
        let stalled = TcpStream::connect(address).await?;
        timeout(Duration::from_secs(1), async {
            while gateway.snapshot().pending_handshakes != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // Empty the initial/refilled credit without returning it on disconnect.
        while gateway.inner.connection_rate.try_acquire() {}
        assert!(!gateway.snapshot().sdk_admission_ready);
        let mut rejected = TcpStream::connect(address).await?;
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), rejected.read(&mut byte)).await??,
            0
        );
        assert_eq!(gateway.snapshot().pending_handshakes, 1);
        assert_eq!(gateway.snapshot().sessions, 1);
        active.send(Frame::Ping { nonce: 77 }).await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), active.next()).await?,
            Some(Ok(Frame::Pong { nonce: 77 }))
        ));
        timeout(Duration::from_secs(2), async {
            while !gateway.snapshot().sdk_admission_ready {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let recovered = timeout(Duration::from_secs(2), connect()).await??;
        drop((stalled, active, recovered));
        Ok(())
    }
    .await;
    shutdown.cancel();
    timeout(Duration::from_secs(7), server).await???;
    result?;
    assert_eq!(gateway.snapshot().sessions, 0);
    assert_eq!(gateway.snapshot().pending_handshakes, 0);
    assert_eq!(gateway.snapshot().session_slots_used, 0);
    Ok(())
}
