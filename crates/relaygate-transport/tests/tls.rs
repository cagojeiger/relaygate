use std::time::Duration;

use rcgen::{CertifiedKey, generate_simple_self_signed};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

#[tokio::test]
async fn tls_requires_the_configured_server_name() -> Result<(), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let server =
        ServerTlsConfig::server_authenticated(certificate.as_bytes(), private_key.as_bytes())?;
    let client = ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut stream = server.accept(stream).await?;
        stream.write_all(b"tls-ok").await?;
        stream.shutdown().await?;
        Ok::<_, std::io::Error>(())
    });
    let mut stream = client.connect(TcpStream::connect(address).await?).await?;
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await?;
    assert_eq!(payload, b"tls-ok");
    accepted.await??;

    let wrong_client = ClientTlsConfig::server_authenticated("wrong.test", certificate.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server =
        ServerTlsConfig::server_authenticated(certificate.as_bytes(), private_key.as_bytes())?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let _ = server.accept(stream).await;
        Ok::<_, std::io::Error>(())
    });
    let result = timeout(
        Duration::from_secs(1),
        wrong_client.connect(TcpStream::connect(address).await?),
    )
    .await?;
    assert!(result.is_err(), "an invalid TLS server name was accepted");
    accepted.await??;
    Ok(())
}

#[tokio::test]
async fn mutual_tls_requires_a_client_certificate() -> Result<(), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.internal".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let server = ServerTlsConfig::mutually_authenticated(
        certificate.as_bytes(),
        certificate.as_bytes(),
        private_key.as_bytes(),
    )?;
    let client = ClientTlsConfig::mutually_authenticated(
        "relaygate.internal",
        certificate.as_bytes(),
        certificate.as_bytes(),
        private_key.as_bytes(),
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut stream = server.accept(stream).await?;
        stream.write_all(b"mtls-ok").await?;
        stream.shutdown().await?;
        Ok::<_, std::io::Error>(())
    });
    let mut stream = client.connect(TcpStream::connect(address).await?).await?;
    let mut payload = Vec::new();
    stream.read_to_end(&mut payload).await?;
    assert_eq!(payload, b"mtls-ok");
    accepted.await??;

    let server = ServerTlsConfig::mutually_authenticated(
        certificate.as_bytes(),
        certificate.as_bytes(),
        private_key.as_bytes(),
    )?;
    let anonymous =
        ClientTlsConfig::server_authenticated("relaygate.internal", certificate.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        assert!(server.accept(stream).await.is_err());
        Ok::<_, std::io::Error>(())
    });
    let mut anonymous = anonymous
        .connect(TcpStream::connect(address).await?)
        .await?;
    let mut byte = [0_u8; 1];
    let client_result = timeout(Duration::from_secs(1), anonymous.read(&mut byte)).await?;
    assert!(client_result.is_err() || matches!(client_result, Ok(0)));
    accepted.await??;
    Ok(())
}
