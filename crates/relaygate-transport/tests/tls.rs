use std::{
    io::{self, Cursor},
    sync::Arc,
    time::Duration,
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

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

#[tokio::test]
async fn tls_client_rejects_a_server_without_alpn() -> Result<(), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let server = raw_server_tls(certificate.as_bytes(), private_key.as_bytes(), Vec::new())?;
    let client = ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        server.accept(stream).await?;
        Ok::<_, io::Error>(())
    });

    let error = client
        .connect(TcpStream::connect(address).await?)
        .await
        .expect_err("a TLS server without ALPN was accepted");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    accepted.await??;
    Ok(())
}

#[tokio::test]
async fn tls_rejects_a_peer_with_the_wrong_alpn() -> Result<(), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let server = raw_server_tls(
        certificate.as_bytes(),
        private_key.as_bytes(),
        vec![b"other/1".to_vec()],
    )?;
    let client = ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        assert!(server.accept(stream).await.is_err());
        Ok::<_, io::Error>(())
    });

    assert!(
        client
            .connect(TcpStream::connect(address).await?)
            .await
            .is_err(),
        "a TLS server with the wrong ALPN was accepted"
    );
    accepted.await??;
    Ok(())
}

#[tokio::test]
async fn tls_server_rejects_a_client_without_alpn() -> Result<(), Box<dyn std::error::Error>> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let server =
        ServerTlsConfig::server_authenticated(certificate.as_bytes(), private_key.as_bytes())?;
    let client = raw_client_tls(certificate.as_bytes(), Vec::new())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let error = server
            .accept(stream)
            .await
            .expect_err("a TLS client without ALPN was accepted");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        Ok::<_, io::Error>(())
    });

    client
        .connect(
            ServerName::try_from("relaygate.test".to_owned())?,
            TcpStream::connect(address).await?,
        )
        .await?;
    accepted.await??;
    Ok(())
}

fn raw_server_tls(
    certificate_pem: &[u8],
    private_key_pem: &[u8],
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            certificates(certificate_pem)?,
            private_key(private_key_pem)?,
        )?;
    config.alpn_protocols = alpn_protocols;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn raw_client_tls(
    ca_pem: &[u8],
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<TlsConnector, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates(ca_pem)? {
        roots.add(certificate)?;
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn_protocols;
    Ok(TlsConnector::from(Arc::new(config)))
}

fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error>> {
    Ok(rustls_pemfile::certs(&mut Cursor::new(pem)).collect::<Result<Vec<_>, _>>()?)
}

fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "test TLS private key is missing",
        )
        .into()
    })
}
