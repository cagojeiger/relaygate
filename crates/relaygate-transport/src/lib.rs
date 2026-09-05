use std::{
    fmt,
    io::{self, Cursor},
    sync::Arc,
};

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

const ALPN_PROTOCOL: &[u8] = b"relaygate/2";

pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type BoxedIo = Box<dyn AsyncIo>;

#[derive(Clone)]
pub struct ClientTlsConfig {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl ClientTlsConfig {
    pub fn server_authenticated(
        server_name: impl Into<String>,
        ca_pem: &[u8],
    ) -> Result<Self, TlsConfigError> {
        let roots = root_store(ca_pem)?;
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        Self::new(server_name, config)
    }

    pub fn mutually_authenticated(
        server_name: impl Into<String>,
        ca_pem: &[u8],
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, TlsConfigError> {
        let roots = root_store(ca_pem)?;
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                certificates(certificate_pem)?,
                private_key(private_key_pem)?,
            )
            .map_err(|error| TlsConfigError::InvalidIdentity(error.to_string()))?;
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        Self::new(server_name, config)
    }

    fn new(server_name: impl Into<String>, config: ClientConfig) -> Result<Self, TlsConfigError> {
        let server_name = ServerName::try_from(server_name.into())
            .map_err(|error| TlsConfigError::InvalidServerName(error.to_string()))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    pub async fn connect(
        &self,
        stream: TcpStream,
    ) -> Result<client::TlsStream<TcpStream>, io::Error> {
        self.connector
            .connect(self.server_name.clone(), stream)
            .await
    }

    pub async fn connect_boxed(&self, stream: TcpStream) -> Result<BoxedIo, io::Error> {
        self.connect(stream)
            .await
            .map(|stream| Box::new(stream) as BoxedIo)
    }
}

impl fmt::Debug for ClientTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientTlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ServerTlsConfig {
    acceptor: TlsAcceptor,
}

impl ServerTlsConfig {
    pub fn server_authenticated(
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, TlsConfigError> {
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                certificates(certificate_pem)?,
                private_key(private_key_pem)?,
            )
            .map_err(|error| TlsConfigError::InvalidIdentity(error.to_string()))?;
        Ok(Self::new(config))
    }

    pub fn mutually_authenticated(
        ca_pem: &[u8],
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, TlsConfigError> {
        let verifier = WebPkiClientVerifier::builder(Arc::new(root_store(ca_pem)?))
            .build()
            .map_err(|error| TlsConfigError::InvalidClientAuthority(error.to_string()))?;
        let config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                certificates(certificate_pem)?,
                private_key(private_key_pem)?,
            )
            .map_err(|error| TlsConfigError::InvalidIdentity(error.to_string()))?;
        Ok(Self::new(config))
    }

    fn new(mut config: ServerConfig) -> Self {
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        }
    }

    pub async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<server::TlsStream<TcpStream>, io::Error> {
        self.acceptor.accept(stream).await
    }

    pub async fn accept_boxed(&self, stream: TcpStream) -> Result<BoxedIo, io::Error> {
        self.accept(stream)
            .await
            .map(|stream| Box::new(stream) as BoxedIo)
    }
}

impl fmt::Debug for ServerTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerTlsConfig")
            .finish_non_exhaustive()
    }
}

fn root_store(ca_pem: &[u8]) -> Result<RootCertStore, TlsConfigError> {
    let mut roots = RootCertStore::empty();
    let certificates = certificates(ca_pem)?;
    let (accepted, _) = roots.add_parsable_certificates(certificates);
    if accepted == 0 {
        return Err(TlsConfigError::EmptyCertificateAuthority);
    }
    Ok(roots)
}

fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(TlsConfigError::InvalidPem)?;
    if certificates.is_empty() {
        return Err(TlsConfigError::EmptyCertificateChain);
    }
    Ok(certificates)
}

fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(TlsConfigError::InvalidPem)?
        .ok_or(TlsConfigError::MissingPrivateKey)
}

#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    #[error("TLS PEM could not be parsed: {0}")]
    InvalidPem(io::Error),
    #[error("TLS certificate chain is empty")]
    EmptyCertificateChain,
    #[error("TLS certificate authority has no usable certificates")]
    EmptyCertificateAuthority,
    #[error("TLS private key is missing")]
    MissingPrivateKey,
    #[error("TLS identity is invalid: {0}")]
    InvalidIdentity(String),
    #[error("TLS client authority is invalid: {0}")]
    InvalidClientAuthority(String),
    #[error("TLS server name is invalid: {0}")]
    InvalidServerName(String),
}

#[must_use]
pub fn insecure_boxed(stream: TcpStream) -> BoxedIo {
    Box::new(stream)
}
