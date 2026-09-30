//! TLS and mutually authenticated TLS helpers for RelayGate transports.
//!
//! Configurations created by this crate require the `relaygate/3` ALPN
//! protocol. Certificate storage, trust rotation, and transport policy remain
//! responsibilities of the embedding application or platform.
#![deny(missing_docs)]

use std::{fmt, io, sync::Arc};

use rustls::{ClientConfig, RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

mod error;
pub use error::TlsErrorKind;

const ALPN_PROTOCOL: &[u8] = b"relaygate/3";

/// Object-safe asynchronous byte stream accepted by RelayGate protocol code.
pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Type-erased asynchronous byte stream used by RelayGate connections.
pub type BoxedIo = Box<dyn AsyncIo>;

/// Reusable client-side TLS configuration with RelayGate ALPN enforcement.
#[derive(Clone)]
pub struct ClientTlsConfig {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl ClientTlsConfig {
    /// Trusts the public Mozilla root certificate set bundled by `webpki-roots`.
    pub fn with_webpki_roots(server_name: impl Into<String>) -> Result<Self, TlsConfigError> {
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Self::with_root_store(server_name, roots)
    }

    /// Trusts certificates issued by the supplied PEM certificate authorities.
    ///
    /// `server_name` is verified against the server certificate.
    pub fn server_authenticated(
        server_name: impl Into<String>,
        ca_pem: &[u8],
    ) -> Result<Self, TlsConfigError> {
        let roots = root_store(ca_pem)?;
        Self::with_root_store(server_name, roots)
    }

    fn with_root_store(
        server_name: impl Into<String>,
        roots: RootCertStore,
    ) -> Result<Self, TlsConfigError> {
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        Self::new(server_name, config)
    }

    /// Configures mutual TLS using the supplied client identity and authorities.
    ///
    /// `server_name` is verified against the server certificate. The client
    /// certificate and unencrypted private key must be PEM encoded.
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

    /// Checks that `server_name` is a DNS name or IP address rustls can verify
    /// a certificate against, without building a trust store.
    pub fn validate_server_name(server_name: &str) -> Result<(), TlsConfigError> {
        ServerName::try_from(server_name)
            .map(|_| ())
            .map_err(|error| TlsConfigError::InvalidServerName(error.to_string()))
    }

    fn new(server_name: impl Into<String>, config: ClientConfig) -> Result<Self, TlsConfigError> {
        let server_name = ServerName::try_from(server_name.into())
            .map_err(|error| TlsConfigError::InvalidServerName(error.to_string()))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    /// Performs a TLS handshake and verifies that RelayGate ALPN was negotiated.
    pub async fn connect(
        &self,
        stream: TcpStream,
    ) -> Result<client::TlsStream<TcpStream>, io::Error> {
        let stream = self
            .connector
            .connect(self.server_name.clone(), stream)
            .await?;
        require_relaygate_alpn(stream.get_ref().1.alpn_protocol())?;
        Ok(stream)
    }

    /// Performs [`Self::connect`] and type-erases the resulting stream.
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

/// Reusable server-side TLS configuration with RelayGate ALPN enforcement.
#[derive(Clone)]
pub struct ServerTlsConfig {
    acceptor: TlsAcceptor,
    client_name: Option<ServerName<'static>>,
}

impl ServerTlsConfig {
    /// Configures server-authenticated TLS from a PEM certificate chain and key.
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

    /// Configures mutual TLS and requires a client certificate for `client_name`.
    ///
    /// The certificate authority, server certificate, and unencrypted server
    /// private key must be PEM encoded.
    pub fn mutually_authenticated(
        client_name: impl Into<String>,
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
        let mut config = Self::new(config);
        config.client_name = Some(
            ServerName::try_from(client_name.into())
                .map_err(|error| TlsConfigError::InvalidServerName(error.to_string()))?,
        );
        Ok(config)
    }

    fn new(mut config: ServerConfig) -> Self {
        config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
        Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            client_name: None,
        }
    }

    /// Accepts a TLS connection and verifies RelayGate ALPN and client identity.
    ///
    /// Client identity verification is performed when this configuration was
    /// created with [`Self::mutually_authenticated`].
    pub async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<server::TlsStream<TcpStream>, io::Error> {
        let stream = self.acceptor.accept(stream).await?;
        if let Some(name) = &self.client_name {
            let certificate = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "missing client certificate",
                    )
                })?;
            let certificate = rustls::server::ParsedCertificate::try_from(certificate)
                .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
            rustls::client::verify_server_name(&certificate, name)
                .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
        }
        require_relaygate_alpn(stream.get_ref().1.alpn_protocol())?;
        Ok(stream)
    }

    /// Performs [`Self::accept`] and type-erases the resulting stream.
    pub async fn accept_boxed(&self, stream: TcpStream) -> Result<BoxedIo, io::Error> {
        self.accept(stream)
            .await
            .map(|stream| Box::new(stream) as BoxedIo)
    }
}

fn require_relaygate_alpn(negotiated: Option<&[u8]>) -> Result<(), io::Error> {
    if negotiated == Some(ALPN_PROTOCOL) {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        error::AlpnMismatch,
    ))
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
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_pem)?;
    if certificates.is_empty() {
        return Err(TlsConfigError::EmptyCertificateChain);
    }
    Ok(certificates)
}

fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|error| match error {
        rustls_pki_types::pem::Error::NoItemsFound => TlsConfigError::MissingPrivateKey,
        error => invalid_pem(error),
    })
}

fn invalid_pem(error: rustls_pki_types::pem::Error) -> TlsConfigError {
    TlsConfigError::InvalidPem(io::Error::new(
        io::ErrorKind::InvalidData,
        error.to_string(),
    ))
}

/// Failure to construct a TLS client or server configuration.
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    /// A supplied PEM document could not be decoded.
    #[error("TLS PEM could not be parsed: {0}")]
    InvalidPem(io::Error),
    /// The PEM certificate chain contained no certificates.
    #[error("TLS certificate chain is empty")]
    EmptyCertificateChain,
    /// The PEM certificate authority contained no usable certificates.
    #[error("TLS certificate authority has no usable certificates")]
    EmptyCertificateAuthority,
    /// The private-key PEM document did not contain a supported private key.
    #[error("TLS private key is missing")]
    MissingPrivateKey,
    /// Rustls rejected the certificate and private-key identity.
    #[error("TLS identity is invalid: {0}")]
    InvalidIdentity(String),
    /// Rustls could not construct a client-certificate verifier.
    #[error("TLS client authority is invalid: {0}")]
    InvalidClientAuthority(String),
    /// The configured DNS name or IP address is not a valid TLS server name.
    #[error("TLS server name is invalid: {0}")]
    InvalidServerName(String),
}

/// Type-erases a plaintext TCP stream without performing a TLS handshake.
///
/// Callers should expose plaintext only through an explicit deployment policy;
/// this function does not provide fallback after a TLS failure.
#[must_use]
pub fn insecure_boxed(stream: TcpStream) -> BoxedIo {
    Box::new(stream)
}
