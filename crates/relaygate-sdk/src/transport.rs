use std::fmt;

use relaygate_transport::insecure_boxed;
use relaygate_transport::{BoxedIo, ClientTlsConfig};
use tokio::net::TcpStream;

use crate::{Error, ErrorCode, PeerObservation, Result};

/// Connection settings for the SDK-facing Gateway transport.
///
/// Relay's public `listen`, `dial`, `accept`, and `Pipe` API is independent of
/// this choice. Endpoint configuration defaults to TLS over TCP and accepts
/// explicitly selected plaintext TCP. Neither mode falls back to the other.
#[derive(Clone)]
pub struct GatewayTransportConfig {
    kind: GatewayTransport,
}

#[derive(Clone)]
enum GatewayTransport {
    TlsTcp {
        gateway_addr: String,
        tls: ClientTlsConfig,
        endpoint_name: Option<String>,
    },
    InsecureTcp {
        gateway_addr: String,
    },
}

impl GatewayTransportConfig {
    pub(crate) fn from_endpoint(endpoint: &str) -> Result<Self> {
        let (address, name, plaintext) = endpoint_parts(endpoint)?;
        if plaintext {
            return Ok(Self::insecure_tcp(address));
        }
        let tls =
            ClientTlsConfig::with_webpki_roots(name.clone()).map_err(|_| invalid_endpoint())?;
        Ok(Self {
            kind: GatewayTransport::TlsTcp {
                gateway_addr: address,
                tls,
                endpoint_name: Some(name),
            },
        })
    }

    pub(crate) fn with_ca_certificate(self, pem: &[u8]) -> Result<Self> {
        match self.kind {
            GatewayTransport::TlsTcp {
                gateway_addr,
                endpoint_name: Some(name),
                ..
            } => {
                let tls =
                    ClientTlsConfig::server_authenticated(name.clone(), pem).map_err(|_| {
                        Error::new(
                            ErrorCode::InvalidArgument,
                            PeerObservation::NotObserved,
                            "invalid private CA configuration",
                        )
                    })?;
                Ok(Self {
                    kind: GatewayTransport::TlsTcp {
                        gateway_addr,
                        tls,
                        endpoint_name: Some(name),
                    },
                })
            }
            GatewayTransport::TlsTcp {
                endpoint_name: None,
                ..
            } => Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "configure custom transport CA through ClientTlsConfig to preserve its identity",
            )),
            GatewayTransport::InsecureTcp { .. } => Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "a CA certificate requires a TLS endpoint",
            )),
        }
    }

    /// Uses RelayGate framing over a server-authenticated TLS/TCP connection.
    #[must_use]
    pub fn tls_tcp(gateway_addr: impl Into<String>, tls: ClientTlsConfig) -> Self {
        Self {
            kind: GatewayTransport::TlsTcp {
                gateway_addr: gateway_addr.into(),
                tls,
                endpoint_name: None,
            },
        }
    }

    pub(crate) fn insecure_tcp(gateway_addr: impl Into<String>) -> Self {
        Self {
            kind: GatewayTransport::InsecureTcp {
                gateway_addr: gateway_addr.into(),
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.gateway_addr().trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "Gateway address must not be empty",
            ));
        }
        Ok(())
    }

    pub(crate) async fn connect(&self) -> Result<BoxedIo> {
        let stream = TcpStream::connect(self.gateway_addr())
            .await
            .map_err(|error| Error::unavailable(format!("Gateway connection failed: {error}")))?;
        let _ = stream.set_nodelay(true);

        match &self.kind {
            GatewayTransport::TlsTcp { tls, .. } => {
                tls.connect_boxed(stream).await.map_err(|error| {
                    Error::unavailable(format!("Gateway TLS handshake failed: {error}"))
                })
            }
            GatewayTransport::InsecureTcp { .. } => Ok(insecure_boxed(stream)),
        }
    }

    fn gateway_addr(&self) -> &str {
        match &self.kind {
            GatewayTransport::TlsTcp { gateway_addr, .. } => gateway_addr,
            GatewayTransport::InsecureTcp { gateway_addr } => gateway_addr,
        }
    }
}

impl fmt::Debug for GatewayTransportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            GatewayTransport::TlsTcp { gateway_addr, .. } => formatter
                .debug_struct("TlsTcp")
                .field("gateway_addr", gateway_addr)
                .finish(),
            GatewayTransport::InsecureTcp { gateway_addr } => formatter
                .debug_struct("InsecureTcp")
                .field("gateway_addr", gateway_addr)
                .finish(),
        }
    }
}

fn invalid_endpoint() -> Error {
    Error::new(
        ErrorCode::InvalidArgument,
        PeerObservation::NotObserved,
        "expected host:port, tls://host:port or tcp://host:port (IPv6 requires brackets)",
    )
}

fn endpoint_parts(endpoint: &str) -> Result<(String, String, bool)> {
    let (address, plaintext) = if let Some(value) = endpoint.strip_prefix("tls://") {
        (value, false)
    } else if let Some(value) = endpoint.strip_prefix("tcp://") {
        (value, true)
    } else {
        (endpoint, false)
    };
    if address
        .chars()
        .any(|c| c.is_whitespace() || matches!(c, '/' | '?' | '#' | '@'))
    {
        return Err(invalid_endpoint());
    }
    let (host, port) = address.rsplit_once(':').ok_or_else(invalid_endpoint)?;
    if port.is_empty()
        || !port.bytes().all(|c| c.is_ascii_digit())
        || port.parse::<u16>().map_or(true, |value| value == 0)
    {
        return Err(invalid_endpoint());
    }
    let name = if let Some(host) = host.strip_prefix('[') {
        let host = host.strip_suffix(']').ok_or_else(invalid_endpoint)?;
        host.parse::<std::net::Ipv6Addr>()
            .map_err(|_| invalid_endpoint())?;
        host
    } else {
        if host.is_empty() || host.contains(':') {
            return Err(invalid_endpoint());
        }
        host
    };
    // Validate DNS/IP identity even for plaintext, keeping one address grammar.
    ClientTlsConfig::validate_server_name(name).map_err(|_| invalid_endpoint())?;
    Ok((address.to_owned(), name.to_owned(), plaintext))
}

#[cfg(test)]
mod endpoint_tests;
