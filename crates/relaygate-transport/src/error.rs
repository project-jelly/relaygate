use std::io;

use rustls::{AlertDescription, Error};

/// Recognizable TLS failures, independent of the caller's retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TlsErrorKind {
    /// Local certificate validation failed, or the peer rejected a certificate.
    Authentication,
    /// TLS messages, protocol parameters, or RelayGate ALPN were incompatible.
    Protocol,
}

impl TlsErrorKind {
    /// Classifies typed TLS errors from handshake or subsequent stream I/O.
    ///
    /// Returns `None` for ordinary I/O and unclassified TLS failures. Diagnostic
    /// strings are never parsed, and the original error remains intact.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<Self> {
        let cause = error.get_ref()?;
        if cause.is::<AlpnMismatch>() {
            return Some(Self::Protocol);
        }
        match cause.downcast_ref::<Error>()? {
            Error::InvalidCertificate(_)
            | Error::NoCertificatesPresented
            | Error::UnsupportedNameType
            | Error::AlertReceived(
                AlertDescription::BadCertificate
                | AlertDescription::UnsupportedCertificate
                | AlertDescription::CertificateRevoked
                | AlertDescription::CertificateExpired
                | AlertDescription::CertificateUnknown
                | AlertDescription::UnknownCA
                | AlertDescription::CertificateRequired,
            ) => Some(Self::Authentication),
            Error::NoApplicationProtocol
            | Error::PeerIncompatible(_)
            | Error::PeerMisbehaved(_)
            | Error::InvalidMessage(_)
            | Error::InappropriateMessage { .. }
            | Error::InappropriateHandshakeMessage { .. }
            | Error::PeerSentOversizedRecord
            | Error::DecryptError
            | Error::AlertReceived(
                AlertDescription::NoApplicationProtocol
                | AlertDescription::ProtocolVersion
                | AlertDescription::HandshakeFailure
                | AlertDescription::DecodeError
                | AlertDescription::UnexpectedMessage
                | AlertDescription::IllegalParameter,
            ) => Some(Self::Protocol),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("TLS peer did not negotiate the relaygate/3 ALPN protocol")]
pub(crate) struct AlpnMismatch;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_uses_typed_causes_not_messages_or_io_kinds() {
        for (cause, expected) in [
            (
                Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
                Some(TlsErrorKind::Authentication),
            ),
            (
                Error::AlertReceived(AlertDescription::CertificateRequired),
                Some(TlsErrorKind::Authentication),
            ),
            (Error::NoApplicationProtocol, Some(TlsErrorKind::Protocol)),
            (
                Error::AlertReceived(AlertDescription::NoApplicationProtocol),
                Some(TlsErrorKind::Protocol),
            ),
            (Error::AlertReceived(AlertDescription::InternalError), None),
        ] {
            let error = io::Error::new(io::ErrorKind::InvalidData, cause);
            assert_eq!(TlsErrorKind::from_io(&error), expected);
            assert!(error.get_ref().is_some_and(|cause| cause.is::<Error>()));
        }
        assert_eq!(
            TlsErrorKind::from_io(&io::Error::new(io::ErrorKind::InvalidData, AlpnMismatch)),
            Some(TlsErrorKind::Protocol)
        );
        for kind in [
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::ConnectionReset,
        ] {
            let error = io::Error::new(kind, "invalid peer certificate: UnknownIssuer");
            assert_eq!(TlsErrorKind::from_io(&error), None);
        }
    }
}
