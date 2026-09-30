use relaygate_protocol::{ErrorCode as WireErrorCode, PeerObservation as WirePeerObservation};

/// Declares the SDK [`ErrorCode`] together with its metric name and both wire
/// conversions from one variant list, so adding a code cannot leave a table
/// behind.
macro_rules! error_codes {
    ($($(#[$doc:meta])* $variant:ident => $name:literal,)+) => {
        /// Stable SDK failure reason. This type deliberately does not expose the wire
        /// protocol enum.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum ErrorCode {
            $($(#[$doc])* $variant,)+
        }

        impl ErrorCode {
            /// Canonical snake_case name for metric labels and structured logs.
            pub(crate) const fn metric_name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub(crate) fn from_wire(value: WireErrorCode) -> Self {
                match value {
                    $(WireErrorCode::$variant => Self::$variant,)+
                }
            }

            pub(crate) fn to_wire(self) -> WireErrorCode {
                match self {
                    $(Self::$variant => WireErrorCode::$variant,)+
                }
            }
        }
    };
}

error_codes! {
    /// An input or configuration value was invalid.
    InvalidArgument => "invalid_argument",
    /// Application token supply or the operation credential could not authenticate the caller.
    Unauthenticated => "unauthenticated",
    /// Application token supply or operation permission was denied.
    PermissionDenied => "permission_denied",
    /// The requested destination or resource was not found.
    NotFound => "not_found",
    /// The operation conflicts with the current state.
    FailedPrecondition => "failed_precondition",
    /// A required session or service is temporarily unavailable.
    Unavailable => "unavailable",
    /// The operation did not finish before its deadline.
    DeadlineExceeded => "deadline_exceeded",
    /// A bounded local or Gateway resource was exhausted.
    ResourceExhausted => "resource_exhausted",
    /// The operation or owning runtime was cancelled.
    Cancelled => "cancelled",
    /// A peer violated the RelayGate wire contract.
    ProtocolError => "protocol_error",
    /// RelayGate encountered an internal failure.
    Internal => "internal",
    /// The requested registration already exists.
    AlreadyExists => "already_exists",
}

/// Peer observation for a connection or registration control operation.
///
/// On Pipe I/O errors this metadata is diagnostic only: it does not describe
/// payload delivery or revoke the fact that the Pipe was already established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerObservation {
    /// The operation was not committed to the peer.
    NotObserved,
    /// The SDK cannot determine whether the peer observed the operation.
    MaybeObserved,
    /// The peer observed and answered the operation.
    Observed,
}

/// Boundary at which the SDK observed a failure, not its ultimate root cause.
/// Combine this with [`ErrorCode`] to choose an application response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorOrigin {
    /// SDK validation, lifecycle, or local resource handling.
    Sdk,
    /// Application-owned access token source.
    TokenSource,
    /// Network, TLS, or Relay session transport.
    Transport,
    /// A failure response received from the Gateway.
    Gateway,
}

/// A terminal SDK operation error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message} ({observation:?})")]
pub struct Error {
    code: ErrorCode,
    observation: PeerObservation,
    origin: ErrorOrigin,
    message: String,
}

/// SDK result type using [`Error`] for terminal operation failures.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn new(
        code: ErrorCode,
        observation: PeerObservation,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            observation,
            origin: ErrorOrigin::Sdk,
            message: message.into(),
        }
    }

    pub(crate) fn with_origin(mut self, origin: ErrorOrigin) -> Self {
        self.origin = origin;
        self
    }

    pub(crate) fn with_observation(mut self, observation: PeerObservation) -> Self {
        self.observation = observation;
        self
    }

    pub(crate) fn from_protocol(error: relaygate_protocol::ProtocolError, context: &str) -> Self {
        let code = match &error {
            relaygate_protocol::ProtocolError::Io(_) => ErrorCode::Unavailable,
            _ => ErrorCode::ProtocolError,
        };
        Self::new(
            code,
            PeerObservation::NotObserved,
            format!("{context}: {error}"),
        )
        .with_origin(ErrorOrigin::Transport)
    }

    pub(crate) fn transport_deadline(message: &str) -> Self {
        Self::new(
            ErrorCode::DeadlineExceeded,
            PeerObservation::NotObserved,
            message,
        )
        .with_origin(ErrorOrigin::Transport)
    }

    pub(crate) fn from_gateway(
        code: WireErrorCode,
        observation: PeerObservation,
        message: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::from_wire(code), observation, message)
            .with_origin(ErrorOrigin::Gateway)
    }

    pub(crate) fn from_gateway_operation(
        action: &'static str,
        code: WireErrorCode,
        observation: PeerObservation,
        message: String,
    ) -> Self {
        let message = match code {
            WireErrorCode::Unauthenticated => {
                format!(
                    "{action} authentication failed; check the operation JWT and Gateway authentication configuration"
                )
            }
            WireErrorCode::PermissionDenied => {
                format!(
                    "{action} authorization was denied; check action and Destination grants or Gateway dependency permissions"
                )
            }
            _ => message,
        };
        Self::from_gateway(code, observation, message)
    }

    /// Returns the stable SDK failure category.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns control-operation observation metadata, not a payload receipt.
    /// Do not use this value to decide whether to replay failed Pipe I/O.
    #[must_use]
    pub const fn observation(&self) -> PeerObservation {
        self.observation
    }

    /// Returns the boundary at which the SDK observed this error. This is independent of
    /// whether the peer observed the operation.
    #[must_use]
    pub const fn origin(&self) -> ErrorOrigin {
        self.origin
    }

    /// Returns an unstructured diagnostic; branch on [`Self::code`] and
    /// [`Self::origin`] instead.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Recovers the structured SDK error carried by a Pipe I/O error from
    /// Tokio's [`AsyncRead`](tokio::io::AsyncRead) and
    /// [`AsyncWrite`](tokio::io::AsyncWrite) adapters.
    ///
    /// Returns `None` for errors a Pipe did not produce, including the ones
    /// Tokio helpers synthesize themselves (for example `read_exact`'s
    /// `UnexpectedEof` or `write_all`'s `WriteZero`).
    #[must_use]
    pub fn from_io(error: &std::io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref()
    }

    /// Classifies transient, not-observed errors for a new connection or
    /// registration control operation. The caller still decides whether to
    /// start it; the SDK does not replay the failed operation.
    ///
    /// Do not apply this hint to Pipe I/O errors, including errors recovered
    /// from Tokio I/O adapters. It can be `true` after payload was exchanged:
    /// neither that value nor `false` determines delivery or safe payload retry.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.observation, PeerObservation::NotObserved)
            && matches!(
                self.code,
                ErrorCode::Unavailable | ErrorCode::DeadlineExceeded | ErrorCode::ResourceExhausted
            )
    }

    pub(crate) fn closed() -> Self {
        Self::new(
            ErrorCode::Cancelled,
            PeerObservation::NotObserved,
            "SDK runtime is closed",
        )
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::Unavailable,
            PeerObservation::NotObserved,
            message,
        )
    }

    pub(crate) fn transport_unavailable(message: impl Into<String>) -> Self {
        Self::unavailable(message).with_origin(ErrorOrigin::Transport)
    }

    pub(crate) fn token_source_deadline() -> Self {
        Self::new(
            ErrorCode::DeadlineExceeded,
            PeerObservation::NotObserved,
            "application token source exceeded the operation deadline",
        )
        .with_origin(ErrorOrigin::TokenSource)
    }

    pub(crate) fn maybe_observed(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCode::Unavailable,
            PeerObservation::MaybeObserved,
            message,
        )
        .with_origin(ErrorOrigin::Transport)
    }

    pub(crate) fn deadline(observation: PeerObservation) -> Self {
        Self::new(
            ErrorCode::DeadlineExceeded,
            observation,
            "operation deadline exceeded",
        )
    }
}

impl PeerObservation {
    pub(crate) fn from_wire(value: WirePeerObservation) -> Self {
        match value {
            WirePeerObservation::NotObserved => Self::NotObserved,
            WirePeerObservation::MaybeObserved => Self::MaybeObserved,
            WirePeerObservation::Observed => Self::Observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorCode, ErrorOrigin, PeerObservation};

    #[test]
    fn retryable_requires_a_transient_not_observed_failure() {
        for code in [
            ErrorCode::Unavailable,
            ErrorCode::DeadlineExceeded,
            ErrorCode::ResourceExhausted,
        ] {
            let not_observed = Error::new(code, PeerObservation::NotObserved, "transient");
            let maybe_observed = Error::new(code, PeerObservation::MaybeObserved, "uncertain");
            let observed = Error::new(code, PeerObservation::Observed, "observed");
            assert!(not_observed.is_retryable());
            assert!(!maybe_observed.is_retryable());
            assert!(!observed.is_retryable());
        }

        for code in [ErrorCode::InvalidArgument, ErrorCode::PermissionDenied] {
            assert!(
                !Error::new(code, PeerObservation::NotObserved, "terminal").is_retryable(),
                "{code:?} must not produce a retry hint"
            );
        }
    }

    #[test]
    fn gateway_authorization_errors_have_stable_actionable_diagnostics() {
        let unauthenticated = Error::from_gateway_operation(
            "PUBLISH",
            relaygate_protocol::ErrorCode::Unauthenticated,
            PeerObservation::NotObserved,
            "untrusted remote text: raw-token".to_owned(),
        );
        assert_eq!(unauthenticated.code(), ErrorCode::Unauthenticated);
        assert_eq!(unauthenticated.origin(), ErrorOrigin::Gateway);
        assert!(unauthenticated.message().contains("PUBLISH authentication"));
        assert!(!unauthenticated.message().contains("raw-token"));

        let denied = Error::from_gateway_operation(
            "DIAL",
            relaygate_protocol::ErrorCode::PermissionDenied,
            PeerObservation::NotObserved,
            "untrusted remote text: raw-token".to_owned(),
        );
        assert_eq!(denied.code(), ErrorCode::PermissionDenied);
        assert_eq!(denied.origin(), ErrorOrigin::Gateway);
        assert!(denied.message().contains("DIAL authorization"));
        assert!(!denied.message().contains("raw-token"));
    }
    #[test]
    fn protocol_io_failure_remains_transport_unavailable() {
        let io = Error::from_protocol(
            relaygate_protocol::ProtocolError::Io(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset,
            )),
            "frame read",
        );
        assert_eq!(io.code(), ErrorCode::Unavailable);
        assert_eq!(io.origin(), ErrorOrigin::Transport);
        let malformed = Error::from_protocol(
            relaygate_protocol::ProtocolError::InvalidMagic,
            "frame read",
        );
        assert_eq!(malformed.code(), ErrorCode::ProtocolError);
        assert_eq!(malformed.origin(), ErrorOrigin::Transport);
    }
}
