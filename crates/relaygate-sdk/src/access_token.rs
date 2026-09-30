use std::{future::Future, pin::Pin, sync::Arc};

use relaygate_protocol::{BearerToken, MAX_BEARER_TOKEN_BYTES};

use crate::{Destination, Error, ErrorCode, ErrorOrigin, PeerObservation};

/// An access token supplied for one `listen` or `dial` admission decision.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken(BearerToken);

impl AccessToken {
    /// Validates and wraps a bearer token supplied by the application.
    ///
    /// The token is rejected when it is empty or exceeds the wire limit. Its
    /// contents are redacted from the [`Debug`](std::fmt::Debug) output.
    pub fn new(value: impl Into<String>) -> Result<Self, AccessTokenError> {
        let value = value.into();
        if value.is_empty() {
            return Err(AccessTokenError::Empty);
        }
        let actual = value.len();
        BearerToken::new(value)
            .map(Self)
            .map_err(|_| AccessTokenError::TooLong {
                actual,
                maximum: MAX_BEARER_TOKEN_BYTES,
            })
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccessToken([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
/// Validation failure returned while constructing an [`AccessToken`].
pub enum AccessTokenError {
    /// The supplied token was empty.
    #[error("access token must not be empty")]
    Empty,
    /// The supplied token exceeded the protocol byte limit.
    #[error("access token is {actual} bytes, maximum {maximum}")]
    TooLong {
        /// Actual token length in bytes.
        actual: usize,
        /// Maximum token length accepted by the protocol.
        maximum: usize,
    },
}

/// Admission operation for which an application token is requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessAction {
    /// Publish a destination through [`crate::Relay::listen`].
    Publish,
    /// Open a Pipe through [`crate::Relay::dial`].
    Dial,
}

/// Context passed to an application-owned dynamic token source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenRequest {
    /// Admission operation the returned token must permit.
    pub action: AccessAction,
    /// Exact destination the returned token must cover.
    pub destination: Destination,
}

/// Failure reported by an application-owned dynamic token source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("access token source is unavailable")]
pub struct AccessTokenSourceError;

type AccessTokenFuture = Pin<
    Box<
        dyn Future<Output = std::result::Result<AccessToken, AccessTokenSourceError>>
            + Send
            + 'static,
    >,
>;
type AccessTokenCallback = dyn Fn(AccessTokenRequest) -> AccessTokenFuture + Send + Sync + 'static;

#[derive(Clone)]
enum AccessTokenSourceInner {
    Static(AccessToken),
    Dynamic(Arc<AccessTokenCallback>),
}

/// Supplies a token for each new admission attempt without owning refresh policy.
#[derive(Clone)]
pub struct AccessTokenSource(AccessTokenSourceInner);

impl AccessTokenSource {
    /// Reuses one already-issued token for every admission attempt.
    ///
    /// This is appropriate only when the application owns a token whose
    /// lifetime covers reconnect and Listener republish. For renewable tokens,
    /// use [`Self::dynamic`].
    #[must_use]
    pub fn static_token(token: AccessToken) -> Self {
        Self(AccessTokenSourceInner::Static(token))
    }

    /// Calls an application-owned provider for each admission attempt.
    ///
    /// RelayGate does not issue or refresh application credentials. The
    /// callback should obtain a token from the application's backend and must
    /// not persist raw token material in RelayGate state.
    pub fn dynamic<F, Fut>(callback: F) -> Self
    where
        F: Fn(AccessTokenRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<AccessToken, AccessTokenSourceError>>
            + Send
            + 'static,
    {
        Self(AccessTokenSourceInner::Dynamic(Arc::new(move |request| {
            Box::pin(callback(request))
        })))
    }

    pub(crate) async fn supply(&self, request: AccessTokenRequest) -> Result<BearerToken, Error> {
        let action = request.action;
        let token = match &self.0 {
            AccessTokenSourceInner::Static(token) => token.clone(),
            AccessTokenSourceInner::Dynamic(callback) => callback(request).await.map_err(|_| {
                Error::new(
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    format!("application token source failed to supply {action:?} token"),
                )
                .with_origin(ErrorOrigin::TokenSource)
            })?,
        };
        Ok(token.0)
    }
}

impl From<AccessToken> for AccessTokenSource {
    fn from(value: AccessToken) -> Self {
        Self::static_token(value)
    }
}

impl std::fmt::Debug for AccessTokenSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            AccessTokenSourceInner::Static(_) => {
                formatter.write_str("AccessTokenSource::Static([REDACTED])")
            }
            AccessTokenSourceInner::Dynamic(_) => {
                formatter.write_str("AccessTokenSource::Dynamic(..)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn destination() -> Result<Destination, relaygate_destination::DestinationError> {
        "inference/stt.seoul".parse()
    }

    #[test]
    fn access_token_is_bounded_and_redacted() -> Result<(), AccessTokenError> {
        assert_eq!(AccessToken::new(""), Err(AccessTokenError::Empty));
        assert!(matches!(
            AccessToken::new("x".repeat(MAX_BEARER_TOKEN_BYTES + 1)),
            Err(AccessTokenError::TooLong { .. })
        ));
        let token = AccessToken::new("must-not-appear")?;
        assert_eq!(format!("{token:?}"), "AccessToken([REDACTED])");
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_source_runs_for_each_supply() -> Result<(), Box<dyn std::error::Error>> {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let expected = destination()?;
        let source = AccessTokenSource::dynamic(move |request| {
            let observed = Arc::clone(&observed);
            let expected = expected.clone();
            async move {
                assert_eq!(request.destination, expected);
                observed.fetch_add(1, Ordering::Relaxed);
                AccessToken::new("grant").map_err(|_| AccessTokenSourceError)
            }
        });
        for action in [AccessAction::Publish, AccessAction::Dial] {
            source
                .supply(AccessTokenRequest {
                    action,
                    destination: destination()?,
                })
                .await?;
        }
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn token_source_failure_identifies_application_dependency()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = AccessTokenSource::dynamic(|_| async { Err(AccessTokenSourceError) });
        let result = source
            .supply(AccessTokenRequest {
                action: AccessAction::Publish,
                destination: destination()?,
            })
            .await;
        let error = match result {
            Ok(_) => return Err("token source failure must be returned".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(error.origin(), ErrorOrigin::TokenSource);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
        assert!(error.message().contains("Publish token"));
        assert!(error.is_retryable());
        Ok(())
    }
}
