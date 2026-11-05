//! Bounded retries before a control operation can have reached a Listener.
use std::future::Future;

use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, session::ReconnectBackoff};

/// Only use for session establishment and token supply, never wire operations
/// or Pipe I/O. A retry hint alone does not authorize replay of an operation.
pub(crate) async fn retry_precommit<T, F, Fut>(
    deadline: Instant,
    mut backoff: ReconnectBackoff,
    cancel: &CancellationToken,
    expired: Error,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut deadline_error = expired.clone();
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::closed()),
            _ = sleep_until(deadline) => return Err(deadline_error),
            result = async { attempt().await } => result,
        };
        match result {
            Err(error) if error.is_retryable() => {
                // Keep the last completed failure visible when the retry budget
                // expires, without accumulating every attempt's diagnostics.
                deadline_error = Error::new(
                    expired.code(),
                    expired.observation(),
                    format!("{}; last attempt: {error}", expired.message()),
                )
                .with_origin(expired.origin());
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.precommit.retry",
                    error_code = ?error.code(),
                    error_origin = ?error.origin(),
                    "Retrying before operation commit"
                );
            }
            result => return result,
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::closed()),
            _ = sleep_until(deadline) => return Err(deadline_error),
            _ = sleep(backoff.next_delay()) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorCode, PeerObservation};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    fn backoff() -> ReconnectBackoff {
        ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(100))
    }

    #[tokio::test(start_paused = true)]
    async fn retries_share_one_deadline() {
        let calls = AtomicUsize::new(0);
        let start = Instant::now();
        let result: Result<()> = retry_precommit(
            start + Duration::from_millis(250),
            backoff(),
            &CancellationToken::new(),
            Error::token_source_deadline(),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(Error::unavailable("temporary")))
            },
        )
        .await;
        assert!(
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.message().contains("temporary"))
        );
        assert_eq!(
            result.err().map(|error| error.code()),
            Some(ErrorCode::DeadlineExceeded)
        );
        assert_eq!(start.elapsed(), Duration::from_millis(250));
        assert!((3..=4).contains(&calls.load(Ordering::SeqCst)));
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_and_uncertain_failures_are_not_retried() {
        for error in [
            Error::new(
                ErrorCode::Unauthenticated,
                PeerObservation::NotObserved,
                "denied",
            ),
            Error::new(
                ErrorCode::PermissionDenied,
                PeerObservation::NotObserved,
                "denied",
            ),
            Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::NotObserved,
                "invalid frame",
            ),
            Error::new(
                ErrorCode::Unavailable,
                PeerObservation::MaybeObserved,
                "uncertain",
            ),
            Error::new(
                ErrorCode::Unavailable,
                PeerObservation::Observed,
                "observed",
            ),
        ] {
            let calls = AtomicUsize::new(0);
            let result: Result<()> = retry_precommit(
                Instant::now() + Duration::from_secs(1),
                backoff(),
                &CancellationToken::new(),
                Error::token_source_deadline(),
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err(error.clone()))
                },
            )
            .await;
            assert_eq!(result.err(), Some(error));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_interrupts_retry_backoff() -> Result<()> {
        let cancel = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, ready) = tokio::sync::oneshot::channel();
        let task_cancel = cancel.clone();
        let task_calls = Arc::clone(&calls);
        let task = tokio::spawn(async move {
            let mut started = Some(started);
            retry_precommit::<(), _, _>(
                Instant::now() + Duration::from_secs(1),
                backoff(),
                &task_cancel,
                Error::token_source_deadline(),
                || {
                    task_calls.fetch_add(1, Ordering::SeqCst);
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                    std::future::ready(Err(Error::unavailable("temporary")))
                },
            )
            .await
        });
        ready.await.map_err(|_| Error::closed())?;
        cancel.cancel();
        let error = task.await.map_err(|_| Error::closed())?.err();
        assert_eq!(error.map(|error| error.code()), Some(ErrorCode::Cancelled));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
