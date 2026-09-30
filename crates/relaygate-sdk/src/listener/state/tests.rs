use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak, atomic::AtomicU64},
    time::Duration,
};

use tokio::{
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{ListenerLifecycle, ListenerRuntime, ListenerState, RelayInner};
use crate::{
    AccessToken, AccessTokenSource, Config, Destination, Error, ListenerStatus, RelayStatus,
    lifetime::RuntimeLifetime, resource::RelayResources, session::ReconnectBackoff,
};

use super::super::RelaySession;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn precommit_session_end_keeps_initial_listener_retryable_with_original_deadline() -> TestResult {
    let destination: Destination = "inference/stt.seoul".parse()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let (status, _) = watch::channel(ListenerStatus::Registering);
    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    let state = Arc::new(ListenerState {
        destination,
        access_token_source: AccessTokenSource::static_token(AccessToken::new("grant")?),
        status,
        incoming_tx,
        incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        initial_deadline: deadline,
        runtime: StdMutex::new(ListenerRuntime::new(ListenerLifecycle::Pending)),
        live_pipe_slots: Arc::new(Semaphore::new(1)),
    });

    state.handle_precommit_session_end(Error::unavailable(
        "RelaySession ended before managed PUBLISH commit",
    ));

    assert_eq!(*state.status.borrow(), ListenerStatus::Registering);
    assert_eq!(state.initial_deadline, deadline);
    assert_eq!(state.lifecycle(), ListenerLifecycle::Pending);
    assert!(state.last_error().is_none());

    assert!(state.begin_registration_commit());
    assert!(state.activate());
    assert_eq!(*state.status.borrow(), ListenerStatus::Active);
    assert!(state.promote_returned());
    assert!(state.was_returned());
    Ok(())
}

#[test]
fn cancelled_relay_status_transition_closes_instead_of_resurrecting() {
    let config = Config::new_insecure_for_tests("127.0.0.1:1");
    let limits = config.resource_limits;
    let (current, _) = watch::channel::<Option<Arc<RelaySession>>>(None);
    let (status, _) = watch::channel(RelayStatus::Active);
    let cancel = CancellationToken::new();
    let inner = RelayInner {
        resources: RelayResources::new(limits),
        reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
        desired_settlement_calls: AtomicU64::new(0),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
            config.reconnect_initial,
            config.reconnect_maximum,
        ))),
        config,
        desired: StdMutex::new(HashMap::new()),
        current,
        status,
        last_error: StdMutex::new(None),
        reconcile: Arc::new(Notify::new()),
        cancel: cancel.clone(),
        lifetime: Weak::<RuntimeLifetime>::new(),
    };

    inner.set_relay_error(Some(Error::unavailable("reconnect failed")));
    cancel.cancel();
    inner.set_relay_status(RelayStatus::Reconnecting);
    assert_eq!(*inner.status.borrow(), RelayStatus::Closed);
    assert!(inner.relay_error().is_none());
    inner.set_relay_status(RelayStatus::Active);
    assert_eq!(*inner.status.borrow(), RelayStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn repeated_republish_failures_share_one_bounded_retry_timer() -> TestResult {
    let initial = Duration::from_secs(10);
    let maximum = Duration::from_secs(40);
    let config =
        Config::new_insecure_for_tests("127.0.0.1:1").with_reconnect_backoff(initial, maximum);
    let limits = config.resource_limits;
    let (current, _) = watch::channel::<Option<Arc<RelaySession>>>(None);
    let (status, _) = watch::channel(RelayStatus::Reconnecting);
    let cancel = CancellationToken::new();
    let inner = RelayInner {
        resources: RelayResources::new(limits),
        reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
        desired_settlement_calls: AtomicU64::new(0),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(initial, maximum))),
        config,
        desired: StdMutex::new(HashMap::new()),
        current,
        status,
        last_error: StdMutex::new(None),
        reconcile: Arc::new(Notify::new()),
        cancel: cancel.clone(),
        lifetime: Weak::<RuntimeLifetime>::new(),
    };

    inner.schedule_reconcile();
    inner.schedule_reconcile();

    let next = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(next, initial.saturating_mul(2));
    assert!(
        inner
            .republish_retry_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            == 1
    );
    assert!(!inner.republish_retry_is_ready());

    tokio::time::advance(initial).await;
    inner.reconcile.notified().await;
    assert!(
        inner
            .republish_retry_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            & 1
            == 0
    );
    assert!(inner.republish_retry_is_ready());

    inner.schedule_reconcile();
    let next = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(next, maximum);

    inner.reset_republish_backoff();
    assert!(inner.republish_retry_is_ready());
    let reset = inner
        .republish_backoff
        .lock()
        .map_err(|_| "republish backoff lock poisoned")?
        .current_delay();
    assert_eq!(reset, initial);
    tokio::time::advance(maximum).await;
    assert!(inner.republish_retry_is_ready());
    cancel.cancel();
    tokio::task::yield_now().await;
    Ok(())
}

#[test]
fn closed_listener_status_is_terminal() -> TestResult {
    let destination: Destination = "inference/stt.seoul".parse()?;
    let (status, mut subscription) = watch::channel(ListenerStatus::Active);
    let (incoming_tx, incoming_rx) = mpsc::channel(1);
    let state = Arc::new(ListenerState {
        destination,
        access_token_source: AccessTokenSource::static_token(AccessToken::new("grant")?),
        status,
        incoming_tx,
        incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        initial_deadline: Instant::now() + Duration::from_secs(10),
        runtime: StdMutex::new(ListenerRuntime::new(ListenerLifecycle::Returned)),
        live_pipe_slots: Arc::new(Semaphore::new(1)),
    });

    state.close(None);
    assert_eq!(*subscription.borrow_and_update(), ListenerStatus::Closed);
    state.set_status(
        ListenerStatus::Suspended,
        Some(Error::unavailable("late token source failure")),
    );
    assert_eq!(*state.status.borrow(), ListenerStatus::Closed);
    assert!(!subscription.has_changed()?);
    assert!(state.last_error().is_none());
    Ok(())
}

#[test]
fn reconnect_settlement_ignores_initial_listens_that_were_never_returned() -> TestResult {
    let config = Config::new_insecure_for_tests("127.0.0.1:1");
    let limits = config.resource_limits;
    let listener = |name: &str, status: ListenerStatus, lifecycle: ListenerLifecycle| {
        let (status, _) = watch::channel(status);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            name.parse::<Destination>()?,
            Arc::new(ListenerState {
                destination: name.parse()?,
                access_token_source: AccessTokenSource::static_token(AccessToken::new("grant")?),
                status,
                incoming_tx,
                incoming_rx: tokio::sync::Mutex::new(incoming_rx),
                initial_deadline: Instant::now() + Duration::from_secs(10),
                runtime: StdMutex::new(ListenerRuntime::new(lifecycle)),
                live_pipe_slots: Arc::new(Semaphore::new(1)),
            }),
        ))
    };
    let returned = listener(
        "inference/returned",
        ListenerStatus::Active,
        ListenerLifecycle::Returned,
    )?;
    let pending = listener(
        "inference/pending",
        ListenerStatus::Registering,
        ListenerLifecycle::Pending,
    )?;
    let (current, _) = watch::channel(None::<Arc<RelaySession>>);
    let (relay_status, _) = watch::channel(RelayStatus::Active);
    let inner = RelayInner {
        config,
        desired: StdMutex::new(HashMap::from([returned, pending])),
        current,
        status: relay_status,
        last_error: StdMutex::new(None),
        reconcile: Arc::new(Notify::new()),
        cancel: CancellationToken::new(),
        lifetime: Weak::<RuntimeLifetime>::new(),
        resources: RelayResources::new(limits),
        reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
        desired_settlement_calls: AtomicU64::new(0),
        republish_retry_epoch: Arc::new(AtomicU64::new(0)),
        republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
        ))),
    };
    assert!(matches!(
        inner.desired_settlement(),
        super::DesiredSettlement::Recovered
    ));
    Ok(())
}

#[test]
fn initial_deadline_uses_registration_progress() -> TestResult {
    use crate::{ErrorCode, ErrorOrigin, PeerObservation};
    for (progress, origin, observation) in [
        (0, ErrorOrigin::Transport, PeerObservation::NotObserved),
        (1, ErrorOrigin::TokenSource, PeerObservation::NotObserved),
        (2, ErrorOrigin::Transport, PeerObservation::MaybeObserved),
    ] {
        let (status, _) = watch::channel(ListenerStatus::Registering);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        let state = ListenerState {
            destination: "test/deadline".parse()?,
            access_token_source: AccessToken::new("grant")?.into(),
            status,
            incoming_tx,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
            initial_deadline: Instant::now(),
            runtime: StdMutex::new(ListenerRuntime::new(ListenerLifecycle::Pending)),
            live_pipe_slots: Arc::new(Semaphore::new(1)),
        };
        if progress > 0 {
            assert!(state.begin_token_supply());
        }
        if progress > 1 {
            assert!(state.begin_registration_commit());
        }
        let error = state
            .terminate_initial_operation(ErrorCode::DeadlineExceeded, "deadline", true)
            .ok_or("initial operation did not expire")?;
        assert_eq!(error.origin(), origin);
        assert_eq!(error.observation(), observation);
        assert_eq!(state.last_error(), Some(error));
        assert_eq!(*state.status.borrow(), ListenerStatus::Closed);
    }
    Ok(())
}
