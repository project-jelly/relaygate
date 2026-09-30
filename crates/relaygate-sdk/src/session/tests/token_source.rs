use super::*;
use crate::{AccessAction, AccessTokenSourceFailure, Listener};
use tokio::net::TcpStream;

type MockGateway = Framed<TcpStream, FrameCodec>;

async fn welcome(socket: &TcpListener) -> TestResult<MockGateway> {
    let (stream, _) = socket.accept().await?;
    let mut gateway = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
    assert!(matches!(gateway.next().await, Some(Ok(Frame::Hello))));
    gateway
        .send(Frame::Welcome {
            session_id: SessionId::new(),
        })
        .await?;
    Ok(gateway)
}

async fn publish(gateway: &mut MockGateway, expected: &Destination) -> TestResult {
    match gateway.next().await.ok_or("missing PUBLISH")?? {
        Frame::Publish {
            request_id,
            destination,
            ..
        } => {
            assert_eq!(&destination, expected);
            gateway
                .send(Frame::Published {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
        }
        frame => return Err(format!("unexpected frame: {frame:?}").into()),
    }
    Ok(())
}

async fn wait_status(listener: &Listener, expected: ListenerStatus) -> TestResult {
    let mut status = listener.subscribe_status();
    while status.current() != expected {
        status.changed().await.ok_or("status subscription ended")?;
    }
    Ok(())
}

fn config(socket: &TcpListener) -> TestResult<Config> {
    Ok(
        Config::new_insecure_for_tests(socket.local_addr()?.to_string())
            .with_operation_timeout(Duration::from_secs(2))
            .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20)),
    )
}

#[tokio::test]
async fn permanent_token_failures_end_initial_operations_without_wire_commit() -> TestResult {
    timeout(Duration::from_secs(3), async {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let (relay, gateway) = tokio::join!(Relay::connect(config(&socket)?), welcome(&socket));
        let relay = relay?;
        let mut gateway = gateway?;
        for (failure, code, message) in [
            (
                AccessTokenSourceFailure::Unauthenticated,
                ErrorCode::Unauthenticated,
                "restore application authentication",
            ),
            (
                AccessTokenSourceFailure::PermissionDenied,
                ErrorCode::PermissionDenied,
                "verify application permission",
            ),
        ] {
            for action in [AccessAction::Publish, AccessAction::Dial] {
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&calls);
                let source = AccessTokenSource::dynamic_with_errors(move |request| {
                    assert_eq!(request.action, action);
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { Err(failure) }
                });
                let destination: Destination = "test/permanent".parse()?;
                let result = timeout(Duration::from_millis(200), async {
                    match action {
                        AccessAction::Publish => {
                            relay.listen(destination, source).await.map(|_| ())
                        }
                        AccessAction::Dial => relay.dial(destination, source).await.map(|_| ()),
                    }
                })
                .await?;
                let error = result
                    .err()
                    .ok_or("permanent token failure unexpectedly succeeded")?;
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(error.code(), code);
                assert_eq!(error.origin(), ErrorOrigin::TokenSource);
                assert_eq!(error.observation(), PeerObservation::NotObserved);
                assert!(!error.is_retryable());
                assert!(error.message().contains(message));
                assert!(error.message().contains(&format!("{action:?} token")));
            }
        }
        assert_eq!(relay.status(), RelayStatus::Active);
        relay.close();
        // A supplier rejection must never send PUBLISH/DIAL or end the session.
        assert!(gateway.next().await.is_none());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn classified_transient_token_failure_recovers_initial_and_returned_listener() -> TestResult {
    timeout(Duration::from_secs(3), async {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let (relay, gateway) = tokio::join!(Relay::connect(config(&socket)?), welcome(&socket));
        let relay = relay?;
        let mut gateway = gateway?;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let source = AccessTokenSource::dynamic_with_errors(move |_| {
            let attempt = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt % 3 != 2 {
                    return Err(AccessTokenSourceFailure::Unavailable);
                }
                AccessToken::new("grant").map_err(|_| AccessTokenSourceFailure::Unauthenticated)
            }
        });
        let destination: Destination = "test/transient".parse()?;
        let (listener, published) = tokio::join!(
            relay.listen(destination.clone(), source),
            publish(&mut gateway, &destination)
        );
        let listener = listener?;
        published?;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        drop(gateway);
        let mut replacement = welcome(&socket).await?;
        publish(&mut replacement, &destination).await?;
        wait_status(&listener, ListenerStatus::Active).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        assert!(listener.last_error().is_none());
        assert_eq!(relay.status(), RelayStatus::Active);
        relay.close();
        Ok(())
    })
    .await?
}

async fn permanent_republish_failure(
    failure: AccessTokenSourceFailure,
    code: ErrorCode,
) -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let (relay, gateway) = tokio::join!(Relay::connect(config(&socket)?), welcome(&socket));
    let relay = relay?;
    let mut gateway = gateway?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let source = AccessTokenSource::dynamic_with_errors(move |_| {
        let attempt = observed.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt > 0 {
                return Err(failure);
            }
            AccessToken::new("grant").map_err(|_| failure)
        }
    });
    let destination: Destination = "test/blocked".parse()?;
    let sibling_destination: Destination = "test/sibling".parse()?;
    let (listener, published) = tokio::join!(
        relay.listen(destination.clone(), source),
        publish(&mut gateway, &destination)
    );
    let listener = listener?;
    published?;
    let (sibling, published) = tokio::join!(
        relay.listen(
            sibling_destination.clone(),
            AccessTokenSource::static_token(AccessToken::new("grant")?)
        ),
        publish(&mut gateway, &sibling_destination)
    );
    let sibling = sibling?;
    published?;
    drop(gateway);
    let mut gateway = welcome(&socket).await?;
    publish(&mut gateway, &sibling_destination).await?;
    wait_status(&sibling, ListenerStatus::Active).await?;
    wait_status(&listener, ListenerStatus::Blocked).await?;
    let error = listener.last_error().ok_or("missing token source error")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.origin(), ErrorOrigin::TokenSource);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
    assert!(!error.is_retryable());
    assert_eq!(listener.accept().await.err(), Some(error.clone()));
    // No retry after multiple backoff intervals; other Listeners remain active.
    sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(relay.status(), RelayStatus::Active);
    assert!(relay.last_error().is_none());
    assert!(
        timeout(Duration::from_millis(30), gateway.next())
            .await
            .is_err()
    );
    // Further reconnects must not resurrect a blocked Listener.
    drop(gateway);
    let mut gateway = welcome(&socket).await?;
    publish(&mut gateway, &sibling_destination).await?;
    wait_status(&sibling, ListenerStatus::Active).await?;
    assert_eq!(listener.status(), ListenerStatus::Blocked);
    assert_eq!(listener.last_error(), Some(error));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // Application repair: close the blocked handle and publish with a new source.
    listener.close().await?;
    let (restored, published) = tokio::join!(
        relay.listen(
            destination.clone(),
            AccessTokenSource::static_token(AccessToken::new("new-grant")?)
        ),
        publish(&mut gateway, &destination)
    );
    assert_eq!(restored?.status(), ListenerStatus::Active);
    published?;
    assert_eq!(sibling.status(), ListenerStatus::Active);
    relay.close();
    Ok(())
}

#[test]
fn permanent_republish_token_failure_blocks_only_its_listener_and_records_degraded() -> TestResult {
    let _guard = crate::observability::RECONNECT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                for (failure, code) in [
                    (
                        AccessTokenSourceFailure::Unauthenticated,
                        ErrorCode::Unauthenticated,
                    ),
                    (
                        AccessTokenSourceFailure::PermissionDenied,
                        ErrorCode::PermissionDenied,
                    ),
                ] {
                    timeout(
                        Duration::from_secs(3),
                        permanent_republish_failure(failure, code),
                    )
                    .await??;
                }
                Ok::<_, Box<dyn StdError + Send + Sync>>(())
            })
    })?;
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(snapshot.iter().any(|(key, _, _, value)| {
        key.key().name() == "relaygate_sdk_reconnect_episodes_total"
            && key
                .key()
                .labels()
                .any(|label| label.key() == "outcome" && label.value() == "degraded")
            && matches!(value, DebugValue::Counter(2))
    }));
    Ok(())
}
