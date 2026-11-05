use std::{error::Error as StdError, future::Future, time::Duration};

use futures_util::{SinkExt, StreamExt};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use relaygate_protocol::{DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, SessionId};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{Config, ErrorCode, ErrorOrigin, PeerObservation, Relay};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;
type Gateway = Framed<TcpStream, FrameCodec>;

fn check_metric<F: Future<Output = TestResult>>(
    outcome: &str,
    code: &str,
    exercise: F,
) -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async { timeout(Duration::from_secs(3), exercise).await? })
    })?;
    let snapshot = snapshotter.snapshot().into_vec();
    let results = snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "relaygate_sdk_operation_results_total")
        .collect::<Vec<_>>();
    assert_eq!(results.len(), 1, "unexpected extra outcomes: {results:?}");
    let (key, _, _, value) = results[0];
    assert!(matches!(value, DebugValue::Counter(1)), "{value:?}");
    for (label, expected) in [
        ("operation", "session_connect"),
        ("outcome", outcome),
        ("code", code),
    ] {
        assert!(
            key.key()
                .labels()
                .any(|l| l.key() == label && l.value() == expected),
            "{key:?}"
        );
    }
    let durations = snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "relaygate_sdk_operation_duration_seconds")
        .collect::<Vec<_>>();
    assert_eq!(durations.len(), 1);
    let (key, _, _, value) = durations[0];
    assert!(
        key.key()
            .labels()
            .any(|l| l.key() == "outcome" && l.value() == outcome)
    );
    assert!(matches!(value, DebugValue::Histogram(values) if values.len() == 1));
    Ok(())
}

async fn hello(socket: &TcpListener) -> TestResult<Gateway> {
    let (stream, _) = socket.accept().await?;
    let mut gateway = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
    assert!(matches!(gateway.next().await, Some(Ok(Frame::Hello))));
    Ok(gateway)
}

fn config(socket: &TcpListener) -> TestResult<Config> {
    Ok(
        Config::new_insecure_for_tests(socket.local_addr()?.to_string())
            .with_connect_timeout(Duration::from_millis(100)),
    )
}

#[test]
fn initial_connection_deadline_records_error_during_handshake_and_backoff() -> TestResult {
    for reject in [false, true] {
        check_metric("error", "deadline_exceeded", async move {
            let socket = TcpListener::bind("127.0.0.1:0").await?;
            let config = config(&socket)?
                .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1));
            let (result, server) = tokio::join!(Relay::connect(config), async {
                let mut gateway = hello(&socket).await?;
                if reject {
                    gateway
                        .send(Frame::SessionRejected {
                            code: relaygate_protocol::ErrorCode::Unavailable,
                            message: "draining".into(),
                        })
                        .await?;
                }
                // Keep WELCOME pending, or force the retry wait past the total deadline.
                assert!(gateway.next().await.is_none());
                Ok::<_, Box<dyn StdError + Send + Sync>>(())
            });
            server?;
            let error = result.err().ok_or("connection unexpectedly succeeded")?;
            assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
            assert_eq!(error.origin(), ErrorOrigin::Transport);
            assert_eq!(error.observation(), PeerObservation::NotObserved);
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn initial_connection_retries_record_one_final_success() -> TestResult {
    check_metric("success", "ok", async {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let config = config(&socket)?
            .with_connect_timeout(Duration::from_secs(2))
            .with_reconnect_backoff(Duration::from_millis(1), Duration::from_millis(2));
        let (result, server) = tokio::join!(Relay::connect(config), async {
            for _ in 0..2 {
                hello(&socket)
                    .await?
                    .send(Frame::SessionRejected {
                        code: relaygate_protocol::ErrorCode::Unavailable,
                        message: "draining".into(),
                    })
                    .await?;
            }
            let mut gateway = hello(&socket).await?;
            gateway
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            Ok::<_, Box<dyn StdError + Send + Sync>>(gateway)
        });
        let relay = result?;
        let mut gateway = server?;
        relay.close();
        assert!(gateway.next().await.is_none());
        Ok(())
    })
}

#[test]
fn initial_connection_caller_cancellation_is_recorded_once() -> TestResult {
    for reject in [false, true] {
        check_metric("cancelled", "cancelled", async move {
            let socket = TcpListener::bind("127.0.0.1:0").await?;
            let config = config(&socket)?
                .with_connect_timeout(Duration::from_secs(2))
                .with_reconnect_backoff(Duration::from_secs(1), Duration::from_secs(1));
            let mut connection = Box::pin(Relay::connect(config));
            let mut gateway = tokio::select! {
                result = &mut connection => return Err(format!("unexpected result: {:?}", result.err()).into()),
                gateway = hello(&socket) => gateway?,
            };
            if reject {
                gateway
                    .send(Frame::SessionRejected {
                        code: relaygate_protocol::ErrorCode::Unavailable,
                        message: "draining".into(),
                    })
                    .await?;
                // EOF confirms the attempt ended and the outer call is in backoff.
                tokio::select! {
                    result = &mut connection => return Err(format!("unexpected result: {:?}", result.err()).into()),
                    frame = gateway.next() => assert!(frame.is_none()),
                }
            }
            drop(connection);
            if !reject {
                assert!(gateway.next().await.is_none());
            }
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn reconnect_attempt_deadline_keeps_its_error_metric() -> TestResult {
    check_metric("error", "deadline_exceeded", async {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let config = config(&socket)?;
        let (result, server) = tokio::join!(crate::session::establish(&config), async {
            let mut gateway = hello(&socket).await?;
            assert!(gateway.next().await.is_none());
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        });
        server?;
        let error = result.err().ok_or("connection unexpectedly succeeded")?;
        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        Ok(())
    })
}
