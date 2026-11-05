use crate::{
    Gateway, GatewayConfig,
    test_support::{TestAction, authorization_config, bearer_token, destination},
};
use futures_util::{SinkExt, StreamExt, task::AtomicWaker};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use relaygate_protocol::{BearerToken, ErrorCode, Frame, FrameCodec, SessionId};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::{codec::Framed, sync::CancellationToken};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct BlockingGate {
    release: Option<std::sync::mpsc::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl BlockingGate {
    // With max_blocking_threads(1), the real verification job stays queued
    // until this gate is released. No verifier hook or timing sleep is needed.
    async fn occupy() -> TestResult<Self> {
        let (release, released) = std::sync::mpsc::channel();
        let (started, ready) = oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = released.recv();
        });
        let gate = Self {
            release: Some(release),
            task: Some(task),
        };
        ready.await?;
        Ok(gate)
    }
    async fn finish(mut self) -> TestResult {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(task) = self.task.take() {
            task.await?;
        }
        Ok(())
    }
}
impl Drop for BlockingGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[derive(Debug, Default)]
struct Counts {
    requests: u64,
    results: u64,
    duration_samples: usize,
    authorization_results: u64,
    authorization_samples: usize,
    result_labels: Vec<(String, String, String)>,
    duration_outcomes: Vec<String>,
}
fn counts(snapshotter: &Snapshotter) -> Counts {
    let mut counts = Counts::default();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        match (key.key().name(), value) {
            ("relaygate_gateway_dial_requests_total", DebugValue::Counter(n)) => {
                counts.requests += n
            }
            ("relaygate_gateway_dial_results_total", DebugValue::Counter(n)) => {
                counts.results += n;
                if n > 0 {
                    let label = |name| {
                        key.key()
                            .labels()
                            .find(|label| label.key() == name)
                            .map(|label| label.value().to_owned())
                            .unwrap_or_default()
                    };
                    counts
                        .result_labels
                        .push((label("outcome"), label("code"), label("class")));
                }
            }
            ("relaygate_gateway_dial_duration_seconds", DebugValue::Histogram(n)) => {
                counts.duration_samples += n.len();
                assert!(n.iter().all(|value| value.is_finite() && value.0 >= 0.0));
                if !n.is_empty() {
                    counts.duration_outcomes.extend(
                        key.key()
                            .labels()
                            .filter(|label| label.key() == "outcome")
                            .map(|label| label.value().to_owned()),
                    );
                }
            }
            ("relaygate_gateway_authorization_results_total", DebugValue::Counter(n))
                if key
                    .key()
                    .labels()
                    .any(|label| label.key() == "operation" && label.value() == "dial") =>
            {
                counts.authorization_results += n;
            }
            ("relaygate_gateway_authorization_duration_seconds", DebugValue::Histogram(n))
                if key
                    .key()
                    .labels()
                    .any(|label| label.key() == "operation" && label.value() == "dial") =>
            {
                counts.authorization_samples += n.len();
            }
            _ => {}
        }
    }
    counts
}
impl Counts {
    fn add(&mut self, other: Counts) {
        self.requests += other.requests;
        self.results += other.results;
        self.duration_samples += other.duration_samples;
        self.authorization_results += other.authorization_results;
        self.authorization_samples += other.authorization_samples;
        self.result_labels.extend(other.result_labels);
        self.duration_outcomes.extend(other.duration_outcomes);
    }
}
async fn wait_request(snapshotter: &Snapshotter) -> TestResult<Counts> {
    let mut total = Counts::default();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            // DebuggingRecorder snapshots drain counters/histograms; retain every delta.
            total.add(counts(snapshotter));
            if total.requests == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(total)
}
fn dial_frame() -> Frame {
    let target = destination("metrics-cancel");
    let access_token = bearer_token(&target, TestAction::Dial);
    Frame::Dial {
        connection_id: 1,
        destination: target,
        access_token,
    }
}
fn assert_terminal(counts: Counts, code: &str, outcome: &str, class: &str, authorizations: u64) {
    assert_eq!(counts.requests, 1);
    assert_eq!(
        (counts.results, counts.duration_samples),
        (1, 1),
        "admitted DIAL must have exactly one terminal result and duration"
    );
    assert_eq!(
        counts.result_labels,
        [(outcome.to_owned(), code.to_owned(), class.to_owned())]
    );
    assert_eq!(counts.duration_outcomes, [outcome]);
    assert_eq!(counts.authorization_results, authorizations);
    assert_eq!(counts.authorization_samples as u64, authorizations);
}

#[test]
fn session_cancellation_during_authorization_records_one_dial_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gate = BlockingGate::occupy().await?;
            let gateway = Gateway::new(
                GatewayConfig::new(authorization_config())
                    .with_authorization_limits(1, Duration::from_secs(5)),
            )?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let shutdown = CancellationToken::new();
            let server_gateway = gateway.clone();
            let server_shutdown = shutdown.clone();
            let server =
                tokio::spawn(async move { server_gateway.serve(listener, server_shutdown).await });
            let mut client = Framed::new(TcpStream::connect(address).await?, FrameCodec::default());
            client.send(Frame::Hello).await?;
            assert!(matches!(
                client.next().await,
                Some(Ok(Frame::Welcome { .. }))
            ));
            client.send(dial_frame()).await?;
            let mut total = wait_request(&snapshotter).await?;
            shutdown.cancel();
            tokio::time::timeout(Duration::from_secs(3), server).await???;
            gate.finish().await?;
            total.add(counts(&snapshotter));
            assert_eq!(gateway.snapshot().sessions, 0);
            assert_terminal(total, "cancelled", "cancelled", "cancelled", 0);
            Ok(())
        })
    })
}

#[derive(Default)]
struct WriteControl {
    state: AtomicU8,
    blocked: AtomicBool,
    waker: AtomicWaker,
}
impl WriteControl {
    fn poll_gate(&self, cx: &Context<'_>) -> Poll<io::Result<()>> {
        match self.state.load(Ordering::SeqCst) {
            0 => Poll::Ready(Ok(())),
            1 => {
                self.waker.register(cx.waker());
                self.blocked.store(true, Ordering::SeqCst);
                Poll::Pending
            }
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "test-only writer failure",
            ))),
        }
    }
}
struct ControlledIo {
    inner: DuplexStream,
    control: Arc<WriteControl>,
}
impl AsyncRead for ControlledIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for ControlledIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.control.poll_gate(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.control.poll_gate(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[test]
fn writer_failure_during_authorization_records_one_dial_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gate = BlockingGate::occupy().await?;
            let gateway = Gateway::new(
                GatewayConfig::new(authorization_config())
                    .with_authorization_limits(1, Duration::from_secs(5)),
            )?;
            let (server_io, client_io) = tokio::io::duplex(4096);
            let control = Arc::new(WriteControl::default());
            let stream = Box::new(ControlledIo {
                inner: server_io,
                control: Arc::clone(&control),
            });
            let permit = Arc::clone(&gateway.inner.handshake_slots)
                .acquire_owned()
                .await?;
            let session_inner = Arc::clone(&gateway.inner);
            let server = tokio::spawn(async move {
                session_inner
                    .run_session(stream, CancellationToken::new(), permit)
                    .await
            });
            let mut client = Framed::new(client_io, FrameCodec::default());
            client.send(Frame::Hello).await?;
            assert!(matches!(
                client.next().await,
                Some(Ok(Frame::Welcome { .. }))
            ));
            control.state.store(1, Ordering::SeqCst);
            client.send(Frame::Ping { nonce: 17 }).await?;
            client.send(dial_frame()).await?;
            let mut total = wait_request(&snapshotter).await?;
            tokio::time::timeout(Duration::from_secs(2), async {
                while !control.blocked.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            control.state.store(2, Ordering::SeqCst);
            control.waker.wake();
            let result = tokio::time::timeout(Duration::from_secs(2), server).await??;
            assert!(result.is_err(), "writer failure must terminate the session");
            gate.finish().await?;
            total.add(counts(&snapshotter));
            assert_eq!(gateway.snapshot().sessions, 0);
            assert_terminal(total, "cancelled", "cancelled", "cancelled", 0);
            Ok(())
        })
    })
}

struct TestSession {
    client: Framed<DuplexStream, FrameCodec>,
    id: SessionId,
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), super::SessionError>>,
}

impl TestSession {
    async fn start(gateway: &Gateway) -> TestResult<Self> {
        let (server_io, client_io) = tokio::io::duplex(4096);
        let permit = Arc::clone(&gateway.inner.handshake_slots)
            .acquire_owned()
            .await?;
        let inner = Arc::clone(&gateway.inner);
        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let task =
            tokio::spawn(async move { inner.run_session(Box::new(server_io), stop, permit).await });
        let mut client = Framed::new(client_io, FrameCodec::default());
        client.send(Frame::Hello).await?;
        let Frame::Welcome { session_id } =
            tokio::time::timeout(Duration::from_secs(2), client.next())
                .await?
                .ok_or("session closed before WELCOME")??
        else {
            return Err("expected WELCOME".into());
        };
        Ok(Self {
            client,
            id: session_id,
            cancellation,
            task,
        })
    }

    async fn next(&mut self) -> TestResult<Frame> {
        Ok(
            tokio::time::timeout(Duration::from_secs(2), self.client.next())
                .await?
                .ok_or("session closed before response")??,
        )
    }

    async fn close(self) -> TestResult {
        self.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task).await???;
        Ok(())
    }
}

#[test]
fn authorization_failure_records_one_dial_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
            let mut session = TestSession::start(&gateway).await?;
            session
                .client
                .send(Frame::Dial {
                    connection_id: 1,
                    destination: destination("metrics-cancel"),
                    access_token: BearerToken::new("invalid-jwt")?,
                })
                .await?;
            assert!(matches!(
                session.next().await?,
                Frame::DialFailed {
                    code: ErrorCode::Unauthenticated,
                    ..
                }
            ));
            session.close().await?;
            assert_eq!(gateway.snapshot().sessions, 0);
            assert_terminal(
                counts(&snapshotter),
                "unauthenticated",
                "error",
                "request",
                1,
            );
            Ok(())
        })
    })
}

#[test]
fn authorized_dial_state_failure_records_one_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
            let mut session = TestSession::start(&gateway).await?;
            session.client.send(dial_frame()).await?;
            assert!(matches!(
                session.next().await?,
                Frame::DialFailed {
                    code: ErrorCode::NotFound,
                    ..
                }
            ));
            session.close().await?;
            assert_eq!(gateway.snapshot().sessions, 0);
            assert_terminal(counts(&snapshotter), "not_found", "error", "request", 1);
            Ok(())
        })
    })
}

#[test]
fn authorized_dial_state_handoff_records_one_success_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
            let mut listener = TestSession::start(&gateway).await?;
            let target = destination("metrics-cancel");
            listener
                .client
                .send(Frame::Publish {
                    request_id: 1,
                    destination: target.clone(),
                    access_token: bearer_token(&target, TestAction::Publish),
                })
                .await?;
            assert!(matches!(listener.next().await?, Frame::Published { .. }));
            let mut dialer = TestSession::start(&gateway).await?;
            dialer.client.send(dial_frame()).await?;
            let Frame::Offer { pipe_id, .. } = listener.next().await? else {
                return Err("expected OFFER".into());
            };
            let mut total = counts(&snapshotter);
            assert_eq!(total.requests, 1);
            assert_eq!(total.results, 0, "state owns the still-pending DIAL");
            listener
                .client
                .send(Frame::OfferAccepted { pipe_id })
                .await?;
            assert!(matches!(dialer.next().await?, Frame::Opened { .. }));
            dialer.close().await?;
            listener.close().await?;
            assert_eq!(gateway.snapshot().sessions, 0);
            assert_eq!(gateway.snapshot().live_pipes, 0);
            total.add(counts(&snapshotter));
            assert_terminal(total, "ok", "success", "success", 1);
            Ok(())
        })
    })
}

#[test]
fn state_handoff_after_session_removal_records_one_dial_result() -> TestResult {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
            let session = TestSession::start(&gateway).await?;
            let (operation, token) = crate::authorization::ControlOperation::take(dial_frame())
                .map_err(|_| "expected DIAL operation")?;
            assert!(
                gateway
                    .inner
                    .transition(|state| state.prepare_authorization(
                        session.id,
                        &operation,
                        std::time::Instant::now()
                    ))
                    .is_none()
            );
            let mut observation = super::PendingDialObservation::new(&operation);
            let verified = gateway
                .inner
                .authorization
                .start(token, &operation)
                .map_err(|_| "verification admission failed")?
                .finish(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .map_err(|_| "verification failed")?;
            session.cancellation.cancel();
            let session_id = session.id;
            session.close().await?;
            assert_eq!(gateway.snapshot().sessions, 0);
            let actions = gateway.inner.transition(|state| {
                state.commit_authorized(session_id, operation, verified, std::time::Instant::now())
            });
            observation.disarm();
            drop(observation);
            assert!(actions.is_empty());
            assert_terminal(
                counts(&snapshotter),
                "cancelled",
                "cancelled",
                "cancelled",
                0,
            );
            Ok(())
        })
    })
}
