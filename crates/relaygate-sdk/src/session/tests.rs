mod diagnostics;
mod token_source;

use std::{
    error::Error as StdError,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use relaygate_protocol::{BindingId, DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, PipeId, SessionId};
use relaygate_transport::{ClientTlsConfig, ServerTlsConfig};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    time::{Instant, sleep, timeout},
};
use tokio_util::codec::Framed;

use super::{ReconnectBackoff, SessionHeartbeat};
use crate::{
    AccessToken, AccessTokenSource, Config, Destination, ErrorCode, ErrorOrigin,
    GatewayTransportConfig, ListenerStatus, PeerObservation, Relay, RelayStatus,
};

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::test]
async fn connect_timeout_covers_tls_and_welcome_together() -> TestResult {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let certificate = cert.pem();
    let server = ServerTlsConfig::server_authenticated(
        certificate.as_bytes(),
        signing_key.serialize_pem().as_bytes(),
    )?;
    let client = ClientTlsConfig::server_authenticated("localhost", certificate.as_bytes())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gateway = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        sleep(Duration::from_millis(300)).await;
        let stream = server.accept(stream).await?;
        let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        assert!(matches!(transport.next().await, Some(Ok(Frame::Hello))));
        sleep(Duration::from_millis(300)).await;
        let _ = transport
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let config =
        Config::with_transport(GatewayTransportConfig::tls_tcp(address.to_string(), client))
            .with_connect_timeout(Duration::from_millis(500));
    let result = Relay::connect(config).await;
    assert!(
        matches!(
            &result,
            Err(error) if error.code() == ErrorCode::DeadlineExceeded
                && error.observation() == PeerObservation::NotObserved
                && error.origin() == ErrorOrigin::Transport
        ),
        "connection result: {:?}",
        result.as_ref().map(|_| ())
    );
    gateway.await??;
    Ok(())
}

fn heartbeat() -> SessionHeartbeat {
    let config = Config::new_insecure_for_tests("127.0.0.1:0")
        .with_heartbeat(Duration::from_secs(60), Duration::from_secs(20));
    SessionHeartbeat::new(&config, SessionId::new(), 0x43)
}

#[test]
fn inbound_activity_without_pending_probe_resets_idle_deadline() {
    let mut heartbeat = heartbeat();
    heartbeat.last_inbound = Instant::now() - Duration::from_secs(30);
    let previous_deadline = heartbeat.next_deadline();

    heartbeat.observe_inbound(&Frame::Ping { nonce: 7 });

    assert!(heartbeat.next_deadline() > previous_deadline);
    assert!(heartbeat.pending.is_none());
}

#[test]
fn pending_probe_requires_matching_pong() {
    let mut heartbeat = heartbeat();
    heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);

    assert!(matches!(
        heartbeat.on_deadline(),
        Some(Frame::Ping { nonce: 1 })
    ));
    assert!(heartbeat.pending.is_some());

    heartbeat.observe_inbound(&Frame::Data {
        pipe_id: PipeId::new(SessionId::new(), 1),
        payload: Bytes::from_static(b"x"),
    });
    assert!(heartbeat.pending.is_some());

    heartbeat.observe_inbound(&Frame::Pong { nonce: 999 });
    assert!(heartbeat.pending.is_some());

    heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });
    assert!(heartbeat.pending.is_none());
}

#[test]
fn probe_commit_starts_full_response_window() {
    let mut heartbeat = heartbeat();
    heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
    assert!(matches!(
        heartbeat.on_deadline(),
        Some(Frame::Ping { nonce: 1 })
    ));
    if let Some(pending) = heartbeat.pending.as_mut() {
        pending.deadline = Instant::now() - Duration::from_millis(1);
    }

    let committed_at = Instant::now();
    heartbeat.mark_probe_committed();

    assert!(!heartbeat.response_timed_out());
    assert!(heartbeat.next_deadline() >= committed_at + heartbeat.response_timeout);
}

#[test]
fn late_matching_pong_does_not_clear_pending_probe() {
    let mut heartbeat = heartbeat();
    heartbeat.last_inbound = Instant::now() - Duration::from_secs(60);
    assert!(matches!(
        heartbeat.on_deadline(),
        Some(Frame::Ping { nonce: 1 })
    ));
    if let Some(pending) = heartbeat.pending.as_mut() {
        pending.deadline = Instant::now() - Duration::from_millis(1);
    }

    heartbeat.observe_inbound(&Frame::Pong { nonce: 1 });

    assert!(heartbeat.pending.is_some());
    assert!(heartbeat.response_timed_out());
}

#[test]
fn reconnect_backoff_uses_bounded_jitter_and_resets() {
    let initial = Duration::from_millis(100);
    let maximum = Duration::from_millis(400);
    let mut backoff = ReconnectBackoff::new(initial, maximum);

    let first = backoff.next_delay();
    let second = backoff.next_delay();
    let third = backoff.next_delay();
    let capped = backoff.next_delay();
    assert!((Duration::from_millis(66)..=initial).contains(&first));
    assert!((Duration::from_millis(133)..=Duration::from_millis(200)).contains(&second));
    assert!((Duration::from_millis(266)..=maximum).contains(&third));
    assert!((Duration::from_millis(266)..=maximum).contains(&capped));

    backoff.reset();
    let reset = backoff.next_delay();
    assert!((Duration::from_millis(66)..=initial).contains(&reset));
}

#[tokio::test]
async fn steady_state_data_frames_do_not_rescan_listener_settlement() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let session_id = SessionId::new();
    let pipe_id = PipeId::new(session_id, 1);
    let (send_data_tx, send_data_rx) = oneshot::channel();
    let (processed_tx, processed_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            transport.next().await.ok_or("SDK closed before HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        transport.send(Frame::Welcome { session_id }).await?;
        send_data_rx.await.map_err(|_| "DATA trigger was dropped")?;
        for _ in 0..100 {
            transport
                .send(Frame::Data {
                    pipe_id,
                    payload: Bytes::from_static(b"x"),
                })
                .await?;
        }
        transport.send(Frame::Ping { nonce: 7 }).await?;
        loop {
            if matches!(
                transport.next().await.ok_or("SDK closed before PONG")??,
                Frame::Pong { nonce: 7 }
            ) {
                break;
            }
        }
        let _ = processed_tx.send(());
        let _ = shutdown_rx.await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(Config::new_insecure_for_tests(address.to_string())).await?;
    timeout(Duration::from_secs(1), async {
        while relay.desired_settlement_calls() == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let baseline = relay.desired_settlement_calls();
    send_data_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before DATA trigger")?;
    timeout(Duration::from_secs(1), processed_rx).await??;
    assert_eq!(relay.desired_settlement_calls(), baseline);

    relay.close();
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn initial_unexpected_non_welcome_frame_returns_protocol_error() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        let first = transport.next().await.ok_or("SDK closed before HELLO")??;
        if !matches!(first, Frame::Hello) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        transport.send(Frame::Ping { nonce: 1 }).await?;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let error = match Relay::connect(Config::new_insecure_for_tests(address.to_string())).await {
        Ok(relay) => {
            relay.close();
            return Err("unexpected non-WELCOME frame established RelaySession".into());
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::ProtocolError);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
    assert!(
        error
            .to_string()
            .contains("first Gateway response was not WELCOME")
    );
    server.await??;
    Ok(())
}

#[tokio::test]
async fn relay_status_snapshot_subscription_and_clones_share_state() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            transport.next().await.ok_or("SDK closed before HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        transport
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        let _ = shutdown_rx.await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(Config::new_insecure_for_tests(address.to_string())).await?;
    assert_eq!(relay.status(), RelayStatus::Active);
    let relay_clone = relay.clone();
    timeout(Duration::from_secs(1), relay.wait_ready()).await??;
    assert_eq!(relay.status(), RelayStatus::Active);
    assert_eq!(relay_clone.status(), RelayStatus::Active);

    let mut subscription = relay.subscribe_status();
    assert_eq!(subscription.current(), RelayStatus::Active);
    relay.close();
    assert_eq!(
        timeout(Duration::from_secs(1), subscription.changed()).await?,
        Some(RelayStatus::Closed)
    );
    assert_eq!(relay_clone.status(), RelayStatus::Closed);

    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn relay_status_reports_reconnecting_and_wait_ready_recovers() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let (second_hello_tx, second_hello_rx) = oneshot::channel();
    let (welcome_second_tx, welcome_second_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first_stream, _) = listener.accept().await?;
        let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            first
                .next()
                .await
                .ok_or("SDK closed before first HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        first
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        drop_first_rx
            .await
            .map_err(|_| "first-session drop trigger was dropped")?;
        drop(first);

        let (second_stream, _) = listener.accept().await?;
        let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            second
                .next()
                .await
                .ok_or("SDK closed before second HELLO")??,
            Frame::Hello
        ) {
            return Err("SDK second frame was not HELLO".into());
        }
        let _ = second_hello_tx.send(());
        welcome_second_rx
            .await
            .map_err(|_| "replacement WELCOME trigger was dropped")?;
        second
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        let _ = shutdown_rx.await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    timeout(Duration::from_secs(1), relay.wait_ready()).await??;
    let mut status = relay.subscribe_status();
    drop_first_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before first-session drop")?;
    timeout(Duration::from_secs(1), second_hello_rx).await??;
    assert_eq!(
        timeout(Duration::from_secs(1), status.changed()).await?,
        Some(RelayStatus::Reconnecting)
    );

    let mut waiting = {
        let relay = relay.clone();
        tokio::spawn(async move { relay.wait_ready().await })
    };
    sleep(Duration::from_millis(50)).await;
    assert_eq!(relay.status(), RelayStatus::Reconnecting);
    assert!(
        timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err()
    );
    welcome_second_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before replacement WELCOME")?;
    timeout(Duration::from_secs(1), waiting).await???;
    assert_eq!(relay.status(), RelayStatus::Active);

    relay.close();
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn reconnect_error_is_visible_until_recovery() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let (third_hello_tx, third_hello_rx) = oneshot::channel();
    let (welcome_third_tx, welcome_third_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first_stream, _) = listener.accept().await?;
        let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            first.next().await.ok_or("missing first HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>("invalid first HELLO".into());
        }
        first
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        drop_first_rx.await?;
        drop(first);

        let (second_stream, _) = listener.accept().await?;
        let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            second.next().await.ok_or("missing second HELLO")??,
            Frame::Hello
        ) {
            return Err("invalid second HELLO".into());
        }
        second
            .send(Frame::SessionRejected {
                code: relaygate_protocol::ErrorCode::Unavailable,
                message: "temporary Gateway refusal".to_owned(),
            })
            .await?;
        drop(second);

        let (third_stream, _) = listener.accept().await?;
        let mut third = Framed::new(third_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            third.next().await.ok_or("missing third HELLO")??,
            Frame::Hello
        ) {
            return Err("invalid third HELLO".into());
        }
        let _ = third_hello_tx.send(());
        welcome_third_rx.await?;
        third
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        let _ = shutdown_rx.await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    assert!(relay.last_error().is_none());
    drop_first_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before first-session drop")?;
    timeout(Duration::from_secs(1), third_hello_rx).await??;
    assert_eq!(relay.status(), RelayStatus::Reconnecting);
    assert_eq!(
        relay.last_error().map(|error| error.code()),
        Some(ErrorCode::Unavailable)
    );
    assert_eq!(
        relay.last_error().map(|error| error.origin()),
        Some(ErrorOrigin::Gateway)
    );

    welcome_third_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before third WELCOME")?;
    timeout(Duration::from_secs(1), relay.wait_ready()).await??;
    assert_eq!(relay.status(), RelayStatus::Active);
    assert!(relay.last_error().is_none());
    relay.close();
    assert!(relay.last_error().is_none());
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn relay_close_is_terminal_during_reconnect_handshake() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (drop_first_tx, drop_first_rx) = oneshot::channel();
    let (second_hello_tx, second_hello_rx) = oneshot::channel();
    let (try_welcome_tx, try_welcome_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first_stream, _) = listener.accept().await?;
        let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            first
                .next()
                .await
                .ok_or("SDK closed before first HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        first
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        drop_first_rx
            .await
            .map_err(|_| "first-session drop trigger was dropped")?;
        drop(first);

        let (second_stream, _) = listener.accept().await?;
        let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            second
                .next()
                .await
                .ok_or("SDK closed before second HELLO")??,
            Frame::Hello
        ) {
            return Err("SDK second frame was not HELLO".into());
        }
        let _ = second_hello_tx.send(());
        let _ = try_welcome_rx.await;
        let _ = second
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    timeout(Duration::from_secs(1), relay.wait_ready()).await??;
    let mut status = relay.subscribe_status();
    drop_first_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before first-session drop")?;
    timeout(Duration::from_secs(1), second_hello_rx).await??;
    assert_eq!(
        timeout(Duration::from_secs(1), status.changed()).await?,
        Some(RelayStatus::Reconnecting)
    );
    relay.close();
    assert_eq!(
        timeout(Duration::from_secs(1), status.changed()).await?,
        Some(RelayStatus::Closed)
    );
    let _ = try_welcome_tx.send(());
    sleep(Duration::from_millis(50)).await;
    assert_eq!(relay.status(), RelayStatus::Closed);

    server.await??;
    Ok(())
}

#[tokio::test]
async fn returned_listener_republishes_after_unexpected_runtime_frame() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let destination: Destination = "inference/stt.seoul".parse()?;
    let expected_destination = destination.clone();
    let (send_unexpected, receive_unexpected) = oneshot::channel();
    let (second_hello_tx, second_hello_rx) = oneshot::channel();
    let (welcome_second_tx, welcome_second_rx) = oneshot::channel();
    let (second_publish_tx, second_publish_rx) = oneshot::channel();
    let (send_second_published, receive_second_published) = oneshot::channel();
    let (send_republished, receive_republished) = oneshot::channel();
    let (send_shutdown, receive_shutdown) = oneshot::channel();
    let server = tokio::spawn(async move {
        let first_session_id = SessionId::new();
        let (first_stream, _) = listener.accept().await?;
        let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        let first_hello = first
            .next()
            .await
            .ok_or("SDK closed before first HELLO")??;
        if !matches!(first_hello, Frame::Hello) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        first
            .send(Frame::Welcome {
                session_id: first_session_id,
            })
            .await?;
        let first_publish = first
            .next()
            .await
            .ok_or("SDK closed before first PUBLISH")??;
        let (first_request_id, first_destination) = match first_publish {
            Frame::Publish {
                request_id,
                destination,
                ..
            } => (request_id, destination),
            _ => return Err("SDK did not PUBLISH on the first session".into()),
        };
        if first_destination != expected_destination {
            return Err("SDK published an unexpected Destination".into());
        }
        first
            .send(Frame::Published {
                request_id: first_request_id,
                binding_id: BindingId::new(),
            })
            .await?;

        receive_unexpected
            .await
            .map_err(|_| "runtime-frame trigger was dropped")?;
        first.send(Frame::Hello).await?;
        let first_end = timeout(Duration::from_secs(1), first.next()).await?;
        if matches!(first_end, Some(Ok(_))) {
            return Err("SDK kept the invalid runtime session open".into());
        }

        let second_session_id = SessionId::new();
        let (second_stream, _) = listener.accept().await?;
        let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        let second_hello = second
            .next()
            .await
            .ok_or("SDK closed before replacement HELLO")??;
        if !matches!(second_hello, Frame::Hello) {
            return Err("SDK replacement first frame was not HELLO".into());
        }
        let _ = second_hello_tx.send(());
        welcome_second_rx
            .await
            .map_err(|_| "replacement WELCOME trigger was dropped")?;
        second
            .send(Frame::Welcome {
                session_id: second_session_id,
            })
            .await?;
        let second_publish = second
            .next()
            .await
            .ok_or("SDK closed before replacement PUBLISH")??;
        let (second_request_id, second_destination) = match second_publish {
            Frame::Publish {
                request_id,
                destination,
                ..
            } => (request_id, destination),
            _ => return Err("SDK did not republish on the replacement session".into()),
        };
        let _ = second_publish_tx.send(());
        receive_second_published
            .await
            .map_err(|_| "replacement PUBLISHED trigger was dropped")?;
        second
            .send(Frame::Published {
                request_id: second_request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        let _ = send_republished.send((first_session_id, second_session_id, second_destination));
        let _ = receive_shutdown.await;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let config = Config::new_insecure_for_tests(address.to_string())
        .with_operation_timeout(Duration::from_secs(2))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20));
    let relay = Relay::connect(config).await?;
    let publication = relay
        .listen(
            destination.clone(),
            AccessTokenSource::static_token(AccessToken::new("grant")?),
        )
        .await?;
    assert_eq!(publication.status(), ListenerStatus::Active);
    assert!(publication.last_error().is_none());
    let mut publication_status = publication.subscribe_status();
    send_unexpected
        .send(())
        .map_err(|_| "fake Gateway stopped before runtime-frame trigger")?;
    timeout(Duration::from_secs(1), second_hello_rx).await??;
    assert_eq!(
        timeout(Duration::from_secs(1), publication_status.changed()).await?,
        Some(ListenerStatus::Suspended)
    );
    let failure = publication.last_error().ok_or("missing protocol failure")?;
    assert_eq!(failure.code(), ErrorCode::ProtocolError);
    assert_eq!(failure.origin(), ErrorOrigin::Transport);
    assert_eq!(
        relay.last_error().map(|error| error.code()),
        Some(ErrorCode::ProtocolError)
    );
    welcome_second_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before replacement WELCOME")?;
    timeout(Duration::from_secs(1), second_publish_rx).await??;
    send_second_published
        .send(())
        .map_err(|_| "fake Gateway stopped before replacement PUBLISHED")?;

    let (first_session_id, second_session_id, republished_destination) =
        timeout(Duration::from_secs(2), receive_republished).await??;
    assert_ne!(first_session_id, second_session_id);
    assert_eq!(republished_destination, destination);
    timeout(Duration::from_secs(1), async {
        loop {
            match publication_status.changed().await {
                Some(ListenerStatus::Active) => break Ok::<(), &'static str>(()),
                Some(_) => {}
                None => break Err("Listener status subscription closed before ACTIVE"),
            }
        }
    })
    .await??;
    assert!(publication.last_error().is_none());

    relay.close();
    let _ = send_shutdown.send(());
    server.await??;
    Ok(())
}

#[test]
fn permanent_republish_failure_settles_reconnect_episode_as_degraded() -> TestResult {
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
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let destination: Destination = "inference/stt.seoul".parse()?;
                let expected_destination = destination.clone();
                let (drop_first_tx, drop_first_rx) = oneshot::channel();
                let (republish_failed_tx, republish_failed_rx) = oneshot::channel();
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                let server = tokio::spawn(async move {
                    let first_session_id = SessionId::new();
                    let (first_stream, _) = listener.accept().await?;
                    let mut first =
                        Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
                    if !matches!(
                        first
                            .next()
                            .await
                            .ok_or("SDK closed before first HELLO")??,
                        Frame::Hello
                    ) {
                        return Err::<(), Box<dyn StdError + Send + Sync>>(
                            "SDK first frame was not HELLO".into(),
                        );
                    }
                    first
                        .send(Frame::Welcome {
                            session_id: first_session_id,
                        })
                        .await?;
                    let first_publish = first
                        .next()
                        .await
                        .ok_or("SDK closed before first PUBLISH")??;
                    let first_request_id = match first_publish {
                        Frame::Publish {
                            request_id,
                            destination,
                            ..
                        } if destination == expected_destination => request_id,
                        _ => return Err("SDK did not publish the expected Destination".into()),
                    };
                    first
                        .send(Frame::Published {
                            request_id: first_request_id,
                            binding_id: BindingId::new(),
                        })
                        .await?;
                    drop_first_rx
                        .await
                        .map_err(|_| "first-session drop trigger was dropped")?;
                    drop(first);

                    let second_session_id = SessionId::new();
                    let (second_stream, _) = listener.accept().await?;
                    let mut second =
                        Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
                    if !matches!(
                        second
                            .next()
                            .await
                            .ok_or("SDK closed before second HELLO")??,
                        Frame::Hello
                    ) {
                        return Err("SDK second frame was not HELLO".into());
                    }
                    second
                        .send(Frame::Welcome {
                            session_id: second_session_id,
                        })
                        .await?;
                    let second_publish = second
                        .next()
                        .await
                        .ok_or("SDK closed before replacement PUBLISH")??;
                    let second_request_id = match second_publish {
                        Frame::Publish {
                            request_id,
                            destination,
                            ..
                        } if destination == expected_destination => request_id,
                        _ => {
                            return Err("SDK did not republish the expected Destination".into());
                        }
                    };
                    second
                        .send(Frame::PublishFailed {
                            request_id: second_request_id,
                            code: relaygate_protocol::ErrorCode::PermissionDenied,
                            message: "revoked grant".to_owned(),
                        })
                        .await?;
                    let _ = republish_failed_tx.send(());
                    let _ = shutdown_rx.await;
                    Ok::<(), Box<dyn StdError + Send + Sync>>(())
                });

                let relay = Relay::connect(
                    Config::new_insecure_for_tests(address.to_string())
                        .with_operation_timeout(Duration::from_secs(2))
                        .with_reconnect_backoff(
                            Duration::from_millis(10),
                            Duration::from_millis(20),
                        ),
                )
                .await?;
                assert_eq!(relay.status(), RelayStatus::Active);
                let publication = relay
                    .listen(
                        destination,
                        AccessTokenSource::static_token(AccessToken::new("grant")?),
                    )
                    .await?;
                assert_eq!(publication.status(), ListenerStatus::Active);
                let mut listener_status = publication.subscribe_status();
                drop_first_tx
                    .send(())
                    .map_err(|_| "fake Gateway stopped before first-session drop")?;
                timeout(Duration::from_secs(1), republish_failed_rx).await??;

                timeout(Duration::from_secs(1), async {
                    loop {
                        match listener_status.changed().await {
                            Some(ListenerStatus::Blocked) => {
                                break Ok::<(), &'static str>(());
                            }
                            Some(_) => {}
                            None => {
                                break Err("Listener status subscription closed before BLOCKED");
                            }
                        }
                    }
                })
                .await??;
                assert_eq!(relay.status(), RelayStatus::Active);
                assert_eq!(publication.status(), ListenerStatus::Blocked);
                assert_eq!(
                    publication.last_error().map(|error| error.code()),
                    Some(ErrorCode::PermissionDenied)
                );
                assert_eq!(
                    publication.last_error().map(|error| error.origin()),
                    Some(ErrorOrigin::Gateway)
                );
                drop(publication);

                timeout(Duration::from_secs(1), async {
                    let mut reconnect_gauge = 0.0;
                    loop {
                        let snapshot = snapshotter.snapshot().into_vec();
                        for (key, _, _, value) in &snapshot {
                            if key.key().name() == "relaygate_sdk_reconnect_in_progress"
                                && let DebugValue::Gauge(value) = value
                            {
                                reconnect_gauge += value.into_inner();
                            }
                        }
                        let degraded = snapshot.iter().any(|(key, _, _, value)| {
                            key.key().name() == "relaygate_sdk_reconnect_episodes_total"
                                && key.key().labels().any(|label| {
                                    label.key() == "outcome" && label.value() == "degraded"
                                })
                                && matches!(value, DebugValue::Counter(1))
                        });
                        if degraded && reconnect_gauge == 0.0 {
                            break;
                        }
                        sleep(Duration::from_millis(5)).await;
                    }
                })
                .await?;

                relay.close();
                let _ = shutdown_tx.send(());
                server.await??;
                Ok::<(), Box<dyn StdError + Send + Sync>>(())
            })
    })
}

#[tokio::test]
async fn dial_supplies_token_only_after_replacement_session_is_ready() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let destination: Destination = "inference/stt.seoul".parse()?;
    let expected_destination = destination.clone();
    let (close_first_tx, close_first_rx) = oneshot::channel();
    let (second_hello_tx, second_hello_rx) = oneshot::channel();
    let (welcome_second_tx, welcome_second_rx) = oneshot::channel();
    let (observed_token_tx, observed_token_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first_stream, _) = listener.accept().await?;
        let mut first = Framed::new(first_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            first
                .next()
                .await
                .ok_or("SDK closed before first HELLO")??,
            Frame::Hello
        ) {
            return Err::<(), Box<dyn StdError + Send + Sync>>(
                "SDK first frame was not HELLO".into(),
            );
        }
        first
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        close_first_rx
            .await
            .map_err(|_| "first-session close trigger was dropped")?;
        drop(first);

        let (second_stream, _) = listener.accept().await?;
        let mut second = Framed::new(second_stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
        if !matches!(
            second
                .next()
                .await
                .ok_or("SDK closed before replacement HELLO")??,
            Frame::Hello
        ) {
            return Err("SDK replacement first frame was not HELLO".into());
        }
        let _ = second_hello_tx.send(());
        welcome_second_rx
            .await
            .map_err(|_| "replacement WELCOME trigger was dropped")?;
        second
            .send(Frame::Welcome {
                session_id: SessionId::new(),
            })
            .await?;
        let dial = second
            .next()
            .await
            .ok_or("SDK closed before replacement DIAL")??;
        let (connection_id, access_token) = match dial {
            Frame::Dial {
                connection_id,
                destination,
                access_token,
            } if destination == expected_destination => (connection_id, access_token),
            _ => return Err("SDK did not DIAL the expected Destination".into()),
        };
        let _ = observed_token_tx.send(access_token.expose_secret().to_owned());
        second
            .send(Frame::DialFailed {
                connection_id,
                code: relaygate_protocol::ErrorCode::Unavailable,
                observation: relaygate_protocol::PeerObservation::Observed,
                message: "test rejection".to_owned(),
            })
            .await?;
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });

    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_secs(2))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)),
    )
    .await?;
    close_first_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before first-session close")?;
    timeout(Duration::from_secs(1), second_hello_rx).await??;

    let supplies = Arc::new(AtomicUsize::new(0));
    let observed_supplies = Arc::clone(&supplies);
    let source = AccessTokenSource::dynamic(move |_| {
        let observed_supplies = Arc::clone(&observed_supplies);
        async move {
            observed_supplies.fetch_add(1, Ordering::SeqCst);
            AccessToken::new("fresh-grant").map_err(|_| crate::AccessTokenSourceError)
        }
    });
    let dial_relay = relay.clone();
    let dial = tokio::spawn(async move { dial_relay.dial(destination, source).await });

    sleep(Duration::from_millis(50)).await;
    assert_eq!(supplies.load(Ordering::SeqCst), 0);
    welcome_second_tx
        .send(())
        .map_err(|_| "fake Gateway stopped before replacement WELCOME")?;
    assert_eq!(
        timeout(Duration::from_secs(1), observed_token_rx).await??,
        "fresh-grant"
    );
    assert_eq!(supplies.load(Ordering::SeqCst), 1);
    let error = match timeout(Duration::from_secs(1), dial).await?? {
        Ok(_) => return Err("fake Gateway unexpectedly accepted the test DIAL".into()),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Unavailable);

    relay.close();
    server.await??;
    Ok(())
}
