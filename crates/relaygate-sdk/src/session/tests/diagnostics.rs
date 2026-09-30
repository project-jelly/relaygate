use super::*;
use crate::{AccessTokenSourceError, Error};
use tokio::{io::AsyncReadExt, net::TcpStream};

async fn welcome(socket: &TcpListener) -> TestResult<(Framed<TcpStream, FrameCodec>, SessionId)> {
    let (stream, _) = socket.accept().await?;
    let mut framed = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
    assert!(matches!(framed.next().await, Some(Ok(Frame::Hello))));
    let session_id = SessionId::new();
    framed.send(Frame::Welcome { session_id }).await?;
    Ok((framed, session_id))
}

#[tokio::test]
async fn initial_admission_rejection_has_no_committed_operation() -> TestResult {
    for (code, retryable) in [
        (relaygate_protocol::ErrorCode::Unavailable, true),
        (relaygate_protocol::ErrorCode::ResourceExhausted, true),
        (relaygate_protocol::ErrorCode::Unauthenticated, false),
    ] {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = socket.accept().await?;
            let mut framed = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            assert!(matches!(framed.next().await, Some(Ok(Frame::Hello))));
            framed
                .send(Frame::SessionRejected {
                    code,
                    message: "admission rejected".into(),
                })
                .await?;
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        });
        let error =
            match crate::session::establish(&Config::new_insecure_for_tests(address.to_string()))
                .await
            {
                Err(error) => error,
                Ok(_) => return Err("rejected connection succeeded".into()),
            };
        assert_eq!(error.origin(), ErrorOrigin::Gateway);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
        assert_eq!(error.is_retryable(), retryable);
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn publish_and_dial_token_deadlines_have_consistent_origin() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut framed, _) = welcome(&socket).await?;
        while let Some(frame) = framed.next().await {
            match frame? {
                Frame::Ping { nonce } => framed.send(Frame::Pong { nonce }).await?,
                _ => return Err("operation was sent without a supplied token".into()),
            }
        }
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_millis(25)),
    )
    .await?;
    // Exercise both the outer listen deadline and the token future's deadline.
    for attempt in 0..8 {
        let pending_source = AccessTokenSource::dynamic(|_| {
            std::future::pending::<Result<AccessToken, AccessTokenSourceError>>()
        });
        let error = match relay
            .listen(format!("test/deadline{attempt}").parse()?, pending_source)
            .await
        {
            Err(error) => error,
            Ok(_) => return Err("pending token unexpectedly published".into()),
        };
        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert_eq!(error.origin(), ErrorOrigin::TokenSource);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
    }
    let error = match relay
        .dial(
            "test/dial".parse()?,
            AccessTokenSource::dynamic(|_| {
                std::future::pending::<Result<AccessToken, AccessTokenSourceError>>()
            }),
        )
        .await
    {
        Err(error) => error,
        Ok(_) => return Err("pending token unexpectedly dialed".into()),
    };
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(error.origin(), ErrorOrigin::TokenSource);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
    relay.close();
    timeout(Duration::from_secs(1), server).await???;
    Ok(())
}

#[tokio::test]
async fn heartbeat_failure_reaches_relay_and_pipe_without_replay() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let (shutdown, stop) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut framed, session_id) = welcome(&socket).await?;
        match framed.next().await.ok_or("missing DIAL")?? {
            Frame::Dial { connection_id, .. } => {
                framed
                    .send(Frame::Opened {
                        pipe_id: PipeId::new(session_id, connection_id),
                    })
                    .await?;
            }
            _ => return Err("expected DIAL".into()),
        }
        // Keep TCP alive without answering the SDK's heartbeat.
        let _ = stop.await;
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_heartbeat(Duration::from_millis(100), Duration::from_millis(30))
            .with_reconnect_backoff(Duration::from_secs(5), Duration::from_secs(5)),
    )
    .await?;
    let mut status = relay.subscribe_status();
    let mut pipe = relay
        .dial("test/heartbeat".parse()?, AccessToken::new("grant")?.into())
        .await?;
    let error = match timeout(Duration::from_secs(2), pipe.read_u8()).await? {
        Err(error) => error,
        Ok(_) => return Err("heartbeat failure did not fail the Pipe".into()),
    };
    let error = Error::from_io(&error).ok_or("Pipe lost its SDK error")?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(error.origin(), ErrorOrigin::Transport);
    assert!(error.message().contains("heartbeat"));
    assert_eq!(
        timeout(Duration::from_secs(1), status.changed()).await?,
        Some(RelayStatus::Reconnecting)
    );
    let error = relay.last_error().ok_or("missing heartbeat failure")?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert!(error.message().contains("heartbeat"));
    relay.close();
    assert!(relay.last_error().is_none());
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn session_wait_deadlines_are_transport_failures() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let (disconnect, trigger) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (_framed, _) = welcome(&socket).await?;
        trigger.await?;
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_millis(30))
            .with_reconnect_backoff(Duration::from_secs(5), Duration::from_secs(5)),
    )
    .await?;
    let mut status = relay.subscribe_status();
    disconnect
        .send(())
        .map_err(|_| "disconnect trigger dropped")?;
    assert_eq!(
        timeout(Duration::from_secs(1), status.changed()).await?,
        Some(RelayStatus::Reconnecting)
    );
    assert_eq!(
        relay.last_error().map(|error| error.code()),
        Some(ErrorCode::Unavailable)
    );
    let tokens = AccessTokenSource::static_token(AccessToken::new("grant")?);
    let dial = match relay.dial("test/wait".parse()?, tokens.clone()).await {
        Err(error) => error,
        Ok(_) => return Err("dial succeeded without session".into()),
    };
    let listen = match relay.listen("test/wait".parse()?, tokens).await {
        Err(error) => error,
        Ok(_) => return Err("listen succeeded without session".into()),
    };
    for error in [dial, listen] {
        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert_eq!(error.origin(), ErrorOrigin::Transport);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
        assert!(error.message().contains("waiting for a RelaySession"));
    }
    relay.close();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn session_writer_preserves_timeout_and_io_failure() -> TestResult {
    use crate::session::send_bounded;
    use relaygate_transport::BoxedIo;
    use tokio_util::sync::CancellationToken;
    let (writer, reader) = tokio::io::duplex(1);
    let mut transport = Framed::new(
        Box::new(writer) as BoxedIo,
        FrameCodec::new(DEFAULT_MAX_FRAME_LEN),
    );
    let cancellation = CancellationToken::new();
    let error = send_bounded(
        &mut transport,
        Frame::Hello,
        Duration::from_millis(10),
        &cancellation,
    )
    .await
    .err()
    .ok_or("write should time out")?;
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(error.origin(), ErrorOrigin::Transport);
    drop(reader);
    let error = send_bounded(
        &mut transport,
        Frame::Hello,
        Duration::from_secs(1),
        &cancellation,
    )
    .await
    .err()
    .ok_or("write should fail")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.origin(), ErrorOrigin::Transport);
    Ok(())
}

#[tokio::test]
async fn initial_connect_recovers_from_transient_admission_rejection() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let server = tokio::spawn(async move {
        for code in [
            relaygate_protocol::ErrorCode::Unavailable,
            relaygate_protocol::ErrorCode::ResourceExhausted,
        ] {
            let (stream, _) = socket.accept().await?;
            let mut framed = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            assert!(matches!(framed.next().await, Some(Ok(Frame::Hello))));
            framed
                .send(Frame::SessionRejected {
                    code,
                    message: "draining".into(),
                })
                .await?;
        }
        let (mut framed, _) = welcome(&socket).await?;
        assert!(framed.next().await.is_none());
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = timeout(
        Duration::from_secs(2),
        Relay::connect(
            Config::new_insecure_for_tests(address.to_string())
                .with_connect_timeout(Duration::from_secs(1))
                .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(10)),
        ),
    )
    .await??;
    assert_eq!(relay.status(), RelayStatus::Active);
    assert!(relay.last_error().is_none());
    relay.close();
    timeout(Duration::from_secs(1), server).await???;
    Ok(())
}

#[tokio::test]
async fn token_supply_recovers_before_first_publish_and_dial() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut framed, _) = welcome(&socket).await?;
        let request_id = match framed.next().await.ok_or("missing PUBLISH")?? {
            Frame::Publish { request_id, .. } => request_id,
            _ => return Err("expected PUBLISH".into()),
        };
        framed
            .send(Frame::Published {
                request_id,
                binding_id: BindingId::new(),
            })
            .await?;
        let connection_id = match framed.next().await.ok_or("missing DIAL")?? {
            Frame::Dial { connection_id, .. } => connection_id,
            _ => return Err("expected DIAL".into()),
        };
        // The token supplier may retry; an issued wire DIAL still has one result.
        framed
            .send(Frame::DialFailed {
                connection_id,
                code: relaygate_protocol::ErrorCode::Unavailable,
                observation: relaygate_protocol::PeerObservation::NotObserved,
                message: "selected target unavailable".into(),
            })
            .await?;
        assert!(
            framed.next().await.is_none(),
            "DIAL was replayed after its result"
        );
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_secs(2))
            .with_reconnect_backoff(Duration::from_millis(5), Duration::from_millis(10)),
    )
    .await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let source = AccessTokenSource::dynamic(move |_| {
        let call = observed.fetch_add(1, Ordering::SeqCst);
        async move {
            if call % 3 != 2 {
                Err(AccessTokenSourceError)
            } else {
                AccessToken::new("grant").map_err(|_| AccessTokenSourceError)
            }
        }
    });
    let listener = relay.listen("test/retry".parse()?, source.clone()).await?;
    assert_eq!(listener.status(), ListenerStatus::Active);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    let error = relay
        .dial("test/retry".parse()?, source)
        .await
        .err()
        .ok_or("unexpected Pipe")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.origin(), ErrorOrigin::Gateway);
    assert_eq!(attempts.load(Ordering::SeqCst), 6);
    relay.close();
    timeout(Duration::from_secs(1), server).await???;
    Ok(())
}

#[tokio::test]
async fn relay_close_cancels_pending_dial_token_supply() -> TestResult {
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut framed, _) = welcome(&socket).await?;
        assert!(
            framed.next().await.is_none(),
            "DIAL committed without a token"
        );
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_secs(30)),
    )
    .await?;
    let (supplied, mut supply_started) = tokio::sync::mpsc::unbounded_channel();
    let source = AccessTokenSource::dynamic(move |_| {
        let _ = supplied.send(());
        std::future::pending::<Result<AccessToken, AccessTokenSourceError>>()
    });
    let dial_relay = relay.clone();
    let dial = tokio::spawn(async move {
        dial_relay
            .dial("test/cancel".parse()?, source)
            .await
            .map_err(Into::into)
    });
    timeout(Duration::from_secs(1), supply_started.recv())
        .await?
        .ok_or("source not called")?;
    relay.close();
    let result: TestResult<crate::Pipe> = timeout(Duration::from_secs(1), dial).await??;
    let error = result.err().ok_or("unexpected Pipe")?;
    assert_eq!(
        error.downcast_ref::<crate::Error>().map(crate::Error::code),
        Some(ErrorCode::Cancelled)
    );
    timeout(Duration::from_secs(1), server).await???;
    Ok(())
}

#[tokio::test]
async fn cancelled_initial_listen_drops_token_supply() -> TestResult {
    struct SupplyGuard(tokio::sync::mpsc::UnboundedSender<()>);
    impl Drop for SupplyGuard {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut framed, _) = welcome(&socket).await?;
        assert!(
            framed.next().await.is_none(),
            "PUBLISH committed without a token"
        );
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(
        Config::new_insecure_for_tests(address.to_string())
            .with_operation_timeout(Duration::from_secs(30)),
    )
    .await?;
    let (started, mut supply_started) = tokio::sync::mpsc::unbounded_channel();
    let (dropped, mut supply_dropped) = tokio::sync::mpsc::unbounded_channel();
    let source = AccessTokenSource::dynamic(move |_| {
        let started = started.clone();
        let dropped = dropped.clone();
        async move {
            let _guard = SupplyGuard(dropped);
            let _ = started.send(());
            std::future::pending::<Result<AccessToken, AccessTokenSourceError>>().await
        }
    });
    let listen_relay = relay.clone();
    let listen = tokio::spawn(async move {
        let destination: Destination = "test/cancel".parse()?;
        listen_relay
            .listen(destination, source)
            .await
            .map_err(Into::into)
    });
    timeout(Duration::from_secs(1), supply_started.recv())
        .await?
        .ok_or("source not called")?;
    listen.abort();
    let result: Result<TestResult<crate::Listener>, _> = listen.await;
    assert!(result.is_err_and(|error| error.is_cancelled()));
    timeout(Duration::from_secs(1), supply_dropped.recv())
        .await?
        .ok_or("source did not stop")?;
    assert_eq!(relay.status(), RelayStatus::Active);
    relay.close();
    timeout(Duration::from_secs(1), server).await???;
    Ok(())
}
