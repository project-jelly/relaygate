use super::*;
use rcgen::{CertificateParams, KeyPair, date_time_ymd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn client_config(socket: &TcpListener, trust: &str) -> TestResult<Config> {
    Ok(Config::with_transport(GatewayTransportConfig::tls_tcp(
        socket.local_addr()?.to_string(),
        ClientTlsConfig::server_authenticated("localhost", trust.as_bytes())?,
    ))
    .with_connect_timeout(Duration::from_secs(2))
    .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20)))
}

fn assert_tls_error(error: &crate::Error, code: ErrorCode) {
    assert_eq!(error.code(), code, "{error}");
    assert_eq!(error.origin(), ErrorOrigin::Transport);
    assert_eq!(error.observation(), PeerObservation::NotObserved);
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn initial_certificate_failures_return_authentication_without_retry() -> TestResult {
    for condition in ["wrong-name", "untrusted", "expired"] {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec![if condition == "wrong-name" {
            "other.test".to_owned()
        } else {
            "localhost".to_owned()
        }])?;
        if condition == "expired" {
            params.not_before = date_time_ymd(2000, 1, 1);
            params.not_after = date_time_ymd(2001, 1, 1);
        }
        let certificate = params.self_signed(&key)?.pem();
        let trust = if condition == "untrusted" {
            generate_simple_self_signed(vec!["localhost".into()])?
                .cert
                .pem()
        } else {
            certificate.clone()
        };
        let server = ServerTlsConfig::server_authenticated(
            certificate.as_bytes(),
            key.serialize_pem().as_bytes(),
        )?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let config = client_config(&socket, &trust)?;
        let (result, server) = timeout(Duration::from_secs(3), async {
            tokio::join!(Relay::connect(config), async {
                let (stream, _) = socket.accept().await?;
                assert!(server.accept(stream).await.is_err());
                Ok::<_, Box<dyn StdError + Send + Sync>>(())
            })
        })
        .await?;
        server?;
        let error = result.err().ok_or("invalid certificate accepted")?;
        assert_tls_error(&error, ErrorCode::Unauthenticated);
        assert!(error.message().contains("certificate"));
        // Listener remains open: any retry would queue a second TCP connection.
        assert!(
            timeout(Duration::from_millis(30), socket.accept())
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn tls_protocol_rejection_is_not_retried() -> TestResult {
    let certificate = generate_simple_self_signed(vec!["localhost".into()])?
        .cert
        .pem();
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let config = client_config(&socket, &certificate)?;
    let (result, server) = timeout(Duration::from_secs(3), async {
        tokio::join!(Relay::connect(config), async {
            let (mut stream, _) = socket.accept().await?;
            let mut hello = [0; 1024];
            assert!(stream.read(&mut hello).await? > 0);
            // TLS fatal no_application_protocol alert (RFC 8446).
            stream.write_all(&[21, 3, 3, 0, 2, 2, 120]).await?;
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        })
    })
    .await?;
    server?;
    assert_tls_error(
        &result.err().ok_or("TLS alert accepted")?,
        ErrorCode::ProtocolError,
    );
    assert!(
        timeout(Duration::from_millis(30), socket.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn tls_client_certificate_alert_after_handshake_keeps_authentication_code() -> TestResult {
    let CertifiedKey { cert, signing_key } = generate_simple_self_signed(vec!["localhost".into()])?;
    let certificate = cert.pem();
    let server = ServerTlsConfig::mutually_authenticated(
        "localhost",
        certificate.as_bytes(),
        certificate.as_bytes(),
        signing_key.serialize_pem().as_bytes(),
    )?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    // Client deliberately has no mTLS identity. TLS 1.3 can deliver the server's
    // certificate_required alert after client connect(), on HELLO/WELCOME I/O.
    let config = client_config(&socket, &certificate)?;
    let (result, server) = timeout(Duration::from_secs(3), async {
        tokio::join!(Relay::connect(config), async {
            let (stream, _) = socket.accept().await?;
            assert!(server.accept(stream).await.is_err());
            Ok::<_, Box<dyn StdError + Send + Sync>>(())
        })
    })
    .await?;
    server?;
    assert_tls_error(
        &result.err().ok_or("anonymous mTLS accepted")?,
        ErrorCode::Unauthenticated,
    );
    Ok(())
}

#[tokio::test]
async fn initial_tls_io_failure_retries_and_recovers() -> TestResult {
    let good = generate_simple_self_signed(vec!["localhost".into()])?;
    let server = ServerTlsConfig::server_authenticated(
        good.cert.pem().as_bytes(),
        good.signing_key.serialize_pem().as_bytes(),
    )?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let config = client_config(&socket, &good.cert.pem())?;
    let (result, server) = timeout(Duration::from_secs(3), async {
        tokio::join!(Relay::connect(config), async {
            let (mut interrupted, _) = socket.accept().await?;
            let mut hello = [0; 1024];
            assert!(interrupted.read(&mut hello).await? > 0);
            drop(interrupted);
            welcome_tls(&socket, &server).await
        })
    })
    .await?;
    let relay = result?;
    let _session = server?;
    assert_eq!(relay.status(), RelayStatus::Active);
    relay.close();
    Ok(())
}

#[tokio::test]
async fn reconnect_survives_certificate_failure_and_recovers() -> TestResult {
    let good = generate_simple_self_signed(vec!["localhost".into()])?;
    let bad = generate_simple_self_signed(vec!["wrong.test".into()])?;
    let good_server = ServerTlsConfig::server_authenticated(
        good.cert.pem().as_bytes(),
        good.signing_key.serialize_pem().as_bytes(),
    )?;
    let bad_server = ServerTlsConfig::server_authenticated(
        bad.cert.pem().as_bytes(),
        bad.signing_key.serialize_pem().as_bytes(),
    )?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let config = client_config(&socket, &format!("{}{}", good.cert.pem(), bad.cert.pem()))?;
    let (disconnect, disconnected) = oneshot::channel();
    let (restore, restored) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut session = welcome_tls(&socket, &good_server).await?;
        publish(&mut session).await?;
        disconnected.await?;
        drop(session);
        let (stream, _) = socket.accept().await?;
        assert!(bad_server.accept(stream).await.is_err());
        restored.await?;
        session = welcome_tls(&socket, &good_server).await?;
        publish(&mut session).await?;
        assert!(!matches!(session.next().await, Some(Ok(_))));
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    });
    let relay = Relay::connect(config).await?;
    let listener = relay
        .listen("test/tls".parse()?, AccessToken::new("test-grant")?.into())
        .await?;
    let mut statuses = listener.subscribe_status();
    disconnect
        .send(())
        .map_err(|_| "disconnect receiver closed")?;
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(error) = relay.last_error()
                && error.code() == ErrorCode::Unauthenticated
            {
                assert_tls_error(&error, ErrorCode::Unauthenticated);
                assert_eq!(relay.status(), RelayStatus::Reconnecting);
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    })
    .await??;
    restore.send(()).map_err(|_| "restore receiver closed")?;
    timeout(Duration::from_secs(3), relay.wait_ready()).await??;
    assert_eq!(relay.status(), RelayStatus::Active);
    assert!(relay.last_error().is_none());
    timeout(Duration::from_secs(3), async {
        while statuses.current() != ListenerStatus::Active {
            statuses
                .changed()
                .await
                .ok_or("Listener closed before republish")?;
        }
        Ok::<_, Box<dyn StdError + Send + Sync>>(())
    })
    .await??;
    relay.close();
    timeout(Duration::from_secs(3), server).await???;
    Ok(())
}

async fn welcome_tls(
    socket: &TcpListener,
    server: &ServerTlsConfig,
) -> TestResult<Framed<relaygate_transport::BoxedIo, FrameCodec>> {
    let (stream, _) = socket.accept().await?;
    let mut session = Framed::new(
        server.accept_boxed(stream).await?,
        FrameCodec::new(DEFAULT_MAX_FRAME_LEN),
    );
    assert!(matches!(session.next().await, Some(Ok(Frame::Hello))));
    session
        .send(Frame::Welcome {
            session_id: SessionId::new(),
        })
        .await?;
    Ok(session)
}

async fn publish(session: &mut Framed<relaygate_transport::BoxedIo, FrameCodec>) -> TestResult {
    match session.next().await.ok_or("missing PUBLISH")?? {
        Frame::Publish {
            request_id,
            destination,
            ..
        } => {
            assert_eq!(destination.to_string(), "test/tls");
            session
                .send(Frame::Published {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
        }
        _ => return Err("expected PUBLISH".into()),
    }
    Ok(())
}
