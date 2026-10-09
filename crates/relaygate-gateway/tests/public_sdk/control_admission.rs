use super::*;

#[test]
fn returned_listeners_republish_after_rate_limited_reconnect() -> TestResult {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let (address, first_shutdown, first_server) = start_gateway().await?;
            let config = Config::new(format!("tcp://{address}"))?
                .with_operation_timeout(Duration::from_secs(2))
                .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(100));
            let relay = Relay::connect(config.clone()).await?;
            let first_address = unique_destination()?;
            let second_address = unique_destination()?;
            let first = relay
                .listen(
                    first_address.clone(),
                    token_source(&first_address, AccessAction::Publish)?,
                )
                .await?;
            let second = relay
                .listen(
                    second_address.clone(),
                    token_source(&second_address, AccessAction::Publish)?,
                )
                .await?;
            first_shutdown.cancel();
            timeout(Duration::from_secs(3), first_server).await???;
            timeout(Duration::from_secs(2), async {
                while first.status() == ListenerStatus::Active
                    || second.status() == ListenerStatus::Active
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;

            let socket = TcpListener::bind(address).await?;
            let gateway = Gateway::new(
                GatewayConfig::new(authorization_config()?)
                    .with_session_control_rate_limit(1, 1)
                    .with_drain_timeout(Duration::from_millis(20)),
            )?;
            let shutdown = CancellationToken::new();
            let serve_shutdown = shutdown.clone();
            let server = tokio::spawn(async move { gateway.serve(socket, serve_shutdown).await });
            let result: TestResult = async {
                timeout(Duration::from_secs(5), async {
                    while first.status() != ListenerStatus::Active
                        || second.status() != ListenerStatus::Active
                    {
                        sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
                assert!(
                    snapshotter
                        .snapshot()
                        .into_vec()
                        .iter()
                        .any(|(key, _, _, value)| {
                            key.key().name() == "relaygate_gateway_control_rejections_total"
                                && key.key().labels().any(|label| {
                                    label.key() == "operation" && label.value() == "publish"
                                })
                                && key.key().labels().any(|label| {
                                    label.key() == "scope" && label.value() == "session"
                                })
                                && matches!(value, DebugValue::Counter(count) if *count > 0)
                        }),
                    "reconnect must exercise a rejected republish before recovery"
                );
                // Separate caller sessions avoid consuming a shared caller budget in this recovery test.
                for listener in [&first, &second] {
                    let caller = Relay::connect(config.clone()).await?;
                    let listener_address = listener.destination().clone();
                    let dial_token = token_source(&listener_address, AccessAction::Dial)?;
                    let (outgoing, incoming) = timeout(Duration::from_secs(2), async {
                        tokio::join!(
                            caller.dial(listener_address.clone(), dial_token),
                            listener.accept()
                        )
                    })
                    .await?;
                    let mut outgoing = outgoing?;
                    let mut incoming = incoming?;
                    outgoing.write_all(b"ok").await?;
                    let mut bytes = [0; 2];
                    timeout(Duration::from_secs(1), incoming.read_exact(&mut bytes)).await??;
                    assert_eq!(&bytes, b"ok");
                    caller.close();
                }
                first.close().await?;
                second.close().await?;
                Ok(())
            }
            .await;
            relay.close();
            shutdown.cancel();
            timeout(Duration::from_secs(3), server).await???;
            result
        })
    })
}

#[tokio::test]
async fn control_throttle_returns_request_errors_preserves_pipe_and_recovers() -> TestResult {
    let (address, shutdown, server) = start_gateway_with_config(
        GatewayConfig::new(authorization_config()?).with_session_control_rate_limit(1, 1),
    )
    .await?;
    let result: TestResult = async {
        let config = Config::new(format!("tcp://{address}"))?
            .with_operation_timeout(Duration::from_secs(2));
        let receiver = Relay::connect(config.clone()).await?;
        let caller = Relay::connect(config).await?;
        let destination = unique_destination()?;
        let listener = receiver
            .listen(
                destination.clone(),
                token_source(&destination, AccessAction::Publish)?,
            )
            .await?;
        let (outgoing, incoming) = tokio::join!(
            caller.dial(
                destination.clone(),
                token_source(&destination, AccessAction::Dial)?,
            ),
            listener.accept()
        );
        let mut outgoing = outgoing?;
        let mut incoming = incoming?;
        // Repeated operations tolerate a token refill but must eventually reject.
        timeout(Duration::from_secs(3), async {
            loop {
                let destination = unique_destination()?;
                match caller
                    .dial(
                        destination.clone(),
                        token_source(&destination, AccessAction::Dial)?,
                    )
                    .await
                {
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::ResourceExhausted => {
                        assert_eq!(
                            error.observation(),
                            relaygate_sdk::PeerObservation::NotObserved
                        );
                        break;
                    }
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::NotFound => {}
                    other => return Err(format!("unexpected dial result: {other:?}").into()),
                }
            }
            TestResult::Ok(())
        })
        .await??;
        timeout(Duration::from_secs(3), async {
            loop {
                let destination = unique_destination()?;
                match receiver
                    .listen(
                        destination.clone(),
                        token_source(&destination, AccessAction::Publish)?,
                    )
                    .await
                {
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::ResourceExhausted => {
                        break;
                    }
                    Ok(extra) => extra.close().await?,
                    Err(error) => return Err(error.into()),
                }
            }
            TestResult::Ok(())
        })
        .await??;
        outgoing.write_all(b"ping").await?;
        let mut bytes = [0; 4];
        timeout(Duration::from_secs(1), incoming.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"ping");
        incoming.write_all(b"pong").await?;
        timeout(Duration::from_secs(1), outgoing.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"pong");
        sleep(Duration::from_millis(1100)).await;
        let (recovered, accepted) = tokio::join!(
            caller.dial(
                destination.clone(),
                token_source(&destination, AccessAction::Dial)?,
            ),
            listener.accept()
        );
        drop((recovered?, accepted?));
        listener.close().await?;
        caller.close();
        receiver.close();
        Ok(())
    }
    .await;
    shutdown.cancel();
    timeout(Duration::from_secs(3), server).await???;
    result
}
