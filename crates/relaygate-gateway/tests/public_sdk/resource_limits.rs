use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn listener_live_pipe_limit_rejects_only_the_excess_dial_and_recovers_after_cleanup()
-> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let publisher_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1))
        .with_resource_limits(
            ResourceLimits::default()
                .with_max_pending_pipes_per_listener(1)
                .with_max_live_pipes_per_listener(1)
                .with_max_live_pipes_per_relay(2),
        );
    let caller_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1));
    let publisher = Relay::connect(publisher_config).await?;
    let caller = Relay::connect(caller_config).await?;
    let destination = unique_destination()?;
    let listener = publisher
        .listen(
            destination.clone(),
            token_source(&destination, AccessAction::Publish)?,
        )
        .await?;

    let first_dial_token = token_source(&destination, AccessAction::Dial)?;
    let (first_dialed, first_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), first_dial_token),
            listener.accept(),
        )
    })
    .await?;
    let mut first_dialed = first_dialed?;
    let mut first_accepted = first_accepted?;

    let excess = caller
        .dial(
            destination.clone(),
            token_source(&destination, AccessAction::Dial)?,
        )
        .await
        .err()
        .ok_or("Listener accepted a Pipe beyond its live limit")?;
    assert_eq!(excess.code(), relaygate_sdk::ErrorCode::ResourceExhausted);

    first_dialed.write_all(b"still-alive").await?;
    let mut payload = [0_u8; 11];
    first_accepted.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"still-alive");
    drop(first_dialed);
    drop(first_accepted);

    let replacement: TestResult<_> = timeout(Duration::from_secs(2), async {
        let accepted = listener.accept();
        tokio::pin!(accepted);
        loop {
            let dial = caller.dial(
                destination.clone(),
                token_source(&destination, AccessAction::Dial)?,
            );
            tokio::pin!(dial);
            tokio::select! {
                result = &mut dial => match result {
                    Ok(dialed) => break Ok((dialed, accepted.await?)),
                    Err(error)
                        if error.code() == relaygate_sdk::ErrorCode::ResourceExhausted =>
                    {
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => break Err(error.into()),
                },
                result = &mut accepted => {
                    let accepted = result?;
                    break Ok((dial.await?, accepted));
                }
            }
        }
    })
    .await?;
    let (replacement_dialed, replacement_accepted) = replacement?;
    drop(replacement_dialed);
    drop(replacement_accepted);

    publisher.close();
    caller.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn listener_pending_pipe_limit_recovers_after_application_accepts_the_queued_pipe()
-> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let publisher_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1))
        .with_resource_limits(
            ResourceLimits::default()
                .with_max_pending_pipes_per_listener(1)
                .with_max_live_pipes_per_listener(2)
                .with_max_live_pipes_per_relay(2),
        );
    let caller_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1));
    let publisher = Relay::connect(publisher_config).await?;
    let caller = Relay::connect(caller_config).await?;
    let destination = unique_destination()?;
    let listener = publisher
        .listen(
            destination.clone(),
            token_source(&destination, AccessAction::Publish)?,
        )
        .await?;

    let mut first_dialed = caller
        .dial(
            destination.clone(),
            token_source(&destination, AccessAction::Dial)?,
        )
        .await?;
    let excess = caller
        .dial(
            destination.clone(),
            token_source(&destination, AccessAction::Dial)?,
        )
        .await
        .err()
        .ok_or("Listener accepted a Pipe beyond its pending queue limit")?;
    assert_eq!(excess.code(), relaygate_sdk::ErrorCode::ResourceExhausted);

    let mut first_accepted = listener.accept().await?;
    first_dialed.write_all(b"queued").await?;
    let mut payload = [0_u8; 6];
    first_accepted.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"queued");

    let replacement_token = token_source(&destination, AccessAction::Dial)?;
    let (replacement_dialed, replacement_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), replacement_token),
            listener.accept(),
        )
    })
    .await?;
    drop(replacement_dialed?);
    drop(replacement_accepted?);
    drop(first_dialed);
    drop(first_accepted);

    publisher.close();
    caller.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn pipe_byte_limit_resets_only_the_overflowed_pipe_and_preserves_sibling() -> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let publisher_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1))
        .with_resource_limits(
            ResourceLimits::default()
                .with_max_pending_pipes_per_listener(2)
                .with_max_live_pipes_per_listener(2)
                .with_max_live_pipes_per_relay(2)
                .with_max_buffered_frames_per_pipe(2)
                .with_max_buffered_bytes_per_pipe(4)
                .with_max_buffered_bytes_per_relay(8),
        );
    let caller_config = Config::new(format!("tcp://{address}"))?
        .with_operation_timeout(Duration::from_secs(1));
    let publisher = Relay::connect(publisher_config).await?;
    let caller = Relay::connect(caller_config).await?;
    let destination = unique_destination()?;
    let listener = publisher
        .listen(
            destination.clone(),
            token_source(&destination, AccessAction::Publish)?,
        )
        .await?;

    let overflowing_token = token_source(&destination, AccessAction::Dial)?;
    let (overflowing_dialed, overflowing_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), overflowing_token),
            listener.accept(),
        )
    })
    .await?;
    let mut overflowing_dialed = overflowing_dialed?;
    let mut overflowing_accepted = overflowing_accepted?;

    let sibling_token = token_source(&destination, AccessAction::Dial)?;
    let (sibling_dialed, sibling_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), sibling_token),
            listener.accept(),
        )
    })
    .await?;
    let mut sibling_dialed = sibling_dialed?;
    let mut sibling_accepted = sibling_accepted?;

    overflowing_dialed.write_all(b"12345").await?;
    let mut byte = [0_u8; 1];
    let overflow = timeout(Duration::from_secs(1), overflowing_accepted.read(&mut byte))
        .await?
        .err()
        .ok_or("Pipe accepted payload beyond its byte limit")?;
    let overflow = overflow
        .get_ref()
        .and_then(|source| source.downcast_ref::<relaygate_sdk::Error>())
        .ok_or("Pipe overflow lost its structured SDK error")?;
    assert_eq!(overflow.code(), relaygate_sdk::ErrorCode::ResourceExhausted);

    sibling_dialed.write_all(b"ok").await?;
    let mut sibling_payload = [0_u8; 2];
    sibling_accepted.read_exact(&mut sibling_payload).await?;
    assert_eq!(&sibling_payload, b"ok");

    drop(overflowing_dialed);
    drop(overflowing_accepted);

    let replacement_token = token_source(&destination, AccessAction::Dial)?;
    let (replacement_dialed, replacement_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), replacement_token),
            listener.accept(),
        )
    })
    .await?;
    let mut replacement_dialed = replacement_dialed?;
    let mut replacement_accepted = replacement_accepted?;
    replacement_dialed.write_all(b"next").await?;
    let mut replacement_payload = [0_u8; 4];
    replacement_accepted
        .read_exact(&mut replacement_payload)
        .await?;
    assert_eq!(&replacement_payload, b"next");

    drop(sibling_dialed);
    drop(sibling_accepted);
    drop(replacement_dialed);
    drop(replacement_accepted);
    publisher.close();
    caller.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn local_dial_capacity_rejection_still_supplies_the_token_exactly_once() -> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let publisher = Relay::connect(Config::new(format!("tcp://{address}"))?).await?;
    let caller = Relay::connect(
        Config::new(format!("tcp://{address}"))?.with_resource_limits(
            ResourceLimits::default()
                .with_max_live_pipes_per_listener(1)
                .with_max_live_pipes_per_relay(1),
        ),
    )
    .await?;
    let destination = unique_destination()?;
    let listener = publisher
        .listen(
            destination.clone(),
            token_source(&destination, AccessAction::Publish)?,
        )
        .await?;
    let first_token = token_source(&destination, AccessAction::Dial)?;
    let (first_dialed, first_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(
            caller.dial(destination.clone(), first_token),
            listener.accept()
        )
    })
    .await?;
    let first_dialed = first_dialed?;
    let first_accepted = first_accepted?;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let token = access_token(&destination, AccessAction::Dial)?;
    let source = AccessTokenSource::dynamic(move |_| {
        let observed_calls = Arc::clone(&observed_calls);
        let token = token.clone();
        async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(token)
        }
    });
    let error = caller
        .dial(destination, source)
        .await
        .err()
        .ok_or("Relay accepted a DIAL beyond its local live Pipe limit")?;
    assert_eq!(error.code(), relaygate_sdk::ErrorCode::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(first_dialed);
    drop(first_accepted);
    publisher.close();
    caller.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}
