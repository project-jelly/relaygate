//! Opt-in, single-process TLS recovery soak; never extends ordinary CI runtime.
use super::*;
use relaygate_sdk::{AccessTokenSourceError, ErrorCode, ErrorOrigin, Pipe, RelayStatus};
use std::{net::SocketAddr, time::Instant};
use tokio::task::JoinHandle;

type Server = (
    Gateway,
    CancellationToken,
    JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
);

async fn serve(address: SocketAddr, tls: &ServerTlsConfig) -> TestResult<Server> {
    let socket = TcpListener::bind(address).await?;
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config()?)
            .with_sdk_tls(tls.clone())
            .with_drain_timeout(Duration::from_millis(20)),
    )?;
    let stop = CancellationToken::new();
    let worker_gateway = gateway.clone();
    let worker_stop = stop.clone();
    let worker = tokio::spawn(async move { worker_gateway.serve(socket, worker_stop).await });
    Ok((gateway, stop, worker))
}

async fn stop(server: Server) -> TestResult {
    server.1.cancel();
    timeout(Duration::from_secs(5), server.2).await???;
    let state = server.0.snapshot();
    assert_eq!(
        (
            state.sessions,
            state.bindings,
            state.live_pipes,
            state.pending_offers
        ),
        (0, 0, 0, 0)
    );
    Ok(())
}

async fn pair(
    caller: &Relay,
    listener: &relaygate_sdk::Listener,
    destination: &relaygate_sdk::Destination,
) -> TestResult<(Pipe, Pipe)> {
    let token = token_source(destination, AccessAction::Dial)?;
    let (outgoing, incoming) = timeout(Duration::from_secs(5), async {
        tokio::join!(caller.dial(destination.clone(), token), listener.accept())
    })
    .await?;
    Ok((outgoing?, incoming?))
}

async fn exchange(
    caller: &Relay,
    listener: &relaygate_sdk::Listener,
    destination: &relaygate_sdk::Destination,
    cycle: u64,
) -> TestResult {
    let mut pipes = Vec::new();
    for _ in 0..8 {
        pipes.push(pair(caller, listener, destination).await?);
    }
    for (index, (mut outgoing, mut incoming)) in pipes.into_iter().enumerate() {
        let payload = vec![(cycle as u8).wrapping_add(index as u8); 4096];
        timeout(Duration::from_secs(5), async {
            outgoing.write_all(&payload).await?;
            let mut received = vec![0; payload.len()];
            incoming.read_exact(&mut received).await?;
            assert_eq!(received, payload);
            incoming.write_all(&received).await?;
            outgoing.read_exact(&mut received).await?;
            assert_eq!(received, payload);
            outgoing.shutdown_write().await?;
            incoming.shutdown_write().await?;
            assert_eq!(outgoing.read(&mut [0]).await?, 0);
            assert_eq!(incoming.read(&mut [0]).await?, 0);
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .await??;
    }
    Ok(())
}

/// Run alone with --ignored --exact --nocapture and RELAYGATE_RECOVERY_SOAK_SECS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in 30-minute TLS recovery/resource soak"]
async fn tls_recovery_soak() -> TestResult {
    let duration = std::env::var("RELAYGATE_RECOVERY_SOAK_SECS")
        .unwrap_or_else(|_| "1800".into())
        .parse::<u64>()?;
    assert!(duration > 0);
    let runtime = tokio::runtime::Handle::current();
    let baseline_tasks = runtime.metrics().num_alive_tasks();
    let good = generate_simple_self_signed(vec!["relaygate.test".into()])?;
    let bad = generate_simple_self_signed(vec!["wrong.test".into()])?;
    let tls = ServerTlsConfig::server_authenticated(
        good.cert.pem().as_bytes(),
        good.signing_key.serialize_pem().as_bytes(),
    )?;
    let wrong_tls = ServerTlsConfig::server_authenticated(
        bad.cert.pem().as_bytes(),
        bad.signing_key.serialize_pem().as_bytes(),
    )?;
    let trust = format!("{}{}", good.cert.pem(), bad.cert.pem());
    let client = ClientTlsConfig::server_authenticated("relaygate.test", trust.as_bytes())?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    drop(socket);
    let mut server = serve(address, &tls).await?;
    let config =
        Config::with_transport(GatewayTransportConfig::tls_tcp(address.to_string(), client))
            .with_connect_timeout(Duration::from_secs(2))
            .with_operation_timeout(Duration::from_secs(5))
            .with_reconnect_backoff(Duration::from_millis(20), Duration::from_millis(100));
    let publisher = Relay::connect(config.clone()).await?;
    let caller = Relay::connect(config).await?;
    let destination = unique_destination()?;
    let token_destination = destination.clone();
    let source = AccessTokenSource::dynamic(move |_| {
        std::future::ready(
            access_token(&token_destination, AccessAction::Publish)
                .map_err(|_| AccessTokenSourceError),
        )
    });
    let listener = publisher.listen(destination.clone(), source).await?;
    let started = Instant::now();
    let mut cycles = 0;
    let mut tls_failures = 0;
    let mut settled_tasks = None;
    println!(
        "SOAK_START pid={} seconds={duration} baseline_tasks={baseline_tasks}",
        std::process::id()
    );
    while started.elapsed() < Duration::from_secs(duration) {
        exchange(&caller, &listener, &destination, cycles).await?;
        let (mut old_outgoing, mut old_incoming) = pair(&caller, &listener, &destination).await?;
        stop(server).await?;
        for pipe in [&mut old_outgoing, &mut old_incoming] {
            let end = timeout(Duration::from_secs(3), pipe.read(&mut [0])).await?;
            assert!(
                matches!(end, Ok(0) | Err(_)),
                "old Pipe survived Gateway shutdown"
            );
        }
        drop((old_outgoing, old_incoming));
        if cycles % 5 == 0 {
            let rejected = serve(address, &wrong_tls).await?;
            timeout(Duration::from_secs(5), async {
                loop {
                    if [&publisher, &caller].iter().all(|relay| {
                        relay.last_error().is_some_and(|error| {
                            error.code() == ErrorCode::Unauthenticated
                                && error.origin() == ErrorOrigin::Transport
                        })
                    }) {
                        break;
                    }
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            assert_eq!(publisher.status(), RelayStatus::Reconnecting);
            assert_eq!(caller.status(), RelayStatus::Reconnecting);
            stop(rejected).await?;
            tls_failures += 1;
        } else {
            sleep(Duration::from_millis(100)).await;
        }
        server = serve(address, &tls).await?;
        timeout(Duration::from_secs(5), async {
            caller.wait_ready().await?;
            publisher.wait_ready().await?;
            while listener.status() != ListenerStatus::Active {
                sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .await??;
        exchange(&caller, &listener, &destination, cycles).await?;
        timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = server.0.snapshot();
                if snapshot.live_pipes == 0 && snapshot.pending_offers == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        sleep(Duration::from_millis(100)).await;
        let snapshot = server.0.snapshot();
        assert_eq!(
            (
                snapshot.sessions,
                snapshot.bindings,
                snapshot.live_pipes,
                snapshot.pending_offers
            ),
            (2, 1, 0, 0)
        );
        let tasks = runtime.metrics().num_alive_tasks();
        let stable = *settled_tasks.get_or_insert(tasks);
        assert!(tasks <= stable + 4, "tasks grew from {stable} to {tasks}");
        cycles += 1;
        println!(
            "SOAK_SAMPLE elapsed_ms={} cycles={cycles} tls_failures={tls_failures} tasks={tasks} sessions={} bindings={} pipes={} pending={}",
            started.elapsed().as_millis(),
            snapshot.sessions,
            snapshot.bindings,
            snapshot.live_pipes,
            snapshot.pending_offers
        );
        sleep(Duration::from_secs(1)).await;
    }
    listener.close().await?;
    publisher.close();
    caller.close();
    drop((listener, publisher, caller));
    stop(server).await?;
    timeout(Duration::from_secs(5), async {
        while runtime.metrics().num_alive_tasks() > baseline_tasks {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    println!(
        "SOAK_PASS elapsed_secs={} cycles={cycles} tls_failures={tls_failures} verified_echo_pipes={} final_tasks={}",
        started.elapsed().as_secs(),
        cycles * 16,
        runtime.metrics().num_alive_tasks()
    );
    Ok(())
}
