use std::{error::Error, io, net::SocketAddr, time::Duration};

use relaygate_gateway::{
    Gateway, GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    check_plaintext,
};
use relaygate_route_table::{
    GatewayId, GatewayLocator, RouteTableConfig, RouteTableShard, ShardDirectory, ShardId,
};
use relaygate_route_table_transport::{
    GatewayName, RouteTableClient, RouteTableClientConfig, RouteTableService,
    RouteTableServiceConfig, TransportError,
};
use relaygate_sdk::{
    AccessAction, Config as SdkConfig, Destination, ErrorCode as SdkErrorCode, Listener,
    PeerObservation as SdkPeerObservation, Pipe, Relay,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

mod support;

use support::{authorization_config, destination as test_destination, token_source};
const DESTINATION_A: &str = "11111111-1111-4111-8111-111111111111";
const DESTINATION_B: &str = "22222222-2222-4222-8222-222222222222";
const DESTINATION_C: &str = "33333333-3333-4333-8333-333333333333";
const DESTINATION_MISSING: &str = "99999999-9999-4999-8999-999999999999";
const DESTINATION_SHARED: &str = "44444444-4444-4444-8444-444444444444";
const GATEWAY_A: &str = "gateway-a";
const GATEWAY_B: &str = "gateway-b";
const GATEWAY_C: &str = "gateway-c";
const SHARD_ID: &str = "rt-0";

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rt_one_gateway_three_forms_a_closed_current_state_relay() -> TestResult {
    timeout(Duration::from_secs(12), three_gateway_case()).await??;
    Ok(())
}

async fn three_gateway_case() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let route_endpoint = route_listener.local_addr()?;
    let directory = ShardDirectory::from_json_bytes(one_shard_directory(route_endpoint))?;
    let generation = directory.generation();
    let route_table = RunningRouteTable::start(route_listener, directory.clone())?;

    let mut gateway_a = RunningGateway::start(GATEWAY_A, directory.clone()).await?;
    let mut gateway_b = RunningGateway::start(GATEWAY_B, directory.clone()).await?;
    let mut gateway_c = RunningGateway::start(GATEWAY_C, directory.clone()).await?;

    let listener_runtime_a = Relay::connect(sdk_config(gateway_a.sdk_address)?).await?;
    let listener_runtime_b = Relay::connect(sdk_config(gateway_b.sdk_address)?).await?;
    let listener_runtime_c = Relay::connect(sdk_config(gateway_c.sdk_address)?).await?;
    let listener_a = listen(&listener_runtime_a, DESTINATION_A).await?;
    let listener_b = listen(&listener_runtime_b, DESTINATION_B).await?;
    let listener_c = listen(&listener_runtime_c, DESTINATION_C).await?;
    let shared_b = listen(&listener_runtime_b, DESTINATION_SHARED).await?;
    let shared_c = listen(&listener_runtime_c, DESTINATION_SHARED).await?;

    wait_until("all registrations synced", Duration::from_secs(2), || {
        [&gateway_a, &gateway_b, &gateway_c]
            .into_iter()
            .all(|gateway| gateway.gateway.snapshot().route_registrations_synced == 1)
    })
    .await?;
    gateway_a.assert_running().await?;
    gateway_b.assert_running().await?;
    gateway_c.assert_running().await?;

    let route_observer = RouteTableClient::connect(
        route_endpoint,
        GatewayName::new(GATEWAY_A)?,
        GatewayId::new(),
        route_client_config()?,
    )
    .await?;
    wait_for_binding_count(
        &route_observer,
        generation,
        DESTINATION_SHARED,
        2,
        Duration::from_secs(2),
    )
    .await?;

    let dialer_a = Relay::connect(sdk_config(gateway_a.sdk_address)?).await?;
    let dialer_b = Relay::connect(sdk_config(gateway_b.sdk_address)?).await?;
    let dialer_c = Relay::connect(sdk_config(gateway_c.sdk_address)?).await?;

    exercise_pipe(&dialer_a, DESTINATION_A, &listener_a, "local-a").await?;
    exercise_pipe(&dialer_b, DESTINATION_B, &listener_b, "local-b").await?;
    exercise_pipe(&dialer_c, DESTINATION_C, &listener_c, "local-c").await?;

    exercise_pipe(&dialer_a, DESTINATION_B, &listener_b, "a-to-b").await?;
    exercise_pipe(&dialer_a, DESTINATION_C, &listener_c, "a-to-c").await?;
    exercise_pipe(&dialer_b, DESTINATION_A, &listener_a, "b-to-a").await?;
    exercise_pipe(&dialer_b, DESTINATION_C, &listener_c, "b-to-c").await?;
    exercise_pipe(&dialer_c, DESTINATION_A, &listener_a, "c-to-a").await?;
    exercise_pipe(&dialer_c, DESTINATION_B, &listener_b, "c-to-b").await?;

    wait_until(
        "three peer pairs idle on shared transports",
        Duration::from_secs(2),
        || {
            [&gateway_a, &gateway_b, &gateway_c]
                .into_iter()
                .all(|gateway| {
                    let snapshot = gateway.gateway.snapshot();
                    snapshot.peer_transports_ready == 2
                        && snapshot.peer_streams == 0
                        && snapshot.live_pipes == 0
                })
        },
    )
    .await?;

    let (mut shared_dialer, mut shared_acceptor, shared_owner) =
        open_shared_pipe(&dialer_a, &shared_b, &shared_c).await?;
    let (owner, non_owner) = match shared_owner {
        SharedOwner::B => (&gateway_b, &gateway_c),
        SharedOwner::C => (&gateway_c, &gateway_b),
    };
    wait_until(
        "one shared binding is selected without fan-out",
        Duration::from_secs(2),
        || {
            let entry = gateway_a.gateway.snapshot();
            let owner = owner.gateway.snapshot();
            let non_owner = non_owner.gateway.snapshot();
            entry.live_pipes == 1
                && entry.originated_pipes == 1
                && owner.originated_pipes == 0
                && non_owner.originated_pipes == 0
                && entry.peer_streams == 1
                && owner.live_pipes == 1
                && owner.peer_streams == 1
                && non_owner.live_pipes == 0
                && non_owner.peer_streams == 0
        },
    )
    .await?;
    assert_bidirectional(
        &mut shared_dialer,
        &mut shared_acceptor,
        "a-to-one-of-shared-b-c",
    )
    .await?;
    shared_dialer.close().await?;
    shared_acceptor.close().await?;
    wait_until(
        "shared selected Pipe cleanup",
        Duration::from_secs(2),
        || {
            [&gateway_a, &gateway_b, &gateway_c]
                .into_iter()
                .all(|gateway| {
                    let snapshot = gateway.gateway.snapshot();
                    snapshot.peer_streams == 0 && snapshot.live_pipes == 0
                })
        },
    )
    .await?;

    exercise_repeated_failure_recovery(
        &dialer_a,
        &listener_c,
        [&gateway_a, &gateway_b, &gateway_c],
    )
    .await?;

    let (mut durable_dialer, mut durable_acceptor) =
        open_pipe(&dialer_a, DESTINATION_C, &listener_c).await?;
    let (mut reused_dialer, mut reused_acceptor) =
        open_pipe(&dialer_a, DESTINATION_C, &listener_c).await?;
    wait_until(
        "a-c pair reuses its ready transport",
        Duration::from_secs(2),
        || {
            let a = gateway_a.gateway.snapshot();
            let b = gateway_b.gateway.snapshot();
            let c = gateway_c.gateway.snapshot();
            a.peer_transports_ready == 2
                && c.peer_transports_ready == 2
                && a.peer_streams == 2
                && c.peer_streams == 2
                && b.peer_streams == 0
        },
    )
    .await?;
    assert_bidirectional(&mut reused_dialer, &mut reused_acceptor, "a-c-reused").await?;
    reused_dialer.close().await?;
    reused_acceptor.close().await?;
    wait_until(
        "only the durable a-c stream remains",
        Duration::from_secs(2),
        || {
            gateway_a.gateway.snapshot().peer_streams == 1
                && gateway_c.gateway.snapshot().peer_streams == 1
        },
    )
    .await?;

    dialer_b.close();
    listener_runtime_b.close();
    gateway_b.stop().await?;
    gateway_a.assert_running().await?;
    gateway_c.assert_running().await?;
    wait_for_binding_count(
        &route_observer,
        generation,
        DESTINATION_SHARED,
        1,
        Duration::from_secs(2),
    )
    .await?;
    exercise_pipe(
        &dialer_a,
        DESTINATION_SHARED,
        &shared_c,
        "shared-survives-b-stop",
    )
    .await?;
    assert_bidirectional(
        &mut durable_dialer,
        &mut durable_acceptor,
        "a-c-after-b-stop",
    )
    .await?;

    drop(route_observer);
    route_table.stop().await?;
    let failed_destination = sdk_destination(DESTINATION_C)?;
    let failed_token = token_source(&failed_destination, AccessAction::Dial)?;
    let failed_open = dialer_a
        .dial(failed_destination, failed_token)
        .await
        .err()
        .ok_or("remote open unexpectedly succeeded while RouteTable was unavailable")?;
    assert_eq!(failed_open.code(), SdkErrorCode::Unavailable);
    assert_eq!(failed_open.observation(), SdkPeerObservation::NotObserved);
    wait_until(
        "failed open leaves no pending attempt",
        Duration::from_secs(2),
        || {
            gateway_a.gateway.snapshot().remote_open_attempts == 0
                && gateway_a.gateway.snapshot().peer_streams == 1
                && gateway_c.gateway.snapshot().peer_streams == 1
        },
    )
    .await?;
    assert_bidirectional(
        &mut durable_dialer,
        &mut durable_acceptor,
        "a-c-after-rt-stop",
    )
    .await?;

    let route_listener = TcpListener::bind(route_endpoint).await?;
    let restarted_route_table = RunningRouteTable::start(route_listener, directory)?;
    let route_observer = RouteTableClient::connect(
        route_endpoint,
        GatewayName::new(GATEWAY_A)?,
        GatewayId::new(),
        route_client_config()?,
    )
    .await?;
    wait_for_binding_count(
        &route_observer,
        generation,
        DESTINATION_C,
        1,
        Duration::from_secs(2),
    )
    .await?;
    exercise_pipe(
        &dialer_a,
        DESTINATION_C,
        &listener_c,
        "a-c-after-rt-restart",
    )
    .await?;
    drop(route_observer);
    restarted_route_table.stop().await?;

    durable_dialer.close().await?;
    durable_acceptor.close().await?;
    wait_until("durable stream cleanup", Duration::from_secs(2), || {
        gateway_a.gateway.snapshot().peer_streams == 0
            && gateway_c.gateway.snapshot().peer_streams == 0
    })
    .await?;

    dialer_a.close();
    dialer_c.close();
    listener_runtime_a.close();
    listener_runtime_c.close();
    gateway_a.stop().await?;
    gateway_c.stop().await?;
    Ok(())
}

async fn exercise_repeated_failure_recovery(
    dialer: &Relay,
    listener: &Listener,
    gateways: [&RunningGateway; 3],
) -> TestResult {
    for cycle in 0..100 {
        let missing_destination = sdk_destination(DESTINATION_MISSING)?;
        let missing_token = token_source(&missing_destination, AccessAction::Dial)?;
        let failure = dialer
            .dial(missing_destination, missing_token)
            .await
            .err()
            .ok_or("missing Destination unexpectedly opened a Pipe")?;
        assert_eq!(failure.code(), SdkErrorCode::NotFound);
        assert_eq!(failure.observation(), SdkPeerObservation::NotObserved);

        exercise_pipe(
            dialer,
            DESTINATION_C,
            listener,
            &format!("failure-recovery-{cycle}"),
        )
        .await?;
    }

    wait_until(
        "100 failure/recovery cycles return to current-state baseline",
        Duration::from_secs(2),
        || {
            gateways.iter().all(|gateway| {
                let snapshot = gateway.gateway.snapshot();
                snapshot.pending_offers == 0
                    && snapshot.originated_pipes == 0
                    && snapshot.live_pipes == 0
                    && snapshot.remote_open_attempts == 0
                    && snapshot.peer_transports_connecting == 0
                    && snapshot.peer_transports_ready == 2
                    && snapshot.peer_streams == 0
            })
        },
    )
    .await
}

async fn listen(relay: &Relay, destination: &str) -> TestResult<Listener> {
    let destination = sdk_destination(destination)?;
    let access_token = token_source(&destination, AccessAction::Publish)?;
    Ok(relay.listen(destination, access_token).await?)
}

async fn dial(relay: &Relay, destination: &str) -> TestResult<Pipe> {
    let destination = sdk_destination(destination)?;
    let access_token = token_source(&destination, AccessAction::Dial)?;
    Ok(relay.dial(destination, access_token).await?)
}

async fn exercise_pipe(
    dialer: &Relay,
    destination: &str,
    listener: &Listener,
    marker: &str,
) -> TestResult {
    let (mut dialer_pipe, mut acceptor_pipe) = open_pipe(dialer, destination, listener).await?;
    assert_bidirectional(&mut dialer_pipe, &mut acceptor_pipe, marker).await?;
    dialer_pipe.close().await?;
    acceptor_pipe.close().await?;
    Ok(())
}

async fn open_pipe(
    dialer: &Relay,
    destination: &str,
    listener: &Listener,
) -> TestResult<(Pipe, Pipe)> {
    let dialer_pipe = dial(dialer, destination).await?;
    let acceptor_pipe = listener.accept().await?;
    Ok((dialer_pipe, acceptor_pipe))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedOwner {
    B,
    C,
}

async fn open_shared_pipe(
    dialer: &Relay,
    listener_b: &Listener,
    listener_c: &Listener,
) -> TestResult<(Pipe, Pipe, SharedOwner)> {
    let destination = sdk_destination(DESTINATION_SHARED)?;
    let access_token = token_source(&destination, AccessAction::Dial)?;
    timeout(Duration::from_secs(2), async {
        let accepted = async {
            tokio::select! {
                result = listener_b.accept() => result.map(|pipe| (pipe, SharedOwner::B)),
                result = listener_c.accept() => result.map(|pipe| (pipe, SharedOwner::C)),
            }
        };
        let (opened, accepted) =
            tokio::join!(dialer.dial(destination.clone(), access_token), accepted);
        let dialer_pipe = opened?;
        let (acceptor_pipe, owner) = accepted?;
        Ok::<_, relaygate_sdk::Error>((dialer_pipe, acceptor_pipe, owner))
    })
    .await
    .map_err(|_| "shared OPEN or concurrent accept timed out")?
    .map_err(Into::into)
}

async fn assert_bidirectional(dialer: &mut Pipe, listener: &mut Pipe, marker: &str) -> TestResult {
    let toward_acceptor = format!("dialer:{marker}").into_bytes();
    dialer.write_all(&toward_acceptor).await?;
    let mut received = vec![0; toward_acceptor.len()];
    listener.read_exact(&mut received).await?;
    assert_eq!(received, toward_acceptor);

    let toward_dialer = format!("listener:{marker}").into_bytes();
    listener.write_all(&toward_dialer).await?;
    let mut received = vec![0; toward_dialer.len()];
    dialer.read_exact(&mut received).await?;
    assert_eq!(received, toward_dialer);
    Ok(())
}

struct RunningGateway {
    name: String,
    sdk_address: SocketAddr,
    gateway: Gateway,
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl RunningGateway {
    async fn start(name: &str, directory: ShardDirectory) -> TestResult<Self> {
        let sdk_listener = TcpListener::bind("127.0.0.1:0").await?;
        let sdk_address = sdk_listener.local_addr()?;
        let peer_listener = TcpListener::bind("127.0.0.1:0").await?;
        let peer_address = peer_listener.local_addr()?;
        let routing = GatewayRoutingConfig::new(
            directory,
            GatewayName::new(name)?,
            GatewayLocator::new(peer_address.to_string())?,
            route_client_config()?,
        )
        .with_command_queue_capacity(32)
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_desired_scan_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(200));
        let peer = GatewayPeerConfig::new(name)?.with_timeouts(
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_secs(1),
        );
        let shutdown = CancellationToken::new();
        let gateway = Gateway::new_distributed(
            GatewayConfig::new(authorization_config()?)
                .with_max_pending_offers(16)
                .with_drain_timeout(Duration::from_millis(100)),
            routing,
            peer,
        )?;
        let served = gateway.clone();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            served
                .serve_distributed(sdk_listener, peer_listener, serve_shutdown)
                .await
        });
        check_plaintext(sdk_address, Duration::from_secs(1)).await?;
        Ok(Self {
            name: name.to_owned(),
            sdk_address,
            gateway,
            shutdown,
            task,
        })
    }

    async fn assert_running(&mut self) -> TestResult {
        if !self.task.is_finished() {
            return Ok(());
        }
        let result = (&mut self.task).await.map_err(|error| {
            io::Error::other(format!("{} task could not be joined: {error}", self.name))
        })?;
        match result {
            Ok(()) => Err(io::Error::other(format!(
                "{} distributed runtime stopped unexpectedly",
                self.name
            ))
            .into()),
            Err(error) => Err(io::Error::other(format!(
                "{} distributed runtime failed: {error}",
                self.name
            ))
            .into()),
        }
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        let result = timeout(Duration::from_secs(2), self.task)
            .await
            .map_err(|_| io::Error::other(format!("{} shutdown timed out", self.name)))?
            .map_err(|error| {
                io::Error::other(format!("{} task could not be joined: {error}", self.name))
            })?;
        result.map_err(|error| {
            io::Error::other(format!("{} shutdown failed: {error}", self.name)).into()
        })
    }
}

struct RunningRouteTable {
    shutdown: CancellationToken,
    task: JoinHandle<Result<(), TransportError>>,
}

impl RunningRouteTable {
    fn start(listener: TcpListener, directory: ShardDirectory) -> Result<Self, TransportError> {
        let shard = RouteTableShard::new(
            directory,
            ShardId::new(SHARD_ID).map_err(TransportError::from)?,
            RouteTableConfig::new(Duration::from_secs(2)).map_err(TransportError::from)?,
        )
        .map_err(TransportError::from)?;
        let service = RouteTableService::new(
            shard,
            RouteTableServiceConfig::new(64, 32, 16, 256 * 1024, Duration::from_millis(200))?,
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(service.serve(listener, shutdown.clone()));
        Ok(Self { shutdown, task })
    }

    async fn stop(self) -> TestResult {
        self.shutdown.cancel();
        timeout(Duration::from_secs(1), self.task)
            .await
            .map_err(|_| "RouteTable shutdown timed out")???;
        Ok(())
    }
}

fn route_client_config() -> Result<RouteTableClientConfig, TransportError> {
    RouteTableClientConfig::new(
        32,
        256 * 1024,
        Duration::from_millis(200),
        Duration::from_millis(200),
        Duration::from_millis(200),
    )
}

fn sdk_config(endpoint: SocketAddr) -> TestResult<SdkConfig> {
    Ok(SdkConfig::new(format!("tcp://{endpoint}"))?
        .with_connect_timeout(Duration::from_millis(200))
        .with_operation_timeout(Duration::from_secs(2))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40)))
}

async fn wait_for_binding_count(
    client: &RouteTableClient,
    generation: relaygate_route_table::ShardDirectoryGeneration,
    destination: &str,
    expected: usize,
    deadline: Duration,
) -> TestResult {
    let destination = sdk_destination(destination)?;
    let expires = Instant::now() + deadline;
    loop {
        if client
            .resolve(generation, &destination)
            .await
            .is_ok_and(|bindings| bindings.len() == expected)
        {
            return Ok(());
        }
        if Instant::now() >= expires {
            return Err(format!(
                "RouteTable did not converge to {expected} bindings for {destination}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn sdk_destination(value: &str) -> Result<Destination, relaygate_destination::DestinationError> {
    test_destination(value)
}

async fn wait_until(
    label: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> bool,
) -> TestResult {
    let expires = Instant::now() + deadline;
    while !condition() {
        if Instant::now() >= expires {
            return Err(format!("{label} did not converge before deadline").into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

fn one_shard_directory(endpoint: SocketAddr) -> Vec<u8> {
    format!(
        r#"{{"format_version":2,"authority_hash":"sha256-destination-modulo-v2","shards":[{{"id":"{SHARD_ID}","endpoint":"{endpoint}"}}]}}"#
    )
    .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_gateway_rejects_local_serve_after_serve_distributed() -> TestResult {
    let route_listener = TcpListener::bind("127.0.0.1:0").await?;
    let directory =
        ShardDirectory::from_json_bytes(one_shard_directory(route_listener.local_addr()?))?;
    let running = RunningGateway::start(GATEWAY_A, directory).await?;

    let extra_listener = TcpListener::bind("127.0.0.1:0").await?;
    let result = running
        .gateway
        .serve(extra_listener, CancellationToken::new())
        .await;
    assert!(matches!(result, Err(GatewayError::InvalidConfig(_))));

    running.stop().await
}
