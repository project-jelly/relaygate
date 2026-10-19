use std::{
    error::Error,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::future::BoxFuture;
use relaygate_protocol::{BearerToken, Frame, PipeId};
use relaygate_route_table::{
    BindingId as RouteBindingId, BindingProjection, BindingSet, RelaySessionId,
};
use tokio::{net::TcpListener, time::timeout};

use super::*;
use crate::{
    Gateway, GatewayConfig, GatewayPeerConfig,
    gateway::route_resolver::RouteResolveFailure,
    peer::{PeerEvents, PeerRuntime},
    state::{GatewayLimits, GatewayState, SdkWriterItem},
    test_support::{authorization_config, unique_destination},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct ControlledResolver {
    bindings: Mutex<BindingSet>,
    entered: AtomicUsize,
    release: Semaphore,
}

impl RouteResolver for ControlledResolver {
    fn resolve(
        &self,
        _destination: Destination,
    ) -> BoxFuture<'_, Result<BindingSet, RouteResolveFailure>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            if let Ok(permit) = self.release.acquire().await {
                permit.forget();
            }
            let bindings = match self.bindings.lock() {
                Ok(bindings) => bindings,
                Err(poisoned) => poisoned.into_inner(),
            };
            Ok(bindings.clone())
        })
    }
}

struct Fixture {
    gateway: Gateway,
    resolver: Arc<ControlledResolver>,
    results: mpsc::Receiver<ControlResult>,
    sdk: mpsc::Receiver<SdkWriterItem>,
    session: SessionId,
    destination: Destination,
    peer_events: PeerEvents,
    peer_runtime: PeerRuntime,
    shutdown: CancellationToken,
}

impl Fixture {
    fn new(capacity: usize, result_capacity: usize) -> TestResult<Self> {
        let gateway_id = GatewayId::new();
        let shutdown = CancellationToken::new();
        let (peer, peer_events, peer_runtime) = PeerRuntime::start(
            GatewayPeerConfig::new("gateway-a")?,
            gateway_id,
            shutdown.child_token(),
        )?;
        let (sender, sdk) = mpsc::channel(64);
        let mut state = GatewayState::new_distributed(
            GatewayLimits {
                max_pending_offers: capacity,
                max_remote_dial_attempts: capacity,
                ..GatewayLimits::default()
            },
            gateway_id,
        );
        let session = state
            .add_session(sender, CancellationToken::new())
            .ok_or("missing caller session")?;
        let destination = unique_destination();
        // A self-only result terminates the attempt without starting peer I/O.
        let resolver = Arc::new(ControlledResolver {
            bindings: Mutex::new(BindingSet::from_entries(vec![BindingProjection::new(
                destination.clone(),
                gateway_id,
                RelaySessionId::from_uuid(session.as_uuid()),
                RouteBindingId::new(),
                GatewayLocator::new("127.0.0.1:1")?,
            )])?),
            entered: AtomicUsize::new(0),
            release: Semaphore::new(0),
        });
        let (sender, results) = mpsc::channel(result_capacity);
        let mut gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        let inner = Arc::get_mut(&mut gateway.inner).ok_or("Gateway unexpectedly shared")?;
        inner.state = Mutex::new(state);
        inner.peer = Some(peer);
        inner.control_effects = Some(ControlEffects::new(
            capacity,
            resolver.clone(),
            sender,
            shutdown.clone(),
        ));
        Ok(Self {
            gateway,
            resolver,
            results,
            sdk,
            session,
            destination,
            peer_events,
            peer_runtime,
            shutdown,
        })
    }

    fn control(&self) -> TestResult<&ControlEffects> {
        self.gateway
            .inner
            .control_effects
            .as_ref()
            .ok_or_else(|| "missing control effects".into())
    }

    async fn dial(&self, connection_id: u64) -> TestResult {
        let token = BearerToken::new("control-effect-test-token")?;
        let actions = self.gateway.inner.transition(|state| {
            state.handle_at(
                self.session,
                Frame::Dial {
                    connection_id,
                    destination: self.destination.clone(),
                    access_token: token,
                },
                Instant::now(),
            )
        })?;
        self.gateway.inner.execute_all(actions).await;
        Ok(())
    }
}

// This is only a deadlock guard; assertions use exact state and slot counts.
async fn wait_until(mut condition: impl FnMut() -> bool) -> TestResult {
    timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn paused_control_result_consumer_keeps_effects_within_capacity() -> TestResult {
    let mut fixture = Fixture::new(1, 1)?;
    fixture.dial(1).await?;
    fixture.resolver.release.add_permits(1);
    fixture.control()?.close_and_wait().await;
    assert_eq!(fixture.results.len(), 1);
    assert_eq!(fixture.gateway.snapshot().remote_open_attempts, 0);
    assert_eq!(fixture.control()?.slots.available_permits(), 0);

    for connection_id in 2..=32 {
        fixture.dial(connection_id).await?;
        assert!(matches!(
            fixture.sdk.try_recv()?,
            SdkWriterItem::Single(Frame::DialFailed {
                connection_id: failed,
                code: ErrorCode::ResourceExhausted,
                observation: PeerObservation::NotObserved,
                ..
            }) if failed == connection_id
        ));
    }
    assert_eq!(fixture.resolver.entered.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.control()?.tasks.len(), 0);
    assert_eq!(fixture.results.len(), 1);
    assert_eq!(fixture.gateway.snapshot().remote_open_attempts, 0);
    drop(
        fixture
            .results
            .recv()
            .await
            .ok_or("missing retained result")?,
    );
    assert_eq!(fixture.control()?.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn capacity_one_route_result_releases_slot_before_peer_open() -> TestResult {
    let fixture = Fixture::new(1, 1)?;
    let owner_id = GatewayId::new();
    let owner_listener = TcpListener::bind("127.0.0.1:0").await?;
    let owner_locator = GatewayLocator::new(owner_listener.local_addr()?.to_string())?;
    let entry_listener = TcpListener::bind("127.0.0.1:0").await?;
    let (owner, mut owner_events, owner_runtime) = PeerRuntime::start(
        GatewayPeerConfig::new("gateway-b")?,
        owner_id,
        fixture.shutdown.child_token(),
    )?;
    *fixture
        .resolver
        .bindings
        .lock()
        .map_err(|_| "resolver lock poisoned")? =
        BindingSet::from_entries(vec![BindingProjection::new(
            fixture.destination.clone(),
            owner_id,
            RelaySessionId::new(),
            RouteBindingId::new(),
            owner_locator,
        )])?;
    fixture.dial(1).await?;
    fixture.resolver.release.add_permits(1);
    fixture.control()?.close_and_wait().await;
    assert_eq!(fixture.results.len(), 1);
    assert_eq!(fixture.control()?.slots.available_permits(), 0);

    let Fixture {
        gateway,
        results,
        mut sdk,
        session,
        peer_events,
        peer_runtime,
        shutdown,
        ..
    } = fixture;
    let entry = tokio::spawn(peer_runtime.serve(entry_listener));
    let owner_runtime = tokio::spawn(owner_runtime.serve(owner_listener));
    let events =
        tokio::spawn(Arc::clone(&gateway.inner).run_peer_events(peer_events, shutdown.clone()));
    let consumer =
        tokio::spawn(Arc::clone(&gateway.inner).run_control_results(results, shutdown.clone()));
    let result: TestResult = async {
        let incoming = timeout(Duration::from_secs(2), owner_events.recv())
            .await?
            .ok_or("missing peer OPEN")?;
        let PeerEvent::IncomingOpen { key, .. } = incoming else {
            return Err("expected peer OPEN".into());
        };
        owner.send_opened(key).await?;
        assert!(matches!(
            timeout(Duration::from_secs(2), sdk.recv()).await?,
            Some(SdkWriterItem::Single(Frame::Opened { pipe_id }))
                if pipe_id == PipeId::new(session, 1)
        ));
        gateway.inner.wait_control_effects().await;
        assert_eq!(gateway.snapshot().remote_open_attempts, 0);
        assert_eq!(gateway.snapshot().live_pipes, 1);
        let control = gateway
            .inner
            .control_effects
            .as_ref()
            .ok_or("missing effects")?;
        assert_eq!(control.slots.available_permits(), 1);
        Ok(())
    }
    .await;
    shutdown.cancel();
    entry.await??;
    owner_runtime.await??;
    events.await??;
    consumer.await??;
    result
}

#[tokio::test]
async fn control_result_receiver_loss_returns_effect_capacity() -> TestResult {
    for queued in [false, true] {
        let fixture = Fixture::new(1, 1)?;
        fixture.dial(1).await?;
        if queued {
            fixture.resolver.release.add_permits(1);
            fixture.control()?.close_and_wait().await;
            assert_eq!(fixture.results.len(), 1);
            assert_eq!(fixture.control()?.slots.available_permits(), 0);
        }
        let Fixture {
            gateway,
            resolver,
            results,
            ..
        } = fixture;
        drop(results);
        if !queued {
            resolver.release.add_permits(1);
        }
        gateway.inner.wait_control_effects().await;
        let control = gateway
            .inner
            .control_effects
            .as_ref()
            .ok_or("missing effects")?;
        assert_eq!(control.slots.available_permits(), 1);
        assert_eq!(gateway.snapshot().remote_open_attempts, 0);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_resolve_releases_effect_capacity_without_result() -> TestResult {
    let fixture = Fixture::new(1, 1)?;
    fixture.dial(1).await?;
    let actions = fixture.gateway.inner.transition(|state| {
        state.handle(
            fixture.session,
            Frame::Cancel {
                pipe_id: PipeId::new(fixture.session, 1),
            },
        )
    })?;
    fixture.gateway.inner.execute_all(actions).await;
    fixture.resolver.release.add_permits(1);
    fixture.control()?.close_and_wait().await;
    assert_eq!(fixture.gateway.snapshot().remote_open_attempts, 0);
    assert_eq!(fixture.results.len(), 0);
    assert_eq!(fixture.control()?.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn shutdown_drops_queued_and_waiting_control_results() -> TestResult {
    // A smaller result queue also exercises cancellation of a waiting send.
    let fixture = Fixture::new(2, 1)?;
    fixture.dial(1).await?;
    fixture.dial(2).await?;
    fixture.resolver.release.add_permits(2);
    wait_until(|| {
        fixture.results.len() == 1
            && fixture.gateway.snapshot().remote_open_attempts == 0
            && fixture
                .control()
                .is_ok_and(|control| control.tasks.len() == 1)
    })
    .await?;
    assert_eq!(fixture.control()?.slots.available_permits(), 0);
    fixture.shutdown.cancel();
    fixture.control()?.close_and_wait().await;
    assert_eq!(fixture.control()?.slots.available_permits(), 1);
    let Fixture {
        gateway,
        results,
        shutdown,
        ..
    } = fixture;
    Arc::clone(&gateway.inner)
        .run_control_results(results, shutdown)
        .await?;
    let control = gateway
        .inner
        .control_effects
        .as_ref()
        .ok_or("missing effects")?;
    assert_eq!(control.slots.available_permits(), 2);
    Ok(())
}
