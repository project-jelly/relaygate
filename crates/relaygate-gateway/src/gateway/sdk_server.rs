use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{Frame, FrameCodec};
use relaygate_transport::{BoxedIo, ClientTlsConfig, insecure_boxed};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::OwnedSemaphorePermit,
    task::{JoinError, JoinSet},
    time::{MissedTickBehavior, timeout},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::GatewayError;

use super::{Gateway, Inner};

const MIN_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(10);
const MAX_OFFER_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

impl Gateway {
    pub(super) async fn serve_sdk(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        self.serve_sdk_with_force(listener, shutdown, CancellationToken::new())
            .await
    }

    pub(super) async fn serve_sdk_with_force(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
        force_shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let mut sessions = JoinSet::new();
        let mut offer_sweep = tokio::time::interval(offer_sweep_interval(self.inner.offer_timeout));
        offer_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let session_lifecycle = CancellationToken::new();
        let mut first_error = None;
        let mut graceful_shutdown = false;
        loop {
            tokio::select! {
                biased;
                _ = force_shutdown.cancelled() => break,
                _ = shutdown.cancelled() => {
                    graceful_shutdown = true;
                    self.inner.begin_draining();
                    break;
                },
                _ = offer_sweep.tick() => self.inner.expire_offers().await,
                completed = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::error!(
                            component = "gateway",
                            event = "gateway.session.task_failed",
                            %error,
                            "SDK session task failed; stopping Gateway runtime"
                        );
                        first_error.get_or_insert(session_task_failure(error));
                        session_lifecycle.cancel();
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer_addr) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::error!(
                                component = "gateway",
                                event = "gateway.sdk_listener.accept_failed",
                                %error,
                                "Gateway SDK listener failed; stopping Gateway runtime"
                            );
                            first_error.get_or_insert(GatewayError::Io(error));
                            shutdown.cancel();
                            break;
                        }
                    };
                    if let Some(slots) = self.inner.admit_transport(peer_addr) {
                        self.inner.spawn_session(&mut sessions, stream, peer_addr, slots, &session_lifecycle);
                    } else {
                        drop(stream);
                    }
                }
            }
        }

        drop(listener);
        if graceful_shutdown {
            self.drain().await;
        }
        session_lifecycle.cancel();
        join_sessions(sessions, first_error).await
    }

    /// Drains existing work after admission stopped, bounded by `drain_timeout`.
    async fn drain(&self) {
        let snapshot = self.snapshot();
        tracing::info!(
            component = "gateway",
            event = "gateway.drain.started",
            pending_offers = snapshot.pending_offers,
            live_pipes = snapshot.live_pipes,
            remote_open_attempts = snapshot.remote_open_attempts,
            drain_timeout_ms = self.inner.drain_timeout.as_millis(),
            "Gateway stopped admission and started draining existing work"
        );
        if wait_until_drained(&self.inner, self.inner.drain_timeout).await {
            tracing::info!(
                component = "gateway",
                event = "gateway.drain.completed",
                "Gateway drain completed"
            );
        } else {
            let snapshot = self.snapshot();
            tracing::warn!(
                component = "gateway",
                event = "gateway.drain.timed_out",
                pending_offers = snapshot.pending_offers,
                live_pipes = snapshot.live_pipes,
                remote_open_attempts = snapshot.remote_open_attempts,
                "Gateway drain deadline expired; forcing remaining work to close"
            );
        }
    }
}

/// Admission slots held for one accepted SDK socket until its session ends.
struct TransportSlots {
    session: OwnedSemaphorePermit,
    handshake: OwnedSemaphorePermit,
}

impl Inner {
    /// Rate budget, then the whole-transport slot, then the handshake slot;
    /// each rejection is a bounded-label counter and closes only this socket.
    fn admit_transport(&self, peer_addr: SocketAddr) -> Option<TransportSlots> {
        if !self.connection_rate.try_acquire() {
            metrics::counter!(
                "relaygate_gateway_sdk_transport_rejections_total",
                "reason" => "rate_limit"
            )
            .increment(1);
            return None;
        }
        let Ok(session) = Arc::clone(&self.session_slots).try_acquire_owned() else {
            reject_transport(
                "session_limit",
                "rejecting SDK connection because the session limit is reached",
                peer_addr,
            );
            return None;
        };
        let Ok(handshake) = Arc::clone(&self.handshake_slots).try_acquire_owned() else {
            reject_transport(
                "handshake_limit",
                "rejecting SDK connection because the handshake limit is reached",
                peer_addr,
            );
            return None;
        };
        Some(TransportSlots { session, handshake })
    }

    fn spawn_session(
        self: &Arc<Self>,
        sessions: &mut JoinSet<()>,
        stream: TcpStream,
        peer_addr: SocketAddr,
        slots: TransportSlots,
        session_lifecycle: &CancellationToken,
    ) {
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(
                component = "gateway",
                event = "gateway.socket.configure_failed",
                %peer_addr,
                %error,
                "failed to enable TCP_NODELAY"
            );
        }
        let inner = Arc::clone(self);
        let session_shutdown = session_lifecycle.child_token();
        sessions.spawn(async move {
            let _session_slot = slots.session;
            let Some(stream) = inner.accept_sdk_transport(stream, peer_addr).await else {
                return;
            };
            if let Err(error) = inner
                .run_session(stream, session_shutdown, slots.handshake)
                .await
            {
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.session.task_ended",
                    %peer_addr,
                    %error,
                    "SDK session ended"
                );
            }
        });
    }

    /// TLS-terminates the socket when SDK TLS is configured; `None` closes it.
    async fn accept_sdk_transport(
        &self,
        stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> Option<BoxedIo> {
        let Some(tls) = &self.sdk_tls else {
            return Some(insecure_boxed(stream));
        };
        match timeout(TLS_HANDSHAKE_TIMEOUT, tls.accept_boxed(stream)).await {
            Ok(Ok(stream)) => Some(stream),
            Ok(Err(error)) => {
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.session.tls_rejected",
                    %peer_addr,
                    %error,
                    "SDK TLS handshake failed"
                );
                None
            }
            Err(_) => {
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.session.tls_timeout",
                    %peer_addr,
                    "SDK TLS handshake timed out"
                );
                None
            }
        }
    }
}

fn reject_transport(reason: &'static str, message: &'static str, peer_addr: SocketAddr) {
    metrics::counter!(
        "relaygate_gateway_sdk_transport_rejections_total",
        "reason" => reason
    )
    .increment(1);
    // `message` first keeps the text-log layout of the former inline sites.
    tracing::debug!(
        message,
        component = "gateway",
        event = "gateway.session.rejected",
        %peer_addr,
        reason
    );
}

async fn join_sessions(
    mut sessions: JoinSet<()>,
    first_error: Option<GatewayError>,
) -> Result<(), GatewayError> {
    let mut shutdown_error = first_error;
    while let Some(result) = sessions.join_next().await {
        match result {
            Ok(()) => {}
            Err(error) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.session.shutdown_task_failed",
                    %error,
                    "SDK session task failed during shutdown"
                );
                shutdown_error.get_or_insert(session_shutdown_task_failure(error));
            }
        }
    }
    match shutdown_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Drain also waits for in-flight handshakes (bounded by the TLS and HELLO
/// deadlines) so a HELLO refused during drain is answered with
/// SESSION_REJECTED instead of a bare close.
async fn wait_until_drained(inner: &Arc<Inner>, drain_timeout: Duration) -> bool {
    if inner.is_drained() && inner.handshakes_idle() {
        return true;
    }
    timeout(drain_timeout, async {
        let mut poll = tokio::time::interval(DRAIN_POLL_INTERVAL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            poll.tick().await;
            inner.expire_offers().await;
            if inner.is_drained() && inner.handshakes_idle() {
                return;
            }
        }
    })
    .await
    .is_ok()
}

impl Inner {
    fn handshakes_idle(&self) -> bool {
        self.handshake_slots.available_permits() == self.max_pending_handshakes
    }

    async fn expire_offers(self: &Arc<Self>) {
        let actions = self.transition(|state| state.expire_offers(std::time::Instant::now()));
        self.execute_all(actions).await;
    }
}

fn offer_sweep_interval(offer_timeout: Duration) -> Duration {
    offer_timeout.clamp(MIN_OFFER_SWEEP_INTERVAL, MAX_OFFER_SWEEP_INTERVAL)
}

fn session_task_failure(error: JoinError) -> GatewayError {
    GatewayError::Runtime(format!("SDK session task failed: {error}"))
}

fn session_shutdown_task_failure(error: JoinError) -> GatewayError {
    GatewayError::Runtime(format!("SDK session task failed during shutdown: {error}"))
}

/// Checks SDK admission readiness through a TCP `HELLO`/`WELCOME` exchange.
///
/// This does not check RouteTable availability, Bindings, Pipe
/// establishment, or application payload processing.
pub async fn check(
    address: impl ToSocketAddrs,
    tls: &ClientTlsConfig,
    deadline: Duration,
) -> Result<(), GatewayError> {
    timeout(deadline, async {
        let stream = TcpStream::connect(address).await?;
        probe_admission(tls.connect_boxed(stream).await?).await
    })
    .await
    .map_err(|_| GatewayError::AdmissionCheckTimeout)?
}

/// Same readiness probe as [`check`] for an explicitly plaintext SDK
/// listener (`RELAYGATE_SDK_TRANSPORT=plaintext`).
pub async fn check_plaintext(
    address: impl ToSocketAddrs,
    deadline: Duration,
) -> Result<(), GatewayError> {
    timeout(deadline, async {
        let stream = TcpStream::connect(address).await?;
        probe_admission(insecure_boxed(stream)).await
    })
    .await
    .map_err(|_| GatewayError::AdmissionCheckTimeout)?
}

async fn probe_admission(io: BoxedIo) -> Result<(), GatewayError> {
    let mut framed = Framed::new(io, FrameCodec::default());
    framed.send(Frame::Hello).await?;
    match framed.next().await {
        Some(Ok(Frame::Welcome { .. })) => Ok(()),
        Some(Ok(_)) | None => Err(GatewayError::UnexpectedAdmissionResponse),
        Some(Err(error)) => Err(GatewayError::Protocol(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::check_plaintext;
    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{Destination, Frame, FrameCodec};
    use tokio::{
        net::{TcpListener, TcpStream},
        time::{Duration, sleep, timeout},
    };
    use tokio_util::{codec::Framed, sync::CancellationToken};

    use crate::{
        Gateway, GatewayConfig, GatewayError,
        test_support::{TestAction, authorization_config, bearer_token, destination},
    };

    const DESTINATION: &str = "sdk-server";

    #[tokio::test]
    async fn admitted_session_panic_cleans_siblings_and_stops_gateway()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let mut sibling = open_sdk_session(address).await?;
        assert_eq!(gateway.snapshot().sessions, 1);
        gateway
            .inner
            .panic_next_session_after_admission
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let stream = TcpStream::connect(address).await?;
        let mut panicking = Framed::new(stream, FrameCodec::default());
        panicking.send(Frame::Hello).await?;

        let result = timeout(Duration::from_secs(2), serving).await??;
        assert!(matches!(result, Err(GatewayError::Runtime(_))));
        assert_eq!(gateway.snapshot().sessions, 0);
        let sibling_end = timeout(Duration::from_secs(1), sibling.next()).await?;
        assert!(sibling_end.is_none() || sibling_end.is_some_and(|frame| frame.is_err()));
        shutdown.cancel();
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_admission_and_waits_for_existing_pipe()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new(authorization_config()).with_drain_timeout(Duration::from_secs(1)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let (mut acceptor_sdk, mut dialer_sdk, pipe_id) = open_pipe(address).await?;
        assert_eq!(gateway.snapshot().live_pipes, 1);
        shutdown.cancel();
        sleep(Duration::from_millis(50)).await;

        assert!(gateway.snapshot().draining);
        assert!(!serving.is_finished());
        assert!(
            check_plaintext(address, Duration::from_millis(50))
                .await
                .is_err()
        );

        let destination = destination(DESTINATION);
        dialer_sdk
            .send(Frame::Dial {
                connection_id: 2,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Dial),
            })
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), dialer_sdk.next()).await?,
            Some(Ok(Frame::DialFailed {
                connection_id: 2,
                code: relaygate_protocol::ErrorCode::Unavailable,
                ..
            }))
        ));
        acceptor_sdk
            .send(Frame::Publish {
                request_id: 2,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Publish),
            })
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), acceptor_sdk.next()).await?,
            Some(Ok(Frame::PublishFailed {
                request_id: 2,
                code: relaygate_protocol::ErrorCode::Unavailable,
                ..
            }))
        ));

        dialer_sdk.send(Frame::Close { pipe_id }).await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), acceptor_sdk.next()).await?,
            Some(Ok(Frame::Close { pipe_id: closed })) if closed == pipe_id
        ));
        timeout(Duration::from_secs(1), serving).await???;
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_forces_remaining_pipe_closed_at_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new(authorization_config())
                .with_drain_timeout(Duration::from_millis(50)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let (_acceptor_sdk, _dialer_sdk, _pipe_id) = open_pipe(address).await?;
        shutdown.cancel();
        timeout(Duration::from_secs(1), serving).await???;
        assert!(gateway.snapshot().draining);
        assert_eq!(gateway.snapshot().sessions, 0);
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn forced_shutdown_skips_drain_and_closes_existing_pipe()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new(authorization_config()).with_drain_timeout(Duration::from_secs(10)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let graceful = CancellationToken::new();
        let force = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_force = force.clone();
        let serving = tokio::spawn(async move {
            serving_gateway
                .serve_sdk_with_force(listener, graceful, serving_force)
                .await
        });

        let (_acceptor_sdk, _dialer_sdk, _pipe_id) = open_pipe(address).await?;
        force.cancel();
        timeout(Duration::from_secs(1), serving).await???;

        assert!(!gateway.snapshot().draining);
        assert_eq!(gateway.snapshot().sessions, 0);
        assert_eq!(gateway.snapshot().live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn graceful_shutdown_keeps_expiring_pending_offers()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new(authorization_config())
                .with_offer_timeout(Duration::from_millis(50))
                .with_drain_timeout(Duration::from_secs(10)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        let destination = destination(DESTINATION);
        let mut acceptor = open_sdk_session(address).await?;
        acceptor
            .send(Frame::Publish {
                request_id: 1,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Publish),
            })
            .await?;
        assert!(matches!(
            acceptor.next().await,
            Some(Ok(Frame::Published { .. }))
        ));
        let mut dialer = open_sdk_session(address).await?;
        dialer
            .send(Frame::Dial {
                connection_id: 1,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Dial),
            })
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), acceptor.next()).await?,
            Some(Ok(Frame::Offer { .. }))
        ));
        assert_eq!(gateway.snapshot().pending_offers, 1);

        shutdown.cancel();
        let started = std::time::Instant::now();
        let frame = timeout(Duration::from_secs(1), dialer.next()).await?;
        assert!(matches!(
            frame,
            Some(Ok(Frame::DialFailed {
                connection_id: 1,
                code: relaygate_protocol::ErrorCode::DeadlineExceeded,
                ..
            }))
        ));
        timeout(Duration::from_secs(2), serving).await???;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(gateway.snapshot().pending_offers, 0);
        Ok(())
    }

    #[tokio::test]
    async fn draining_gateway_rejects_a_late_hello_with_unavailable()
    -> Result<(), Box<dyn std::error::Error>> {
        let gateway = Gateway::new(
            GatewayConfig::new(authorization_config()).with_drain_timeout(Duration::from_secs(5)),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let serving_gateway = gateway.clone();
        let serving_shutdown = shutdown.clone();
        let serving =
            tokio::spawn(
                async move { serving_gateway.serve_sdk(listener, serving_shutdown).await },
            );

        // Idle Gateway: only this in-flight handshake keeps drain open. The
        // socket is accepted before drain starts; HELLO arrives after it.
        let late_stream = TcpStream::connect(address).await?;
        sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        sleep(Duration::from_millis(50)).await;
        assert!(gateway.snapshot().draining);
        assert!(!serving.is_finished());

        let mut late = Framed::new(late_stream, FrameCodec::default());
        late.send(Frame::Hello).await?;
        assert!(matches!(
            timeout(Duration::from_secs(1), late.next()).await?,
            Some(Ok(Frame::SessionRejected {
                code: relaygate_protocol::ErrorCode::Unavailable,
                ..
            }))
        ));
        assert!(
            timeout(Duration::from_secs(1), late.next())
                .await?
                .is_none()
        );
        timeout(Duration::from_secs(2), serving).await???;
        Ok(())
    }

    async fn open_pipe(
        gateway_address: std::net::SocketAddr,
    ) -> Result<
        (
            Framed<TcpStream, FrameCodec>,
            Framed<TcpStream, FrameCodec>,
            relaygate_protocol::PipeId,
        ),
        Box<dyn std::error::Error>,
    > {
        let destination: Destination = destination(DESTINATION);
        let mut listener = open_sdk_session(gateway_address).await?;
        listener
            .send(Frame::Publish {
                request_id: 1,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Publish),
            })
            .await?;
        assert!(matches!(
            listener.next().await,
            Some(Ok(Frame::Published { .. }))
        ));

        let mut dialer = open_sdk_session(gateway_address).await?;
        dialer
            .send(Frame::Dial {
                connection_id: 1,
                destination: destination.clone(),
                access_token: bearer_token(&destination, TestAction::Dial),
            })
            .await?;
        let Some(Ok(Frame::Offer { pipe_id, .. })) = listener.next().await else {
            return Err("Listener did not receive an OFFER".into());
        };
        listener.send(Frame::OfferAccepted { pipe_id }).await?;
        assert!(matches!(
            dialer.next().await,
            Some(Ok(Frame::Opened { pipe_id: opened })) if opened == pipe_id
        ));
        Ok((listener, dialer, pipe_id))
    }

    async fn open_sdk_session(
        address: std::net::SocketAddr,
    ) -> Result<Framed<TcpStream, FrameCodec>, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(address).await?;
        let mut framed = Framed::new(stream, FrameCodec::default());
        framed.send(Frame::Hello).await?;
        assert!(matches!(
            framed.next().await,
            Some(Ok(Frame::Welcome { .. }))
        ));
        Ok(framed)
    }
}
