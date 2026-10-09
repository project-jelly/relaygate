#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use relaygate_transport::ServerTlsConfig;
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    GatewayConfig, GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    authorization::Authorization,
    peer::PeerHandle,
    routing::RoutingHandle,
    state::{GatewayLimits, GatewayState},
};

#[cfg(test)]
mod admission_tests;
mod connection_rate;
#[cfg(test)]
mod connection_rate_tests;
mod distributed;
mod effects;
mod heartbeat;
mod route_resolver;
mod sdk_server;
mod session;
mod snapshot;
#[cfg(test)]
mod tests;
mod transition;

use connection_rate::ConnectionRateLimit;
use distributed::DistributedRuntime;
use effects::ControlEffects;
pub use sdk_server::{check, check_plaintext};

#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<GatewayState>,
    authorization: Authorization,
    authorization_timeout: Duration,
    sdk_tls: Option<ServerTlsConfig>,
    writer_queue_capacity: usize,
    max_frame_len: usize,
    offer_timeout: Duration,
    heartbeat_idle_interval: Duration,
    heartbeat_response_timeout: Duration,
    drain_timeout: Duration,
    session_slots: Arc<Semaphore>,
    handshake_slots: Arc<Semaphore>,
    max_pending_handshakes: usize,
    connection_rate: ConnectionRateLimit,
    routing: Option<RoutingHandle>,
    peer: Option<PeerHandle>,
    control_effects: Option<ControlEffects>,
    distributed_runtime: Mutex<Option<DistributedRuntime>>,
    #[cfg(test)]
    panic_next_session_after_admission: AtomicBool,
}

impl fmt::Debug for Gateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Gateway")
            .field("distributed", &self.inner.is_distributed())
            .finish_non_exhaustive()
    }
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Result<Self, GatewayError> {
        Self::build(config, None)
    }

    /// Creates a distributed Gateway with RouteTable discovery and one-hop
    /// Gateway peer relay.
    ///
    /// RouteTable availability is not a startup prerequisite. The manager
    /// reconnects in the background while local SDK sessions remain usable.
    /// Construction must occur inside a running Tokio runtime.
    pub fn new_distributed(
        config: GatewayConfig,
        routing_config: GatewayRoutingConfig,
        peer_config: GatewayPeerConfig,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            GatewayError::Routing(format!(
                "distributed Gateway must be constructed inside a Tokio runtime: {error}"
            ))
        })?;
        let gateway_id = relaygate_route_table::GatewayId::new();
        let action_result_capacity = config.max_pending_offers;
        let distributed = DistributedRuntime::start(
            gateway_id,
            routing_config,
            peer_config,
            action_result_capacity,
            CancellationToken::new(),
        )?;
        Self::build(config, Some(distributed))
    }

    fn build(
        config: GatewayConfig,
        distributed: Option<DistributedRuntime>,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let limits = GatewayLimits {
            max_sessions: config.max_sessions,
            max_bindings: config.max_bindings,
            max_pending_offers: config.max_pending_offers,
            max_remote_dial_attempts: config.max_remote_dial_attempts,
            max_live_pipes: config.max_live_pipes,
            offer_timeout: config.offer_timeout,
            control_rate_per_second: config.control_rate_per_second,
            control_burst: config.control_burst,
            session_control_rate_per_second: config.session_control_rate_per_second,
            session_control_burst: config.session_control_burst,
        };
        let gateway_id = distributed.as_ref().map(DistributedRuntime::gateway_id);
        let routing = distributed.as_ref().map(DistributedRuntime::routing);
        let peer = distributed.as_ref().map(DistributedRuntime::peer);
        let control_effects = distributed.as_ref().map(|runtime| {
            ControlEffects::new(
                config.max_pending_offers,
                Arc::new(runtime.routing()),
                runtime.action_results(),
                runtime.shutdown(),
            )
        });
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(match gateway_id {
                    Some(gateway_id) => GatewayState::new_distributed(limits, gateway_id),
                    None => GatewayState::new(limits),
                }),
                authorization: Authorization::new(
                    config.authorization,
                    config.authorization_concurrency,
                ),
                authorization_timeout: config.authorization_timeout,
                sdk_tls: config.sdk_tls,
                writer_queue_capacity: config.writer_queue_capacity,
                max_frame_len: config.max_frame_len,
                offer_timeout: config.offer_timeout,
                heartbeat_idle_interval: config.heartbeat_idle_interval,
                heartbeat_response_timeout: config.heartbeat_response_timeout,
                drain_timeout: config.drain_timeout,
                session_slots: Arc::new(Semaphore::new(config.max_sessions)),
                handshake_slots: Arc::new(Semaphore::new(
                    config.max_pending_handshakes.min(config.max_sessions),
                )),
                max_pending_handshakes: config.max_pending_handshakes.min(config.max_sessions),
                connection_rate: ConnectionRateLimit::new(
                    config.sdk_connection_rate_per_second,
                    config.sdk_connection_burst,
                ),
                routing,
                peer,
                control_effects,
                distributed_runtime: Mutex::new(distributed),
                #[cfg(test)]
                panic_next_session_after_admission: AtomicBool::new(false),
            }),
        })
    }

    /// Serves SDK sessions until `shutdown` is cancelled.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        if self.inner.is_distributed() {
            return Err(GatewayError::InvalidConfig(
                "a distributed Gateway must be served with serve_distributed".to_owned(),
            ));
        }
        self.serve_sdk(listener, shutdown).await
    }
}

impl Inner {
    fn begin_draining(&self) {
        self.transition(GatewayState::begin_draining);
    }

    fn is_drained(&self) -> bool {
        self.lock_state().is_drained()
    }

    fn is_distributed(&self) -> bool {
        self.routing.is_some()
    }

    /// Read-only access and action-free admission. Mutations that return
    /// `GatewayAction`s must use `transition` so registrations are committed.
    fn lock_state(&self) -> MutexGuard<'_, GatewayState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.state.lock_poisoned",
                    "recovering poisoned Gateway state lock"
                );
                poisoned.into_inner()
            }
        }
    }
}
