use std::sync::Arc;

use relaygate_route_table::GatewayId;
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    GatewayError, GatewayPeerConfig, GatewayRoutingConfig,
    peer::{PeerEvents, PeerHandle, PeerRuntime},
    routing::{RoutingHandle, RoutingRuntime},
};

use super::{Gateway, Inner, effects::ControlResult};

pub(super) struct DistributedRuntime {
    gateway_id: GatewayId,
    routing: RoutingHandle,
    routing_runtime: RoutingRuntime,
    peer: PeerHandle,
    peer_events: PeerEvents,
    peer_runtime: PeerRuntime,
    action_result_sender: mpsc::Sender<ControlResult>,
    action_results: mpsc::Receiver<ControlResult>,
    shutdown: CancellationToken,
}

impl DistributedRuntime {
    pub(super) fn start(
        gateway_id: GatewayId,
        routing_config: GatewayRoutingConfig,
        peer_config: GatewayPeerConfig,
        action_result_capacity: usize,
        shutdown: CancellationToken,
    ) -> Result<Self, GatewayError> {
        let routing_runtime =
            RoutingRuntime::start(routing_config, gateway_id, shutdown.child_token())?;
        let routing = routing_runtime.handle();
        let (peer, peer_events, peer_runtime) =
            PeerRuntime::start(peer_config, gateway_id, shutdown.child_token())
                .map_err(|error| GatewayError::Peer(error.to_string()))?;
        let (action_result_sender, action_results) = mpsc::channel(action_result_capacity);
        Ok(Self {
            gateway_id,
            routing,
            routing_runtime,
            peer,
            peer_events,
            peer_runtime,
            action_result_sender,
            action_results,
            shutdown,
        })
    }

    pub(super) const fn gateway_id(&self) -> GatewayId {
        self.gateway_id
    }

    pub(super) fn routing(&self) -> RoutingHandle {
        self.routing.clone()
    }

    pub(super) fn peer(&self) -> PeerHandle {
        self.peer.clone()
    }

    pub(super) fn action_results(&self) -> mpsc::Sender<ControlResult> {
        self.action_result_sender.clone()
    }

    pub(super) fn shutdown(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

impl Gateway {
    /// Serves SDK and Gateway peer listeners for one distributed runtime.
    ///
    /// A normal external shutdown drains SDK work before stopping distributed
    /// components. A fatal component failure bypasses drain and stops the whole
    /// runtime; current SDK Pipes receive their ordinary cleanup events rather
    /// than being replayed or rerouted.
    pub async fn serve_distributed(
        &self,
        sdk_listener: TcpListener,
        peer_listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), GatewayError> {
        let Some(runtime) = self.inner.take_distributed_runtime() else {
            return Err(GatewayError::InvalidConfig(
                "distributed Gateway runtime is missing or was already served".to_owned(),
            ));
        };
        let DistributedRuntime {
            routing_runtime,
            peer_events,
            peer_runtime,
            action_results,
            shutdown: lifecycle,
            ..
        } = runtime;
        let mut tasks = JoinSet::new();
        let sdk_stop = CancellationToken::new();
        let sdk_force_stop = CancellationToken::new();

        let sdk_gateway = self.clone();
        let sdk_shutdown = sdk_stop.clone();
        let sdk_force_shutdown = sdk_force_stop.clone();
        let sdk_lifecycle = lifecycle.clone();
        tasks.spawn(async move {
            let result = sdk_gateway
                .serve_sdk_with_force(sdk_listener, sdk_shutdown, sdk_force_shutdown)
                .await;
            sdk_lifecycle.cancel();
            ("sdk", result)
        });

        tasks.spawn(async move {
            (
                "routing",
                routing_runtime.wait().await.map_err(GatewayError::from),
            )
        });

        let peer_shutdown = lifecycle.clone();
        tasks.spawn(async move {
            let result = peer_runtime
                .serve(peer_listener)
                .await
                .map_err(|error| GatewayError::Peer(error.to_string()));
            peer_shutdown.cancel();
            ("peer", result)
        });

        let event_inner = Arc::clone(&self.inner);
        let event_shutdown = lifecycle.clone();
        tasks.spawn(async move {
            (
                "peer_events",
                event_inner
                    .run_peer_events(peer_events, event_shutdown)
                    .await,
            )
        });

        let result_inner = Arc::clone(&self.inner);
        let result_shutdown = lifecycle.clone();
        tasks.spawn(async move {
            (
                "control_results",
                result_inner
                    .run_control_results(action_results, result_shutdown)
                    .await,
            )
        });

        let bridge_lifecycle = lifecycle.clone();
        let bridge_sdk_stop = sdk_stop;
        tasks.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    bridge_sdk_stop.cancel();
                    bridge_lifecycle.cancelled().await;
                },
                _ = bridge_lifecycle.cancelled() => {},
            }
            ("external_shutdown", Ok(()))
        });

        let force_lifecycle = lifecycle.clone();
        tasks.spawn(async move {
            force_lifecycle.cancelled().await;
            sdk_force_stop.cancel();
            ("lifecycle_shutdown", Ok(()))
        });

        let mut first_error = None;
        while let Some(completed) = tasks.join_next().await {
            match completed {
                Ok((component, Ok(()))) => {
                    if !lifecycle.is_cancelled() {
                        first_error.get_or_insert_with(|| {
                            GatewayError::Peer(format!(
                                "distributed Gateway component {component} stopped unexpectedly"
                            ))
                        });
                    }
                }
                Ok((_, Err(error))) => {
                    first_error.get_or_insert(error);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        GatewayError::Peer(format!(
                            "distributed Gateway component task failed: {error}"
                        ))
                    });
                }
            }
            lifecycle.cancel();
        }

        self.inner.wait_control_effects().await;
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Inner {
    pub(super) fn take_distributed_runtime(&self) -> Option<DistributedRuntime> {
        match self.distributed_runtime.lock() {
            Ok(mut runtime) => runtime.take(),
            Err(poisoned) => {
                tracing::error!(
                    component = "gateway",
                    event = "gateway.distributed_runtime.lock_poisoned",
                    "recovering poisoned distributed runtime lock"
                );
                poisoned.into_inner().take()
            }
        }
    }
}
