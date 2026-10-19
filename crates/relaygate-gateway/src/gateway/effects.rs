use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{Destination, GatewayId, GatewayLocator};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    peer::{OpenIdentity, PeerEvent, PeerFailure, PeerHandle, PeerOpenRequest, PeerTarget},
    state::{DeliveryFailure, GatewayAction, PeerDelivery},
};

use super::{Inner, route_resolver::RouteResolver};

pub(super) struct ControlEffects {
    slots: Arc<Semaphore>,
    route_resolver: Arc<dyn RouteResolver>,
    tasks: TaskTracker,
    results: mpsc::Sender<ControlResult>,
    shutdown: CancellationToken,
}

/// Keeps both running effects and results awaiting consumption within the
/// same admission bound. The consumer releases the slot before executing
/// successor actions, so a capacity-one Resolve can start its peer OPEN.
pub(super) struct ControlResult {
    actions: Vec<GatewayAction>,
    permit: OwnedSemaphorePermit,
}

impl ControlResult {
    fn into_actions(self) -> Vec<GatewayAction> {
        let Self { actions, permit } = self;
        drop(permit);
        actions
    }
}

enum ControlAction {
    ResolveRoute {
        open_identity: OpenIdentity,
        destination: Destination,
    },
    OpenPeer {
        open_identity: OpenIdentity,
        gateway_id: GatewayId,
        gateway_locator: GatewayLocator,
        destination: Destination,
        relay_session_id: SessionId,
        binding_id: BindingId,
    },
    CancelPeerOpen {
        open_identity: OpenIdentity,
    },
}

impl ControlEffects {
    pub(super) fn new(
        capacity: usize,
        route_resolver: Arc<dyn RouteResolver>,
        results: mpsc::Sender<ControlResult>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(capacity)),
            route_resolver,
            tasks: TaskTracker::new(),
            results,
            shutdown,
        }
    }

    pub(super) async fn close_and_wait(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

impl Inner {
    pub(super) async fn execute_all(self: &Arc<Self>, actions: Vec<GatewayAction>) {
        let mut pending = VecDeque::from(actions);
        let mut cleaned = HashSet::new();
        while let Some(action) = pending.pop_front() {
            let failure = match action {
                GatewayAction::SendSdkFrame(delivery) => delivery.deliver(),
                GatewayAction::SendSdkTerminalBatch(delivery) => delivery.deliver(),
                GatewayAction::PublishRegistration { .. } => continue,
                GatewayAction::ResolveRoute {
                    open_identity,
                    destination,
                } => {
                    pending.extend(self.spawn_control_effect(ControlAction::ResolveRoute {
                        open_identity,
                        destination,
                    }));
                    continue;
                }
                GatewayAction::OpenPeer {
                    open_identity,
                    gateway_id,
                    gateway_locator,
                    destination,
                    relay_session_id,
                    binding_id,
                } => {
                    pending.extend(self.spawn_control_effect(ControlAction::OpenPeer {
                        open_identity,
                        gateway_id,
                        gateway_locator,
                        destination,
                        relay_session_id,
                        binding_id,
                    }));
                    continue;
                }
                GatewayAction::CancelPeerOpen { open_identity } => {
                    pending.extend(
                        self.spawn_control_effect(ControlAction::CancelPeerOpen { open_identity }),
                    );
                    continue;
                }
                GatewayAction::SendPeerFrame(delivery) => {
                    pending.extend(self.send_peer_delivery(delivery).await);
                    continue;
                }
            };
            let Some(failure) = failure else {
                continue;
            };
            match failure {
                DeliveryFailure::OfferQueueFull { acceptor, pipe_id } => {
                    pending.extend(
                        self.transition(|state| state.offer_delivery_rejected(acceptor, pipe_id)),
                    );
                }
                DeliveryFailure::SessionUnavailable(failed_session)
                    if cleaned.insert(failed_session) =>
                {
                    pending.extend(self.transition(|state| state.remove_session(failed_session)));
                }
                DeliveryFailure::SessionUnavailable(_) => {}
            }
        }
    }

    fn spawn_control_effect(self: &Arc<Self>, action: ControlAction) -> Vec<GatewayAction> {
        // Both handles come from one `DistributedRuntime`, so they are present
        // together; this is the only place an effect checks for them.
        let (Some(control), Some(peer)) = (&self.control_effects, &self.peer) else {
            return self.reject_control_effect(action, "distributed control runtime is disabled");
        };
        let Ok(permit) = Arc::clone(&control.slots).try_acquire_owned() else {
            return self.reject_control_effect(action, "Gateway control effect limit reached");
        };
        let inner = Arc::clone(self);
        let route_resolver = Arc::clone(&control.route_resolver);
        let peer = peer.clone();
        let results = control.results.clone();
        let shutdown = control.shutdown.clone();
        control.tasks.spawn(async move {
            let actions = inner.run_control_effect(route_resolver, peer, action).await;
            if actions.is_empty() {
                return;
            }
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = results.send(ControlResult { actions, permit }) => {
                    if result.is_err() && !shutdown.is_cancelled() {
                        tracing::warn!(
                            component = "gateway",
                            event = "gateway.control_result.dropped",
                            "distributed control result loop stopped"
                        );
                    }
                }
            }
        });
        Vec::new()
    }

    fn reject_control_effect(&self, action: ControlAction, message: &str) -> Vec<GatewayAction> {
        match action {
            ControlAction::ResolveRoute { open_identity, .. } => self.transition(|state| {
                state.route_failed(open_identity, ErrorCode::ResourceExhausted, message)
            }),
            ControlAction::OpenPeer { open_identity, .. } => self.transition(|state| {
                state.peer_open_commit_failed(
                    open_identity,
                    ErrorCode::ResourceExhausted,
                    PeerObservation::NotObserved,
                    message,
                )
            }),
            ControlAction::CancelPeerOpen { .. } => Vec::new(),
        }
    }

    async fn run_control_effect(
        &self,
        route_resolver: Arc<dyn RouteResolver>,
        peer: PeerHandle,
        action: ControlAction,
    ) -> Vec<GatewayAction> {
        match action {
            ControlAction::ResolveRoute {
                open_identity,
                destination,
            } => match route_resolver.resolve(destination).await {
                Ok(bindings) => {
                    self.transition(|state| state.route_resolved(open_identity, bindings))
                }
                Err(error) => self.transition(|state| {
                    state.route_failed(open_identity, error.code(), error.message())
                }),
            },
            ControlAction::OpenPeer {
                open_identity,
                gateway_id,
                gateway_locator,
                destination,
                relay_session_id,
                binding_id,
            } => {
                let request = PeerOpenRequest::new(
                    PeerTarget::new(gateway_id, gateway_locator),
                    open_identity,
                    destination,
                    relay_session_id,
                    binding_id,
                );
                match peer.open(request).await {
                    Ok(key) => {
                        self.transition(|state| state.peer_open_committed(open_identity, key))
                    }
                    Err(error) => self.transition(|state| {
                        state.peer_open_commit_failed(
                            open_identity,
                            error.code(),
                            error.observation(),
                            error.message(),
                        )
                    }),
                }
            }
            ControlAction::CancelPeerOpen { open_identity } => {
                if let Err(error) = peer.cancel_open(open_identity).await {
                    tracing::debug!(
                        component = "gateway",
                        event = "gateway.peer_open.cancel_failed",
                        entry_gateway_id = %open_identity.entry_gateway().as_uuid(),
                        origin_session_id = %open_identity.origin_session().as_uuid(),
                        connection_id = open_identity.connection_id(),
                        error_code = ?error.code(),
                        observation = ?error.observation(),
                        "peer OPEN cancellation did not commit; a late result remains terminal locally"
                    );
                }
                Vec::new()
            }
        }
    }

    async fn send_peer_delivery(&self, delivery: PeerDelivery) -> Vec<GatewayAction> {
        let Some(peer) = &self.peer else {
            return self.cleanup_unsent_peer_delivery(
                delivery,
                ErrorCode::Internal,
                "Gateway peer relay is not configured",
            );
        };
        let (key, recover_stream, close_transport_on_failure, result) = match delivery {
            PeerDelivery::Opened { key } => (key, true, false, peer.send_opened(key).await),
            PeerDelivery::Failed {
                key,
                code,
                observation,
                message,
            } => (
                key,
                false,
                false,
                peer.send_failed(key, PeerFailure::new(code, observation, message))
                    .await,
            ),
            PeerDelivery::Data { key, payload } => {
                (key, true, false, peer.send_data(key, payload).await)
            }
            PeerDelivery::Fin { key } => (key, true, false, peer.send_fin(key).await),
            PeerDelivery::Close { key } => (key, false, false, peer.send_close(key).await),
            PeerDelivery::Reset { key, code, message } => {
                (key, false, true, peer.send_reset(key, code, message).await)
            }
        };
        let Err(error) = result else {
            return Vec::new();
        };
        tracing::debug!(
            component = "gateway",
            event = "gateway.peer_frame.commit_failed",
            peer_gateway_id = %key.peer_gateway_id(),
            peer_transport_id = %key.peer_transport_id().as_uuid(),
            stream_id = key.stream_id().raw(),
            error_code = ?error.code(),
            observation = ?error.observation(),
            "peer frame did not commit"
        );
        if close_transport_on_failure {
            peer.close_transport(key);
            return Vec::new();
        }
        if !recover_stream {
            return Vec::new();
        }

        let actions = self
            .transition(|state| state.peer_reset(key, error.code(), error.message().to_owned()));
        if peer
            .send_reset(key, error.code(), error.message().to_owned())
            .await
            .is_err()
        {
            peer.close_transport(key);
        }
        actions
    }

    fn cleanup_unsent_peer_delivery(
        &self,
        delivery: PeerDelivery,
        code: ErrorCode,
        message: &str,
    ) -> Vec<GatewayAction> {
        match delivery {
            PeerDelivery::Opened { key }
            | PeerDelivery::Data { key, .. }
            | PeerDelivery::Fin { key } => {
                self.transition(|state| state.peer_reset(key, code, message.to_owned()))
            }
            PeerDelivery::Failed { .. }
            | PeerDelivery::Close { .. }
            | PeerDelivery::Reset { .. } => Vec::new(),
        }
    }

    pub(super) async fn run_peer_events(
        self: Arc<Self>,
        mut events: crate::peer::PeerEvents,
        shutdown: CancellationToken,
    ) -> Result<(), crate::GatewayError> {
        loop {
            let event = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                event = events.recv() => event,
            };
            let Some(event) = event else {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                return Err(crate::GatewayError::Peer(
                    "peer event stream stopped unexpectedly".to_owned(),
                ));
            };
            let actions = self.handle_peer_event(event);
            self.execute_all(actions).await;
        }
    }

    fn handle_peer_event(&self, event: PeerEvent) -> Vec<GatewayAction> {
        self.transition(|state| match event {
            PeerEvent::IncomingOpen {
                key,
                open_identity,
                destination,
                relay_session_id,
                binding_id,
            } => state.receive_peer_open(
                key,
                open_identity,
                destination,
                relay_session_id,
                binding_id,
            ),
            PeerEvent::Opened { key, open_identity } => state.peer_opened(key, open_identity),
            PeerEvent::Failed {
                key,
                open_identity,
                failure,
            } => state.peer_open_failed(
                key,
                open_identity,
                failure.code(),
                failure.observation(),
                failure.message(),
            ),
            PeerEvent::Data { key, payload } => state.peer_data(key, payload),
            PeerEvent::Fin { key } => state.peer_fin(key),
            PeerEvent::Close { key } => state.peer_close(key),
            PeerEvent::Reset { key, code, message } => state.peer_reset(key, code, message),
            PeerEvent::TransportLost { streams, .. } => {
                let actions = streams
                    .into_iter()
                    .flat_map(|stream| {
                        state.peer_transport_lost_stream(
                            stream.key,
                            stream.open_identity,
                            stream.progress.failure_observation(),
                        )
                    })
                    .collect();
                batch_transport_lost_terminal_frames(actions)
            }
        })
    }

    pub(super) async fn run_control_results(
        self: Arc<Self>,
        mut results: mpsc::Receiver<ControlResult>,
        shutdown: CancellationToken,
    ) -> Result<(), crate::GatewayError> {
        loop {
            let result = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = results.recv() => result,
            };
            let Some(result) = result else {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                return Err(crate::GatewayError::Peer(
                    "distributed control result stream stopped unexpectedly".to_owned(),
                ));
            };
            self.execute_all(result.into_actions()).await;
        }
    }

    pub(super) async fn wait_control_effects(&self) {
        if let Some(control) = &self.control_effects {
            control.close_and_wait().await;
        }
    }
}

#[cfg(test)]
mod control_tests;

fn batch_transport_lost_terminal_frames(actions: Vec<GatewayAction>) -> Vec<GatewayAction> {
    let mut indexes = HashMap::new();
    let mut batched = Vec::new();
    for action in actions {
        let GatewayAction::SendSdkFrame(delivery) = action else {
            batched.push(action);
            continue;
        };
        let target = delivery.target;
        if let Some(index) = indexes.get(&delivery.target).copied() {
            let GatewayAction::SendSdkTerminalBatch(batch) = &mut batched[index] else {
                batched.push(GatewayAction::SendSdkFrame(delivery));
                continue;
            };
            if let Err(delivery) = batch.push(delivery) {
                indexes.remove(&target);
                batched.push(GatewayAction::SendSdkFrame(delivery));
            }
            continue;
        }
        let batch = match delivery.into_terminal_batch() {
            Ok(batch) => batch,
            Err(delivery) => {
                batched.push(GatewayAction::SendSdkFrame(delivery));
                continue;
            }
        };
        indexes.insert(target, batched.len());
        batched.push(GatewayAction::SendSdkTerminalBatch(batch));
    }
    batched
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Gateway, GatewayConfig,
        peer::{
            LostPeerStream, OpenIdentity, PeerOpenProgress, PeerStreamKey, PeerTransportId,
            StreamId,
        },
        state::SdkWriterItem,
        test_support::{authorization_config, unique_destination},
    };
    use relaygate_protocol::{BearerToken, Frame, PipeId};
    use std::error::Error;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    struct PeerLossFixture {
        gateway: Gateway,
        receiver: mpsc::Receiver<SdkWriterItem>,
        cancellation: CancellationToken,
        event: PeerEvent,
    }

    fn peer_loss_fixture(queue_capacity: usize, fill_queue: bool) -> TestResult<PeerLossFixture> {
        let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        let destination = unique_destination();
        let (sender, receiver) = mpsc::channel(queue_capacity);
        if fill_queue {
            sender.try_send(SdkWriterItem::Single(Frame::Ping { nonce: 99 }))?;
        }
        let cancellation = CancellationToken::new();
        let peer_gateway_id = GatewayId::new();
        let peer_transport_id = PeerTransportId::new();
        let mut streams = Vec::new();
        {
            let mut state = gateway.inner.lock_state();
            let listener = state
                .add_session(sender, cancellation.clone())
                .ok_or("missing listener session")?;
            let published = state.handle_at(
                listener,
                Frame::Publish {
                    request_id: 1,
                    destination: destination.clone(),
                    access_token: BearerToken::new("effects-test-token")?,
                },
                std::time::Instant::now(),
            )?;
            let binding_id = published
                .iter()
                .find_map(|action| match action {
                    GatewayAction::SendSdkFrame(delivery) => match &delivery.frame {
                        Frame::Published { binding_id, .. } => Some(*binding_id),
                        _ => None,
                    },
                    _ => None,
                })
                .ok_or("missing binding")?;
            let origin_session = SessionId::new();
            for connection_id in 1..=3 {
                let key = PeerStreamKey::new(
                    peer_gateway_id,
                    peer_transport_id,
                    StreamId::from_raw((connection_id - 1) * 2),
                );
                let open_identity =
                    OpenIdentity::new(peer_gateway_id, origin_session, connection_id);
                let offered = state.receive_peer_open(
                    key,
                    open_identity,
                    destination.clone(),
                    listener,
                    binding_id,
                );
                let pipe_id = offered
                    .iter()
                    .find_map(|action| match action {
                        GatewayAction::SendSdkFrame(delivery) => match &delivery.frame {
                            Frame::Offer { pipe_id, .. } => Some(*pipe_id),
                            _ => None,
                        },
                        _ => None,
                    })
                    .ok_or("missing offer")?;
                state.handle(listener, Frame::OfferAccepted { pipe_id })?;
                streams.push(LostPeerStream {
                    key,
                    open_identity,
                    progress: PeerOpenProgress::Opened,
                });
            }
        }
        Ok(PeerLossFixture {
            gateway,
            receiver,
            cancellation,
            event: PeerEvent::TransportLost {
                peer_gateway_id,
                peer_transport_id,
                streams,
            },
        })
    }

    #[test]
    fn transport_loss_coalescer_batches_only_reset_and_dial_failed() -> TestResult {
        let gateway = Gateway::new(GatewayConfig::new(authorization_config()))?;
        let delivery = {
            let (sender, _receiver) = mpsc::channel(4);
            let mut state = gateway.inner.lock_state();
            let session = state
                .add_session(sender, CancellationToken::new())
                .ok_or("missing session")?;
            let mut actions = state.handle(session, Frame::Ping { nonce: 7 })?;
            let Some(GatewayAction::SendSdkFrame(delivery)) = actions.pop() else {
                return Err("missing PONG delivery".into());
            };
            delivery
        };
        let mut reset = delivery.clone();
        reset.frame = Frame::Reset {
            pipe_id: PipeId::new(SessionId::new(), 1),
            code: ErrorCode::Unavailable,
            message: "PeerTransport was lost".to_owned(),
        };
        let mut dial_failed = delivery.clone();
        dial_failed.frame = Frame::DialFailed {
            connection_id: 2,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "PeerTransport was lost during remote OPEN".to_owned(),
        };

        let actions = batch_transport_lost_terminal_frames(vec![
            GatewayAction::SendSdkFrame(reset),
            GatewayAction::SendSdkFrame(dial_failed),
            GatewayAction::SendSdkFrame(delivery),
        ]);
        assert_eq!(actions.len(), 2);
        let GatewayAction::SendSdkTerminalBatch(batch) = &actions[0] else {
            return Err("terminal frames were not batched".into());
        };
        assert!(matches!(
            batch.frames(),
            [Frame::Reset { .. }, Frame::DialFailed { .. }]
        ));
        assert!(matches!(
            &actions[1],
            GatewayAction::SendSdkFrame(delivery)
                if matches!(delivery.frame, Frame::Pong { nonce: 7 })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn capacity_one_batches_peer_loss_without_removing_listener() -> TestResult {
        let mut fixture = peer_loss_fixture(1, false)?;
        let actions = fixture.gateway.inner.handle_peer_event(fixture.event);
        assert_eq!(actions.len(), 1);
        let GatewayAction::SendSdkTerminalBatch(batch) = &actions[0] else {
            return Err("peer loss did not produce one terminal batch".into());
        };
        assert_eq!(batch.frames().len(), 3);
        assert!(batch.frames().iter().all(|frame| matches!(
            frame,
            Frame::Reset {
                code: ErrorCode::Unavailable,
                message,
                ..
            } if message == "PeerTransport was lost"
        )));

        fixture.gateway.inner.execute_all(actions).await;

        let SdkWriterItem::TerminalBatch(frames) = fixture.receiver.try_recv()? else {
            return Err("writer did not receive the terminal batch".into());
        };
        assert_eq!(frames.len(), 3);
        assert!(!fixture.cancellation.is_cancelled());
        let snapshot = fixture.gateway.snapshot();
        assert_eq!(snapshot.sessions, 1);
        assert_eq!(snapshot.bindings, 1);
        assert_eq!(snapshot.pending_offers, 0);
        assert_eq!(snapshot.live_pipes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn full_terminal_batch_removes_session_without_partial_enqueue() -> TestResult {
        let mut fixture = peer_loss_fixture(1, true)?;
        let actions = fixture.gateway.inner.handle_peer_event(fixture.event);
        fixture.gateway.inner.execute_all(actions).await;

        assert!(fixture.cancellation.is_cancelled());
        let snapshot = fixture.gateway.snapshot();
        assert_eq!(snapshot.sessions, 0);
        assert_eq!(snapshot.bindings, 0);
        assert_eq!(snapshot.pending_offers, 0);
        assert_eq!(snapshot.live_pipes, 0);
        assert!(matches!(
            fixture.receiver.try_recv()?,
            SdkWriterItem::Single(Frame::Ping { nonce: 99 })
        ));
        assert!(fixture.receiver.try_recv().is_err());
        Ok(())
    }
}
