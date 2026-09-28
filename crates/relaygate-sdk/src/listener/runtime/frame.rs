use std::sync::Arc;

use bytes::Bytes;
use relaygate_protocol::{
    BindingId, Destination, ErrorCode as WireErrorCode, Frame,
    PeerObservation as WirePeerObservation, PipeId, SessionId,
};
use tokio::sync::mpsc;

use super::{LivePipe, Registration, RelayFrameAction, RelaySessionState};
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{ListenerStatus, RelayInner, is_current_desired},
    pipe::PipeState,
    resource::{ResourceLimitKind, resource_exhausted},
    session::{SessionLink, SessionOutbound},
};

#[cfg(test)]
mod tests;

pub(super) async fn handle_relay_frame(
    frame: Frame,
    session_id: SessionId,
    session: &mut RelaySessionState,
    outbound: &SessionOutbound,
    abandoned: &mpsc::UnboundedSender<PipeId>,
    inner: &RelayInner,
    link: &mut SessionLink<'_>,
) -> RelayFrameAction {
    let mut frames = FrameContext {
        session_id,
        session,
        outbound,
        abandoned,
        inner,
        link,
    };
    match frame {
        Frame::Published {
            request_id,
            binding_id,
        } => frames.on_published(request_id, binding_id).await,
        Frame::PublishFailed {
            request_id,
            code,
            message,
        } => frames.on_publish_failed(request_id, code, message).await,
        Frame::Offer {
            pipe_id,
            binding_id,
            destination,
        } => frames.on_offer(pipe_id, binding_id, destination).await,
        Frame::Opened { pipe_id } if pipe_id.origin_session_id() == session_id => {
            frames.on_opened(pipe_id).await
        }
        Frame::DialFailed {
            connection_id,
            code,
            observation,
            message,
        } => frames.on_dial_failed(connection_id, code, observation, message),
        Frame::Data { pipe_id, payload } => frames.on_data(pipe_id, payload).await,
        Frame::Fin { pipe_id } => frames.on_fin(pipe_id),
        Frame::Close { pipe_id } => frames.on_close(pipe_id),
        Frame::Reset {
            pipe_id,
            code,
            message,
        } => frames.on_reset(pipe_id, code, message),
        Frame::Ping { nonce } => frames.on_ping(nonce).await,
        Frame::Pong { .. } | Frame::Unpublished { .. } => frames.settle(),
        other => {
            // `Frame`'s Debug redacts tokens and payloads; the kind is enough
            // to explain why the session ends.
            tracing::warn!(
                component = "sdk",
                event = "sdk.session.unexpected_frame",
                session_id = %session_id.as_uuid(),
                frame = ?other,
                "unexpected frame ended the Relay session"
            );
            RelayFrameAction::Stop(
                Error::new(
                    ErrorCode::ProtocolError,
                    PeerObservation::NotObserved,
                    "unexpected Gateway frame in active RelaySession",
                )
                .with_origin(crate::ErrorOrigin::Transport),
            )
        }
    }
}

/// One inbound frame's handling context: the session state plus the lanes a
/// frame may write to. Each `on_*` method owns exactly one frame kind.
struct FrameContext<'a, 'l> {
    session_id: SessionId,
    session: &'a mut RelaySessionState,
    outbound: &'a SessionOutbound,
    abandoned: &'a mpsc::UnboundedSender<PipeId>,
    inner: &'a RelayInner,
    link: &'a mut SessionLink<'l>,
}

impl FrameContext<'_, '_> {
    /// Default outcome for a frame whose handler produced no verdict of its
    /// own: stop once the runtime is cancelled, otherwise continue.
    fn settle(&self) -> RelayFrameAction {
        if self.inner.cancel.is_cancelled() {
            RelayFrameAction::Stop(Error::closed())
        } else {
            RelayFrameAction::Continue
        }
    }

    async fn on_published(&mut self, request_id: u64, binding_id: BindingId) -> RelayFrameAction {
        let Some(pending) = self.session.pending.remove(&request_id) else {
            return RelayFrameAction::Continue;
        };
        self.session
            .pending_by_destination
            .remove(&pending.state.destination);
        // Keep commit visible until activate updates status under the state lock.
        // A concurrent initial deadline must not turn this PUBLISH into NotObserved.
        if is_current_desired(self.inner, &pending.state) && pending.state.activate() {
            tracing::debug!(
                component = "sdk",
                event = "sdk.listener.active",
                session_id = %self.session_id.as_uuid(),
                request_id,
                destination = %pending.state.destination,
                binding_id = %binding_id.as_uuid(),
                "Listener registration is active"
            );
            self.session.registrations.insert(
                pending.state.destination.clone(),
                Registration {
                    state: pending.state,
                    binding_id,
                },
            );
            return RelayFrameAction::RegistrationSucceeded;
        }
        let Some(request_id) = self.session.next_request_id() else {
            return RelayFrameAction::Stop(Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "RelaySession exhausted request IDs",
            ));
        };
        if let Err(error) = self
            .link
            .send(Frame::Unpublish {
                request_id,
                binding_id,
            })
            .await
        {
            return RelayFrameAction::Stop(error);
        }
        RelayFrameAction::Reconcile
    }

    async fn on_publish_failed(
        &mut self,
        request_id: u64,
        code: WireErrorCode,
        message: String,
    ) -> RelayFrameAction {
        let Some(pending) = self.session.pending.remove(&request_id) else {
            return RelayFrameAction::Continue;
        };
        self.session
            .pending_by_destination
            .remove(&pending.state.destination);
        pending.state.finish_registration_attempt();
        if !is_current_desired(self.inner, &pending.state)
            || *pending.state.status.borrow() == ListenerStatus::Closed
        {
            return RelayFrameAction::Reconcile;
        }
        let error =
            Error::from_gateway_operation("PUBLISH", code, PeerObservation::NotObserved, message);
        if permanent_registration_failure(code) {
            if pending.state.was_returned() {
                pending.state.block(error);
                self.inner.mark_reconnect_degraded();
                pending.state.drain_unaccepted(true).await;
            } else {
                self.inner.fail_initial_listener(&pending.state, error);
                return RelayFrameAction::Reconcile;
            }
        } else if pending.state.was_returned() {
            pending
                .state
                .set_status(ListenerStatus::Suspended, Some(error));
            pending.state.drain_unaccepted(false).await;
            self.inner.schedule_reconcile();
        } else {
            self.inner.fail_initial_listener(&pending.state, error);
            return RelayFrameAction::Reconcile;
        }
        if self.inner.cancel.is_cancelled() {
            RelayFrameAction::Stop(Error::closed())
        } else {
            RelayFrameAction::SettlementChanged
        }
    }

    async fn on_offer(
        &mut self,
        pipe_id: PipeId,
        binding_id: BindingId,
        destination: Destination,
    ) -> RelayFrameAction {
        if let Some(existing) = self.session.pipes.get(&pipe_id) {
            if !existing.state.is_finished()
                && let Err(error) = self.link.send(Frame::OfferAccepted { pipe_id }).await
            {
                return RelayFrameAction::Stop(error);
            }
            return RelayFrameAction::Continue;
        }
        let Some(registration) = self.session.registrations.get(&destination) else {
            return reject_offer(
                self.link,
                pipe_id,
                WireErrorCode::NotFound,
                "Listener is not active",
            )
            .await;
        };
        if registration.binding_id != binding_id {
            return reject_offer(
                self.link,
                pipe_id,
                WireErrorCode::FailedPrecondition,
                "Binding incarnation is stale",
            )
            .await;
        }
        if !is_current_desired(self.inner, &registration.state)
            || *registration.state.status.borrow() != ListenerStatus::Active
        {
            return reject_offer(
                self.link,
                pipe_id,
                WireErrorCode::Unavailable,
                "Listener is not active",
            )
            .await;
        }
        if !registration.state.try_compact_terminal_queue() {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_queue.invariant_failed",
                destination = %destination,
                "Listener incoming queue compaction could not preserve live Pipes"
            );
            return RelayFrameAction::Stop(Error::new(
                ErrorCode::Internal,
                PeerObservation::NotObserved,
                "Listener queue or runtime invariant failed",
            ));
        }
        let permit = match registration.state.incoming_tx.try_reserve() {
            Ok(permit) => permit,
            Err(error) => {
                let error = match error {
                    mpsc::error::TrySendError::Full(()) => resource_exhausted(
                        ResourceLimitKind::ListenerPendingPipes,
                        PeerObservation::Observed,
                    ),
                    mpsc::error::TrySendError::Closed(()) => Error::new(
                        ErrorCode::Unavailable,
                        PeerObservation::Observed,
                        "Listener incoming queue is closed",
                    ),
                };
                return reject_offer(self.link, pipe_id, error.code().to_wire(), error.message())
                    .await;
            }
        };
        let live = match self
            .inner
            .resources
            .try_reserve_incoming(&registration.state.live_pipe_slots)
        {
            Ok(live) => live,
            Err(error) => {
                return reject_offer(self.link, pipe_id, error.code().to_wire(), error.message())
                    .await;
            }
        };
        let pipe_resources = self.inner.resources.pipe_resources(
            live,
            self.inner
                .config
                .resource_limits
                .max_buffered_bytes_per_pipe,
        );
        let listener = Arc::clone(&registration.state);
        let admitted = {
            let desired = match self.inner.desired.lock() {
                Ok(desired) => desired,
                Err(_) => {
                    tracing::error!(
                        component = "sdk",
                        event = "sdk.listener_registry.lock_poisoned",
                        "Listener desired registry lock is poisoned during Pipe admission"
                    );
                    self.inner.cancel.cancel();
                    return RelayFrameAction::Stop(Error::closed());
                }
            };
            if !desired
                .get(&destination)
                .is_some_and(|current| Arc::ptr_eq(current, &listener))
                || *listener.status.borrow() != ListenerStatus::Active
            {
                false
            } else {
                let Some(lifetime) = self.inner.lifetime.upgrade() else {
                    self.inner.cancel.cancel();
                    return RelayFrameAction::Stop(Error::closed());
                };
                let (pipe, state) = PipeState::pair_with_lifetime(
                    pipe_id,
                    self.outbound.clone(),
                    self.inner
                        .config
                        .resource_limits
                        .max_buffered_frames_per_pipe,
                    self.abandoned.clone(),
                    lifetime,
                    pipe_resources,
                );
                self.session.pipes.insert(
                    pipe_id,
                    LivePipe {
                        state,
                        listener: Some(Arc::downgrade(&listener)),
                    },
                );
                permit.send(pipe);
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.pipe.admitted",
                    destination = %destination,
                    binding_id = %binding_id.as_uuid(),
                    dialer_session_id = %pipe_id.origin_session_id().as_uuid(),
                    connection_id = pipe_id.connection_id(),
                    "Listener admitted a Pipe"
                );
                true
            }
        };
        if !admitted {
            return reject_offer(
                self.link,
                pipe_id,
                WireErrorCode::Unavailable,
                "Listener closed during Pipe admission",
            )
            .await;
        }
        if let Err(error) = self.link.send(Frame::OfferAccepted { pipe_id }).await {
            return RelayFrameAction::Stop(error);
        }
        self.settle()
    }

    async fn on_opened(&mut self, pipe_id: PipeId) -> RelayFrameAction {
        let Some(pending) = self.session.pending_dials.remove(&pipe_id.connection_id()) else {
            return RelayFrameAction::Continue;
        };
        let Some(lifetime) = self.inner.lifetime.upgrade() else {
            return RelayFrameAction::Stop(Error::closed());
        };
        let (pipe, state) = PipeState::pair_with_lifetime(
            pipe_id,
            self.outbound.clone(),
            self.inner
                .config
                .resource_limits
                .max_buffered_frames_per_pipe,
            self.abandoned.clone(),
            lifetime,
            self.inner.resources.pipe_resources(
                pending.resources,
                self.inner
                    .config
                    .resource_limits
                    .max_buffered_bytes_per_pipe,
            ),
        );
        if pending.response.send(Ok(pipe)).is_ok() {
            self.session.pipes.insert(
                pipe_id,
                LivePipe {
                    state,
                    listener: None,
                },
            );
        } else if let Err(error) = self.link.send(Frame::Cancel { pipe_id }).await {
            return RelayFrameAction::Stop(error);
        }
        self.settle()
    }

    fn on_dial_failed(
        &mut self,
        connection_id: u64,
        code: WireErrorCode,
        observation: WirePeerObservation,
        message: String,
    ) -> RelayFrameAction {
        if let Some(pending) = self.session.pending_dials.remove(&connection_id) {
            let _ = pending.response.send(Err(Error::from_gateway_operation(
                "DIAL",
                code,
                PeerObservation::from_wire(observation),
                message,
            )));
        }
        self.settle()
    }

    async fn on_data(&mut self, pipe_id: PipeId, payload: Bytes) -> RelayFrameAction {
        let error = self
            .session
            .pipes
            .get(&pipe_id)
            .and_then(|pipe| pipe.state.push_data(payload).err());
        if let Some(error) = error {
            if let Some(pipe) = self.session.pipes.remove(&pipe_id) {
                pipe.state.fail(error.clone());
                if !pipe.compact_listener_queue() {
                    return RelayFrameAction::Stop(Error::new(
                        ErrorCode::Internal,
                        PeerObservation::NotObserved,
                        "Listener queue or runtime invariant failed",
                    ));
                }
            }
            if let Err(error) = self
                .link
                .send(Frame::Reset {
                    pipe_id,
                    code: error.code().to_wire(),
                    message: error.message().to_owned(),
                })
                .await
            {
                return RelayFrameAction::Stop(error);
            }
        }
        self.settle()
    }

    fn on_fin(&mut self, pipe_id: PipeId) -> RelayFrameAction {
        if let Some(pipe) = self.session.pipes.get(&pipe_id) {
            pipe.state.remote_fin();
        }
        // A locally terminal Pipe keeps its entry until its CLOSE/RESET is sent.
        let finished = self
            .session
            .pipes
            .get(&pipe_id)
            .is_some_and(|pipe| pipe.state.is_protocol_finished());
        if finished
            && let Some(pipe) = self.session.pipes.remove(&pipe_id)
            && !pipe.compact_listener_queue()
        {
            return RelayFrameAction::Stop(Error::new(
                ErrorCode::Internal,
                PeerObservation::NotObserved,
                "Listener queue or runtime invariant failed",
            ));
        }
        self.settle()
    }

    fn on_close(&mut self, pipe_id: PipeId) -> RelayFrameAction {
        if let Some(pipe) = self.session.pipes.remove(&pipe_id) {
            pipe.state.close_normal();
            if !pipe.compact_listener_queue() {
                return RelayFrameAction::Stop(Error::new(
                    ErrorCode::Internal,
                    PeerObservation::NotObserved,
                    "Listener queue or runtime invariant failed",
                ));
            }
        }
        self.settle()
    }

    fn on_reset(
        &mut self,
        pipe_id: PipeId,
        code: WireErrorCode,
        message: String,
    ) -> RelayFrameAction {
        if let Some(pipe) = self.session.pipes.remove(&pipe_id) {
            pipe.state.fail(Error::from_gateway(
                code,
                PeerObservation::Observed,
                message,
            ));
            if !pipe.compact_listener_queue() {
                return RelayFrameAction::Stop(Error::new(
                    ErrorCode::Internal,
                    PeerObservation::NotObserved,
                    "Listener queue or runtime invariant failed",
                ));
            }
        } else if pipe_id.origin_session_id() == self.session_id
            && let Some(pending) = self.session.pending_dials.remove(&pipe_id.connection_id())
        {
            let _ = pending.response.send(Err(Error::from_gateway(
                code,
                PeerObservation::Observed,
                message,
            )));
        }
        self.settle()
    }

    async fn on_ping(&mut self, nonce: u64) -> RelayFrameAction {
        if let Err(error) = self.link.send(Frame::Pong { nonce }).await {
            return RelayFrameAction::Stop(error);
        }
        self.settle()
    }
}

async fn reject_offer(
    link: &mut SessionLink<'_>,
    pipe_id: PipeId,
    code: WireErrorCode,
    message: impl Into<String>,
) -> RelayFrameAction {
    listener_frame_action(
        link.send(Frame::OfferRejected {
            pipe_id,
            code,
            message: message.into(),
        })
        .await,
    )
}

fn listener_frame_action<T>(result: crate::Result<T>) -> RelayFrameAction {
    match result {
        Ok(_) => RelayFrameAction::Continue,
        Err(error) => RelayFrameAction::Stop(error),
    }
}

const fn permanent_registration_failure(code: WireErrorCode) -> bool {
    matches!(
        code,
        WireErrorCode::InvalidArgument
            | WireErrorCode::Unauthenticated
            | WireErrorCode::PermissionDenied
            | WireErrorCode::FailedPrecondition
            | WireErrorCode::AlreadyExists
    )
}
