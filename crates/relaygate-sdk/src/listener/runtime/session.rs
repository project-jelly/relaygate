use std::{
    collections::hash_map::Entry,
    ops::ControlFlow::{self, Break, Continue},
};

use futures_util::StreamExt;
use relaygate_protocol::{BearerToken, Frame, PipeId, ProtocolError};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{
    RelayFrameAction, RelaySessionState,
    cleanup::cleanup_relay_session,
    frame::handle_relay_frame,
    registration::{
        commit_registration_token, reconcile_registrations, wait_for_registration_deadline,
    },
};
use crate::listener::state::DesiredSettlement;
use crate::{
    Error, ErrorCode, PeerObservation,
    listener::{RelayCommand, RelayInner},
    observability::ReconnectEpisode,
    session::{
        EstablishedSession, SessionHeartbeat, SessionLink, SessionOutbound,
        session_outbound_channel, wait_for_heartbeat,
    },
};

pub(super) async fn run_relay_session(
    established: EstablishedSession,
    inner: &RelayInner,
    mut commands: mpsc::Receiver<RelayCommand>,
    mut cancellations: mpsc::UnboundedReceiver<PipeId>,
    session_cancel: CancellationToken,
    reconnect_episode: &mut Option<ReconnectEpisode>,
) -> bool {
    let (outbound_tx, mut outbound_rx) = session_outbound_channel(inner.config.outbound_capacity);
    // One unique Pipe value can abandon one current PipeId, so this lane is
    // logically bounded by the session's live Pipe map rather than history.
    let (abandoned_tx, mut abandoned_rx) = mpsc::unbounded_channel();
    let mut session = RelayLoop {
        heartbeat: SessionHeartbeat::new(&inner.config, established.id, 0x4c),
        established,
        inner,
        state: RelaySessionState::new(),
        session_cancel,
        outbound_tx,
        abandoned_tx,
        needs_reconcile: true,
        settlement_dirty: true,
        timed_out_request: None,
        registration_succeeded: false,
    };

    let failure = loop {
        if session.needs_reconcile {
            if let Err(error) = session.reconcile().await {
                break error;
            }
            session.needs_reconcile = false;
            session.settlement_dirty = true;
        }
        if session.settlement_dirty {
            session.settle_reconnect(reconnect_episode);
            session.settlement_dirty = false;
        }
        let registration_deadline = session.registration_deadline();
        let flow = tokio::select! {
            biased;
            _ = session.session_cancel.cancelled() => break if inner.cancel.is_cancelled() {
                Error::closed()
            } else {
                Error::transport_unavailable("RelaySession cancelled during a control operation")
            },
            incoming = session.established.transport.next() => session.on_inbound(incoming).await,
            () = wait_for_heartbeat(session.heartbeat.next_deadline()) => {
                session.on_heartbeat_deadline().await
            }
            _ = wait_for_registration_deadline(registration_deadline), if registration_deadline.is_some() => {
                session.timed_out_request = registration_deadline.map(|(request_id, _)| request_id);
                session.session_cancel.cancel();
                break Error::transport_deadline("PUBLISH response deadline exceeded");
            }
            _ = session.inner.reconcile.notified() => {
                session.needs_reconcile = true;
                Continue(())
            }
            supplied = session.state.token_supplies.next(), if !session.state.token_supplies.is_empty() => {
                match supplied {
                    Some((request_id, token)) => session.on_token(request_id, token).await,
                    None => Continue(()),
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break Error::closed(); };
                session.on_command(command).await
            }
            cancelled = cancellations.recv() => {
                match cancelled {
                    Some(pipe_id) => session.on_cancelled(pipe_id).await,
                    None => Continue(()),
                }
            }
            frame = outbound_rx.recv() => {
                let Some(frame) = frame else { break Error::closed(); };
                session.on_outbound(frame).await
            }
            abandoned = abandoned_rx.recv() => {
                match abandoned {
                    Some(pipe_id) => session.on_abandoned(pipe_id).await,
                    None => Continue(()),
                }
            }
        };
        if let Break(error) = flow {
            break error;
        }
    };

    inner.set_relay_error(Some(failure.clone()));
    fail_queued_dials(&mut commands, &failure);
    cleanup_relay_session(
        session.established.id,
        inner,
        session.state,
        session.timed_out_request,
        session.registration_succeeded,
        failure,
    )
    .await
}

/// One RelaySession's loop state. Every arm body of the session `select!`
/// is a method here; the select itself and its `biased` order stay in
/// `run_relay_session`.
struct RelayLoop<'a> {
    established: EstablishedSession,
    inner: &'a RelayInner,
    state: RelaySessionState,
    heartbeat: SessionHeartbeat,
    session_cancel: CancellationToken,
    outbound_tx: SessionOutbound,
    abandoned_tx: mpsc::UnboundedSender<PipeId>,
    needs_reconcile: bool,
    settlement_dirty: bool,
    timed_out_request: Option<u64>,
    registration_succeeded: bool,
}

impl RelayLoop<'_> {
    fn link(&mut self) -> SessionLink<'_> {
        SessionLink::new(
            &mut self.established.transport,
            self.inner.config.operation_timeout,
            &self.session_cancel,
        )
    }

    async fn reconcile(&mut self) -> crate::Result<()> {
        reconcile_registrations(
            self.inner,
            &mut self.established,
            &mut self.state,
            &self.session_cancel,
        )
        .await
    }

    fn settle_reconnect(&self, reconnect_episode: &mut Option<ReconnectEpisode>) {
        match self.inner.desired_settlement() {
            DesiredSettlement::Recovered => {
                self.inner.reset_republish_backoff();
                if let Some(episode) = reconnect_episode.take() {
                    episode.recover();
                }
                self.inner.clear_reconnect_degraded();
            }
            DesiredSettlement::Degraded => {
                self.inner.reset_republish_backoff();
                if let Some(episode) = reconnect_episode.take() {
                    episode.degrade();
                }
                self.inner.clear_reconnect_degraded();
            }
            DesiredSettlement::Pending => {}
        }
    }

    fn registration_deadline(&self) -> Option<(u64, Instant)> {
        self.state
            .pending
            .iter()
            .filter(|(_, pending)| pending.committed)
            .min_by_key(|(_, pending)| pending.deadline)
            .map(|(request_id, pending)| (*request_id, pending.deadline))
    }

    fn log_heartbeat_timeout(&self) {
        tracing::debug!(
            component = "sdk",
            event = "sdk.session.heartbeat_timeout",
            session_id = %self.established.id.as_uuid(),
            "Relay session heartbeat response timed out"
        );
    }

    async fn on_inbound(
        &mut self,
        incoming: Option<Result<Frame, ProtocolError>>,
    ) -> ControlFlow<Error> {
        let frame = match incoming {
            Some(Ok(frame)) => frame,
            Some(Err(error)) => {
                return Break(Error::from_protocol(
                    error,
                    "RelaySession frame read failed",
                ));
            }
            None => {
                return Break(Error::transport_unavailable(
                    "Gateway closed the RelaySession transport",
                ));
            }
        };
        self.heartbeat.observe_inbound(&frame);
        if self.heartbeat.response_timed_out() {
            self.log_heartbeat_timeout();
            return Break(Error::transport_deadline(
                "RelaySession heartbeat response timed out",
            ));
        }
        let session_id = self.established.id;
        let action = handle_relay_frame(
            frame,
            session_id,
            &mut self.state,
            &self.outbound_tx,
            &self.abandoned_tx,
            self.inner,
            &mut SessionLink::new(
                &mut self.established.transport,
                self.inner.config.operation_timeout,
                &self.session_cancel,
            ),
        )
        .await;
        match action {
            RelayFrameAction::Continue => {}
            RelayFrameAction::RegistrationSucceeded => {
                self.registration_succeeded = true;
                self.settlement_dirty = true;
            }
            RelayFrameAction::SettlementChanged => self.settlement_dirty = true,
            RelayFrameAction::Reconcile => self.needs_reconcile = true,
            RelayFrameAction::Stop(error) => return Break(error),
        }
        Continue(())
    }

    async fn on_heartbeat_deadline(&mut self) -> ControlFlow<Error> {
        let Some(frame) = self.heartbeat.on_deadline() else {
            self.log_heartbeat_timeout();
            return Break(Error::transport_deadline(
                "RelaySession heartbeat response timed out",
            ));
        };
        if let Err(error) = self.link().send(frame).await {
            return Break(error);
        }
        self.heartbeat.mark_probe_committed();
        Continue(())
    }

    async fn on_token(
        &mut self,
        request_id: u64,
        token: crate::Result<BearerToken>,
    ) -> ControlFlow<Error> {
        let committed = commit_registration_token(
            request_id,
            token,
            self.inner,
            &mut self.established,
            &mut self.state,
            &self.session_cancel,
        )
        .await;
        self.settlement_dirty = true;
        match committed {
            Ok(()) => Continue(()),
            Err(error) => Break(error),
        }
    }

    async fn on_command(&mut self, command: RelayCommand) -> ControlFlow<Error> {
        let RelayCommand::Dial {
            connection_id,
            destination,
            access_token,
            response,
            resources,
        } = command;
        let pending = super::PendingDial {
            response,
            resources,
        };
        match self.state.pending_dials.entry(connection_id) {
            // Ids are allocated in order under the session lock, so a repeat
            // is an internal invariant break; reject the newcomer and leave
            // the in-flight dial untouched.
            Entry::Occupied(_) => {
                debug_assert!(false, "duplicate ConnectionId {connection_id}");
                tracing::error!(
                    component = "sdk",
                    event = "sdk.dial.duplicate_connection_id",
                    session_id = %self.established.id.as_uuid(),
                    connection_id,
                    "duplicate ConnectionId allocated for a Relay session"
                );
                let _ = pending.response.send(Err(Error::new(
                    ErrorCode::AlreadyExists,
                    PeerObservation::NotObserved,
                    "ConnectionId is already in flight",
                )));
                return Continue(());
            }
            Entry::Vacant(slot) => {
                slot.insert(pending);
            }
        }
        if let Err(error) = self
            .link()
            .send(Frame::Dial {
                connection_id,
                destination,
                access_token,
            })
            .await
        {
            return Break(error);
        }
        Continue(())
    }

    async fn on_cancelled(&mut self, pipe_id: PipeId) -> ControlFlow<Error> {
        let mut removed = false;
        if let Some(pending) = self.state.pending_dials.remove(&pipe_id.connection_id()) {
            removed = true;
            let _ = pending.response.send(Err(Error::new(
                ErrorCode::Cancelled,
                PeerObservation::MaybeObserved,
                "committed DIAL was cancelled",
            )));
        }
        if let Some(pipe) = self.state.pipes.remove(&pipe_id) {
            removed = true;
            pipe.state.close_normal();
        }
        if removed && let Err(error) = self.link().send(Frame::Cancel { pipe_id }).await {
            return Break(error);
        }
        Continue(())
    }

    async fn on_outbound(&mut self, frame: Frame) -> ControlFlow<Error> {
        let terminal_pipe = match &frame {
            Frame::Close { pipe_id } | Frame::Reset { pipe_id, .. } => Some(*pipe_id),
            Frame::Fin { pipe_id }
                if self
                    .state
                    .pipes
                    .get(pipe_id)
                    .is_some_and(|pipe| pipe.state.is_protocol_finished()) =>
            {
                Some(*pipe_id)
            }
            _ => None,
        };
        if let Err(error) = self.link().send(frame).await {
            return Break(error);
        }
        if let Some(pipe_id) = terminal_pipe {
            self.state.pipes.remove(&pipe_id);
        }
        Continue(())
    }

    async fn on_abandoned(&mut self, pipe_id: PipeId) -> ControlFlow<Error> {
        if self.state.pipes.remove(&pipe_id).is_some()
            && let Err(error) = self.link().send(Frame::Close { pipe_id }).await
        {
            return Break(error);
        }
        Continue(())
    }
}

/// DIALs still queued when the session ends never reached the wire, so they
/// are reported as `NOT_OBSERVED` instead of being dropped as uncertain.
fn fail_queued_dials(commands: &mut mpsc::Receiver<RelayCommand>, failure: &Error) {
    commands.close();
    while let Ok(command) = commands.try_recv() {
        match command {
            RelayCommand::Dial { response, .. } => {
                let _ = response.send(Err(failure
                    .clone()
                    .with_observation(PeerObservation::NotObserved)));
            }
        }
    }
}
