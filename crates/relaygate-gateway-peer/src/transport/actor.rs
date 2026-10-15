use std::{
    cmp::min,
    future::pending,
    ops::ControlFlow::{self, Break, Continue},
    sync::Arc,
};

use super::{
    ActiveOpenSet, TransportCloseReason, TransportClosure, TransportCommand, TransportNotice,
    liveness::{LivenessAction, TransportLiveness, staggered_interval},
    state::TransportActor,
    writer::run_writer,
};
use crate::metrics::{observe_peer_heartbeat_round_trip, observe_peer_heartbeat_timeout};
use crate::{
    codec::PeerCodecError, config::GatewayPeerConfig, frame::PeerFrame, handshake::EstablishedPeer,
};
use futures_util::StreamExt;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{Instant, sleep_until},
};

/// Owns the read-side lifecycle for one authenticated peer transport.
/// Protocol state transitions live in `command`, `inbound`, and `state`;
/// `writer` remains the only task allowed to write frames to the socket.
pub(super) async fn run_transport_actor(
    established: EstablishedPeer,
    config: GatewayPeerConfig,
    mut commands: mpsc::Receiver<TransportCommand>,
    notices: mpsc::Sender<TransportNotice>,
    active_opens: Arc<ActiveOpenSet>,
    stream_count: Arc<std::sync::atomic::AtomicUsize>,
    closure: TransportClosure,
) {
    let peer_gateway_id = established.remote_gateway_id;
    let peer_transport_id = established.peer_transport_id;
    let heartbeat_idle_interval = staggered_interval(
        config.heartbeat_idle_interval,
        peer_transport_id,
        established.local_endpoint,
    );
    let heartbeat_response_timeout = config.heartbeat_response_timeout;
    let idle_retirement_timeout = config.idle_retirement_timeout;
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(config.writer_queue_capacity);
    let (writer_wake, mut writer_wakes) = mpsc::channel(1);
    let close = closure.token().clone();
    let writer_closure = closure.clone();
    let actor_config = config.clone();
    let actor = TransportActor::new(
        &established,
        actor_config,
        aggregate_writer,
        notices.clone(),
        active_opens,
        stream_count,
        closure,
    );
    let (sink, mut source) = established.framed.split();
    let mut writer_tasks = JoinSet::new();
    writer_tasks.spawn(run_writer(
        sink,
        aggregate_receiver,
        writer_wake,
        writer_closure,
    ));
    let liveness = TransportLiveness::new(
        heartbeat_idle_interval,
        heartbeat_response_timeout,
        idle_retirement_timeout,
    );
    let mut transport = TransportLoop { actor, liveness };
    transport.sync_liveness();

    let close_reason = loop {
        let deadline = transport.next_deadline();
        let outcome = tokio::select! {
            () = close.cancelled() => break TransportCloseReason::LocalClose,
            command = commands.recv() => {
                let Some(command) = command else { break TransportCloseReason::LocalClose };
                transport.on_command(command).await;
                Continue(())
            }
            frame = source.next() => transport.on_inbound(frame).await,
            () = wait_for_deadline(deadline), if deadline.is_some() => {
                transport.on_deadline(&commands).await
            }
            wake = writer_wakes.recv() => {
                if wake.is_none() {
                    break if close.is_cancelled() {
                        TransportCloseReason::LocalClose
                    } else {
                        TransportCloseReason::WriterFailed
                    };
                }
                transport.after_activity().await;
                Continue(())
            }
        };
        if let Break(reason) = outcome {
            break reason;
        }
    };

    let TransportLoop { mut actor, .. } = transport;
    let close_reason = actor.closure.failure_reason().unwrap_or(close_reason);
    close.cancel();
    let losses = actor.drain_losses();
    drop(actor.aggregate_writer);
    while writer_tasks.join_next().await.is_some() {}

    // The manager keeps draining notices until transports report loss (or the
    // bounded shutdown deadline aborts all tasks), preserving per-transport
    // FIFO ordering behind all prior stream events.
    let _ = notices
        .send(TransportNotice::TransportLost {
            peer_gateway_id,
            peer_transport_id,
            reason: close_reason,
            streams: losses,
        })
        .await;
}

/// One transport's read loop state: protocol actor plus heartbeat/idle liveness.
struct TransportLoop {
    actor: TransportActor,
    liveness: TransportLiveness,
}

impl TransportLoop {
    fn next_deadline(&self) -> Option<Instant> {
        earliest_deadline(
            self.actor.next_open_deadline(),
            self.liveness.next_deadline(),
        )
    }

    fn sync_liveness(&mut self) {
        self.liveness
            .sync_stream_state(self.actor.streams.is_empty());
    }

    /// Every state change flushes queued stream frames and re-syncs idle tracking.
    async fn after_activity(&mut self) {
        self.actor.flush_stream_queues().await;
        self.sync_liveness();
    }

    async fn on_command(&mut self, command: TransportCommand) {
        self.actor.handle_command(command).await;
        self.after_activity().await;
    }

    async fn on_inbound(
        &mut self,
        frame: Option<Result<PeerFrame, PeerCodecError>>,
    ) -> ControlFlow<TransportCloseReason> {
        let frame = match frame {
            None => return Break(TransportCloseReason::RemoteClosed),
            Some(Err(error)) if error.is_io() => return Break(TransportCloseReason::RemoteClosed),
            Some(Err(_)) => return Break(TransportCloseReason::ProtocolError),
            Some(Ok(frame)) => frame,
        };
        if let Some(round_trip) = self.liveness.observe_inbound(&frame) {
            observe_peer_heartbeat_round_trip(round_trip);
        }
        if self.liveness.response_timed_out() {
            return Break(self.report_heartbeat_timeout());
        }
        if !self.actor.handle_frame(frame).await {
            return Break(TransportCloseReason::ProtocolError);
        }
        self.after_activity().await;
        Continue(())
    }

    async fn on_deadline(
        &mut self,
        commands: &mpsc::Receiver<TransportCommand>,
    ) -> ControlFlow<TransportCloseReason> {
        let now = Instant::now();
        self.actor.expire_open_deadlines().await;
        self.actor.flush_stream_queues().await;
        // Sampled after the flushes so a command queued meanwhile still defers
        // idle retirement.
        let action =
            self.liveness
                .on_deadline(now, self.actor.streams.is_empty(), commands.is_empty());
        match action {
            Some(LivenessAction::Ping(frame)) => {
                if self.actor.aggregate_writer.try_send(frame).is_err() {
                    return Break(TransportCloseReason::WriterFailed);
                }
                self.liveness.mark_probe_committed();
            }
            Some(LivenessAction::HeartbeatTimeout) => {
                return Break(self.report_heartbeat_timeout());
            }
            Some(LivenessAction::IdleRetired) => {
                tracing::debug!(
                    component = "gateway",
                    event = "gateway.peer.transport.idle_retired",
                    peer_gateway_id = %self.actor.peer_gateway_id.as_uuid(),
                    peer_transport_id = %self.actor.peer_transport_id.as_uuid(),
                    streams = self.actor.streams.len(),
                    "PeerTransport idle retirement timeout expired"
                );
                return Break(TransportCloseReason::IdleRetired);
            }
            None => {}
        }
        self.sync_liveness();
        Continue(())
    }

    /// Records the timeout metric and log, returning the matching close reason.
    fn report_heartbeat_timeout(&self) -> TransportCloseReason {
        observe_peer_heartbeat_timeout();
        tracing::debug!(
            component = "gateway",
            event = "gateway.peer.transport.heartbeat_timeout",
            peer_gateway_id = %self.actor.peer_gateway_id.as_uuid(),
            peer_transport_id = %self.actor.peer_transport_id.as_uuid(),
            streams = self.actor.streams.len(),
            "PeerTransport heartbeat response timed out"
        );
        TransportCloseReason::HeartbeatTimeout
    }
}

fn earliest_deadline(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(min(left, right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

#[cfg(test)]
mod tests;
