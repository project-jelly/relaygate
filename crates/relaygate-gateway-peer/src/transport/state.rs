use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Bound::{Excluded, Unbounded},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use relaygate_protocol::{ErrorCode, PeerObservation};
use relaygate_route_table::GatewayId;
use tokio::{sync::mpsc, time::Instant};

use super::{ActiveOpenSet, TransportCloseReason, TransportClosure, TransportNotice};
use crate::{
    config::GatewayPeerConfig,
    event::{LostPeerStream, PeerEvent, PeerFailure, PeerStreamKey},
    frame::PeerFrame,
    handshake::EstablishedPeer,
    identity::{
        PeerOpenProgress, PeerTransportId, RemoteStreamGuard, StreamEndpoint, StreamId,
        StreamIdAllocator,
    },
    stream::RelayStream,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamOrigin {
    Local,
    Remote,
}

pub(super) struct RuntimeStream {
    pub(super) relay: RelayStream,
    pub(super) queued_frames: VecDeque<PeerFrame>,
    pub(super) terminal_queued: bool,
    pub(super) progress: PeerOpenProgress,
    pub(super) origin: StreamOrigin,
    pub(super) open_deadline: Option<Instant>,
}

/// Current protocol state for one authenticated, ordered peer transport.
pub(super) struct TransportActor {
    pub(super) peer_gateway_id: GatewayId,
    pub(super) peer_transport_id: PeerTransportId,
    pub(super) local_endpoint: StreamEndpoint,
    pub(super) remote_endpoint: StreamEndpoint,
    pub(super) allocator: StreamIdAllocator,
    pub(super) remote_guard: RemoteStreamGuard,
    pub(super) streams: BTreeMap<StreamId, RuntimeStream>,
    pub(super) ready_streams: BTreeSet<StreamId>,
    pub(super) aggregate_writer: mpsc::Sender<PeerFrame>,
    pub(super) notices: mpsc::Sender<TransportNotice>,
    pub(super) active_opens: Arc<ActiveOpenSet>,
    pub(super) stream_count: Arc<AtomicUsize>,
    pub(super) closure: TransportClosure,
    pub(super) config: GatewayPeerConfig,
}

impl TransportActor {
    pub(super) fn new(
        established: &EstablishedPeer,
        config: GatewayPeerConfig,
        aggregate_writer: mpsc::Sender<PeerFrame>,
        notices: mpsc::Sender<TransportNotice>,
        active_opens: Arc<ActiveOpenSet>,
        stream_count: Arc<AtomicUsize>,
        closure: TransportClosure,
    ) -> Self {
        let local_endpoint = established.local_endpoint;
        let remote_endpoint = match local_endpoint {
            StreamEndpoint::Dialer => StreamEndpoint::Acceptor,
            StreamEndpoint::Acceptor => StreamEndpoint::Dialer,
        };
        Self {
            peer_gateway_id: established.remote_gateway_id,
            peer_transport_id: established.peer_transport_id,
            local_endpoint,
            remote_endpoint,
            allocator: StreamIdAllocator::new(local_endpoint),
            remote_guard: RemoteStreamGuard::new(remote_endpoint),
            streams: BTreeMap::new(),
            ready_streams: BTreeSet::new(),
            aggregate_writer,
            notices,
            active_opens,
            stream_count,
            closure,
            config,
        }
    }

    pub(super) fn fail_transport(&mut self, reason: TransportCloseReason) {
        self.closure.fail(reason);
    }

    pub(super) fn next_open_deadline(&self) -> Option<Instant> {
        self.streams
            .values()
            .filter_map(|stream| stream.open_deadline)
            .min()
    }

    pub(super) async fn expire_open_deadlines(&mut self) {
        self.expire_open_deadlines_at(Instant::now()).await;
    }

    #[cfg(test)]
    pub(super) async fn expire_open_deadlines_at(&mut self, now: Instant) {
        self.expire_open_deadlines_before(now).await;
    }

    #[cfg(not(test))]
    async fn expire_open_deadlines_at(&mut self, now: Instant) {
        self.expire_open_deadlines_before(now).await;
    }

    async fn expire_open_deadlines_before(&mut self, now: Instant) {
        let expired: Vec<StreamId> = self
            .streams
            .iter()
            .filter_map(|(stream_id, stream)| {
                (!stream.terminal_queued
                    && stream.open_deadline.is_some_and(|deadline| deadline <= now))
                .then_some(*stream_id)
            })
            .collect();
        for stream_id in expired {
            let queue_capacity = self.config.stream_queue_capacity;
            let Some(stream) = self.streams.get_mut(&stream_id) else {
                continue;
            };
            let Some(open_identity) = stream.relay.owner() else {
                self.fail_transport(TransportCloseReason::ProtocolError);
                return;
            };
            let reset = PeerFrame::Reset {
                stream_id,
                code: ErrorCode::DeadlineExceeded,
                message: "peer OPEN response timed out".to_owned(),
            };
            if enqueue_stream_frame(
                stream,
                &mut self.ready_streams,
                stream_id,
                queue_capacity,
                reset,
                true,
            )
            .is_err()
            {
                self.fail_transport(TransportCloseReason::WriterFailed);
                return;
            }
            stream.open_deadline = None;
            self.emit(PeerEvent::Failed {
                key: self.key(stream_id),
                open_identity,
                failure: PeerFailure::maybe_observed(
                    ErrorCode::DeadlineExceeded,
                    "peer OPEN response timed out",
                ),
            })
            .await;
        }
    }

    pub(super) async fn flush_stream_queues(&mut self) {
        loop {
            let mut moved = false;
            let mut blocked = false;
            let mut finished = Vec::new();
            // One frame per nonempty stream in ascending order, as in the full-map pass.
            let mut lower_bound = Unbounded;
            while let Some(stream_id) = self
                .ready_streams
                .range((lower_bound, Unbounded))
                .next()
                .copied()
            {
                lower_bound = Excluded(stream_id);
                let Some(stream) = self.streams.get_mut(&stream_id) else {
                    self.ready_streams.remove(&stream_id);
                    continue;
                };
                let Some(frame) = stream.queued_frames.pop_front() else {
                    self.ready_streams.remove(&stream_id);
                    continue;
                };
                match self.aggregate_writer.try_send(frame) {
                    Ok(()) => {
                        moved = true;
                        if stream.queued_frames.is_empty() {
                            self.ready_streams.remove(&stream_id);
                            if stream.terminal_queued {
                                finished.push(stream_id);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(frame)) => {
                        stream.queued_frames.push_front(frame);
                        blocked = true;
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        if stream.queued_frames.is_empty() {
                            self.ready_streams.remove(&stream_id);
                        }
                        self.fail_transport(TransportCloseReason::WriterFailed);
                        return;
                    }
                }
            }
            for stream_id in finished {
                self.finish_stream(stream_id).await;
            }
            if blocked || !moved {
                return;
            }
        }
    }

    pub(super) async fn emit(&mut self, event: PeerEvent) {
        if self
            .notices
            .send(TransportNotice::Event(event))
            .await
            .is_err()
        {
            self.fail_transport(TransportCloseReason::ProtocolError);
        }
    }

    pub(super) async fn finish_stream(&mut self, stream_id: StreamId) {
        self.ready_streams.remove(&stream_id);
        let Some(stream) = self.streams.remove(&stream_id) else {
            return;
        };
        let Some(open_identity) = stream.relay.owner() else {
            self.fail_transport(TransportCloseReason::ProtocolError);
            return;
        };
        self.active_opens.release(open_identity);
        self.stream_count.fetch_sub(1, Ordering::Relaxed);
        let _ = self
            .notices
            .send(TransportNotice::StreamEnded {
                key: self.key(stream_id),
                open_identity,
            })
            .await;
    }

    pub(super) fn drain_losses(&mut self) -> Vec<LostPeerStream> {
        self.ready_streams.clear();
        let streams = std::mem::take(&mut self.streams);
        let count = streams.len();
        let mut losses = Vec::with_capacity(count);
        for (stream_id, stream) in streams {
            if let Some(open_identity) = stream.relay.owner() {
                self.active_opens.release(open_identity);
                losses.push(LostPeerStream {
                    key: self.key(stream_id),
                    open_identity,
                    progress: stream.progress,
                });
            }
        }
        if count != 0 {
            self.stream_count.fetch_sub(count, Ordering::Relaxed);
        }
        losses
    }

    pub(super) const fn key(&self, stream_id: StreamId) -> PeerStreamKey {
        PeerStreamKey::new(self.peer_gateway_id, self.peer_transport_id, stream_id)
    }
}

pub(super) fn enqueue_stream_frame(
    stream: &mut RuntimeStream,
    ready_streams: &mut BTreeSet<StreamId>,
    stream_id: StreamId,
    queue_capacity: usize,
    frame: PeerFrame,
    terminal: bool,
) -> Result<(), PeerFailure> {
    if stream.terminal_queued {
        return if terminal {
            stream.open_deadline = None;
            Ok(())
        } else {
            Err(PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "peer RelayStream already has a terminal frame queued",
            ))
        };
    }
    if stream.queued_frames.len() >= queue_capacity {
        return Err(PeerFailure::maybe_observed(
            ErrorCode::ResourceExhausted,
            "peer stream writer queue is full",
        ));
    }
    stream.queued_frames.push_back(frame);
    ready_streams.insert(stream_id);
    stream.terminal_queued = terminal;
    if terminal {
        stream.open_deadline = None;
    }
    Ok(())
}

pub(super) fn failed_not_observed(
    stream_id: StreamId,
    code: ErrorCode,
    message: impl Into<String>,
) -> PeerFrame {
    PeerFrame::Failed {
        stream_id,
        code,
        observation: PeerObservation::NotObserved,
        message: message.into(),
    }
}
