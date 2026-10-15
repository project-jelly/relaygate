use std::collections::VecDeque;

use relaygate_protocol::ErrorCode;
use tokio::{sync::mpsc, time::Instant};

use super::{
    StreamCommand, TransportCloseReason, TransportCommand, TransportNotice,
    state::{RuntimeStream, StreamOrigin, TransportActor, enqueue_stream_frame},
};
use crate::{
    event::{PeerFailure, PeerOpenRequest, PeerStreamKey},
    frame::PeerFrame,
    identity::{OpenIdentity, PeerOpenProgress, StreamId},
    stream::RelayStream,
};

/// Applies Gateway-originated commands to a single transport actor. OPEN
/// allocation and its first ordered-writer commit occur in this actor only.
impl TransportActor {
    pub(super) async fn handle_command(&mut self, command: TransportCommand) {
        match command {
            TransportCommand::Open { request, reply } => {
                let open_identity = request.open_identity();
                let result = self.open(request).await;
                if result.is_err() {
                    let _ = self
                        .notices
                        .send(TransportNotice::AttemptEnded { open_identity })
                        .await;
                }
                let _ = reply.send(result);
            }
            TransportCommand::Cancel {
                open_identity,
                reply,
            } => {
                let result = self.cancel(open_identity).await;
                let _ = reply.send(result);
            }
            TransportCommand::Stream {
                stream_id,
                command,
                reply,
            } => {
                let result = match command {
                    StreamCommand::Opened => self.send_opened(stream_id),
                    StreamCommand::Failed { failure } => self.send_failed(stream_id, failure).await,
                    StreamCommand::Data { payload } => self.send_data(stream_id, payload),
                    StreamCommand::Fin => self.send_fin(stream_id).await,
                    StreamCommand::Close => self.send_close(stream_id).await,
                    StreamCommand::Reset { code, message } => {
                        self.send_reset(stream_id, code, message).await
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    pub(super) async fn open(
        &mut self,
        request: PeerOpenRequest,
    ) -> Result<PeerStreamKey, PeerFailure> {
        if self.streams.len() >= self.config.max_streams_per_transport {
            self.active_opens.release(request.open_identity());
            return Err(PeerFailure::not_observed(
                ErrorCode::ResourceExhausted,
                "peer transport stream limit is reached",
            ));
        }
        let stream_id = self.allocator.allocate().map_err(|_| {
            self.active_opens.release(request.open_identity());
            PeerFailure::not_observed(
                ErrorCode::ResourceExhausted,
                "peer StreamId counter is exhausted",
            )
        })?;
        #[cfg(test)]
        if let Some(gate) = &self.config.open_commit_gate {
            gate.wait().await;
        }
        let key = self.key(stream_id);
        let frame = PeerFrame::Open {
            stream_id,
            open_identity: request.open_identity(),
            destination: request.destination().clone(),
            relay_session_id: request.relay_session_id(),
            binding_id: request.binding_id(),
        };
        if let Err(failure) = PeerFrameCommit::open(&self.aggregate_writer, frame) {
            self.active_opens.release(request.open_identity());
            return Err(failure);
        }
        self.streams.insert(
            stream_id,
            RuntimeStream {
                relay: RelayStream::owned_opening(request.open_identity()),
                queued_frames: VecDeque::new(),
                terminal_queued: false,
                progress: PeerOpenProgress::AfterOpenCommit,
                origin: StreamOrigin::Local,
                open_deadline: Instant::now().checked_add(self.config.open_response_timeout),
            },
        );
        self.stream_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(key)
    }

    async fn cancel(&mut self, identity: OpenIdentity) -> Result<(), PeerFailure> {
        let stream_id = self.streams.iter().find_map(|(stream_id, stream)| {
            (stream.relay.owner() == Some(identity)).then_some(*stream_id)
        });
        let Some(stream_id) = stream_id else {
            return Ok(());
        };
        self.send_reset(
            stream_id,
            ErrorCode::Cancelled,
            "peer OPEN was cancelled".to_owned(),
        )
        .await
    }

    fn send_opened(&mut self, stream_id: StreamId) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.terminal_queued {
            return Ok(());
        }
        if stream.origin != StreamOrigin::Remote {
            return Err(PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "OPENED can only answer a peer-originated OPEN",
            ));
        }
        if stream.progress == PeerOpenProgress::Opened {
            return Ok(());
        }
        let mut relay = stream.relay.clone();
        relay.opened().map_err(|_| {
            PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "peer RelayStream is not opening",
            )
        })?;
        enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            PeerFrame::Opened { stream_id },
            false,
        )?;
        stream.relay = relay;
        stream.progress = PeerOpenProgress::Opened;
        Ok(())
    }

    async fn send_failed(
        &mut self,
        stream_id: StreamId,
        failure: PeerFailure,
    ) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.terminal_queued {
            return Ok(());
        }
        if stream.origin != StreamOrigin::Remote || stream.progress == PeerOpenProgress::Opened {
            return Err(PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "FAILED can only answer an opening peer-originated OPEN",
            ));
        }
        let frame = PeerFrame::Failed {
            stream_id,
            code: failure.code(),
            observation: failure.observation(),
            message: failure.message().to_owned(),
        };
        if let Err(error) = enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            frame,
            true,
        ) {
            self.fail_transport(TransportCloseReason::WriterFailed);
            return Err(error);
        }
        Ok(())
    }

    fn send_data(&mut self, stream_id: StreamId, payload: bytes::Bytes) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            // The stream's terminal event is already on its way to the
            // Gateway; treating late DATA as a failure would replace a
            // remote CLOSE with a synthetic RESET.
            tracing::debug!(
                component = "gateway",
                event = "gateway.peer.stream.late_data",
                stream_id = ?stream_id,
                "dropping DATA for a peer stream that already ended"
            );
            return Ok(());
        };
        stream.relay.data(self.local_endpoint).map_err(|_| {
            PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "peer RelayStream does not accept DATA in this direction",
            )
        })?;
        enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            PeerFrame::Data { stream_id, payload },
            false,
        )
    }

    async fn send_fin(&mut self, stream_id: StreamId) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.terminal_queued {
            return Ok(());
        }
        let mut relay = stream.relay.clone();
        let newly_finished = relay.fin(self.local_endpoint).map_err(|_| {
            PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "peer RelayStream does not accept FIN in this direction",
            )
        })?;
        if !newly_finished {
            return Ok(());
        }
        let terminal = relay.is_closed();
        enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            PeerFrame::Fin { stream_id },
            terminal,
        )?;
        stream.relay = relay;
        Ok(())
    }

    async fn send_close(&mut self, stream_id: StreamId) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.terminal_queued {
            return Ok(());
        }
        if !stream.relay.is_open() {
            return self
                .send_reset(
                    stream_id,
                    ErrorCode::ProtocolError,
                    "CLOSE is invalid before OPENED".to_owned(),
                )
                .await;
        }
        if let Err(error) = enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            PeerFrame::Close { stream_id },
            true,
        ) {
            self.fail_transport(TransportCloseReason::WriterFailed);
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn send_reset(
        &mut self,
        stream_id: StreamId,
        code: ErrorCode,
        message: String,
    ) -> Result<(), PeerFailure> {
        let queue_capacity = self.config.stream_queue_capacity;
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            return Ok(());
        };
        if stream.terminal_queued {
            return Ok(());
        }
        if let Err(error) = enqueue_stream_frame(
            stream,
            &mut self.ready_streams,
            stream_id,
            queue_capacity,
            PeerFrame::Reset {
                stream_id,
                code,
                message,
            },
            true,
        ) {
            self.fail_transport(TransportCloseReason::WriterFailed);
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerFrameCommit;

impl PeerFrameCommit {
    fn open(
        aggregate_writer: &mpsc::Sender<PeerFrame>,
        frame: PeerFrame,
    ) -> Result<Self, PeerFailure> {
        match aggregate_writer.try_send(frame) {
            Ok(()) => Ok(Self),
            Err(mpsc::error::TrySendError::Full(_)) => Err(PeerFailure::not_observed(
                ErrorCode::ResourceExhausted,
                "peer aggregate writer queue is full before OPEN commit",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(PeerFailure::not_observed(
                ErrorCode::Unavailable,
                "peer transport closed before OPEN commit",
            )),
        }
    }
}
