#[cfg(test)]
use std::sync::atomic::Ordering;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    pin::pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use relaygate_protocol::{ErrorCode, Frame, FrameCodec, MAX_HELLO_FRAME_LEN};
use relaygate_transport::BoxedIo;
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc},
    time::{Instant, sleep_until, timeout, timeout_at},
};
use tokio_util::{codec::Framed, sync::CancellationToken};

use crate::authorization::ControlOperation;
use crate::metrics::{observe_sdk_heartbeat_round_trip, observe_sdk_heartbeat_timeout};
use crate::state::{ProtocolViolation, SdkWriterItem, observe_dial_result};

use super::{Inner, heartbeat::SessionHeartbeat};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const SDK_FRAME_INITIAL_CAPACITY: usize = 2 * 1024;
const SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY: usize = 8 * 1024;
const WRITER_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

mod admission;

/// Accounts for an admitted DIAL until the synchronous state transition owns
/// its terminal result. Dropping the read future can bypass that transition.
struct PendingDialObservation(Option<std::time::Instant>);

impl PendingDialObservation {
    fn new(operation: &ControlOperation) -> Self {
        Self(match operation {
            ControlOperation::Dial { started_at, .. } => Some(*started_at),
            ControlOperation::Publish { .. } => None,
        })
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PendingDialObservation {
    fn drop(&mut self) {
        observe_dial_result(self.0, Some(ErrorCode::Cancelled));
    }
}

impl Inner {
    pub(super) async fn run_session(
        self: Arc<Self>,
        stream: BoxedIo,
        cancellation: CancellationToken,
        handshake_slot: OwnedSemaphorePermit,
    ) -> Result<(), SessionError> {
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        let mut framed = Framed::with_capacity(
            stream,
            FrameCodec::new(MAX_HELLO_FRAME_LEN),
            SDK_FRAME_INITIAL_CAPACITY,
        );
        framed.set_backpressure_boundary(SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY);
        let first = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            result = timeout_at(deadline, framed.next()) => {
                result
                    .map_err(|_| SessionError::HandshakeTimeout)?
                    .ok_or(SessionError::HandshakeClosed)??
            }
        };
        let Frame::Hello = first else {
            return Err(SessionError::ExpectedHello);
        };
        *framed.codec_mut() = FrameCodec::new(self.max_frame_len);

        let (sender, receiver) = mpsc::channel(self.writer_queue_capacity);
        let heartbeat_sender = sender.clone();
        let admitted = {
            let mut state = self.lock_state();
            state
                .add_session(sender, cancellation.clone())
                .ok_or_else(|| {
                    state
                        .session_rejection()
                        .unwrap_or((ErrorCode::Internal, "Gateway rejected the session"))
                })
        };
        let session_id = match admitted {
            Ok(session_id) => session_id,
            Err((code, message)) => {
                // Best effort within the remaining handshake budget (SEC-013);
                // the socket closes either way.
                let rejection = Frame::SessionRejected {
                    code,
                    message: message.to_owned(),
                };
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    _ = timeout_at(deadline, framed.send(rejection)) => {}
                }
                return Err(SessionError::SessionRejected(code));
            }
        };
        let run_inner = Arc::clone(&self);
        let run_cancellation = cancellation.clone();
        let read_cancellation = cancellation.clone();
        run_admitted_session(
            async move {
                #[cfg(test)]
                run_inner.panic_after_admission_if_armed();
                tokio::select! {
                    _ = run_cancellation.cancelled() => return Ok(()),
                    result = timeout_at(deadline, framed.send(Frame::Welcome { session_id })) => {
                        result.map_err(|_| SessionError::HandshakeTimeout)??;
                    }
                }
                drop(handshake_slot);
                let (sink, source) = framed.split();
                let read =
                    run_inner.read_frames(session_id, heartbeat_sender, source, read_cancellation);
                let mut write = pin!(write_frames(receiver, sink, run_cancellation.clone()));
                let read_result = tokio::select! {
                    result = read => result,
                    result = &mut write => return result,
                };
                // Flush frames already queued before the session ended, bounded so a
                // stalled socket cannot hold up shutdown.
                run_cancellation.cancel();
                let _ = timeout(WRITER_FLUSH_TIMEOUT, write).await;
                read_result
            },
            || async {
                cancellation.cancel();
                self.cleanup(session_id).await;
            },
        )
        .await
    }

    async fn read_frames(
        self: Arc<Self>,
        session_id: relaygate_protocol::SessionId,
        sender: mpsc::Sender<SdkWriterItem>,
        mut source: futures_util::stream::SplitStream<Framed<BoxedIo, FrameCodec>>,
        cancellation: CancellationToken,
    ) -> Result<(), SessionError> {
        let mut heartbeat = SessionHeartbeat::new(
            self.heartbeat_idle_interval,
            self.heartbeat_response_timeout,
            session_id,
            0x47,
        );
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                () = sleep_until(heartbeat.next_deadline()) => {
                    let Some(frame) = heartbeat.on_deadline() else {
                        observe_sdk_heartbeat_timeout();
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.session.heartbeat_timeout",
                            session_id = %session_id.as_uuid(),
                            "SDK session heartbeat response timed out"
                        );
                        break;
                    };
                    if sender.try_send(SdkWriterItem::Single(frame)).is_err() {
                        cancellation.cancel();
                        break;
                    }
                    heartbeat.mark_probe_committed();
                }
                frame = source.next() => {
                    let Some(frame) = frame else { break; };
                    let frame = frame?;
                    if let Some(round_trip) = heartbeat.observe_inbound(&frame) {
                        observe_sdk_heartbeat_round_trip(round_trip);
                    }
                    if heartbeat.response_timed_out() {
                        observe_sdk_heartbeat_timeout();
                        tracing::debug!(
                            component = "gateway",
                            event = "gateway.session.heartbeat_timeout",
                            session_id = %session_id.as_uuid(),
                            "SDK session heartbeat response timed out"
                        );
                        break;
                    }
                    let (operation, access_token) = match ControlOperation::take(frame) {
                        Ok(control) => control,
                        Err(frame) => {
                            let actions =
                                self.transition(|state| state.handle(session_id, frame))?;
                            self.send_session_actions(
                                actions,
                                session_id,
                                &sender,
                                &cancellation,
                                heartbeat.next_deadline(),
                            ).await?;
                            continue;
                        }
                    };
                    let early = self.transition(|state| {
                        state.prepare_authorization(
                            session_id,
                            &operation,
                            std::time::Instant::now(),
                        )
                    });
                    if let Some(actions) = early {
                        self.send_session_actions(
                            actions,
                            session_id,
                            &sender,
                            &cancellation,
                            heartbeat.next_deadline(),
                        ).await?;
                        continue;
                    }
                    let mut dial_observation = PendingDialObservation::new(&operation);
                    let verification_started = std::time::Instant::now();
                    let verification_deadline = Instant::now()
                        .checked_add(self.authorization_timeout)
                        .ok_or(SessionError::AuthorizationDeadline)?;
                    let verified = match self.authorization.start(access_token, &operation) {
                        Ok(job) => tokio::select! {
                            _ = cancellation.cancelled() => return Ok(()),
                            result = job.finish(verification_deadline) => result,
                        },
                        Err(code) => Err(code),
                    };
                    let (outcome, code) = match &verified {
                        Ok(_) => ("success", "ok"),
                        Err(code) => ("error", code.metric_name()),
                    };
                    metrics::counter!(
                        "relaygate_gateway_authorization_results_total",
                        "operation" => operation.name(),
                        "outcome" => outcome,
                        "code" => code,
                    ).increment(1);
                    metrics::histogram!(
                        "relaygate_gateway_authorization_duration_seconds",
                        "operation" => operation.name(),
                        "outcome" => outcome,
                    ).record(verification_started.elapsed().as_secs_f64());
                    let actions = self.transition(|state| match verified {
                        Ok(verified) => state.commit_authorized(
                            session_id,
                            operation,
                            verified,
                            std::time::Instant::now(),
                        ),
                        Err(code) => state.authorization_failed(session_id, &operation, code),
                    });
                    dial_observation.disarm();
                    self.send_session_actions(
                        actions,
                        session_id,
                        &sender,
                        &cancellation,
                        heartbeat.next_deadline(),
                    ).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_session_actions(
        self: &Arc<Self>,
        mut actions: Vec<crate::state::GatewayAction>,
        session_id: relaygate_protocol::SessionId,
        sender: &mpsc::Sender<SdkWriterItem>,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SessionError> {
        if admission::is_local_rejection(&actions, session_id)
            && let Some(crate::state::GatewayAction::SendSdkFrame(delivery)) = actions.pop()
        {
            admission::send_rejection(sender, delivery.frame, cancellation, deadline).await?;
        } else {
            self.execute_all(actions).await;
        }
        Ok(())
    }

    async fn cleanup(self: &Arc<Self>, session_id: relaygate_protocol::SessionId) {
        let actions = self.transition(|state| state.remove_session(session_id));
        self.execute_all(actions).await;
    }

    #[cfg(test)]
    fn panic_after_admission_if_armed(&self) {
        if self
            .panic_next_session_after_admission
            .swap(false, Ordering::SeqCst)
        {
            resume_unwind(Box::new("synthetic admitted SDK session panic"));
        }
    }
}

async fn run_admitted_session<Run, Cleanup, CleanupFuture>(
    run: Run,
    cleanup: Cleanup,
) -> Result<(), SessionError>
where
    Run: Future<Output = Result<(), SessionError>>,
    Cleanup: FnOnce() -> CleanupFuture,
    CleanupFuture: Future<Output = ()>,
{
    let result = AssertUnwindSafe(run).catch_unwind().await;
    cleanup().await;
    match result {
        Ok(result) => result,
        Err(payload) => resume_unwind(payload),
    }
}

async fn write_frames(
    mut receiver: mpsc::Receiver<SdkWriterItem>,
    mut sink: futures_util::stream::SplitSink<Framed<BoxedIo, FrameCodec>, Frame>,
    cancellation: CancellationToken,
) -> Result<(), SessionError> {
    loop {
        let item = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            item = receiver.recv() => match item {
                Some(item) => item,
                None => return Ok(()),
            },
        };
        write_item(&mut sink, item).await?;
    }
    receiver.close();
    while let Ok(item) = receiver.try_recv() {
        write_item(&mut sink, item).await?;
    }
    Ok(())
}

async fn write_item(
    sink: &mut futures_util::stream::SplitSink<Framed<BoxedIo, FrameCodec>, Frame>,
    item: SdkWriterItem,
) -> Result<(), SessionError> {
    match item {
        SdkWriterItem::Single(frame) => sink.send(frame).await?,
        SdkWriterItem::TerminalBatch(frames) => {
            for frame in frames {
                sink.send(frame).await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SessionError {
    #[error("SDK session closed before HELLO")]
    HandshakeClosed,
    #[error("SDK HELLO exchange did not finish before the handshake deadline")]
    HandshakeTimeout,
    #[error("first SDK frame was not HELLO")]
    ExpectedHello,
    #[error("SDK session rejected: {}", .0.metric_name())]
    SessionRejected(ErrorCode),
    #[error("SDK admission response could not be queued before the liveness deadline")]
    AdmissionResponseUnavailable,
    #[error("authorization timeout is too large to form a deadline")]
    AuthorizationDeadline,
    #[error(transparent)]
    Protocol(#[from] relaygate_protocol::ProtocolError),
    #[error(transparent)]
    ProtocolViolation(#[from] ProtocolViolation),
}

#[cfg(test)]
mod tests {
    use super::*;
    use relaygate_protocol::{ErrorCode, PeerObservation, PipeId, SessionId};
    use std::error::Error;
    use tokio::io::duplex;

    #[tokio::test]
    async fn writer_flushes_singles_and_terminal_batches_in_wire_order()
    -> Result<(), Box<dyn Error>> {
        let (writer_io, reader_io) = duplex(4 * 1024);
        let writer_io: BoxedIo = Box::new(writer_io);
        let framed = Framed::new(writer_io, FrameCodec::default());
        let (sink, _) = framed.split();
        let mut reader = Framed::new(reader_io, FrameCodec::default());
        let (sender, receiver) = mpsc::channel(3);
        let expected = vec![
            Frame::Ping { nonce: 1 },
            Frame::Reset {
                pipe_id: PipeId::new(SessionId::new(), 2),
                code: ErrorCode::Unavailable,
                message: "PeerTransport was lost".to_owned(),
            },
            Frame::DialFailed {
                connection_id: 3,
                code: ErrorCode::Unavailable,
                observation: PeerObservation::MaybeObserved,
                message: "PeerTransport was lost during remote OPEN".to_owned(),
            },
            Frame::Ping { nonce: 4 },
        ];
        sender
            .send(SdkWriterItem::Single(expected[0].clone()))
            .await?;
        sender
            .send(SdkWriterItem::TerminalBatch(expected[1..3].to_vec()))
            .await?;
        sender
            .send(SdkWriterItem::Single(expected[3].clone()))
            .await?;
        drop(sender);

        let writer = tokio::spawn(write_frames(receiver, sink, CancellationToken::new()));
        for expected in expected {
            let frame = reader.next().await.ok_or("writer closed early")??;
            assert_eq!(frame, expected);
        }
        writer.await??;
        Ok(())
    }
}

#[cfg(test)]
mod observation_tests;
