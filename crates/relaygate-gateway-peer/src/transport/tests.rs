use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use relaygate_protocol::{BindingId, ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::{GatewayId, GatewayLocator};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    ActiveOpenSet, StreamCommand, TransportCloseReason, TransportClosure, TransportCommand,
    TransportHandle, TransportNotice,
    state::{RuntimeStream, StreamOrigin, TransportActor},
};
use crate::{
    GatewayPeerConfig,
    event::{PeerEvent, PeerOpenRequest, PeerTarget},
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerOpenProgress, PeerTransportId, RemoteStreamGuard, StreamEndpoint,
        StreamId, StreamIdAllocator,
    },
    stream::RelayStream,
};

mod capacity;
mod lifecycle;
mod remote;
mod terminal;

type TestActor = (
    TransportActor,
    mpsc::Receiver<PeerFrame>,
    mpsc::Receiver<TransportNotice>,
    OpenIdentity,
    PeerOpenRequest,
);

#[test]
fn external_force_close_preserves_first_failure_reason() {
    let (commands, _receiver) = mpsc::channel(1);
    let close = CancellationToken::new();
    let closure = TransportClosure::new(close.clone());
    let handle = TransportHandle {
        peer_gateway_id: GatewayId::new(),
        peer_transport_id: PeerTransportId::new(),
        commands,
        closure: closure.clone(),
    };

    handle.force_close(TransportCloseReason::WriterFailed);
    handle.force_close(TransportCloseReason::ProtocolError);
    assert!(close.is_cancelled());
    assert_eq!(
        closure.failure_reason(),
        Some(TransportCloseReason::WriterFailed)
    );

    let (commands, _receiver) = mpsc::channel(1);
    let close = CancellationToken::new();
    let closure = TransportClosure::new(close.clone());
    let handle = TransportHandle {
        peer_gateway_id: GatewayId::new(),
        peer_transport_id: PeerTransportId::new(),
        commands,
        closure: closure.clone(),
    };
    handle.close();
    assert!(close.is_cancelled());
    assert_eq!(closure.failure_reason(), None);
}

pub(super) fn actor_for_open(writer_capacity: usize) -> Result<TestActor, Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a")?
        .with_queue_bounds(4, 4, 4, writer_capacity, 2)
        .with_timeouts(
            std::time::Duration::from_millis(500),
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(1),
        );
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 1);
    let request = PeerOpenRequest::new(
        PeerTarget::new(
            peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        open_identity,
        crate::test_support::destination("echo.b"),
        SessionId::new(),
        BindingId::new(),
    );
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(writer_capacity);
    let (notices, notice_receiver) = mpsc::channel(4);
    let active_opens = Arc::new(ActiveOpenSet::default());
    let actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams: BTreeMap::new(),
        ready_streams: Default::default(),
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::new(AtomicUsize::new(0)),
        closure: TransportClosure::new(CancellationToken::new()),
        config,
    };
    Ok((
        actor,
        aggregate_receiver,
        notice_receiver,
        open_identity,
        request,
    ))
}

fn opened_local_stream(open_identity: OpenIdentity) -> Result<RuntimeStream, Box<dyn Error>> {
    let mut relay = RelayStream::owned_opening(open_identity);
    relay.opened()?;
    Ok(RuntimeStream {
        relay,
        queued_frames: VecDeque::new(),
        terminal_queued: false,
        progress: PeerOpenProgress::Opened,
        origin: StreamOrigin::Local,
        open_deadline: None,
    })
}

#[tokio::test]
async fn sparse_streams_preserve_fifo_and_ascending_passes_after_writer_pressure()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, _, _) = actor_for_open(4)?;
    for raw in (0..128).step_by(2) {
        let identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), raw);
        assert!(actor.active_opens.reserve(identity)?);
        actor
            .streams
            .insert(StreamId::from_raw(raw), opened_local_stream(identity)?);
    }
    actor.stream_count.store(64, Ordering::Relaxed);

    // Arrival order differs from stream order; most streams have no DATA.
    for sequence in 0..2 {
        for raw in [126, 62, 2] {
            let (reply, result) = oneshot::channel();
            actor
                .handle_command(TransportCommand::Stream {
                    stream_id: StreamId::from_raw(raw),
                    command: StreamCommand::Data {
                        payload: Bytes::from(vec![sequence]),
                    },
                    reply,
                })
                .await;
            result.await??;
        }
    }

    actor.flush_stream_queues().await;
    // A second flush while still full must not lose or duplicate the front.
    actor.flush_stream_queues().await;
    for (raw, sequence) in [(2, 0), (62, 0), (126, 0), (2, 1)] {
        assert!(matches!(
            frames.try_recv()?,
            PeerFrame::Data { stream_id, payload }
                if stream_id == StreamId::from_raw(raw) && payload.as_ref() == [sequence]
        ));
    }
    assert!(frames.try_recv().is_err());

    // The drained low-ID stream becomes ready again before the next pass.
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id: StreamId::from_raw(2),
            command: StreamCommand::Data {
                payload: Bytes::from_static(&[2]),
            },
            reply,
        })
        .await;
    result.await??;
    actor.flush_stream_queues().await;
    for (raw, sequence) in [(2, 2), (62, 1), (126, 1)] {
        assert!(matches!(
            frames.try_recv()?,
            PeerFrame::Data { stream_id, payload }
                if stream_id == StreamId::from_raw(raw) && payload.as_ref() == [sequence]
        ));
    }
    actor.flush_stream_queues().await;
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), 64);
    assert!(!actor.closure.token().is_cancelled());
    Ok(())
}

#[tokio::test]
async fn terminal_notice_backpressure_does_not_interrupt_the_current_stream_pass()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, _, _) = actor_for_open(4)?;
    let identities = [
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 1),
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 2),
    ];
    for (raw, identity) in [0, 2].into_iter().zip(identities) {
        assert!(actor.active_opens.reserve(identity)?);
        actor
            .streams
            .insert(StreamId::from_raw(raw), opened_local_stream(identity)?);
    }
    actor.stream_count.store(2, Ordering::Relaxed);
    for _ in 0..4 {
        actor.notices.try_send(TransportNotice::AttemptEnded {
            open_identity: identities[0],
        })?;
    }
    for (raw, command) in [
        (0, StreamCommand::Close),
        (
            2,
            StreamCommand::Data {
                payload: Bytes::from_static(b"sibling"),
            },
        ),
    ] {
        let (reply, result) = oneshot::channel();
        actor
            .handle_command(TransportCommand::Stream {
                stream_id: StreamId::from_raw(raw),
                command,
                reply,
            })
            .await;
        result.await??;
    }
    {
        let flush = actor.flush_stream_queues();
        tokio::pin!(flush);
        assert!(futures_util::poll!(flush.as_mut()).is_pending());
        assert!(matches!(
            frames.try_recv()?,
            PeerFrame::Close { stream_id } if stream_id == StreamId::from_raw(0)
        ));
        assert!(matches!(
            frames.try_recv()?,
            PeerFrame::Data { stream_id, payload }
                if stream_id == StreamId::from_raw(2) && payload == Bytes::from_static(b"sibling")
        ));
        assert!(frames.try_recv().is_err());
        let _ = notices.try_recv()?;
        flush.await;
    }
    for _ in 0..3 {
        assert!(matches!(
            notices.try_recv()?,
            TransportNotice::AttemptEnded { .. }
        ));
    }
    assert!(matches!(
        notices.try_recv()?,
        TransportNotice::StreamEnded { key, open_identity }
            if key.stream_id() == StreamId::from_raw(0) && open_identity == identities[0]
    ));
    assert!(notices.try_recv().is_err());
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), 1);
    assert!(!actor.active_opens.contains(identities[0]));
    assert!(actor.active_opens.contains(identities[1]));
    Ok(())
}

#[tokio::test]
async fn open_rejected_before_writer_commit_leaves_no_stream_state() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, _notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    actor.aggregate_writer.try_send(PeerFrame::Close {
        stream_id: StreamId::from_raw(99),
    })?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let failure = actor
        .open(request)
        .await
        .err()
        .ok_or("expected OPEN pre-commit rejection")?;

    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert!(actor.streams.is_empty());
    assert_eq!(
        actor
            .stream_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!actor.active_opens.contains(open_identity));
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Close { stream_id }) if stream_id == StreamId::from_raw(99)
    ));
    Ok(())
}

#[tokio::test]
async fn closed_writer_rejects_open_before_commit() -> Result<(), Box<dyn Error>> {
    let (mut actor, aggregate_receiver, _notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    drop(aggregate_receiver);
    assert!(actor.active_opens.reserve(open_identity)?);

    let failure = actor
        .open(request)
        .await
        .err()
        .ok_or("expected closed writer OPEN rejection")?;

    assert_eq!(failure.code(), ErrorCode::Unavailable);
    assert_eq!(failure.observation(), PeerObservation::NotObserved);
    assert!(actor.streams.is_empty());
    assert!(!actor.active_opens.contains(open_identity));
    Ok(())
}

#[tokio::test]
async fn committed_open_timeout_is_reported_as_maybe_observed() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(4)?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let key = actor.open(request).await?;
    let committed = aggregate_receiver
        .recv()
        .await
        .ok_or("expected committed OPEN frame")?;
    assert!(matches!(
        committed,
        PeerFrame::Open {
            stream_id,
            open_identity: identity,
            ..
        } if stream_id == key.stream_id() && identity == open_identity
    ));
    assert!(actor.active_opens.contains(open_identity));
    let timeout_at = actor
        .next_open_deadline()
        .ok_or("expected OPEN response deadline")?;

    actor.expire_open_deadlines_at(timeout_at).await;
    let failure_notice = notice_receiver
        .recv()
        .await
        .ok_or("expected timeout failure event")?;
    assert!(matches!(
        failure_notice,
        TransportNotice::Event(PeerEvent::Failed {
            key: failed_key,
            open_identity: identity,
            failure,
        }) if failed_key == key
            && identity == open_identity
            && failure.code() == ErrorCode::DeadlineExceeded
            && failure.observation() == PeerObservation::MaybeObserved
    ));

    actor.flush_stream_queues().await;
    let reset = aggregate_receiver
        .recv()
        .await
        .ok_or("expected timeout RESET frame")?;
    assert!(matches!(
        reset,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::DeadlineExceeded,
            ..
        } if stream_id == key.stream_id()
    ));
    let ended = notice_receiver
        .recv()
        .await
        .ok_or("expected stream cleanup notice")?;
    assert!(matches!(
        ended,
        TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        } if ended_key == key && identity == open_identity
    ));
    assert!(actor.streams.is_empty());
    assert!(!actor.active_opens.contains(open_identity));
    let (cancel_reply, cancel_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Cancel {
            open_identity,
            reply: cancel_reply,
        })
        .await;
    cancel_result.await??;
    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    actor.flush_stream_queues().await;
    assert!(aggregate_receiver.try_recv().is_err());
    assert!(notice_receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cancel_before_open_deadline_keeps_cancelled_as_the_only_terminal_cause()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);

    let key = actor.open(request).await?;
    let timeout_at = actor
        .next_open_deadline()
        .ok_or("expected OPEN response deadline")?;
    let (cancel_reply, cancel_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Cancel {
            open_identity,
            reply: cancel_reply,
        })
        .await;
    cancel_result.await??;
    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    actor.expire_open_deadlines_at(timeout_at).await;
    assert!(
        notice_receiver.try_recv().is_err(),
        "cancelled OPEN emitted a second deadline terminal event"
    );

    let committed = aggregate_receiver
        .recv()
        .await
        .ok_or("expected committed OPEN frame")?;
    assert!(matches!(
        committed,
        PeerFrame::Open {
            stream_id,
            open_identity: identity,
            ..
        } if stream_id == key.stream_id() && identity == open_identity
    ));
    actor.flush_stream_queues().await;
    let reset = aggregate_receiver
        .recv()
        .await
        .ok_or("expected cancellation RESET frame")?;
    assert!(matches!(
        reset,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            ..
        } if stream_id == key.stream_id()
    ));
    let ended = notice_receiver
        .recv()
        .await
        .ok_or("expected stream cleanup notice")?;
    assert!(matches!(
        ended,
        TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        } if ended_key == key && identity == open_identity
    ));
    assert!(aggregate_receiver.try_recv().is_err());
    assert!(notice_receiver.try_recv().is_err());
    assert!(actor.streams.is_empty());
    assert_eq!(
        actor
            .stream_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!actor.active_opens.contains(open_identity));
    Ok(())
}

#[tokio::test]
async fn committed_open_preserves_order_while_writer_is_stalled() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request).await?;

    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    let _ = notice_receiver
        .recv()
        .await
        .ok_or("expected OPENED event")?;
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id: key.stream_id(),
            command: StreamCommand::Data {
                payload: Bytes::from_static(b"after-opened"),
            },
            reply,
        })
        .await;
    result.await??;

    actor.flush_stream_queues().await;
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Open { stream_id, .. }) if stream_id == key.stream_id()
    ));
    assert!(aggregate_receiver.try_recv().is_err());

    actor.flush_stream_queues().await;
    let data = aggregate_receiver
        .recv()
        .await
        .ok_or("expected DATA after committed OPEN")?;
    assert!(matches!(
        data,
        PeerFrame::Data { stream_id, payload }
            if stream_id == key.stream_id() && payload == Bytes::from_static(b"after-opened")
    ));
    Ok(())
}

#[tokio::test]
async fn data_precedes_terminal_fin_while_writer_is_stalled() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, mut notice_receiver, open_identity, request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request).await?;

    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: key.stream_id()
            })
            .await
    );
    let opened = notice_receiver
        .recv()
        .await
        .ok_or("expected OPENED event")?;
    assert!(matches!(
        opened,
        TransportNotice::Event(PeerEvent::Opened {
            key: opened_key,
            open_identity: identity,
        }) if opened_key == key && identity == open_identity
    ));
    assert!(
        actor
            .handle_frame(PeerFrame::Fin {
                stream_id: key.stream_id()
            })
            .await
    );
    let remote_fin = notice_receiver
        .recv()
        .await
        .ok_or("expected remote FIN event")?;
    assert!(matches!(
        remote_fin,
        TransportNotice::Event(PeerEvent::Fin { key: fin_key }) if fin_key == key
    ));

    let (data_reply, data_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id: key.stream_id(),
            command: StreamCommand::Data {
                payload: Bytes::from_static(b"echo-before-fin"),
            },
            reply: data_reply,
        })
        .await;
    data_result.await??;
    actor.flush_stream_queues().await;

    let (fin_reply, fin_result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id: key.stream_id(),
            command: StreamCommand::Fin,
            reply: fin_reply,
        })
        .await;
    fin_result.await??;
    actor.flush_stream_queues().await;

    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Open { stream_id, .. }) if stream_id == key.stream_id()
    ));
    actor.flush_stream_queues().await;
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Data { stream_id, payload })
            if stream_id == key.stream_id() && payload == Bytes::from_static(b"echo-before-fin")
    ));
    assert!(actor.streams.contains_key(&key.stream_id()));

    actor.flush_stream_queues().await;
    assert!(matches!(
        aggregate_receiver.try_recv(),
        Ok(PeerFrame::Fin { stream_id }) if stream_id == key.stream_id()
    ));
    assert!(actor.streams.is_empty());
    assert!(matches!(
        notice_receiver.try_recv(),
        Ok(TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        }) if ended_key == key && identity == open_identity
    ));
    Ok(())
}

#[tokio::test]
async fn writer_pressure_orders_multiple_opens_and_never_reuses_failed_counter()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut aggregate_receiver, _notice_receiver, first_identity, first_request) =
        actor_for_open(1)?;
    assert!(actor.active_opens.reserve(first_identity)?);
    let first_key = actor.open(first_request).await?;
    assert_eq!(first_key.stream_id().raw(), 0);

    let second_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 2);
    let second_request = PeerOpenRequest::new(
        PeerTarget::new(
            actor.peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        second_identity,
        crate::test_support::destination("echo.b"),
        SessionId::new(),
        BindingId::new(),
    );
    assert!(actor.active_opens.reserve(second_identity)?);
    let second_failure = actor
        .open(second_request)
        .await
        .err()
        .ok_or("expected the second OPEN to fail while the first commit fills the writer")?;
    assert_eq!(second_failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(second_failure.observation(), PeerObservation::NotObserved);
    assert!(!actor.active_opens.contains(second_identity));

    let first_frame = aggregate_receiver
        .recv()
        .await
        .ok_or("expected the first committed OPEN")?;
    assert!(matches!(
        first_frame,
        PeerFrame::Open {
            stream_id,
            open_identity,
            ..
        } if stream_id == first_key.stream_id() && open_identity == first_identity
    ));

    let third_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 3);
    let third_request = PeerOpenRequest::new(
        PeerTarget::new(
            actor.peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        third_identity,
        crate::test_support::destination("echo.b"),
        SessionId::new(),
        BindingId::new(),
    );
    assert!(actor.active_opens.reserve(third_identity)?);
    let third_key = actor.open(third_request).await?;
    assert_eq!(
        third_key.stream_id().raw(),
        4,
        "the failed second OPEN must consume counter 1 instead of reusing it"
    );
    let third_frame = aggregate_receiver
        .recv()
        .await
        .ok_or("expected the third committed OPEN")?;
    assert!(matches!(
        third_frame,
        PeerFrame::Open {
            stream_id,
            open_identity,
            ..
        } if stream_id == third_key.stream_id() && open_identity == third_identity
    ));
    assert!(aggregate_receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cleanup_reset_is_stream_scoped_until_commit_failure_closes_transport()
-> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a")?.with_queue_bounds(4, 4, 4, 4, 1);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let stream_ids = [
        StreamId::from_raw(0),
        StreamId::from_raw(2),
        StreamId::from_raw(4),
    ];
    let identities = [
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 1),
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 2),
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 3),
    ];
    let active_opens = Arc::new(ActiveOpenSet::default());
    let mut streams = BTreeMap::new();
    for (stream_id, open_identity) in stream_ids.into_iter().zip(identities) {
        assert!(active_opens.reserve(open_identity)?);
        streams.insert(stream_id, opened_local_stream(open_identity)?);
    }
    let (aggregate_writer, mut aggregate_receiver) = mpsc::channel(3);
    let (notices, mut notice_receiver) = mpsc::channel(4);
    let close = CancellationToken::new();
    let stream_count = Arc::new(AtomicUsize::new(3));
    let mut actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams,
        ready_streams: Default::default(),
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::clone(&stream_count),
        closure: TransportClosure::new(close.clone()),
        config,
    };

    actor
        .send_reset(
            stream_ids[0],
            ErrorCode::Cancelled,
            "dialer session closed".to_owned(),
        )
        .await?;
    actor.flush_stream_queues().await;

    let reset = aggregate_receiver
        .recv()
        .await
        .ok_or("expected committed RESET frame")?;
    assert!(matches!(
        reset,
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            ..
        } if stream_id == stream_ids[0]
    ));
    let ended = notice_receiver
        .recv()
        .await
        .ok_or("expected target stream cleanup notice")?;
    assert!(matches!(
        ended,
        TransportNotice::StreamEnded {
            key,
            open_identity,
        } if key == actor.key(stream_ids[0]) && open_identity == identities[0]
    ));
    assert!(!close.is_cancelled());
    assert!(!actor.streams.contains_key(&stream_ids[0]));
    assert!(actor.streams.contains_key(&stream_ids[1]));
    assert!(actor.streams.contains_key(&stream_ids[2]));
    assert!(!actor.active_opens.contains(identities[0]));
    assert!(actor.active_opens.contains(identities[1]));
    assert!(actor.active_opens.contains(identities[2]));
    assert_eq!(stream_count.load(Ordering::Relaxed), 2);
    assert!(aggregate_receiver.try_recv().is_err());

    actor
        .streams
        .get_mut(&stream_ids[1])
        .ok_or("missing sibling stream")?
        .queued_frames
        .push_back(PeerFrame::Data {
            stream_id: stream_ids[1],
            payload: Bytes::from_static(b"already queued"),
        });

    let failure = actor
        .send_reset(
            stream_ids[1],
            ErrorCode::Cancelled,
            "dialer session closed".to_owned(),
        )
        .await
        .err()
        .ok_or("expected saturated RESET queue failure")?;
    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert!(close.is_cancelled());
    assert_eq!(
        actor.closure.failure_reason(),
        Some(TransportCloseReason::WriterFailed)
    );
    let losses = actor.drain_losses();
    assert_eq!(losses.len(), 2);
    for index in [1, 2] {
        assert!(losses.iter().any(|loss| {
            loss.key == actor.key(stream_ids[index]) && loss.open_identity == identities[index]
        }));
    }
    assert!(actor.streams.is_empty());
    assert!(
        identities
            .iter()
            .all(|identity| !actor.active_opens.contains(*identity))
    );
    assert_eq!(stream_count.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_frame_during_local_opening_emits_failed_not_reset() -> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a")?.with_queue_bounds(4, 4, 4, 4, 2);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let stream_id = StreamId::from_raw(0);
    let open_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 9);
    let active_opens = Arc::new(ActiveOpenSet::default());
    assert!(active_opens.reserve(open_identity)?);
    let streams = BTreeMap::from([(
        stream_id,
        RuntimeStream {
            relay: RelayStream::owned_opening(open_identity),
            queued_frames: VecDeque::new(),
            terminal_queued: false,
            progress: PeerOpenProgress::AfterOpenCommit,
            origin: StreamOrigin::Local,
            open_deadline: None,
        },
    )]);
    let (aggregate_writer, _aggregate_receiver) = mpsc::channel(2);
    let (notices, mut notice_receiver) = mpsc::channel(4);
    let close = CancellationToken::new();
    let mut actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams,
        ready_streams: Default::default(),
        aggregate_writer,
        notices,
        active_opens,
        stream_count: Arc::new(AtomicUsize::new(1)),
        closure: TransportClosure::new(close),
        config,
    };

    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id,
                payload: Bytes::from_static(b"invalid before OPENED"),
            })
            .await
    );
    let notice = notice_receiver
        .recv()
        .await
        .ok_or("expected local OPEN terminal event")?;
    assert!(matches!(
        notice,
        TransportNotice::Event(PeerEvent::Failed {
            key: _,
            open_identity: identity,
            failure,
        }) if identity == open_identity
            && failure.code() == ErrorCode::ProtocolError
            && failure.observation() == PeerObservation::MaybeObserved
    ));
    Ok(())
}
