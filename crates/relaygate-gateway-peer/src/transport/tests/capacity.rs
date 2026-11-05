use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation, SessionId};
use relaygate_route_table::GatewayId;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{actor_for_open, opened_local_stream};
use crate::{
    GatewayPeerConfig,
    event::PeerEvent,
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerOpenProgress, PeerTransportId, RemoteStreamGuard, StreamEndpoint,
        StreamId, StreamIdAllocator,
    },
    transport::{
        ActiveOpenSet, StreamCommand, TransportCloseReason, TransportClosure, TransportCommand,
        TransportNotice, state::TransportActor,
    },
};

struct DropTrackedPayload {
    bytes: [u8; 1],
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for DropTrackedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for DropTrackedPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn tracked_payload(drops: &Arc<AtomicUsize>) -> Bytes {
    Bytes::from_owner(DropTrackedPayload {
        bytes: [1],
        drops: Arc::clone(drops),
    })
}

#[tokio::test]
async fn remote_close_discards_queued_data_and_releases_capacity_without_stalling_sibling()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, new_identity, request) = actor_for_open(1)?;
    actor.config = actor.config.with_resource_limits(2, 4, 4, 1024 * 1024);
    let identities = [
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 1),
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 2),
    ];
    let mut stream_ids = Vec::new();
    for identity in identities {
        let stream_id = actor.allocator.allocate()?;
        assert!(actor.active_opens.reserve(identity)?);
        actor
            .streams
            .insert(stream_id, opened_local_stream(identity)?);
        stream_ids.push(stream_id);
    }
    actor.stream_count.store(2, Ordering::Relaxed);
    assert!(actor.active_opens.reserve(new_identity)?);
    let failure = actor
        .open(request.clone())
        .await
        .err()
        .ok_or("stream limit accepted OPEN")?;
    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert!(!actor.active_opens.contains(new_identity));

    actor
        .aggregate_writer
        .try_send(PeerFrame::Ping { nonce: 1 })?;
    let drops = Arc::new(AtomicUsize::new(0));
    for (stream_id, payload) in [
        (stream_ids[0], tracked_payload(&drops)),
        (stream_ids[1], Bytes::from_static(b"sibling")),
    ] {
        let (reply, result) = oneshot::channel();
        actor
            .handle_command(TransportCommand::Stream {
                stream_id,
                command: StreamCommand::Data { payload },
                reply,
            })
            .await;
        result.await??;
    }
    actor.flush_stream_queues().await;
    assert_eq!(drops.load(Ordering::Relaxed), 0);

    assert!(
        actor
            .handle_frame(PeerFrame::Close {
                stream_id: stream_ids[0]
            })
            .await
    );
    assert!(
        matches!(notices.try_recv()?, TransportNotice::Event(PeerEvent::Close { key }) if key.stream_id() == stream_ids[0])
    );
    assert!(
        matches!(notices.try_recv()?, TransportNotice::StreamEnded { key, open_identity } if key.stream_id() == stream_ids[0] && open_identity == identities[0])
    );
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), 1);
    assert!(!actor.active_opens.contains(identities[0]));
    assert!(actor.active_opens.contains(identities[1]));

    assert!(matches!(frames.try_recv()?, PeerFrame::Ping { nonce: 1 }));
    actor.flush_stream_queues().await;
    assert!(
        matches!(frames.try_recv()?, PeerFrame::Data { stream_id, payload } if stream_id == stream_ids[1] && payload == Bytes::from_static(b"sibling"))
    );
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());

    assert!(actor.active_opens.reserve(new_identity)?);
    let replacement = actor.open(request).await?;
    assert!(
        matches!(frames.try_recv()?, PeerFrame::Open { stream_id, open_identity, .. } if stream_id == replacement.stream_id() && open_identity == new_identity)
    );
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), 2);
    assert!(actor.active_opens.contains(new_identity));
    Ok(())
}

#[tokio::test]
async fn saturated_stream_and_aggregate_buffers_are_released_on_transport_loss()
-> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a")?.with_queue_bounds(4, 4, 4, 1, 1);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let stream_ids = [StreamId::from_raw(0), StreamId::from_raw(2)];
    let identities = [
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 1),
        OpenIdentity::new(GatewayId::new(), SessionId::new(), 2),
    ];
    let active_opens = Arc::new(ActiveOpenSet::default());
    let mut streams = BTreeMap::new();
    for (stream_id, open_identity) in stream_ids.into_iter().zip(identities) {
        assert!(active_opens.reserve(open_identity)?);
        streams.insert(stream_id, opened_local_stream(open_identity)?);
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(1);
    aggregate_writer.try_send(PeerFrame::Data {
        stream_id: stream_ids[0],
        payload: tracked_payload(&drops),
    })?;
    let (notices, _notice_receiver) = mpsc::channel(4);
    let close = CancellationToken::new();
    let stream_count = Arc::new(AtomicUsize::new(2));
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

    for stream_id in stream_ids {
        let (reply, result) = oneshot::channel();
        actor
            .handle_command(TransportCommand::Stream {
                stream_id,
                command: StreamCommand::Data {
                    payload: tracked_payload(&drops),
                },
                reply,
            })
            .await;
        result.await??;
        actor.flush_stream_queues().await;
        assert_eq!(
            actor
                .streams
                .get(&stream_id)
                .ok_or("missing saturated stream")?
                .queued_frames
                .len(),
            1
        );
    }

    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id: stream_ids[0],
            command: StreamCommand::Data {
                payload: Bytes::from_static(b"rejected"),
            },
            reply,
        })
        .await;
    let failure = result
        .await?
        .err()
        .ok_or("expected per-stream capacity failure")?;
    assert_eq!(failure.code(), ErrorCode::ResourceExhausted);
    assert_eq!(failure.observation(), PeerObservation::MaybeObserved);
    assert!(!close.is_cancelled());
    assert_eq!(actor.streams.len(), 2);
    assert_eq!(aggregate_receiver.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);

    let reset_failure = actor
        .send_reset(
            stream_ids[0],
            ErrorCode::Cancelled,
            "transport cleanup".to_owned(),
        )
        .await
        .err()
        .ok_or("expected saturated terminal queue to close the transport")?;
    assert_eq!(reset_failure.code(), ErrorCode::ResourceExhausted);
    assert!(close.is_cancelled());
    assert_eq!(
        actor.closure.failure_reason(),
        Some(TransportCloseReason::WriterFailed)
    );

    let losses = actor.drain_losses();
    assert_eq!(losses.len(), 2);
    for index in 0..2 {
        assert!(losses.iter().any(|loss| {
            loss.key == actor.key(stream_ids[index])
                && loss.open_identity == identities[index]
                && loss.progress == PeerOpenProgress::Opened
        }));
    }
    assert!(actor.streams.is_empty());
    assert!(
        identities
            .iter()
            .all(|identity| !actor.active_opens.contains(*identity))
    );
    assert_eq!(stream_count.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 2);

    drop(losses);
    drop(actor);
    drop(aggregate_receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    Ok(())
}
