use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, SessionId};
use relaygate_route_table::GatewayId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{assert_stream_removed, send_data, send_fin};
use crate::{
    GatewayPeerConfig,
    event::PeerEvent,
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerTransportId, RemoteStreamGuard, StreamEndpoint, StreamId,
        StreamIdAllocator,
    },
    transport::{ActiveOpenSet, TransportClosure, TransportNotice, state::TransportActor},
};

use super::super::{actor_for_open, opened_local_stream};

#[tokio::test]
async fn local_data_and_fin_survive_remote_fin_under_writer_pressure() -> Result<(), Box<dyn Error>>
{
    let (mut actor, mut frames, mut notices, open_identity, request) = actor_for_open(1)?;
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request).await?;
    let stream_id = key.stream_id();

    assert!(actor.handle_frame(PeerFrame::Opened { stream_id }).await);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Opened {
            key: opened_key,
            open_identity: identity,
        })) if opened_key == key && identity == open_identity
    ));

    send_data(
        &mut actor,
        stream_id,
        Bytes::from_static(b"queued before local FIN"),
    )
    .await?;
    send_fin(&mut actor, stream_id).await?;
    assert_eq!(
        actor
            .streams
            .get(&stream_id)
            .ok_or("missing queued stream")?
            .queued_frames
            .len(),
        2
    );

    assert!(actor.handle_frame(PeerFrame::Fin { stream_id }).await);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Fin { key: fin_key })) if fin_key == key
    ));
    let stream = actor
        .streams
        .get(&stream_id)
        .ok_or("remote FIN removed queued local frames")?;
    assert!(stream.terminal_queued);
    assert_eq!(stream.queued_frames.len(), 2);

    assert!(actor.handle_frame(PeerFrame::Fin { stream_id }).await);
    assert!(notices.try_recv().is_err());

    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Open {
            stream_id: committed,
            ..
        }) if committed == stream_id
    ));
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Data {
            stream_id: committed,
            payload,
        }) if committed == stream_id && payload == Bytes::from_static(b"queued before local FIN")
    ));
    assert!(actor.streams.contains_key(&stream_id));

    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Fin {
            stream_id: committed,
        }) if committed == stream_id
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        }) if ended_key == key && identity == open_identity
    ));
    assert_stream_removed(&actor, open_identity);
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn duplicate_fin_and_data_after_fin_are_stream_scoped() -> Result<(), Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-a")?.with_queue_bounds(4, 4, 4, 4, 2);
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
    let (aggregate_writer, mut frames) = mpsc::channel(4);
    let (notice_writer, mut notices) = mpsc::channel(8);
    let stream_count = Arc::new(AtomicUsize::new(2));
    let close = CancellationToken::new();
    let mut actor = TransportActor {
        peer_gateway_id: GatewayId::new(),
        peer_transport_id: PeerTransportId::new(),
        local_endpoint: StreamEndpoint::Dialer,
        remote_endpoint: StreamEndpoint::Acceptor,
        allocator: StreamIdAllocator::new(StreamEndpoint::Dialer),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Acceptor),
        streams,
        ready_streams: Default::default(),
        aggregate_writer,
        notices: notice_writer,
        active_opens,
        stream_count: Arc::clone(&stream_count),
        closure: TransportClosure::new(close.clone()),
        config,
    };

    assert!(
        actor
            .handle_frame(PeerFrame::Fin {
                stream_id: stream_ids[0],
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Fin { key }))
            if key == actor.key(stream_ids[0])
    ));
    assert!(
        actor
            .handle_frame(PeerFrame::Fin {
                stream_id: stream_ids[0],
            })
            .await
    );
    assert!(notices.try_recv().is_err());

    assert!(
        actor
            .handle_frame(PeerFrame::Fin {
                stream_id: stream_ids[1],
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Fin { key }))
            if key == actor.key(stream_ids[1])
    ));
    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id: stream_ids[1],
                payload: Bytes::from_static(b"invalid after FIN"),
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Reset {
            key,
            code: ErrorCode::ProtocolError,
            ..
        })) if key == actor.key(stream_ids[1])
    ));
    assert!(actor.streams.contains_key(&stream_ids[0]));
    assert!(actor.streams.contains_key(&stream_ids[1]));
    assert!(!close.is_cancelled());

    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Reset {
            stream_id,
            code: ErrorCode::ProtocolError,
            ..
        }) if stream_id == stream_ids[1]
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key,
            open_identity,
        }) if key == actor.key(stream_ids[1]) && open_identity == identities[1]
    ));
    assert!(actor.streams.contains_key(&stream_ids[0]));
    assert!(!actor.streams.contains_key(&stream_ids[1]));
    assert_eq!(stream_count.load(Ordering::Relaxed), 1);

    send_fin(&mut actor, stream_ids[0]).await?;
    send_fin(&mut actor, stream_ids[0]).await?;
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Fin { stream_id }) if stream_id == stream_ids[0]
    ));
    assert!(frames.try_recv().is_err());
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key,
            open_identity,
        }) if key == actor.key(stream_ids[0]) && open_identity == identities[0]
    ));
    assert_stream_removed(&actor, identities[0]);
    assert!(!actor.active_opens.contains(identities[1]));
    assert!(!close.is_cancelled());
    assert!(notices.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn local_data_after_remote_close_is_a_no_op() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, open_identity, request) = actor_for_open(4)?;
    assert!(actor.active_opens.reserve(open_identity)?);
    let key = actor.open(request).await?;
    let stream_id = key.stream_id();
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Open { stream_id: opened, .. }) if opened == stream_id
    ));
    assert!(actor.handle_frame(PeerFrame::Opened { stream_id }).await);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Opened { .. }))
    ));

    assert!(actor.handle_frame(PeerFrame::Close { stream_id }).await);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Close { key: closed })) if closed == key
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded { key: ended, .. }) if ended == key
    ));
    assert_stream_removed(&actor, open_identity);

    send_data(
        &mut actor,
        stream_id,
        Bytes::from_static(b"raced the CLOSE"),
    )
    .await?;
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    Ok(())
}
