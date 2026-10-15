use std::{
    collections::{BTreeMap, BTreeSet},
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

use crate::{
    GatewayPeerConfig,
    event::{PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey, PeerTarget},
    frame::PeerFrame,
    identity::{
        OpenIdentity, PeerTransportId, RemoteStreamGuard, StreamEndpoint, StreamId,
        StreamIdAllocator,
    },
    transport::{
        ActiveOpenSet, StreamCommand, TransportClosure, TransportCommand, TransportNotice,
        state::TransportActor,
    },
};

fn late_stream_frames(stream_id: StreamId) -> [PeerFrame; 6] {
    [
        PeerFrame::Opened { stream_id },
        PeerFrame::Failed {
            stream_id,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::MaybeObserved,
            message: "late FAILED".to_owned(),
        },
        PeerFrame::Data {
            stream_id,
            payload: Bytes::from_static(b"late DATA"),
        },
        PeerFrame::Fin { stream_id },
        PeerFrame::Close { stream_id },
        PeerFrame::Reset {
            stream_id,
            code: ErrorCode::Cancelled,
            message: "late RESET".to_owned(),
        },
    ]
}

type RemoteActor = (
    TransportActor,
    mpsc::Receiver<PeerFrame>,
    mpsc::Receiver<TransportNotice>,
    GatewayId,
);

fn actor_for_remote_open() -> Result<RemoteActor, Box<dyn Error>> {
    let config = GatewayPeerConfig::new("gateway-b")?.with_queue_bounds(16, 16, 16, 16, 8);
    let peer_gateway_id = GatewayId::new();
    let peer_transport_id = PeerTransportId::new();
    let (aggregate_writer, aggregate_receiver) = mpsc::channel(16);
    let (notices, notice_receiver) = mpsc::channel(16);
    let actor = TransportActor {
        peer_gateway_id,
        peer_transport_id,
        local_endpoint: StreamEndpoint::Acceptor,
        remote_endpoint: StreamEndpoint::Dialer,
        allocator: StreamIdAllocator::new(StreamEndpoint::Acceptor),
        remote_guard: RemoteStreamGuard::new(StreamEndpoint::Dialer),
        streams: BTreeMap::new(),
        ready_streams: Default::default(),
        aggregate_writer,
        notices,
        active_opens: Arc::new(ActiveOpenSet::default()),
        stream_count: Arc::new(AtomicUsize::new(0)),
        closure: TransportClosure::new(CancellationToken::new()),
        config,
    };
    Ok((actor, aggregate_receiver, notice_receiver, peer_gateway_id))
}

async fn receive_remote_open(
    actor: &mut TransportActor,
    notices: &mut mpsc::Receiver<TransportNotice>,
    stream_id: StreamId,
    open_identity: OpenIdentity,
) -> Result<PeerStreamKey, Box<dyn Error>> {
    let relay_session_id = SessionId::new();
    let binding_id = BindingId::new();
    assert!(
        actor
            .handle_frame(PeerFrame::Open {
                stream_id,
                open_identity,
                destination: crate::test_support::destination("echo.remote"),
                relay_session_id,
                binding_id,
            })
            .await
    );
    let key = actor.key(stream_id);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::IncomingOpen {
            key: incoming_key,
            open_identity: identity,
            destination,
            relay_session_id: session_id,
            binding_id: incoming_binding_id,
            ..
        })) if incoming_key == key
            && identity == open_identity
            && destination == crate::test_support::destination("echo.remote")
            && session_id == relay_session_id
            && incoming_binding_id == binding_id
    ));
    Ok(key)
}

async fn send_opened(
    actor: &mut TransportActor,
    stream_id: StreamId,
) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id,
            command: StreamCommand::Opened,
            reply,
        })
        .await;
    result.await??;
    Ok(())
}

async fn send_failed(
    actor: &mut TransportActor,
    stream_id: StreamId,
    failure: PeerFailure,
) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id,
            command: StreamCommand::Failed { failure },
            reply,
        })
        .await;
    result.await??;
    Ok(())
}

async fn send_data(
    actor: &mut TransportActor,
    stream_id: StreamId,
    payload: Bytes,
) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id,
            command: StreamCommand::Data { payload },
            reply,
        })
        .await;
    result.await??;
    Ok(())
}

async fn send_fin(actor: &mut TransportActor, stream_id: StreamId) -> Result<(), Box<dyn Error>> {
    let (reply, result) = oneshot::channel();
    actor
        .handle_command(TransportCommand::Stream {
            stream_id,
            command: StreamCommand::Fin,
            reply,
        })
        .await;
    result.await??;
    Ok(())
}

fn assert_streams(
    actor: &TransportActor,
    expected: &[StreamId],
    identities: &[(StreamId, OpenIdentity)],
) {
    assert_eq!(actor.streams.keys().copied().collect::<Vec<_>>(), expected);
    assert_eq!(actor.stream_count.load(Ordering::Relaxed), expected.len());
    for (stream_id, open_identity) in identities {
        assert_eq!(
            actor.active_opens.contains(*open_identity),
            expected.contains(stream_id)
        );
    }
}

#[tokio::test]
async fn endpoint_bits_isolate_remote_open_replay() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, peer_gateway_id) = actor_for_remote_open()?;
    let local_identity = OpenIdentity::new(GatewayId::new(), SessionId::new(), 1);
    let local_request = PeerOpenRequest::new(
        PeerTarget::new(
            peer_gateway_id,
            GatewayLocator::new("127.0.0.1:9999".to_owned())?,
        ),
        local_identity,
        crate::test_support::destination("echo.local"),
        SessionId::new(),
        BindingId::new(),
    );
    assert!(actor.active_opens.reserve(local_identity)?);
    let local_key = actor.open(local_request).await?;
    let local_stream_id = StreamId::from_raw(1);
    assert_eq!(local_key.stream_id(), local_stream_id);
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Open {
            stream_id,
            open_identity,
            ..
        }) if stream_id == local_stream_id && open_identity == local_identity
    ));

    let remote_stream_id = StreamId::from_raw(0);
    let remote_identity = OpenIdentity::new(peer_gateway_id, SessionId::new(), 2);
    let remote_relay_session_id = SessionId::new();
    let remote_binding_id = BindingId::new();
    let remote_open = PeerFrame::Open {
        stream_id: remote_stream_id,
        open_identity: remote_identity,
        destination: crate::test_support::destination("echo.remote"),
        relay_session_id: remote_relay_session_id,
        binding_id: remote_binding_id,
    };
    assert!(actor.handle_frame(remote_open.clone()).await);
    let remote_key = actor.key(remote_stream_id);
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::IncomingOpen {
            key,
            open_identity,
            destination,
            relay_session_id,
            binding_id,
            ..
        })) if key == remote_key
            && open_identity == remote_identity
            && destination == crate::test_support::destination("echo.remote")
            && relay_session_id == remote_relay_session_id
            && binding_id == remote_binding_id
    ));
    assert_streams(
        &actor,
        &[remote_stream_id, local_stream_id],
        &[
            (remote_stream_id, remote_identity),
            (local_stream_id, local_identity),
        ],
    );

    send_failed(
        &mut actor,
        remote_stream_id,
        PeerFailure::not_observed(ErrorCode::Unavailable, "remote OPEN failed"),
    )
    .await?;
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Failed {
            stream_id,
            code: ErrorCode::Unavailable,
            observation: PeerObservation::NotObserved,
            message,
        }) if stream_id == remote_stream_id && message == "remote OPEN failed"
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key,
            open_identity,
        }) if key == remote_key && open_identity == remote_identity
    ));
    assert_streams(
        &actor,
        &[local_stream_id],
        &[
            (remote_stream_id, remote_identity),
            (local_stream_id, local_identity),
        ],
    );

    assert!(actor.handle_frame(remote_open).await);
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Failed {
            stream_id,
            code: ErrorCode::ProtocolError,
            observation: PeerObservation::NotObserved,
            ..
        }) if stream_id == remote_stream_id
    ));
    assert_streams(
        &actor,
        &[local_stream_id],
        &[
            (remote_stream_id, remote_identity),
            (local_stream_id, local_identity),
        ],
    );
    assert!(!actor.closure.token().is_cancelled());
    assert!(notices.try_recv().is_err());

    assert!(
        actor
            .handle_frame(PeerFrame::Opened {
                stream_id: local_stream_id,
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Opened {
            key,
            open_identity,
        })) if key == local_key && open_identity == local_identity
    ));
    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id: local_stream_id,
                payload: Bytes::from_static(b"local stream survived replay"),
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Data { key, payload }))
            if key == local_key && payload == Bytes::from_static(b"local stream survived replay")
    ));

    let fresh_remote_stream_id = StreamId::from_raw(2);
    let fresh_remote_identity = OpenIdentity::new(peer_gateway_id, SessionId::new(), 3);
    let fresh_remote_key = receive_remote_open(
        &mut actor,
        &mut notices,
        fresh_remote_stream_id,
        fresh_remote_identity,
    )
    .await?;
    assert_eq!(fresh_remote_key.stream_id(), fresh_remote_stream_id);
    assert_streams(
        &actor,
        &[local_stream_id, fresh_remote_stream_id],
        &[
            (remote_stream_id, remote_identity),
            (local_stream_id, local_identity),
            (fresh_remote_stream_id, fresh_remote_identity),
        ],
    );
    assert!(!actor.closure.token().is_cancelled());
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn remote_failed_commit_cleans_stream_without_resurrection() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, peer_gateway_id) = actor_for_remote_open()?;
    let stream_id = StreamId::from_raw(0);
    let open_identity = OpenIdentity::new(peer_gateway_id, SessionId::new(), 1);
    let key = receive_remote_open(&mut actor, &mut notices, stream_id, open_identity).await?;
    assert_streams(&actor, &[stream_id], &[(stream_id, open_identity)]);

    send_failed(
        &mut actor,
        stream_id,
        PeerFailure::not_observed(ErrorCode::PermissionDenied, "listener rejected OPEN"),
    )
    .await?;
    assert_streams(&actor, &[stream_id], &[(stream_id, open_identity)]);

    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Failed {
            stream_id: failed_stream_id,
            code: ErrorCode::PermissionDenied,
            observation: PeerObservation::NotObserved,
            message,
        }) if failed_stream_id == stream_id && message == "listener rejected OPEN"
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key: ended_key,
            open_identity: identity,
        }) if ended_key == key && identity == open_identity
    ));
    assert_streams(&actor, &[], &[(stream_id, open_identity)]);

    for frame in late_stream_frames(stream_id) {
        assert!(actor.handle_frame(frame).await);
    }
    actor.flush_stream_queues().await;
    assert_streams(&actor, &[], &[(stream_id, open_identity)]);
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn remote_half_close_preserves_sibling_until_bilateral_fin() -> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, peer_gateway_id) = actor_for_remote_open()?;
    let target = StreamId::from_raw(0);
    let sibling = StreamId::from_raw(2);
    let target_identity = OpenIdentity::new(peer_gateway_id, SessionId::new(), 1);
    let sibling_identity = OpenIdentity::new(peer_gateway_id, SessionId::new(), 2);
    let target_key = receive_remote_open(&mut actor, &mut notices, target, target_identity).await?;
    let sibling_key =
        receive_remote_open(&mut actor, &mut notices, sibling, sibling_identity).await?;

    send_opened(&mut actor, target).await?;
    send_opened(&mut actor, sibling).await?;
    actor.flush_stream_queues().await;
    let mut opened = BTreeSet::new();
    for _ in 0..2 {
        let Some(PeerFrame::Opened { stream_id }) = frames.recv().await else {
            return Err("expected committed OPENED frame".into());
        };
        opened.insert(stream_id);
    }
    assert_eq!(opened, BTreeSet::from([target, sibling]));
    assert_streams(
        &actor,
        &[target, sibling],
        &[(target, target_identity), (sibling, sibling_identity)],
    );

    assert!(
        actor
            .handle_frame(PeerFrame::Fin { stream_id: target })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Fin { key })) if key == target_key
    ));
    send_data(
        &mut actor,
        target,
        Bytes::from_static(b"target opposite direction after FIN"),
    )
    .await?;
    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id: sibling,
                payload: Bytes::from_static(b"sibling remote direction"),
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Data { key, payload }))
            if key == sibling_key && payload == Bytes::from_static(b"sibling remote direction")
    ));
    send_data(
        &mut actor,
        sibling,
        Bytes::from_static(b"sibling local direction"),
    )
    .await?;
    actor.flush_stream_queues().await;
    let mut data = BTreeMap::new();
    for _ in 0..2 {
        let Some(PeerFrame::Data { stream_id, payload }) = frames.recv().await else {
            return Err("expected committed DATA frame".into());
        };
        data.insert(stream_id, payload);
    }
    assert_eq!(
        data,
        BTreeMap::from([
            (
                target,
                Bytes::from_static(b"target opposite direction after FIN")
            ),
            (sibling, Bytes::from_static(b"sibling local direction")),
        ])
    );

    send_fin(&mut actor, target).await?;
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Fin { stream_id }) if stream_id == target
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key,
            open_identity,
        }) if key == target_key && open_identity == target_identity
    ));
    assert_streams(
        &actor,
        &[sibling],
        &[(target, target_identity), (sibling, sibling_identity)],
    );

    assert!(
        actor
            .handle_frame(PeerFrame::Data {
                stream_id: sibling,
                payload: Bytes::from_static(b"sibling survives target close"),
            })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Data { key, payload }))
            if key == sibling_key
                && payload == Bytes::from_static(b"sibling survives target close")
    ));
    send_data(
        &mut actor,
        sibling,
        Bytes::from_static(b"sibling still sends"),
    )
    .await?;
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Data { stream_id, payload })
            if stream_id == sibling && payload == Bytes::from_static(b"sibling still sends")
    ));

    assert!(
        actor
            .handle_frame(PeerFrame::Fin { stream_id: sibling })
            .await
    );
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::Event(PeerEvent::Fin { key })) if key == sibling_key
    ));
    send_fin(&mut actor, sibling).await?;
    actor.flush_stream_queues().await;
    assert!(matches!(
        frames.recv().await,
        Some(PeerFrame::Fin { stream_id }) if stream_id == sibling
    ));
    assert!(matches!(
        notices.recv().await,
        Some(TransportNotice::StreamEnded {
            key,
            open_identity,
        }) if key == sibling_key && open_identity == sibling_identity
    ));
    assert_streams(
        &actor,
        &[],
        &[(target, target_identity), (sibling, sibling_identity)],
    );
    assert!(frames.try_recv().is_err());
    assert!(notices.try_recv().is_err());
    Ok(())
}
