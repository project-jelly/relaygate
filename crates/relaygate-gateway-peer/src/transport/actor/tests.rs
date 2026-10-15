use std::{error::Error, sync::atomic::Ordering, time::Duration};

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation, SessionId};
use tokio::sync::{mpsc, oneshot};

use super::{TransportLiveness, TransportLoop, TransportNotice};
use crate::{
    PeerEvent, PeerOpenRequest,
    frame::PeerFrame,
    identity::OpenIdentity,
    transport::{TransportCommand, tests::actor_for_open},
};

#[tokio::test(start_paused = true)]
async fn open_timeout_preserves_pending_heartbeat_and_established_sibling()
-> Result<(), Box<dyn Error>> {
    let (mut actor, mut frames, mut notices, identity, request) = actor_for_open(4)?;
    assert!(actor.active_opens.reserve(identity)?);
    let first = actor.open(request.clone()).await?;
    assert!(matches!(frames.try_recv()?, PeerFrame::Open { .. }));
    let mut transport = TransportLoop {
        actor,
        liveness: TransportLiveness::new(
            Duration::from_secs(60),
            Duration::from_secs(20),
            Duration::from_secs(300),
        ),
    };
    assert!(
        transport
            .on_inbound(Some(Ok(PeerFrame::Opened {
                stream_id: first.stream_id(),
            })))
            .await
            .is_continue()
    );
    assert!(matches!(
        notices.try_recv()?,
        TransportNotice::Event(PeerEvent::Opened { key, .. }) if key == first
    ));

    let (_commands, command_receiver) = mpsc::channel(1);
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(transport.on_deadline(&command_receiver).await.is_continue());
    assert!(matches!(frames.try_recv()?, PeerFrame::Ping { nonce: 1 }));
    let heartbeat_deadline = transport.next_deadline();

    let opening_identity = OpenIdentity::new(identity.entry_gateway(), SessionId::new(), 2);
    assert!(transport.actor.active_opens.reserve(opening_identity)?);
    let (reply, response) = oneshot::channel();
    transport
        .on_command(TransportCommand::Open {
            request: PeerOpenRequest::new(
                request.target().clone(),
                opening_identity,
                request.destination().clone(),
                request.relay_session_id(),
                request.binding_id(),
            ),
            reply,
        })
        .await;
    let opening = response.await??;
    assert!(matches!(
        frames.try_recv()?,
        PeerFrame::Open { stream_id, .. } if stream_id == opening.stream_id()
    ));
    assert_eq!(
        transport.next_deadline(),
        Some(tokio::time::Instant::now() + Duration::from_secs(1))
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(transport.on_deadline(&command_receiver).await.is_continue());
    assert!(matches!(
        notices.try_recv()?,
        TransportNotice::Event(PeerEvent::Failed { key, failure, .. })
            if key == opening
                && failure.code() == ErrorCode::DeadlineExceeded
                && failure.observation() == PeerObservation::MaybeObserved
    ));
    assert!(matches!(
        notices.try_recv()?,
        TransportNotice::StreamEnded { key, .. } if key == opening
    ));
    assert!(matches!(
        frames.try_recv()?,
        PeerFrame::Reset { stream_id, code: ErrorCode::DeadlineExceeded, .. }
            if stream_id == opening.stream_id()
    ));
    assert!(
        frames.try_recv().is_err(),
        "OPEN timeout replaced the pending PING"
    );
    assert_eq!(transport.next_deadline(), heartbeat_deadline);
    assert!(!transport.actor.active_opens.contains(opening_identity));
    assert!(transport.actor.active_opens.contains(identity));
    assert_eq!(transport.actor.stream_count.load(Ordering::Relaxed), 1);

    assert!(
        transport
            .on_inbound(Some(Ok(PeerFrame::Pong { nonce: 1 })))
            .await
            .is_continue()
    );
    assert_eq!(
        transport.next_deadline(),
        Some(tokio::time::Instant::now() + Duration::from_secs(60))
    );
    let payload = Bytes::from_static(b"sibling");
    assert!(
        transport
            .on_inbound(Some(Ok(PeerFrame::Data {
                stream_id: first.stream_id(),
                payload: payload.clone(),
            })))
            .await
            .is_continue()
    );
    assert!(matches!(
        notices.try_recv()?,
        TransportNotice::Event(PeerEvent::Data { key, payload: received })
            if key == first && received == payload
    ));
    assert!(!transport.actor.closure.token().is_cancelled());
    Ok(())
}
