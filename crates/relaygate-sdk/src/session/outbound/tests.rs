use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use futures_util::{
    future::poll_fn,
    task::{ArcWake, waker},
};
use relaygate_protocol::Frame;
use tokio::sync::mpsc::error::TryRecvError;

use super::{FrameCommit, OutboundItem, session_outbound_channel};
use crate::{Error, ErrorCode, ErrorOrigin, PeerObservation};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl ArcWake for WakeCounter {
    fn wake_by_ref(counter: &Arc<Self>) {
        counter.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn skipped_commit_releases_capacity_and_wakes_another_writer() -> TestResult {
    let (mut first, mut receiver) = session_outbound_channel(1);
    let mut second = first.clone();
    first.send(Frame::Ping { nonce: 1 }).await?;

    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert!(
        second
            .poll_send(&mut context, || Some(Frame::Pong { nonce: 2 }))
            .is_pending()
    );

    // Bypass the receiver's notification: the skipped commit must wake the
    // other writer even though it does not consume the newly free slot.
    assert!(matches!(
        receiver.receiver.recv().await,
        Some(OutboundItem::Frame(Frame::Ping { nonce: 1 }))
    ));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        first.poll_send(&mut context, || None),
        Poll::Ready(Ok(FrameCommit::Skipped))
    );
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        second.poll_send(&mut context, || Some(Frame::Pong { nonce: 2 })),
        Poll::Ready(Ok(FrameCommit::Sent))
    );
    assert_eq!(receiver.recv().await, Some(Frame::Pong { nonce: 2 }));
    Ok(())
}

#[tokio::test]
async fn dropping_the_receiver_fails_a_blocked_writer() -> TestResult {
    let (first, receiver) = session_outbound_channel(1);
    let mut second = first.clone();
    first.send(Frame::Ping { nonce: 1 }).await?;

    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert!(
        second
            .poll_send(&mut context, || Some(Frame::Pong { nonce: 2 }))
            .is_pending()
    );

    drop(receiver);
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        second.poll_send(&mut context, || Some(Frame::Pong { nonce: 2 })),
        Poll::Ready(Err(()))
    );
    Ok(())
}

#[tokio::test]
async fn flush_waits_for_writer_completion_after_its_frame() -> TestResult {
    let (mut sender, mut receiver) = session_outbound_channel(2);
    assert_eq!(
        poll_fn(|context| sender.poll_send(context, || Some(Frame::Ping { nonce: 1 }))).await,
        Ok(FrameCommit::Sent)
    );
    let flush = poll_fn(|context| sender.poll_flush(context));
    tokio::pin!(flush);
    assert!(futures_util::poll!(&mut flush).is_pending());

    assert!(matches!(
        receiver.recv_item().await,
        Some(OutboundItem::Frame(Frame::Ping { nonce: 1 }))
    ));
    assert!(futures_util::poll!(&mut flush).is_pending());
    let Some(OutboundItem::Flush(completion)) = receiver.recv_item().await else {
        return Err("expected a flush marker after the frame".into());
    };
    assert!(futures_util::poll!(&mut flush).is_pending());
    assert!(completion.send(Ok(())).is_ok());
    assert_eq!(futures_util::poll!(&mut flush), Poll::Ready(Ok(())));
    Ok(())
}

#[tokio::test]
async fn cancelled_flush_cannot_complete_a_later_successful_write() -> TestResult {
    let (mut sender, mut receiver) = session_outbound_channel(2);
    assert_eq!(
        poll_fn(|context| sender.poll_send(context, || Some(Frame::Ping { nonce: 1 }))).await,
        Ok(FrameCommit::Sent)
    );
    {
        let cancelled = poll_fn(|context| sender.poll_flush(context));
        tokio::pin!(cancelled);
        assert!(futures_util::poll!(&mut cancelled).is_pending());
    }
    assert!(matches!(
        receiver.recv_item().await,
        Some(OutboundItem::Frame(Frame::Ping { nonce: 1 }))
    ));
    let Some(OutboundItem::Flush(old_completion)) = receiver.recv_item().await else {
        return Err("expected the cancelled flush marker".into());
    };

    assert_eq!(
        poll_fn(|context| sender.poll_send(context, || Some(Frame::Ping { nonce: 2 }))).await,
        Ok(FrameCommit::Sent)
    );
    assert!(old_completion.send(Ok(())).is_err());

    let flush = poll_fn(|context| sender.poll_flush(context));
    tokio::pin!(flush);
    assert!(futures_util::poll!(&mut flush).is_pending());
    assert!(matches!(
        receiver.recv_item().await,
        Some(OutboundItem::Frame(Frame::Ping { nonce: 2 }))
    ));
    let Some(OutboundItem::Flush(completion)) = receiver.recv_item().await else {
        return Err("new data was not followed by a new flush marker".into());
    };
    assert!(futures_util::poll!(&mut flush).is_pending());
    assert!(completion.send(Ok(())).is_ok());
    assert_eq!(futures_util::poll!(&mut flush), Poll::Ready(Ok(())));
    Ok(())
}

#[test]
fn flush_waiting_for_queue_capacity_preserves_session_failure() {
    let (mut sender, mut receiver) = session_outbound_channel(1);
    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert_eq!(
        sender.poll_send(&mut context, || Some(Frame::Ping { nonce: 1 })),
        Poll::Ready(Ok(FrameCommit::Sent))
    );
    assert!(sender.poll_flush(&mut context).is_pending());

    let failure = Error::new(
        ErrorCode::ProtocolError,
        PeerObservation::NotObserved,
        "transport rejected the next frame",
    )
    .with_origin(ErrorOrigin::Transport);
    receiver.fail(&failure);

    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        sender.poll_flush(&mut context),
        Poll::Ready(Err(failure.with_observation(PeerObservation::MaybeObserved)))
    );
}

#[test]
fn queued_flush_marker_preserves_session_failure() {
    let (mut sender, mut receiver) = session_outbound_channel(2);
    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert_eq!(
        sender.poll_send(&mut context, || Some(Frame::Ping { nonce: 1 })),
        Poll::Ready(Ok(FrameCommit::Sent))
    );
    assert!(sender.poll_flush(&mut context).is_pending());

    let failure = Error::new(
        ErrorCode::DeadlineExceeded,
        PeerObservation::NotObserved,
        "RelaySession frame write deadline exceeded",
    )
    .with_origin(ErrorOrigin::Transport);
    receiver.fail(&failure);

    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        sender.poll_flush(&mut context),
        Poll::Ready(Err(failure.with_observation(PeerObservation::MaybeObserved)))
    );
}

#[test]
fn receiver_drop_wakes_flush_waiting_for_capacity_or_completion() {
    for capacity in [1, 2] {
        let (mut sender, receiver) = session_outbound_channel(capacity);
        let wakes = Arc::new(WakeCounter::default());
        let waker = waker(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        assert_eq!(
            sender.poll_send(&mut context, || Some(Frame::Ping { nonce: 1 })),
            Poll::Ready(Ok(FrameCommit::Sent))
        );
        assert!(sender.poll_flush(&mut context).is_pending());

        drop(receiver);

        assert!(wakes.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            sender.poll_flush(&mut context),
            Poll::Ready(Err(Error::transport_unavailable(
                "RelaySession ended before Pipe flush completed"
            )
            .with_observation(PeerObservation::MaybeObserved)))
        );
    }
}

#[tokio::test]
async fn repeated_clean_flush_does_not_enqueue_another_marker() -> TestResult {
    let (mut sender, mut receiver) = session_outbound_channel(2);
    assert_eq!(poll_fn(|context| sender.poll_flush(context)).await, Ok(()));
    assert!(matches!(
        receiver.receiver.try_recv(),
        Err(TryRecvError::Empty)
    ));

    assert_eq!(
        poll_fn(|context| sender.poll_send(context, || Some(Frame::Ping { nonce: 1 }))).await,
        Ok(FrameCommit::Sent)
    );
    {
        let flush = poll_fn(|context| sender.poll_flush(context));
        tokio::pin!(flush);
        assert!(futures_util::poll!(&mut flush).is_pending());
        assert!(matches!(
            receiver.recv_item().await,
            Some(OutboundItem::Frame(Frame::Ping { nonce: 1 }))
        ));
        let Some(OutboundItem::Flush(completion)) = receiver.recv_item().await else {
            return Err("expected a flush marker after the frame".into());
        };
        assert!(completion.send(Ok(())).is_ok());
        assert_eq!(futures_util::poll!(&mut flush), Poll::Ready(Ok(())));
    }
    for _ in 0..2 {
        assert_eq!(poll_fn(|context| sender.poll_flush(context)).await, Ok(()));
        assert!(matches!(
            receiver.receiver.try_recv(),
            Err(TryRecvError::Empty)
        ));
    }
    Ok(())
}
