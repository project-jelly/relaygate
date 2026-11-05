use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::task::{ArcWake, waker};
use relaygate_protocol::{Frame, PipeId, SessionId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::PipeState;
use crate::{
    Error, ErrorCode, ErrorOrigin, PeerObservation,
    config::ResourceLimits,
    resource::RelayResources,
    session::{OutboundItem, session_outbound_channel},
};

fn sdk_error(error: &std::io::Error) -> Result<&Error, &'static str> {
    Error::from_io(error).ok_or("Pipe I/O error lost its SDK Error payload")
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl ArcWake for WakeCounter {
    fn wake_by_ref(counter: &Arc<Self>) {
        counter.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn oversized_reset_is_rejected_before_queue_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 37);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    pipe.write_all(b"queued").await?;
    {
        // A cancelled writer may leave a capacity waiter behind.
        let write = pipe.write_all(b"cancelled");
        tokio::pin!(write);
        assert!(futures_util::poll!(&mut write).is_pending());
    }
    {
        let reset = pipe.reset(ErrorCode::Cancelled, "x".repeat(usize::from(u16::MAX) + 1));
        tokio::pin!(reset);
        let Poll::Ready(Err(error)) = futures_util::poll!(&mut reset) else {
            return Err("invalid RESET must fail without waiting for queue capacity".into());
        };
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert_eq!(error.origin(), ErrorOrigin::Sdk);
        assert_eq!(error.observation(), PeerObservation::NotObserved);
    }
    assert!(state.terminal.borrow().is_none());
    assert_eq!(
        receiver.try_recv()?,
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"queued"),
        }
    );
    pipe.write_all(b"still open").await?;
    assert_eq!(
        receiver.try_recv()?,
        Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"still open"),
        }
    );
    assert!(receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn pending_write_preserves_lane_failure_before_pipe_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 35);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    pipe.write_all(b"fills queue").await?;

    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let write = pipe.write_all(b"blocked write");
    tokio::pin!(write);
    assert!(write.as_mut().poll(&mut context).is_pending());

    let failure = Error::new(
        ErrorCode::ProtocolError,
        PeerObservation::NotObserved,
        "session writer rejected the frame",
    )
    .with_origin(ErrorOrigin::Transport);
    receiver.fail(&failure);
    assert!(state.terminal_failure().is_none());
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Err(error)) = write.as_mut().poll(&mut context) else {
        return Err("blocked write did not fail when its lane closed".into());
    };
    assert_eq!(
        sdk_error(&error)?,
        &failure.with_observation(PeerObservation::MaybeObserved)
    );
    Ok(())
}

#[tokio::test]
async fn pending_shutdown_preserves_lane_failure_before_fin_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 36);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    pipe.write_all(b"fills queue").await?;

    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let shutdown = pipe.shutdown_write();
    tokio::pin!(shutdown);
    assert!(shutdown.as_mut().poll(&mut context).is_pending());
    assert!(!state.local_fin.load(Ordering::Acquire));

    let failure = Error::transport_deadline("session writer deadline expired");
    receiver.fail(&failure);
    assert!(state.terminal_failure().is_none());
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        shutdown.as_mut().poll(&mut context),
        Poll::Ready(Err(failure.with_observation(PeerObservation::MaybeObserved)))
    );
    assert!(!state.local_fin.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test]
async fn cancelled_shutdown_reuses_fin_and_waits_for_writer_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(2);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 32);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let shutdown = pipe.shutdown_write();
        tokio::pin!(shutdown);
        assert!(futures_util::poll!(&mut shutdown).is_pending());
    }
    assert_eq!(receiver.recv().await, Some(Frame::Fin { pipe_id }));
    let Some(OutboundItem::Flush(completion)) = receiver.recv_item().await else {
        return Err("shutdown did not enqueue a completion marker".into());
    };
    let shutdown = pipe.shutdown_write();
    tokio::pin!(shutdown);
    assert!(futures_util::poll!(&mut shutdown).is_pending());
    assert!(
        receiver.try_recv().is_err(),
        "retry must not enqueue another FIN"
    );
    assert!(completion.send(Ok(())).is_ok());
    shutdown.await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_preserves_writer_failure_after_both_halves_close()
-> Result<(), Box<dyn std::error::Error>> {
    // FIN fills the queue, so the flush marker cannot be enqueued yet. The
    // normally terminal Pipe cannot subsequently store a session failure.
    let (outbound, mut receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 33);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state.remote_fin();
    let shutdown = pipe.shutdown_write();
    tokio::pin!(shutdown);
    assert!(futures_util::poll!(&mut shutdown).is_pending());
    assert!(state.is_protocol_finished());
    let failure = Error::transport_deadline("writer deadline");
    receiver.fail(&failure);
    assert_eq!(
        shutdown.await,
        Err(failure.with_observation(PeerObservation::MaybeObserved))
    );
    Ok(())
}

#[tokio::test]
async fn remote_failure_wakes_a_flush_waiting_for_writer_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(2);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 34);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    pipe.write_all(b"pending").await?;
    let wakes = Arc::new(WakeCounter::default());
    let waker = waker(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let flush = pipe.flush();
    tokio::pin!(flush);
    assert!(flush.as_mut().poll(&mut context).is_pending());
    let failure = Error::transport_unavailable("remote session ended")
        .with_observation(PeerObservation::MaybeObserved);
    assert!(state.fail(failure.clone()));
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Err(error)) = flush.as_mut().poll(&mut context) else {
        return Err("flush did not fail after its terminal state changed".into());
    };
    assert_eq!(sdk_error(&error)?, &failure);
    receiver.fail(&failure);
    Ok(())
}

#[tokio::test]
async fn buffered_byte_capacity_is_returned_after_read_and_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 1);
    let (mut pipe, state) =
        PipeState::pair_with_resource_limits(pipe_id, outbound, 2, abandoned, 4, 6);

    state.push_data(Bytes::from_static(b"data"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    let error = state
        .push_data(Bytes::from_static(b"x"))
        .err()
        .ok_or("Pipe byte budget accepted excess DATA")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert_eq!(state.buffered_bytes(), (4, 4));

    let mut received = [0_u8; 4];
    pipe.read_exact(&mut received).await?;
    assert_eq!(&received, b"data");
    assert_eq!(state.buffered_bytes(), (0, 0));

    state.push_data(Bytes::from_static(b"more"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    drop(pipe);
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn partial_read_keeps_the_current_frame_charged_to_the_pipe_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 27);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.push_data(Bytes::from_static(b"ab"))?;
    let mut first = [0_u8; 1];
    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"a");

    let error = state
        .push_data(Bytes::from_static(b"c"))
        .err()
        .ok_or("Pipe frame budget ignored the partially read current frame")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);

    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"b");
    state.push_data(Bytes::from_static(b"c"))?;
    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"c");
    Ok(())
}

#[tokio::test]
async fn empty_data_frame_releases_its_frame_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 28);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.push_data(Bytes::new())?;
    let mut byte = [0_u8; 1];
    state.push_data(Bytes::from_static(b"x"))?;
    pipe.read_exact(&mut byte).await?;
    assert_eq!(&byte, b"x");
    Ok(())
}

#[tokio::test]
async fn terminal_failure_releases_unread_buffer_capacity_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 29);
    let (_pipe, state) =
        PipeState::pair_with_resource_limits(pipe_id, outbound, 2, abandoned, 4, 4);

    state.push_data(Bytes::from_static(b"data"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    assert!(state.fail(Error::unavailable("session failed")));
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn dropped_read_half_rejects_later_data_without_buffering()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 30);
    let (pipe, state) = PipeState::pair_with_resource_limits(pipe_id, outbound, 1, abandoned, 4, 4);
    let (reader, _writer) = pipe.into_split();

    drop(reader);
    let error = state
        .push_data(Bytes::from_static(b"data"))
        .err()
        .ok_or("dropped read half accepted later DATA")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[test]
fn terminal_pipe_releases_its_live_slot_while_the_handle_is_retained()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = ResourceLimits {
        max_live_pipes_per_listener: 1,
        max_live_pipes_per_relay: 1,
        ..ResourceLimits::default()
    };
    let resources = RelayResources::new(limits);
    let pipe_resources = resources.pipe_resources(resources.try_reserve_outgoing()?, usize::MAX);
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 31);
    let (_retained_pipe, state) =
        PipeState::pair_inner(pipe_id, outbound, 1, abandoned, None, pipe_resources);

    assert!(resources.try_reserve_outgoing().is_err());
    assert!(state.fail(Error::unavailable("session failed")));
    assert!(resources.try_reserve_outgoing().is_ok());
    Ok(())
}

#[tokio::test]
async fn dropped_pipe_uses_the_current_pipe_terminal_lane() -> Result<(), Box<dyn std::error::Error>>
{
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 2);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    drop(pipe);

    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cancelled_close_remains_drop_safe() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 3);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    assert!(
        timeout(Duration::from_millis(10), pipe.close())
            .await
            .is_err()
    );
    drop(pipe);

    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    Ok(())
}

#[tokio::test]
async fn remote_failure_wins_while_close_waits_for_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 4);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let close = pipe.close();
        tokio::pin!(close);
        assert!(
            timeout(Duration::from_millis(10), &mut close)
                .await
                .is_err()
        );

        assert!(state.fail(Error::unavailable("remote session failed")));
        assert!(receiver.recv().await.is_some());
        close.await?;
    }
    assert!(receiver.try_recv().is_err());

    let mut buffer = [0_u8; 1];
    let error = pipe
        .read(&mut buffer)
        .await
        .err()
        .ok_or("remote failure was overwritten by local close")?;
    assert_eq!(sdk_error(&error)?.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn remote_failure_wins_while_write_waits_for_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 5);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("remote session failed");

    let write = pipe.write_all(b"blocked payload");
    tokio::pin!(write);
    assert!(
        timeout(Duration::from_millis(10), &mut write)
            .await
            .is_err()
    );

    assert!(state.fail(failure.clone()));
    let error = timeout(Duration::from_secs(1), &mut write)
        .await?
        .err()
        .ok_or("blocked write unexpectedly succeeded after remote failure")?;
    assert_eq!(sdk_error(&error)?, &failure);
    assert_eq!(receiver.recv().await, Some(Frame::Ping { nonce: 1 }));
    assert!(receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn full_failure_is_not_masked_by_remote_fin() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 6);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.remote_fin();
    assert!(state.fail(Error::unavailable("session failed after remote FIN")));

    let mut buffer = [0_u8; 1];
    let error = pipe
        .read(&mut buffer)
        .await
        .err()
        .ok_or("remote FIN masked the later full failure")?;
    assert_eq!(sdk_error(&error)?.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn pipe_implements_async_read_and_write_without_changing_frame_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 7);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state.push_data(Bytes::from_static(b"inbound"))?;

    let mut inbound = [0_u8; 7];
    pipe.read_exact(&mut inbound).await?;
    assert_eq!(&inbound, b"inbound");

    pipe.write_all(b"outbound").await?;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"outbound"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn owned_halves_can_read_and_write_concurrently() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 8);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    let read_task = tokio::spawn(async move {
        let mut payload = [0_u8; 4];
        reader.read_exact(&mut payload).await?;
        Ok::<_, std::io::Error>(payload)
    });
    let write_task = tokio::spawn(async move { writer.write_all(b"pong").await });

    tokio::task::yield_now().await;
    state.push_data(Bytes::from_static(b"ping"))?;

    assert_eq!(read_task.await??, *b"ping");
    write_task.await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"pong"),
        })
    );
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn dropping_split_halves_signals_abandonment_only_after_the_last_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 9);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (reader, writer) = pipe.into_split();

    drop(writer);
    assert!(outbound_rx.try_recv().is_err(), "Drop must not send FIN");
    assert!(abandoned_rx.try_recv().is_err());

    drop(reader);
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn async_shutdown_sends_one_fin_and_keeps_the_read_half_alive()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 10);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    {
        let shutdown = writer.shutdown();
        tokio::pin!(shutdown);
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        assert_eq!(outbound_rx.recv().await, Some(Frame::Fin { pipe_id }));
        let Some(OutboundItem::Flush(completion)) = outbound_rx.recv_item().await else {
            return Err("shutdown did not enqueue a completion marker".into());
        };
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        assert!(completion.send(Ok(())).is_ok());
        shutdown.await?;
    }
    writer.shutdown().await?;
    assert!(outbound_rx.try_recv().is_err());

    drop(writer);
    assert!(abandoned_rx.try_recv().is_err());
    state.push_data(Bytes::from_static(b"reply"))?;
    state.remote_fin();

    let mut payload = [0_u8; 5];
    reader.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"reply");
    assert_eq!(reader.read(&mut payload).await?, 0);
    drop(reader);

    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn fin_racing_after_empty_poll_does_not_discard_accepted_data()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 25);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let state_for_race = state.clone();
    pipe.reader
        .buffer
        .lock()
        .map_err(|_| "Pipe read buffer lock poisoned")?
        .after_inbound_pending = Some(Box::new(move || {
        assert!(
            state_for_race
                .push_data(Bytes::from_static(b"accepted-before-fin"))
                .is_ok(),
            "DATA before FIN must fit the inbound queue"
        );
        state_for_race.remote_fin();
    }));

    let mut payload = [0_u8; 19];
    pipe.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"accepted-before-fin");
    assert_eq!(pipe.read(&mut payload).await?, 0);
    Ok(())
}

#[tokio::test]
async fn remote_fin_final_drain_does_not_treat_cooperative_pending_as_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 26);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let state_for_race = state.clone();
    pipe.reader
        .buffer
        .lock()
        .map_err(|_| "Pipe read buffer lock poisoned")?
        .after_inbound_pending = Some(Box::new(move || {
        assert!(
            state_for_race
                .push_data(Bytes::from_static(b"accepted-before-fin"))
                .is_ok(),
            "DATA before FIN must fit the inbound queue"
        );
        state_for_race.remote_fin();
    }));

    let mut payload = [0_u8; 19];
    let count = std::future::poll_fn(|context| {
        for _ in 0..4096 {
            let mut consume = Box::pin(tokio::task::coop::consume_budget());
            if consume.as_mut().poll(context).is_pending() {
                return match pipe.reader.poll_read(&state, context, &mut payload) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => Poll::Ready(Err(Error::new(
                        ErrorCode::Internal,
                        PeerObservation::NotObserved,
                        "remote FIN final drain depends on cooperative budget",
                    ))),
                };
            }
        }
        Poll::Ready(Err(Error::new(
            ErrorCode::Internal,
            PeerObservation::NotObserved,
            "Tokio cooperative budget was not exhausted",
        )))
    })
    .await?;

    assert_eq!(count, payload.len());
    assert_eq!(&payload, b"accepted-before-fin");
    assert_eq!(pipe.read(&mut payload).await?, 0);
    Ok(())
}

#[tokio::test]
async fn write_half_close_terminates_the_split_read_half() -> Result<(), Box<dyn std::error::Error>>
{
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 13);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    writer.close().await?;
    assert_eq!(outbound_rx.recv().await, Some(Frame::Close { pipe_id }));
    let write_error = writer
        .write_all(b"after-close")
        .await
        .err()
        .ok_or("closed Pipe write unexpectedly succeeded")?;
    assert_eq!(
        sdk_error(&write_error)?.code(),
        ErrorCode::FailedPrecondition
    );

    let mut byte = [0_u8; 1];
    assert_eq!(reader.read(&mut byte).await?, 0);
    drop(writer);
    drop(reader);
    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn async_write_waits_for_the_existing_bounded_session_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 11);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let write = pipe.write_all(b"bounded");
        tokio::pin!(write);
        assert!(
            timeout(Duration::from_millis(10), &mut write)
                .await
                .is_err()
        );
        assert_eq!(outbound_rx.recv().await, Some(Frame::Ping { nonce: 1 }));
        write.await?;
    }

    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"bounded"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_write_does_not_poison_shared_session_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let session_id = SessionId::new();
    let pipe_a_id = PipeId::new(session_id, 21);
    let pipe_b_id = PipeId::new(session_id, 22);
    let (pipe_a, _state_a) = PipeState::pair(pipe_a_id, outbound.clone(), 1, abandoned.clone());
    let (mut pipe_b, _state_b) = PipeState::pair(pipe_b_id, outbound, 1, abandoned);
    let (_reader_a, mut writer_a) = pipe_a.into_split();

    {
        let pending = writer_a.write_all(b"cancelled-a");
        tokio::pin!(pending);
        assert!(
            timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
    }

    assert_eq!(outbound_rx.recv().await, Some(Frame::Ping { nonce: 1 }));
    timeout(Duration::from_secs(1), pipe_b.write_all(b"sibling-b")).await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id: pipe_b_id,
            payload: Bytes::from_static(b"sibling-b"),
        })
    );
    assert!(outbound_rx.try_recv().is_err());

    timeout(Duration::from_secs(1), writer_a.write_all(b"retry-a")).await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id: pipe_a_id,
            payload: Bytes::from_static(b"retry-a"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn closed_session_outbound_wakes_a_pending_write() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 23);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    let pending = pipe.write_all(b"pending");
    tokio::pin!(pending);
    assert!(
        timeout(Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );

    drop(outbound_rx);
    let error = timeout(Duration::from_secs(1), &mut pending)
        .await?
        .err()
        .ok_or("pending write unexpectedly succeeded after outbound receiver closed")?;
    let error = sdk_error(&error)?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.observation(), crate::PeerObservation::MaybeObserved);
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_read_can_be_retried_without_losing_data()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 24);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let mut cancelled_byte = [0_u8; 1];
        let pending = pipe.read(&mut cancelled_byte);
        tokio::pin!(pending);
        assert!(
            timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
    }

    state.push_data(Bytes::from_static(b"retry"))?;
    let mut payload = [0_u8; 5];
    timeout(Duration::from_secs(1), pipe.read_exact(&mut payload)).await??;
    assert_eq!(&payload, b"retry");
    Ok(())
}

#[tokio::test]
async fn async_io_errors_preserve_the_structured_sdk_error_as_their_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 12);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("session failed");
    assert!(state.fail(failure.clone()));

    let mut byte = [0_u8; 1];
    let write_error = pipe
        .write_all(b"trait")
        .await
        .err()
        .ok_or("AsyncWrite unexpectedly succeeded")?;
    assert_eq!(sdk_error(&write_error)?, &failure);

    let read_error = pipe
        .read(&mut byte)
        .await
        .err()
        .ok_or("AsyncRead unexpectedly succeeded")?;
    assert_eq!(sdk_error(&read_error)?, &failure);
    assert!(Error::from_io(&std::io::Error::other("foreign")).is_none());
    Ok(())
}

#[tokio::test]
async fn async_shutdown_reports_terminal_failure_and_preserves_its_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 14);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("session failed before shutdown");
    assert!(state.fail(failure.clone()));

    let error = pipe
        .shutdown()
        .await
        .err()
        .ok_or("AsyncWrite shutdown masked terminal failure")?;
    let payload = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Error>())
        .ok_or("AsyncWrite shutdown lost its SDK Error payload")?;
    assert_eq!(payload, &failure);
    assert_eq!(pipe.shutdown_write().await, Err(failure));
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn close_racing_after_empty_poll_does_not_discard_accepted_data()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 27);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let state_for_race = state.clone();
    pipe.reader
        .buffer
        .lock()
        .map_err(|_| "Pipe read buffer lock poisoned")?
        .after_inbound_pending = Some(Box::new(move || {
        assert!(
            state_for_race
                .push_data(Bytes::from_static(b"tail-before-close"))
                .is_ok(),
            "DATA before CLOSE must fit the inbound queue"
        );
        state_for_race.close_normal();
    }));

    let mut payload = [0_u8; 17];
    pipe.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"tail-before-close");
    assert_eq!(pipe.read(&mut payload).await?, 0);
    Ok(())
}

#[tokio::test]
async fn data_after_local_terminal_is_discarded_without_protocol_error()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 28);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    drop(pipe);
    assert_eq!(abandoned_rx.try_recv()?, pipe_id);
    assert!(state.is_finished());
    assert!(!state.is_protocol_finished());
    assert!(state.push_data(Bytes::from_static(b"late")).is_ok());
    assert_eq!(state.buffered_bytes(), (0, 0));

    state.remote_fin();
    assert!(!state.is_protocol_finished());
    Ok(())
}
