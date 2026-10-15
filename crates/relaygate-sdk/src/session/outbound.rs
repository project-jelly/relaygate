//! Bounded outbound frame lane shared by every Pipe of one Relay session.
//!
//! Capacity is the mpsc channel's; `capacity` only wakes writers that found it
//! full. Wakeups are deliberately broadcast (`notify_waiters`) rather than
//! handed to one waiter: a Pipe's write future can be dropped mid-wait while
//! its `SessionOutbound` (and therefore its registered waiter) lives on inside
//! the `AsyncWrite` object, so a single-waiter handoff — a semaphore permit or
//! `notify_one` — would strand the freed slot on a writer nobody polls and
//! starve its siblings. Every woken writer re-races `try_reserve`, which is
//! O(blocked writers) per dequeue but never leaves a freed slot unclaimed
//! while a live writer waits.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use relaygate_protocol::Frame;
use tokio::sync::{
    Notify,
    futures::OwnedNotified,
    mpsc::{self, error::TrySendError},
    oneshot,
};

use crate::{Error, PeerObservation, Result};

#[derive(Debug)]
pub(crate) enum OutboundItem {
    Frame(Frame),
    Flush(oneshot::Sender<Result<()>>),
}

pub(crate) struct SessionOutbound {
    sender: mpsc::Sender<OutboundItem>,
    capacity: Arc<Notify>,
    waiter: Option<Pin<Box<OwnedNotified>>>,
    failure: Arc<Mutex<Option<Error>>>,
    dirty: bool,
    flushing: Option<oneshot::Receiver<Result<()>>>,
}

pub(crate) struct SessionOutboundReceiver {
    receiver: mpsc::Receiver<OutboundItem>,
    capacity: Arc<Notify>,
    failure: Arc<Mutex<Option<Error>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameCommit {
    Sent,
    Skipped,
}

pub(crate) fn session_outbound_channel(
    capacity: usize,
) -> (SessionOutbound, SessionOutboundReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    let capacity = Arc::new(Notify::new());
    let failure = Arc::new(Mutex::new(None));
    (
        SessionOutbound {
            sender,
            capacity: Arc::clone(&capacity),
            waiter: None,
            failure: Arc::clone(&failure),
            dirty: false,
            flushing: None,
        },
        SessionOutboundReceiver {
            receiver,
            capacity,
            failure,
        },
    )
}

impl Clone for SessionOutbound {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: Arc::clone(&self.capacity),
            waiter: None,
            failure: Arc::clone(&self.failure),
            dirty: false,
            flushing: None,
        }
    }
}

impl SessionOutbound {
    pub(crate) fn cancel_wait(&mut self) {
        self.waiter = None;
    }

    pub(crate) fn poll_send(
        &mut self,
        context: &mut Context<'_>,
        make_frame: impl FnOnce() -> Option<Frame>,
    ) -> Poll<std::result::Result<FrameCommit, ()>> {
        let result = self.poll_enqueue(context, || make_frame().map(OutboundItem::Frame));
        if matches!(result, Poll::Ready(Ok(FrameCommit::Sent))) {
            // A cancelled flush may already have queued its marker. New DATA or
            // FIN must be covered by a later marker, never that older completion.
            self.flushing = None;
            self.dirty = true;
        }
        result
    }

    pub(crate) fn poll_flush(&mut self, context: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.dirty {
            self.cancel_wait();
            return Poll::Ready(Ok(()));
        }
        if self.flushing.is_none() {
            let mut response = None;
            match self.poll_enqueue(context, || {
                let (sender, receiver) = oneshot::channel();
                response = Some(receiver);
                Some(OutboundItem::Flush(sender))
            }) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(())) => return Poll::Ready(Err(self.flush_error())),
                Poll::Ready(Ok(_)) => self.flushing = response,
            }
        }
        let Some(response) = self.flushing.as_mut() else {
            return Poll::Ready(Err(self.flush_error()));
        };
        match Pin::new(response).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.flushing = None;
                let result = result.unwrap_or_else(|_| Err(self.flush_error()));
                if result.is_ok() {
                    self.dirty = false;
                }
                Poll::Ready(result)
            }
        }
    }

    fn flush_error(&self) -> Error {
        self.failure_error("RelaySession ended before Pipe flush completed")
    }

    pub(crate) fn failure_error(&self, fallback_message: &str) -> Error {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .unwrap_or_else(|| Error::maybe_observed(fallback_message))
            .with_observation(PeerObservation::MaybeObserved)
    }

    fn poll_enqueue(
        &mut self,
        context: &mut Context<'_>,
        make_item: impl FnOnce() -> Option<OutboundItem>,
    ) -> Poll<std::result::Result<FrameCommit, ()>> {
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.waiter = None;
                let commit = commit_frame(permit, make_item);
                if commit == FrameCommit::Skipped {
                    self.capacity.notify_waiters();
                }
                return Poll::Ready(Ok(commit));
            }
            Err(TrySendError::Closed(())) => {
                self.waiter = None;
                return Poll::Ready(Err(()));
            }
            Err(TrySendError::Full(())) => {}
        }

        if self.waiter.is_none() {
            self.waiter = Some(Box::pin(Arc::clone(&self.capacity).notified_owned()));
        }
        let notified = self
            .waiter
            .as_mut()
            .is_some_and(|waiter| waiter.as_mut().poll(context).is_ready());

        match self.sender.try_reserve() {
            Ok(permit) => {
                self.waiter = None;
                let commit = commit_frame(permit, make_item);
                if commit == FrameCommit::Skipped {
                    self.capacity.notify_waiters();
                }
                Poll::Ready(Ok(commit))
            }
            Err(TrySendError::Closed(())) => {
                self.waiter = None;
                Poll::Ready(Err(()))
            }
            Err(TrySendError::Full(())) => {
                if notified {
                    self.waiter = None;
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn send(
        &self,
        frame: Frame,
    ) -> std::result::Result<(), mpsc::error::SendError<OutboundItem>> {
        self.sender.send(OutboundItem::Frame(frame)).await
    }
}

fn commit_frame(
    permit: mpsc::Permit<'_, OutboundItem>,
    make_frame: impl FnOnce() -> Option<OutboundItem>,
) -> FrameCommit {
    let Some(frame) = make_frame() else {
        return FrameCommit::Skipped;
    };
    permit.send(frame);
    FrameCommit::Sent
}

impl SessionOutboundReceiver {
    pub(crate) async fn recv_item(&mut self) -> Option<OutboundItem> {
        let frame = self.receiver.recv().await;
        if frame.is_some() {
            self.capacity.notify_waiters();
        }
        frame
    }

    pub(crate) fn fail(&mut self, failure: &Error) {
        let failure = failure
            .clone()
            .with_observation(PeerObservation::MaybeObserved);
        *self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(failure.clone());
        self.receiver.close();
        while let Ok(item) = self.receiver.try_recv() {
            if let OutboundItem::Flush(response) = item {
                let _ = response.send(Err(failure.clone()));
            }
        }
        self.capacity.notify_waiters();
    }

    // Unit-test mock writer: consuming a marker models a completed local send.
    #[cfg(test)]
    pub(crate) async fn recv(&mut self) -> Option<Frame> {
        loop {
            match self.recv_item().await? {
                OutboundItem::Frame(frame) => return Some(frame),
                OutboundItem::Flush(response) => {
                    let _ = response.send(Ok(()));
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> std::result::Result<Frame, mpsc::error::TryRecvError> {
        loop {
            let item = self.receiver.try_recv()?;
            self.capacity.notify_waiters();
            match item {
                OutboundItem::Frame(frame) => return Ok(frame),
                OutboundItem::Flush(response) => {
                    let _ = response.send(Ok(()));
                }
            }
        }
    }
}

impl Drop for SessionOutboundReceiver {
    fn drop(&mut self) {
        self.capacity.notify_waiters();
    }
}

#[cfg(test)]
mod tests;
