use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use futures_util::{future::poll_fn, task::AtomicWaker};
use relaygate_protocol::PipeId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::{
    Error, ErrorCode, PeerObservation, Result,
    lifetime::RuntimeLifetime,
    resource::{BufferedBytes, PipeResources, ResourceLimitKind, resource_exhausted},
    session::SessionOutbound,
};

mod io;

#[cfg(test)]
mod tests;

const DATA_CHUNK_LEN: usize = 64 * 1024;

#[derive(Clone, Debug)]
enum Terminal {
    Closed,
    Failed(Error),
}

pub(crate) struct PipeState {
    id: PipeId,
    inbound: mpsc::Sender<BufferedChunk>,
    read_buffer: Arc<Mutex<PipeReadBuffer>>,
    remote_fin: AtomicBool,
    local_fin: AtomicBool,
    terminal: watch::Sender<Option<Terminal>>,
    read_waker: AtomicWaker,
    write_waker: AtomicWaker,
    abandoned: mpsc::UnboundedSender<PipeId>,
    buffered_frames: Arc<Semaphore>,
    resources: PipeResources,
}

struct PipeOwner {
    state: Arc<PipeState>,
    _lifetime: Option<Arc<RuntimeLifetime>>,
}

struct PipeWriter {
    outbound: SessionOutbound,
}

struct PipeReader {
    buffer: Arc<Mutex<PipeReadBuffer>>,
}

struct PipeReadBuffer {
    inbound: mpsc::Receiver<BufferedChunk>,
    current: Bytes,
    current_bytes: Option<BufferedBytes>,
    current_frame: Option<OwnedSemaphorePermit>,
    read_eof: bool,
    #[cfg(test)]
    after_inbound_pending: Option<Box<dyn FnOnce() + Send>>,
}

struct BufferedChunk {
    payload: Bytes,
    bytes: BufferedBytes,
    frame: OwnedSemaphorePermit,
}

/// One ordered, opaque, bidirectional byte stream.
///
/// Writes enqueue DATA; [`tokio::io::AsyncWriteExt::flush`] waits for the local
/// transport writer to flush it. Neither operation confirms remote receipt.
///
/// I/O errors do not report payload delivery. Do not use their
/// [`Error::is_retryable`] hint to replay bytes on this or a replacement Pipe.
pub struct Pipe {
    owner: Arc<PipeOwner>,
    reader: PipeReader,
    writer: PipeWriter,
}

/// The unique read side of an owned [`Pipe`] split.
///
/// Dropping this half does not send `FIN`. The Pipe is abandoned only after
/// both owned halves have been dropped without an explicit terminal action.
pub struct PipeReadHalf {
    owner: Arc<PipeOwner>,
    reader: PipeReader,
}

/// The unique write side of an owned [`Pipe`] split.
///
/// Call [`PipeWriteHalf::shutdown_write`] or [`tokio::io::AsyncWriteExt::shutdown`]
/// to send `FIN`; dropping this half alone never half-closes the Pipe.
pub struct PipeWriteHalf {
    owner: Arc<PipeOwner>,
    writer: PipeWriter,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Pipe").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PipeReadHalf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipeReadHalf")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PipeWriteHalf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipeWriteHalf")
            .finish_non_exhaustive()
    }
}

impl PipeState {
    #[cfg(test)]
    pub(crate) fn pair(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(
            id,
            outbound,
            inbound_capacity,
            abandoned,
            None,
            PipeResources::for_tests(),
        )
    }

    #[cfg(test)]
    pub(crate) fn pair_with_resource_limits(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        max_buffered_bytes_per_pipe: usize,
        max_buffered_bytes_per_relay: usize,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(
            id,
            outbound,
            inbound_capacity,
            abandoned,
            None,
            PipeResources::with_test_limits(
                max_buffered_bytes_per_pipe,
                max_buffered_bytes_per_relay,
            ),
        )
    }

    pub(crate) fn pair_with_lifetime(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Arc<RuntimeLifetime>,
        resources: PipeResources,
    ) -> (Pipe, Arc<Self>) {
        Self::pair_inner(
            id,
            outbound,
            inbound_capacity,
            abandoned,
            Some(lifetime),
            resources,
        )
    }

    fn pair_inner(
        id: PipeId,
        outbound: SessionOutbound,
        inbound_capacity: usize,
        abandoned: mpsc::UnboundedSender<PipeId>,
        lifetime: Option<Arc<RuntimeLifetime>>,
        resources: PipeResources,
    ) -> (Pipe, Arc<Self>) {
        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_capacity);
        let read_buffer = Arc::new(Mutex::new(PipeReadBuffer {
            inbound: inbound_rx,
            current: Bytes::new(),
            current_bytes: None,
            current_frame: None,
            read_eof: false,
            #[cfg(test)]
            after_inbound_pending: None,
        }));
        let (terminal, _) = watch::channel(None);
        let state = Arc::new(Self {
            id,
            inbound: inbound_tx,
            read_buffer: Arc::clone(&read_buffer),
            remote_fin: AtomicBool::new(false),
            local_fin: AtomicBool::new(false),
            terminal,
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
            abandoned,
            buffered_frames: Arc::new(Semaphore::new(inbound_capacity)),
            resources,
        });
        let owner = Arc::new(PipeOwner {
            state: Arc::clone(&state),
            _lifetime: lifetime,
        });
        let pipe = Pipe {
            owner,
            reader: PipeReader {
                buffer: read_buffer,
            },
            writer: PipeWriter { outbound },
        };
        (pipe, state)
    }

    pub(crate) fn push_data(&self, payload: Bytes) -> Result<()> {
        if self.terminal.borrow().is_some() {
            // A local terminal frame is already queued; late DATA is discarded.
            return Ok(());
        }
        if self.remote_fin.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::Observed,
                "DATA arrived after the remote direction closed",
            ));
        }
        if payload.is_empty() {
            return Ok(());
        }
        let frame = Arc::clone(&self.buffered_frames)
            .try_acquire_owned()
            .map_err(|_| {
                resource_exhausted(
                    ResourceLimitKind::PipeBufferedFrames,
                    PeerObservation::Observed,
                )
            })?;
        let bytes = self.resources.try_reserve_buffered(payload.len())?;
        self.inbound
            .try_send(BufferedChunk {
                payload,
                bytes,
                frame,
            })
            .map_err(|_| {
                resource_exhausted(
                    ResourceLimitKind::PipeBufferedFrames,
                    PeerObservation::Observed,
                )
            })
    }

    pub(crate) fn remote_fin(&self) {
        self.remote_fin.store(true, Ordering::Release);
        self.read_waker.wake();
        if self.local_fin.load(Ordering::Acquire) {
            self.close_normal();
        }
    }

    pub(crate) fn close_normal(&self) -> bool {
        self.try_set_terminal(Terminal::Closed)
    }

    pub(crate) fn fail(&self, error: Error) -> bool {
        self.try_set_terminal(Terminal::Failed(error))
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.terminal.borrow().is_some() || self.is_protocol_finished()
    }

    /// Both directions are FIN-closed; the entry needs no further terminal frame.
    pub(crate) fn is_protocol_finished(&self) -> bool {
        self.local_fin.load(Ordering::Acquire) && self.remote_fin.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn buffered_bytes(&self) -> (usize, usize) {
        self.resources.buffered_bytes()
    }

    fn try_set_terminal(&self, terminal: Terminal) -> bool {
        let failed = matches!(terminal, Terminal::Failed(_));
        let terminal_event = match &terminal {
            Terminal::Closed => None,
            Terminal::Failed(error) => Some((error.code(), error.observation())),
        };
        let changed = self.terminal.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(terminal);
            true
        });
        if changed {
            self.resources.release_live();
            if failed {
                self.discard_inbound();
            }
            self.read_waker.wake();
            self.write_waker.wake();
            if let Some((error_code, observation)) = terminal_event {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.pipe.terminal",
                    dialer_session_id = %self.id.origin_session_id().as_uuid(),
                    connection_id = self.id.connection_id(),
                    outcome = "failed",
                    error_code = ?error_code,
                    observation = ?observation,
                    "Pipe reached a terminal failure"
                );
            } else {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.pipe.terminal",
                    dialer_session_id = %self.id.origin_session_id().as_uuid(),
                    connection_id = self.id.connection_id(),
                    outcome = "closed",
                    "Pipe closed"
                );
            }
        }
        changed
    }

    fn discard_inbound(&self) {
        let mut buffer = self
            .read_buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        buffer.discard();
    }

    fn write_error(&self) -> Option<Error> {
        match self.terminal.borrow().clone() {
            Some(Terminal::Failed(error)) => Some(error),
            Some(Terminal::Closed) => Some(Error::new(
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "Pipe write direction is closed",
            )),
            None if self.local_fin.load(Ordering::Acquire) => Some(Error::new(
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "Pipe write direction is closed",
            )),
            None => None,
        }
    }

    fn terminal_failure(&self) -> Option<Error> {
        match self.terminal.borrow().clone() {
            Some(Terminal::Failed(error)) => Some(error),
            Some(Terminal::Closed) | None => None,
        }
    }
}

impl PipeReadBuffer {
    fn discard(&mut self) {
        self.inbound.close();
        self.current = Bytes::new();
        self.current_bytes = None;
        self.current_frame = None;
        while self.inbound.try_recv().is_ok() {}
    }
}

impl Drop for PipeReader {
    fn drop(&mut self) {
        self.buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .discard();
    }
}

impl Pipe {
    pub(crate) fn is_terminal(&self) -> bool {
        self.owner.state.terminal.borrow().is_some()
    }

    /// Splits this Pipe into independently owned read and write halves.
    ///
    /// The halves share one protocol state and cannot be cloned. The read half
    /// retains the only inbound cursor, while the write half retains all write,
    /// half-close, close, and reset operations.
    #[must_use]
    pub fn into_split(self) -> (PipeReadHalf, PipeWriteHalf) {
        let Self {
            owner,
            reader,
            writer,
        } = self;
        let write_owner = Arc::clone(&owner);
        (
            PipeReadHalf { owner, reader },
            PipeWriteHalf {
                owner: write_owner,
                writer,
            },
        )
    }

    /// Flushes queued DATA and FIN through the local transport writer, closing
    /// only this endpoint's write direction. Does not confirm remote receipt.
    pub async fn shutdown_write(&mut self) -> Result<()> {
        self.writer.shutdown_write(&self.owner.state).await
    }

    /// Closes both directions. Repeated calls are safe.
    pub async fn close(&mut self) -> Result<()> {
        self.writer.close(&self.owner.state).await
    }

    /// Fails both directions. Repeated calls are safe.
    pub async fn reset(&mut self, code: ErrorCode, message: impl Into<String>) -> Result<()> {
        self.writer
            .reset(&self.owner.state, code, message.into())
            .await
    }
}

impl PipeWriteHalf {
    /// Split-write equivalent of [`Pipe::shutdown_write`].
    pub async fn shutdown_write(&mut self) -> Result<()> {
        self.writer.shutdown_write(&self.owner.state).await
    }

    /// Split-write equivalent of [`Pipe::close`].
    pub async fn close(&mut self) -> Result<()> {
        self.writer.close(&self.owner.state).await
    }

    /// Split-write equivalent of [`Pipe::reset`].
    pub async fn reset(&mut self, code: ErrorCode, message: impl Into<String>) -> Result<()> {
        self.writer
            .reset(&self.owner.state, code, message.into())
            .await
    }
}

impl PipeWriter {
    async fn shutdown_write(&mut self, state: &PipeState) -> Result<()> {
        poll_fn(|context| self.poll_shutdown(state, context)).await
    }

    async fn close(&mut self, state: &PipeState) -> Result<()> {
        poll_fn(|context| self.poll_close(state, context)).await
    }

    async fn reset(&mut self, state: &PipeState, code: ErrorCode, message: String) -> Result<()> {
        poll_fn(|context| self.poll_reset(state, context, code, &message)).await
    }
}

impl Drop for PipeOwner {
    fn drop(&mut self) {
        if self.state.close_normal() {
            let _ = self.state.abandoned.send(self.state.id);
        }
    }
}
