use std::{
    io,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use relaygate_protocol::Frame;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc::error::TryRecvError,
};

use super::{
    DATA_CHUNK_LEN, Pipe, PipeReadHalf, PipeReader, PipeState, PipeWriteHalf, PipeWriter, Terminal,
};
use crate::{Error, ErrorCode, PeerObservation, Result, session::FrameCommit};

impl PipeReader {
    pub(super) fn poll_read(
        &mut self,
        state: &PipeState,
        context: &mut Context<'_>,
        destination: &mut [u8],
    ) -> Poll<Result<usize>> {
        if destination.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        loop {
            if buffer.current.has_remaining() {
                let count = destination.len().min(buffer.current.remaining());
                buffer.current.copy_to_slice(&mut destination[..count]);
                if !buffer.current.has_remaining() {
                    buffer.current_bytes = None;
                    buffer.current_frame = None;
                }
                return Poll::Ready(Ok(count));
            }
            if buffer.read_eof {
                return Poll::Ready(Ok(0));
            }

            match Pin::new(&mut buffer.inbound).poll_recv(context) {
                Poll::Ready(Some(chunk)) => {
                    buffer.current = chunk.payload;
                    buffer.current_bytes = Some(chunk.bytes);
                    buffer.current_frame = Some(chunk.frame);
                    continue;
                }
                Poll::Ready(None) => {
                    buffer.read_eof = true;
                    return Poll::Ready(state.terminal_failure().map_or(Ok(0), Err));
                }
                Poll::Pending => {}
            }

            #[cfg(test)]
            if let Some(after_inbound_pending) = buffer.after_inbound_pending.take() {
                after_inbound_pending();
            }

            state.read_waker.register(context.waker());
            let terminal = state.terminal.borrow().clone();
            if let Some(Terminal::Failed(error)) = terminal.as_ref() {
                return Poll::Ready(Err(error.clone()));
            }
            let closed = matches!(terminal, Some(Terminal::Closed));
            if closed || state.remote_fin.load(Ordering::Acquire) {
                match buffer.inbound.try_recv() {
                    Ok(chunk) => {
                        buffer.current = chunk.payload;
                        buffer.current_bytes = Some(chunk.bytes);
                        buffer.current_frame = Some(chunk.frame);
                        continue;
                    }
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                        buffer.read_eof = true;
                        return Poll::Ready(Ok(0));
                    }
                }
            }
            return Poll::Pending;
        }
    }
}

impl PipeWriter {
    pub(super) fn poll_write(
        &mut self,
        state: &PipeState,
        context: &mut Context<'_>,
        payload: &[u8],
    ) -> Poll<Result<usize>> {
        state.write_waker.register(context.waker());
        if let Some(error) = state.write_error() {
            self.outbound.cancel_wait();
            return Poll::Ready(Err(error));
        }
        if payload.is_empty() {
            self.outbound.cancel_wait();
            return Poll::Ready(Ok(0));
        }

        let count = payload.len().min(DATA_CHUNK_LEN);
        match self.outbound.poll_send(context, || {
            state.write_error().is_none().then(|| Frame::Data {
                pipe_id: state.id,
                payload: Bytes::copy_from_slice(&payload[..count]),
            })
        }) {
            Poll::Pending => {
                if let Some(error) = state.write_error() {
                    self.outbound.cancel_wait();
                    Poll::Ready(Err(error))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(())) => Poll::Ready(Err(self
                .outbound
                .failure_error("session ended while writing Pipe data"))),
            Poll::Ready(Ok(FrameCommit::Sent)) => Poll::Ready(Ok(count)),
            Poll::Ready(Ok(FrameCommit::Skipped)) => {
                Poll::Ready(Err(state.write_error().unwrap_or_else(|| {
                    Error::new(
                        ErrorCode::Internal,
                        PeerObservation::NotObserved,
                        "DATA commit was skipped without a terminal Pipe state",
                    )
                })))
            }
        }
    }

    pub(super) fn poll_shutdown(
        &mut self,
        state: &PipeState,
        context: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        state.write_waker.register(context.waker());
        if let Some(error) = state.terminal_failure() {
            self.outbound.cancel_wait();
            return Poll::Ready(Err(error));
        }
        if state.terminal.borrow().is_some() || state.local_fin.load(Ordering::Acquire) {
            return self.poll_flush(state, context);
        }

        match self.outbound.poll_send(context, || {
            if state.terminal.borrow().is_some() || state.local_fin.swap(true, Ordering::AcqRel) {
                None
            } else {
                Some(Frame::Fin { pipe_id: state.id })
            }
        }) {
            Poll::Pending => {
                if let Some(error) = state.terminal_failure() {
                    self.outbound.cancel_wait();
                    Poll::Ready(Err(error))
                } else if state.terminal.borrow().is_some()
                    || state.local_fin.load(Ordering::Acquire)
                {
                    self.poll_flush(state, context)
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(())) => Poll::Ready(Err(self
                .outbound
                .failure_error("session ended while sending FIN"))),
            Poll::Ready(Ok(FrameCommit::Skipped)) => self.poll_flush(state, context),
            Poll::Ready(Ok(FrameCommit::Sent)) => {
                if state.remote_fin.load(Ordering::Acquire) {
                    state.close_normal();
                }
                self.poll_flush(state, context)
            }
        }
    }

    pub(super) fn poll_close(
        &mut self,
        state: &PipeState,
        context: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        state.write_waker.register(context.waker());
        if state.terminal.borrow().is_some() {
            self.outbound.cancel_wait();
            return Poll::Ready(Ok(()));
        }

        match self.outbound.poll_send(context, || {
            state
                .close_normal()
                .then_some(Frame::Close { pipe_id: state.id })
        }) {
            Poll::Pending => {
                if state.terminal.borrow().is_some() {
                    self.outbound.cancel_wait();
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(())) => Poll::Ready(Err(self
                .outbound
                .failure_error("session ended while sending CLOSE"))),
            Poll::Ready(Ok(FrameCommit::Sent | FrameCommit::Skipped)) => Poll::Ready(Ok(())),
        }
    }

    pub(super) fn poll_reset(
        &mut self,
        state: &PipeState,
        context: &mut Context<'_>,
        code: ErrorCode,
        message: &str,
    ) -> Poll<Result<()>> {
        state.write_waker.register(context.waker());
        if state.terminal.borrow().is_some() {
            self.outbound.cancel_wait();
            return Poll::Ready(Ok(()));
        }

        match self.outbound.poll_send(context, || {
            state
                .fail(Error::new(
                    code,
                    PeerObservation::Observed,
                    message.to_owned(),
                ))
                .then(|| Frame::Reset {
                    pipe_id: state.id,
                    code: code.to_wire(),
                    message: message.to_owned(),
                })
        }) {
            Poll::Pending => {
                if state.terminal.borrow().is_some() {
                    self.outbound.cancel_wait();
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Err(())) => Poll::Ready(Err(self
                .outbound
                .failure_error("session ended while sending RESET"))),
            Poll::Ready(Ok(FrameCommit::Sent | FrameCommit::Skipped)) => Poll::Ready(Ok(())),
        }
    }

    fn poll_flush(&mut self, state: &PipeState, context: &mut Context<'_>) -> Poll<Result<()>> {
        state.write_waker.register(context.waker());
        if let Some(error) = state.terminal_failure() {
            self.outbound.cancel_wait();
            return Poll::Ready(Err(error));
        }
        self.outbound.poll_flush(context)
    }
}

impl AsyncRead for Pipe {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let pipe = self.get_mut();
        let result = pipe.reader.poll_read(
            &pipe.owner.state,
            context,
            destination.initialize_unfilled(),
        );
        finish_read(result, destination)
    }
}

impl AsyncRead for PipeReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let half = self.get_mut();
        let result = half.reader.poll_read(
            &half.owner.state,
            context,
            destination.initialize_unfilled(),
        );
        finish_read(result, destination)
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        payload: &[u8],
    ) -> Poll<io::Result<usize>> {
        let pipe = self.get_mut();
        map_io(pipe.writer.poll_write(&pipe.owner.state, context, payload))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let pipe = self.get_mut();
        map_io(pipe.writer.poll_flush(&pipe.owner.state, context))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let pipe = self.get_mut();
        map_io(pipe.writer.poll_shutdown(&pipe.owner.state, context))
    }
}

impl AsyncWrite for PipeWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        payload: &[u8],
    ) -> Poll<io::Result<usize>> {
        let half = self.get_mut();
        map_io(half.writer.poll_write(&half.owner.state, context, payload))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let half = self.get_mut();
        map_io(half.writer.poll_flush(&half.owner.state, context))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let half = self.get_mut();
        map_io(half.writer.poll_shutdown(&half.owner.state, context))
    }
}

fn finish_read(result: Poll<Result<usize>>, destination: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    match result {
        Poll::Ready(Ok(count)) => {
            destination.advance(count);
            Poll::Ready(Ok(()))
        }
        Poll::Ready(Err(error)) => Poll::Ready(Err(to_io_error(error))),
        Poll::Pending => Poll::Pending,
    }
}

fn map_io<T>(result: Poll<Result<T>>) -> Poll<io::Result<T>> {
    match result {
        Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
        Poll::Ready(Err(error)) => Poll::Ready(Err(to_io_error(error))),
        Poll::Pending => Poll::Pending,
    }
}

fn to_io_error(error: Error) -> io::Error {
    let kind = match error.code() {
        ErrorCode::InvalidArgument => io::ErrorKind::InvalidInput,
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => io::ErrorKind::PermissionDenied,
        ErrorCode::NotFound => io::ErrorKind::NotFound,
        ErrorCode::AlreadyExists => io::ErrorKind::AlreadyExists,
        ErrorCode::FailedPrecondition | ErrorCode::Cancelled => io::ErrorKind::BrokenPipe,
        ErrorCode::Unavailable => io::ErrorKind::ConnectionAborted,
        ErrorCode::DeadlineExceeded => io::ErrorKind::TimedOut,
        ErrorCode::ProtocolError => io::ErrorKind::InvalidData,
        ErrorCode::ResourceExhausted | ErrorCode::Internal => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}
