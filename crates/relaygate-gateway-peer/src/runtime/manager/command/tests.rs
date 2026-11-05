use std::{
    error::Error,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use relaygate_protocol::PeerObservation;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    GatewayPeerConfig,
    identity::StreamId,
    runtime::{PeerHandle, PeerRuntime},
    transport::{TransportCloseReason, TransportClosure, TransportHandle},
};

type CommandFuture<'a> = Pin<Box<dyn Future<Output = Result<(), PeerFailure>> + 'a>>;

#[derive(Clone, Copy, Debug)]
enum CommandKind {
    Close,
    Reset,
    Failed,
    Data,
    Fin,
    Opened,
}

impl CommandKind {
    const TERMINAL: [Self; 3] = [Self::Close, Self::Reset, Self::Failed];

    fn call(self, handle: &PeerHandle, key: PeerStreamKey) -> CommandFuture<'_> {
        match self {
            Self::Close => Box::pin(handle.send_close(key)),
            Self::Reset => Box::pin(handle.send_reset(key, ErrorCode::Cancelled, "reset")),
            Self::Failed => Box::pin(handle.send_failed(
                key,
                PeerFailure::not_observed(ErrorCode::Unavailable, "failed"),
            )),
            Self::Data => Box::pin(handle.send_data(key, Bytes::from_static(b"data"))),
            Self::Fin => Box::pin(handle.send_fin(key)),
            Self::Opened => Box::pin(handle.send_opened(key)),
        }
    }

    fn matches(self, command: &StreamCommand) -> bool {
        match (self, command) {
            (Self::Close, StreamCommand::Close)
            | (Self::Fin, StreamCommand::Fin)
            | (Self::Opened, StreamCommand::Opened) => true,
            (Self::Reset, StreamCommand::Reset { code, message }) => {
                *code == ErrorCode::Cancelled && message == "reset"
            }
            (Self::Failed, StreamCommand::Failed { failure }) => {
                failure.code() == ErrorCode::Unavailable
                    && failure.observation() == PeerObservation::NotObserved
                    && failure.message() == "failed"
            }
            (Self::Data, StreamCommand::Data { payload }) => payload == b"data".as_slice(),
            _ => false,
        }
    }
}

struct Fixture {
    handle: PeerHandle,
    manager: Manager,
    key: PeerStreamKey,
    commands: mpsc::Receiver<TransportCommand>,
    closure: TransportClosure,
    unrelated_closure: TransportClosure,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let (handle, _events, runtime) = PeerRuntime::start(
            GatewayPeerConfig::new("terminal-test")?.with_queue_bounds(1, 1, 1, 1, 1),
            GatewayId::new(),
            CancellationToken::new(),
        )?;
        let mut manager = Manager::new(runtime);
        let key = PeerStreamKey::new(
            GatewayId::new(),
            PeerTransportId::new(),
            StreamId::from_raw(0),
        );
        let (sender, commands) = mpsc::channel(1);
        let closure = TransportClosure::new(CancellationToken::new());
        let transport = TransportHandle::for_test(
            key.peer_gateway_id(),
            key.peer_transport_id(),
            sender,
            closure.clone(),
        );
        manager
            .transports
            .insert(key.peer_transport_id(), transport.clone());
        manager
            .shared_transports
            .write()
            .map_err(|_| "transport registry poisoned")?
            .insert(key.peer_transport_id(), transport);

        let unrelated_id = PeerTransportId::new();
        let unrelated_closure = TransportClosure::new(CancellationToken::new());
        let (unrelated_sender, _unrelated_commands) = mpsc::channel(1);
        let unrelated = TransportHandle::for_test(
            GatewayId::new(),
            unrelated_id,
            unrelated_sender,
            unrelated_closure.clone(),
        );
        manager.transports.insert(unrelated_id, unrelated.clone());
        manager
            .shared_transports
            .write()
            .map_err(|_| "transport registry poisoned")?
            .insert(unrelated_id, unrelated);
        Ok(Self {
            handle,
            manager,
            key,
            commands,
            closure,
            unrelated_closure,
        })
    }

    fn refuse_transport_commands(&mut self, closed: bool) -> Result<ErrorCode, Box<dyn Error>> {
        if closed {
            self.commands.close();
            Ok(ErrorCode::Unavailable)
        } else {
            let (reply, _response) = oneshot::channel();
            self.manager
                .transports
                .get(&self.key.peer_transport_id())
                .ok_or("missing transport")?
                .try_send(TransportCommand::Stream {
                    stream_id: self.key.stream_id(),
                    command: StreamCommand::Data {
                        payload: Bytes::from_static(b"occupied"),
                    },
                    reply,
                })?;
            Ok(ErrorCode::ResourceExhausted)
        }
    }

    fn assert_no_terminal_queued(&mut self, closed: bool) -> Result<(), Box<dyn Error>> {
        if !closed {
            let TransportCommand::Stream {
                command: StreamCommand::Data { payload },
                ..
            } = self.commands.try_recv()?
            else {
                return Err("expected occupied queue entry".into());
            };
            assert_eq!(payload, Bytes::from_static(b"occupied"));
        }
        assert!(self.commands.try_recv().is_err());
        assert!(!self.unrelated_closure.token().is_cancelled());
        Ok(())
    }
}

fn poll(call: &mut CommandFuture<'_>) -> Poll<Result<(), PeerFailure>> {
    call.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

fn assert_failure(call: &mut CommandFuture<'_>, code: ErrorCode) -> Result<(), Box<dyn Error>> {
    let Poll::Ready(Err(failure)) = poll(call) else {
        return Err("expected command failure".into());
    };
    assert_eq!(failure.code(), code);
    assert_eq!(failure.observation(), PeerObservation::MaybeObserved);
    Ok(())
}

fn terminal_queue_failure(drop_caller: bool) -> Result<(), Box<dyn Error>> {
    for kind in CommandKind::TERMINAL {
        for closed in [false, true] {
            let mut fixture = Fixture::new()?;
            let expected_code = fixture.refuse_transport_commands(closed)?;
            let handle = fixture.handle.clone();
            let mut call = kind.call(&handle, fixture.key);
            assert!(poll(&mut call).is_pending());
            let accepted = fixture.manager.commands.try_recv()?;
            if drop_caller {
                drop(call);
                fixture.manager.handle_command(accepted);
            } else {
                fixture.manager.handle_command(accepted);
                // This must hold before the caller can run its fallback.
                assert!(
                    fixture.closure.token().is_cancelled(),
                    "{kind:?}, closed={closed}"
                );
                assert_failure(&mut call, expected_code)?;
            }
            assert!(
                fixture.closure.token().is_cancelled(),
                "{kind:?}, closed={closed}"
            );
            assert_eq!(
                fixture.closure.failure_reason(),
                Some(TransportCloseReason::WriterFailed)
            );
            fixture.assert_no_terminal_queued(closed)?;
        }
    }
    Ok(())
}

#[test]
fn terminal_queue_failure_closes_before_caller_resumes() -> Result<(), Box<dyn Error>> {
    terminal_queue_failure(false)
}

#[test]
fn terminal_queue_failure_closes_after_caller_drop() -> Result<(), Box<dyn Error>> {
    terminal_queue_failure(true)
}

#[test]
fn accepted_terminal_command_survives_caller_drop_without_force_close() -> Result<(), Box<dyn Error>>
{
    for kind in CommandKind::TERMINAL {
        for drop_caller in [false, true] {
            let mut fixture = Fixture::new()?;
            let mut call = Some(kind.call(&fixture.handle, fixture.key));
            assert!(poll(call.as_mut().ok_or("missing caller")?).is_pending());
            let accepted = fixture.manager.commands.try_recv()?;
            if drop_caller {
                drop(call.take());
            }
            fixture.manager.handle_command(accepted);
            let TransportCommand::Stream {
                stream_id,
                command,
                reply,
            } = fixture.commands.try_recv()?
            else {
                return Err("expected stream command".into());
            };
            assert_eq!(stream_id, fixture.key.stream_id());
            assert!(kind.matches(&command));
            assert_eq!(reply.send(Ok(())).is_ok(), !drop_caller);
            if let Some(mut call) = call {
                assert!(matches!(poll(&mut call), Poll::Ready(Ok(()))));
            }
            assert!(!fixture.closure.token().is_cancelled());
            assert!(!fixture.unrelated_closure.token().is_cancelled());
            assert!(fixture.commands.try_recv().is_err());
        }
    }
    Ok(())
}

#[test]
fn nonterminal_queue_refusal_does_not_force_close() -> Result<(), Box<dyn Error>> {
    for kind in [CommandKind::Data, CommandKind::Fin, CommandKind::Opened] {
        for closed in [false, true] {
            let mut fixture = Fixture::new()?;
            let code = fixture.refuse_transport_commands(closed)?;
            let handle = fixture.handle.clone();
            let mut call = kind.call(&handle, fixture.key);
            assert!(poll(&mut call).is_pending());
            let accepted = fixture.manager.commands.try_recv()?;
            fixture.manager.handle_command(accepted);
            assert_failure(&mut call, code)?;
            assert!(!fixture.closure.token().is_cancelled());
            fixture.assert_no_terminal_queued(closed)?;
        }
    }
    Ok(())
}

#[test]
fn invalid_terminal_key_does_not_force_close_in_manager() -> Result<(), Box<dyn Error>> {
    for kind in CommandKind::TERMINAL {
        let mut fixture = Fixture::new()?;
        fixture.refuse_transport_commands(false)?;
        for (gateway_id, transport_id, code) in [
            (
                fixture.manager.local_gateway_id,
                fixture.key.peer_transport_id(),
                ErrorCode::FailedPrecondition,
            ),
            (
                GatewayId::new(),
                fixture.key.peer_transport_id(),
                ErrorCode::PermissionDenied,
            ),
            (
                fixture.key.peer_gateway_id(),
                PeerTransportId::new(),
                ErrorCode::Unavailable,
            ),
        ] {
            let key = PeerStreamKey::new(gateway_id, transport_id, fixture.key.stream_id());
            let mut call = kind.call(&fixture.handle, key);
            assert!(poll(&mut call).is_pending());
            let ManagerCommand::Stream { key, command, .. } =
                fixture.manager.commands.try_recv()?
            else {
                return Err("expected stream command".into());
            };
            drop(call);
            // Inspect the manager's response without running caller fallback.
            let (reply, mut response) = oneshot::channel();
            fixture.manager.handle_command(ManagerCommand::Stream {
                key,
                command,
                reply,
            });
            let failure = response
                .try_recv()?
                .err()
                .ok_or("expected invalid key failure")?;
            assert_eq!(failure.code(), code);
            assert_eq!(failure.observation(), PeerObservation::MaybeObserved);
            assert!(!fixture.closure.token().is_cancelled());
            assert!(!fixture.unrelated_closure.token().is_cancelled());
        }
        fixture.assert_no_terminal_queued(false)?;
    }
    Ok(())
}

#[test]
fn terminal_manager_refusal_keeps_caller_force_close() -> Result<(), Box<dyn Error>> {
    for kind in CommandKind::TERMINAL {
        let fixture = Fixture::new()?;
        let mut occupied = CommandKind::Data.call(&fixture.handle, fixture.key);
        assert!(poll(&mut occupied).is_pending());
        let mut call = kind.call(&fixture.handle, fixture.key);
        assert_failure(&mut call, ErrorCode::ResourceExhausted)?;
        assert!(fixture.closure.token().is_cancelled());
        assert_eq!(
            fixture.closure.failure_reason(),
            Some(TransportCloseReason::WriterFailed)
        );
        assert!(!fixture.unrelated_closure.token().is_cancelled());
    }
    Ok(())
}
