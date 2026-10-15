//! OPEN admission, cancellation, and stream-command dispatch.

#[cfg(test)]
mod tests;

use relaygate_protocol::ErrorCode;
use relaygate_route_table::GatewayId;
use tokio::sync::oneshot;

use super::{Manager, PendingOpen};
use crate::runtime::{CommandReply, ManagerCommand};
use crate::{
    error::PeerError,
    event::{PeerFailure, PeerOpenRequest, PeerStreamKey},
    identity::{OpenIdentity, PeerTransportId},
    transport::{StreamCommand, TransportCommand},
};

impl Manager {
    pub(super) fn handle_command(&mut self, command: ManagerCommand) {
        match command {
            ManagerCommand::Open { request, reply } => self.start_open(request, reply),
            ManagerCommand::Cancel {
                open_identity,
                reply,
            } => self.cancel_open(open_identity, reply),
            ManagerCommand::Stream {
                key,
                command,
                reply,
            } => self.dispatch_stream_command(key, command, reply),
        }
    }

    fn start_open(
        &mut self,
        request: PeerOpenRequest,
        reply: oneshot::Sender<Result<PeerStreamKey, PeerFailure>>,
    ) {
        let identity = request.open_identity();
        if identity.entry_gateway() != self.local_gateway_id {
            let _ = reply.send(Err(PeerFailure::not_observed(
                ErrorCode::PermissionDenied,
                "peer OPEN identity does not belong to the local Entry Gateway",
            )));
            return;
        }
        if request.target().gateway_id() == self.local_gateway_id {
            let _ = reply.send(Err(PeerFailure::not_observed(
                ErrorCode::FailedPrecondition,
                "peer OPEN target is the local Gateway",
            )));
            return;
        }
        match self.active_opens.reserve(identity) {
            Ok(true) => {}
            Ok(false) => {
                let _ = reply.send(Err(PeerFailure::not_observed(
                    ErrorCode::AlreadyExists,
                    "peer OPEN identity is already active",
                )));
                return;
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        }

        if let Some(transport_id) = self.pool.ready_transport(request.target().gateway_id()) {
            self.dispatch_open(transport_id, request, reply);
            return;
        }
        if self.pending.len() >= self.config.max_pending_opens {
            self.active_opens.release(identity);
            let _ = reply.send(Err(PeerFailure::not_observed(
                ErrorCode::ResourceExhausted,
                "peer pending OPEN limit is reached",
            )));
            return;
        }
        let remote_gateway_id = request.target().gateway_id();
        let dial_target = request.target().clone();
        self.pending.push(PendingOpen { request, reply });
        let transport_id = PeerTransportId::new();
        match self
            .pool
            .connect(self.local_gateway_id, remote_gateway_id, transport_id)
        {
            Ok(()) => {
                self.counts.update_pool(&self.pool);
                if self.handshakes_inflight >= self.config.max_handshakes {
                    self.pool.remove_transport(transport_id);
                    self.counts.update_pool(&self.pool);
                    self.fail_pending_for(
                        remote_gateway_id,
                        PeerFailure::not_observed(
                            ErrorCode::ResourceExhausted,
                            "peer handshake limit is reached",
                        ),
                    );
                } else {
                    self.start_outbound_handshake(dial_target, transport_id);
                }
            }
            Err(PeerError::AlreadyExists(_)) => {
                // A same-direction candidate is already CONNECTING. The pending
                // request is flushed if that candidate becomes READY.
            }
            Err(_) => {
                self.fail_pending_for(
                    remote_gateway_id,
                    PeerFailure::not_observed(
                        ErrorCode::FailedPrecondition,
                        "peer pair slot could not start a candidate",
                    ),
                );
            }
        }
    }

    fn dispatch_open(
        &mut self,
        transport_id: PeerTransportId,
        request: PeerOpenRequest,
        reply: oneshot::Sender<Result<PeerStreamKey, PeerFailure>>,
    ) {
        let identity = request.open_identity();
        let Some(transport) = self.transports.get(&transport_id) else {
            self.active_opens.release(identity);
            let _ = reply.send(Err(PeerFailure::not_observed(
                ErrorCode::Unavailable,
                "selected peer transport is no longer ready",
            )));
            return;
        };
        self.assignments.insert(identity, transport_id);
        if let Err(error) = transport.try_send(TransportCommand::Open { request, reply }) {
            self.assignments.remove(&identity);
            self.active_opens.release(identity);
            // The transport actor did not receive OPEN, so queue non-admission
            // is proven regardless of the generic post-commit send error.
            let _ = error;
        }
    }

    fn cancel_open(&mut self, identity: OpenIdentity, reply: CommandReply) {
        if let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.request.open_identity() == identity)
        {
            let pending = self.pending.swap_remove(index);
            self.active_opens.release(identity);
            let _ = pending.reply.send(Err(PeerFailure::not_observed(
                ErrorCode::Cancelled,
                "peer OPEN was cancelled before commit",
            )));
            let _ = reply.send(Ok(()));
            return;
        }
        let Some(transport_id) = self.assignments.get(&identity).copied() else {
            let _ = reply.send(Ok(()));
            return;
        };
        let Some(transport) = self.transports.get(&transport_id) else {
            self.assignments.remove(&identity);
            self.active_opens.release(identity);
            let _ = reply.send(Ok(()));
            return;
        };
        if let Err(error) = transport.try_send(TransportCommand::Cancel {
            open_identity: identity,
            reply,
        }) {
            transport.force_close(crate::transport::TransportCloseReason::WriterFailed);
            let _ = error;
        }
    }

    fn dispatch_stream_command(
        &self,
        key: PeerStreamKey,
        command: StreamCommand,
        reply: CommandReply,
    ) {
        if key.peer_gateway_id() == self.local_gateway_id {
            let _ = reply.send(Err(PeerFailure::maybe_observed(
                ErrorCode::FailedPrecondition,
                "peer stream key identifies the local Gateway",
            )));
            return;
        }
        let Some(transport) = self.transports.get(&key.peer_transport_id()) else {
            let _ = reply.send(Err(PeerFailure::maybe_observed(
                ErrorCode::Unavailable,
                "peer transport is no longer ready",
            )));
            return;
        };
        if transport.peer_gateway_id != key.peer_gateway_id() {
            let _ = reply.send(Err(PeerFailure::maybe_observed(
                ErrorCode::PermissionDenied,
                "peer stream key does not belong to the selected transport",
            )));
            return;
        }
        let terminal = matches!(
            &command,
            StreamCommand::Close | StreamCommand::Reset { .. } | StreamCommand::Failed { .. }
        );
        let command = TransportCommand::Stream {
            stream_id: key.stream_id(),
            command,
            reply,
        };
        if let Err(error) = transport.try_send(command) {
            if terminal {
                // Once accepted, cleanup cannot depend on the caller still
                // polling the command reply.
                transport.force_close(crate::transport::TransportCloseReason::WriterFailed);
            }
            let _ = error;
        }
    }

    pub(super) fn flush_pending_for(
        &mut self,
        remote_gateway_id: GatewayId,
        transport_id: PeerTransportId,
    ) {
        let mut retained = Vec::with_capacity(self.pending.len());
        let pending = std::mem::take(&mut self.pending);
        for open in pending {
            if open.request.target().gateway_id() == remote_gateway_id {
                self.dispatch_open(transport_id, open.request, open.reply);
            } else {
                retained.push(open);
            }
        }
        self.pending = retained;
    }

    pub(super) fn fail_pending_for(&mut self, remote_gateway_id: GatewayId, failure: PeerFailure) {
        let mut retained = Vec::with_capacity(self.pending.len());
        for pending in std::mem::take(&mut self.pending) {
            if pending.request.target().gateway_id() == remote_gateway_id {
                self.active_opens.release(pending.request.open_identity());
                let _ = pending.reply.send(Err(failure.clone()));
            } else {
                retained.push(pending);
            }
        }
        self.pending = retained;
    }
}
