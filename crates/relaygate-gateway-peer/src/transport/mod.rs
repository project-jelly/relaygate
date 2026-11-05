//! Bounded reusable Gateway peer transport runtime.
//!
//! The module surface owns commands, scoped transport identity, and the
//! current active OPEN set. `actor` owns protocol state transitions while
//! `writer` owns the single ordered socket sink.

mod actor;
mod command;
mod inbound;
mod liveness;
mod state;
mod writer;

#[cfg(test)]
mod tests;

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use relaygate_protocol::ErrorCode;
use relaygate_route_table::GatewayId;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use super::{
    config::GatewayPeerConfig,
    event::{LostPeerStream, PeerEvent, PeerFailure, PeerOpenRequest, PeerStreamKey},
    handshake::EstablishedPeer,
    identity::{OpenIdentity, PeerTransportId, StreamId},
};

type CommandReply = oneshot::Sender<Result<(), PeerFailure>>;
type OpenReply = oneshot::Sender<Result<PeerStreamKey, PeerFailure>>;

/// A command addressed to one already-open stream. The manager keys it by
/// [`PeerStreamKey`] and the transport actor by [`StreamId`]; the payload is
/// the same on both sides.
#[derive(Debug)]
pub(crate) enum StreamCommand {
    Opened,
    Failed { failure: PeerFailure },
    Data { payload: bytes::Bytes },
    Fin,
    Close,
    Reset { code: ErrorCode, message: String },
}

#[derive(Debug)]
pub(super) enum TransportCommand {
    Open {
        request: PeerOpenRequest,
        reply: OpenReply,
    },
    Cancel {
        open_identity: OpenIdentity,
        reply: CommandReply,
    },
    Stream {
        stream_id: StreamId,
        command: StreamCommand,
        reply: CommandReply,
    },
}

#[derive(Debug)]
pub(super) enum TransportNotice {
    Event(PeerEvent),
    StreamEnded {
        key: PeerStreamKey,
        open_identity: OpenIdentity,
    },
    AttemptEnded {
        open_identity: OpenIdentity,
    },
    TransportLost {
        peer_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        reason: TransportCloseReason,
        streams: Vec<LostPeerStream>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransportCloseReason {
    LocalClose,
    RemoteClosed,
    ProtocolError,
    WriterFailed,
    HeartbeatTimeout,
    IdleRetired,
}

#[derive(Debug, Clone)]
pub(super) struct TransportClosure {
    token: CancellationToken,
    failure_reason: Arc<Mutex<Option<TransportCloseReason>>>,
}

impl TransportClosure {
    pub(super) fn new(token: CancellationToken) -> Self {
        Self {
            token,
            failure_reason: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn token(&self) -> &CancellationToken {
        &self.token
    }

    pub(super) fn close(&self) {
        self.token.cancel();
    }

    pub(super) fn fail(&self, reason: TransportCloseReason) {
        if let Ok(mut current) = self.failure_reason.lock()
            && current.is_none()
        {
            *current = Some(reason);
        }
        self.token.cancel();
    }

    pub(super) fn failure_reason(&self) -> Option<TransportCloseReason> {
        self.failure_reason
            .lock()
            .map_or(Some(TransportCloseReason::ProtocolError), |current| {
                *current
            })
    }
}

impl TransportCloseReason {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::LocalClose => "local_close",
            Self::RemoteClosed => "remote_closed",
            Self::ProtocolError => "protocol_error",
            Self::WriterFailed => "writer_failed",
            Self::HeartbeatTimeout => "heartbeat_timeout",
            Self::IdleRetired => "idle_retired",
        }
    }
}

#[derive(Clone)]
pub(super) struct TransportHandle {
    pub(super) peer_gateway_id: GatewayId,
    pub(super) peer_transport_id: PeerTransportId,
    commands: mpsc::Sender<TransportCommand>,
    closure: TransportClosure,
}

impl std::fmt::Debug for TransportHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportHandle")
            .field("peer_gateway_id", &self.peer_gateway_id)
            .field("peer_transport_id", &self.peer_transport_id)
            .finish_non_exhaustive()
    }
}

impl TransportHandle {
    #[cfg(test)]
    pub(crate) fn for_test(
        peer_gateway_id: GatewayId,
        peer_transport_id: PeerTransportId,
        commands: mpsc::Sender<TransportCommand>,
        closure: TransportClosure,
    ) -> Self {
        Self {
            peer_gateway_id,
            peer_transport_id,
            commands,
            closure,
        }
    }

    pub(super) fn try_send(&self, command: TransportCommand) -> Result<(), PeerFailure> {
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => Err(command.queue_failure(
                ErrorCode::ResourceExhausted,
                "peer transport command queue is full",
            )),
            Err(mpsc::error::TrySendError::Closed(command)) => {
                Err(command.queue_failure(ErrorCode::Unavailable, "peer transport is closed"))
            }
        }
    }

    pub(super) fn close(&self) {
        self.closure.close();
    }

    pub(super) fn force_close(&self, reason: TransportCloseReason) {
        self.closure.fail(reason);
    }
}

impl TransportCommand {
    fn queue_failure(self, code: ErrorCode, message: &'static str) -> PeerFailure {
        let failure = if matches!(&self, Self::Open { .. }) {
            PeerFailure::not_observed(code, message)
        } else {
            PeerFailure::maybe_observed(code, message)
        };
        match self {
            Self::Open { reply, .. } => {
                let _ = reply.send(Err(failure.clone()));
            }
            Self::Cancel { reply, .. } | Self::Stream { reply, .. } => {
                let _ = reply.send(Err(failure.clone()));
            }
        }
        failure
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ActiveIdentityKey {
    entry_gateway: GatewayId,
    origin_session: uuid::Uuid,
    connection_id: u64,
}

impl From<OpenIdentity> for ActiveIdentityKey {
    fn from(value: OpenIdentity) -> Self {
        Self {
            entry_gateway: value.entry_gateway(),
            origin_session: value.origin_session().as_uuid(),
            connection_id: value.connection_id(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ActiveOpenSet(Mutex<BTreeSet<ActiveIdentityKey>>);

impl ActiveOpenSet {
    pub(super) fn reserve(&self, identity: OpenIdentity) -> Result<bool, PeerFailure> {
        let mut active = self.0.lock().map_err(|_| {
            PeerFailure::not_observed(ErrorCode::Internal, "active peer OPEN set is unavailable")
        })?;
        Ok(active.insert(identity.into()))
    }

    pub(super) fn release(&self, identity: OpenIdentity) {
        if let Ok(mut active) = self.0.lock() {
            active.remove(&identity.into());
        }
    }

    pub(super) fn contains(&self, identity: OpenIdentity) -> bool {
        self.0
            .lock()
            .is_ok_and(|active| active.contains(&identity.into()))
    }
}

pub(super) fn spawn_transport(
    established: EstablishedPeer,
    config: &GatewayPeerConfig,
    notices: mpsc::Sender<TransportNotice>,
    active_opens: Arc<ActiveOpenSet>,
    stream_count: Arc<AtomicUsize>,
    parent_shutdown: &CancellationToken,
    tasks: &mut JoinSet<()>,
) -> TransportHandle {
    let (commands, command_receiver) = mpsc::channel(config.transport_queue_capacity);
    let closure = TransportClosure::new(parent_shutdown.child_token());
    let handle = TransportHandle {
        peer_gateway_id: established.remote_gateway_id,
        peer_transport_id: established.peer_transport_id,
        commands,
        closure: closure.clone(),
    };
    let actor_config = config.clone();
    tasks.spawn(async move {
        actor::run_transport_actor(
            established,
            actor_config,
            command_receiver,
            notices,
            active_opens,
            stream_count,
            closure,
        )
        .await;
    });
    handle
}
