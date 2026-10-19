//! Cloneable Gateway-facing command and emergency-close surface.

use std::sync::Arc;

use bytes::Bytes;
use relaygate_protocol::{ErrorCode, PeerObservation};
use tokio::sync::{mpsc, oneshot};

use super::{ManagerCommand, SharedCounts, TransportRegistry};
use crate::transport::StreamCommand;
use crate::{
    event::{PeerCounts, PeerFailure, PeerOpenRequest, PeerStreamKey},
    identity::OpenIdentity,
};

/// Cloneable bounded command surface used by the Gateway state/effect layer.
#[derive(Clone)]
pub struct PeerHandle {
    commands: mpsc::Sender<ManagerCommand>,
    transports: TransportRegistry,
    counts: Arc<SharedCounts>,
}

impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PeerHandle")
            .field("counts", &self.counts.snapshot())
            .finish_non_exhaustive()
    }
}

impl PeerHandle {
    pub(super) fn new(
        commands: mpsc::Sender<ManagerCommand>,
        transports: TransportRegistry,
        counts: Arc<SharedCounts>,
    ) -> Self {
        Self {
            commands,
            transports,
            counts,
        }
    }

    /// Returns only after this OPEN is committed to the selected transport's
    /// ordered writer queue. The returned oneshot and [`crate::PeerEvents`] are
    /// independent bounded channels, so a very fast `Opened`/`Failed` event may
    /// be scheduled before the caller observes this return value. Both paths
    /// carry the same [`PeerStreamKey`] and the Gateway state layer correlates
    /// them without assuming cross-channel delivery order.
    pub async fn open(&self, request: PeerOpenRequest) -> Result<PeerStreamKey, PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(
            ManagerCommand::Open { request, reply },
            PeerObservation::NotObserved,
        )?;
        response.await.map_err(|_| {
            PeerFailure::not_observed(
                ErrorCode::Unavailable,
                "peer transport ended before the OPEN command was processed",
            )
        })?
    }

    pub async fn cancel_open(&self, open_identity: OpenIdentity) -> Result<(), PeerFailure> {
        self.command(|reply| ManagerCommand::Cancel {
            open_identity,
            reply,
        })
        .await
    }

    pub async fn send_opened(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        self.command(|reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Opened,
            reply,
        })
        .await
    }

    pub async fn send_failed(
        &self,
        key: PeerStreamKey,
        failure: PeerFailure,
    ) -> Result<(), PeerFailure> {
        self.command_or_close(key, |reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Failed { failure },
            reply,
        })
        .await
    }

    pub async fn send_data(&self, key: PeerStreamKey, payload: Bytes) -> Result<(), PeerFailure> {
        self.command(|reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Data { payload },
            reply,
        })
        .await
    }

    pub async fn send_fin(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        self.command(|reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Fin,
            reply,
        })
        .await
    }

    pub async fn send_close(&self, key: PeerStreamKey) -> Result<(), PeerFailure> {
        self.command_or_close(key, |reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Close,
            reply,
        })
        .await
    }

    pub async fn send_reset(
        &self,
        key: PeerStreamKey,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Result<(), PeerFailure> {
        let message = message.into();
        self.command_or_close(key, |reply| ManagerCommand::Stream {
            key,
            command: StreamCommand::Reset { code, message },
            reply,
        })
        .await
    }

    async fn command(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<(), PeerFailure>>) -> ManagerCommand,
    ) -> Result<(), PeerFailure> {
        let (reply, response) = oneshot::channel();
        self.try_manager_send(make(reply), PeerObservation::MaybeObserved)?;
        await_command_response(response).await
    }

    /// Terminal stream frames: a commit that fails leaves the transport
    /// ambiguously reusable, so it is force-closed.
    async fn command_or_close(
        &self,
        key: PeerStreamKey,
        make: impl FnOnce(oneshot::Sender<Result<(), PeerFailure>>) -> ManagerCommand,
    ) -> Result<(), PeerFailure> {
        let result = self.command(make).await;
        if result.is_err() {
            self.close_transport(key);
        }
        result
    }

    /// Force-closes the containing transport without going through the bounded
    /// manager queue. Gateway cleanup uses this when a terminal frame cannot be
    /// committed, avoiding an ambiguously reusable transport.
    pub fn close_transport(&self, key: PeerStreamKey) -> bool {
        let Ok(transports) = self.transports.read() else {
            return false;
        };
        let Some(transport) = transports.get(&key.peer_transport_id()) else {
            return false;
        };
        transport.force_close(crate::transport::TransportCloseReason::WriterFailed);
        true
    }

    #[must_use]
    pub fn counts(&self) -> PeerCounts {
        self.counts.snapshot()
    }

    fn try_manager_send(
        &self,
        command: ManagerCommand,
        observation: PeerObservation,
    ) -> Result<(), PeerFailure> {
        self.commands.try_send(command).map_err(|error| {
            let (code, message) = match error {
                mpsc::error::TrySendError::Full(_) => (
                    ErrorCode::ResourceExhausted,
                    "peer manager command queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    (ErrorCode::Unavailable, "peer manager is closed")
                }
            };
            PeerFailure::new(code, observation, message)
        })
    }
}

async fn await_command_response(
    response: oneshot::Receiver<Result<(), PeerFailure>>,
) -> Result<(), PeerFailure> {
    response.await.map_err(|_| {
        PeerFailure::maybe_observed(
            ErrorCode::Unavailable,
            "peer transport ended before the command was processed",
        )
    })?
}
