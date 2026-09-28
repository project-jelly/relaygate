mod relay;
mod runtime;
mod state;
use std::sync::Arc;

use relaygate_protocol::{PipeId, SessionId};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Destination, Error, PeerObservation, Pipe, Result, lifetime::RuntimeLifetime,
    resource::LivePipeReservation,
};

pub use self::relay::{Relay, RelayStatus, RelayStatusSubscription};
// Also the import path the runtime submodules use for these state items.
#[cfg(test)]
use self::state::{ListenerLifecycle, ListenerRuntime};
use self::state::{ListenerState, RelayInner, is_current_desired};

/// Current state of one desired Listener handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListenerStatus {
    /// No current binding exists while initial publication or republish runs.
    Registering,
    /// A current Gateway binding can receive new Pipes.
    Active,
    /// A transient republish failure awaits bounded retry; the Relay may remain active.
    Suspended,
    /// Republish failed permanently; recreate the Listener with new inputs.
    Blocked,
    /// The handle is terminal and will yield no more Pipes.
    Closed,
}

/// Coalescing view of Listener status; intermediate transitions may be skipped.
pub struct ListenerStatusSubscription {
    status: watch::Receiver<ListenerStatus>,
}

/// A desired publication for one [`Destination`].
///
/// The SDK keeps the publication desired across managed Relay reconnects until
/// the Listener is closed or dropped.
pub struct Listener {
    inner: Arc<RelayInner>,
    _lifetime: Arc<RuntimeLifetime>,
    state: Arc<ListenerState>,
}

pub(super) struct RelaySession {
    pub(super) id: SessionId,
    pub(super) next_connection_id: Mutex<u64>,
    pub(super) commands: mpsc::Sender<RelayCommand>,
    pub(super) cancellations: mpsc::UnboundedSender<PipeId>,
    pub(super) cancel: CancellationToken,
}

pub(super) enum RelayCommand {
    Dial {
        connection_id: u64,
        destination: Destination,
        access_token: relaygate_protocol::BearerToken,
        response: oneshot::Sender<Result<Pipe>>,
        resources: LivePipeReservation,
    },
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Listener")
            .field("destination", &self.state.destination)
            .field("status", &self.status())
            .finish()
    }
}

impl Listener {
    /// Returns the destination owned by this Listener.
    #[must_use]
    pub fn destination(&self) -> &Destination {
        &self.state.destination
    }

    /// Returns this Listener's latest status.
    #[must_use]
    pub fn status(&self) -> ListenerStatus {
        *self.state.status.borrow()
    }

    /// Returns the error for the current registration failure, if any.
    ///
    /// Successful publication and close clear the error. An active Relay
    /// session does not imply that this Listener's registration is active.
    #[must_use]
    pub fn last_error(&self) -> Option<Error> {
        self.state.last_error()
    }

    /// Subscribes to coalesced Listener status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> ListenerStatusSubscription {
        ListenerStatusSubscription {
            status: self.state.status.subscribe(),
        }
    }

    /// Returns one incoming Pipe exactly once.
    ///
    /// While registration is suspended or being recovered, this waits for a
    /// Pipe from the next active Relay session. Unaccepted Pipes owned by an
    /// ended session are discarded. A blocked or closed Listener returns its
    /// terminal error without yielding an older queued Pipe.
    pub async fn accept(&self) -> Result<Pipe> {
        let mut status = self.state.status.subscribe();
        loop {
            let current_status = *status.borrow();
            match current_status {
                ListenerStatus::Blocked => {
                    self.state.drain_unaccepted(true).await;
                    return Err(self.state.blocked_error());
                }
                ListenerStatus::Closed => {
                    self.state.drain_unaccepted(true).await;
                    return Err(Error::closed());
                }
                ListenerStatus::Registering | ListenerStatus::Suspended => {
                    if status.changed().await.is_err() {
                        return Err(Error::closed());
                    }
                    continue;
                }
                ListenerStatus::Active => {}
            }

            // Hold the single-consumer lane only while ACTIVE. A session-end
            // status change wins the biased select, releases this lock, and
            // lets the session actor drain the old queue before reconnecting.
            let mut incoming = self.state.incoming_rx.lock().await;
            if *status.borrow() != ListenerStatus::Active {
                drop(incoming);
                continue;
            }
            match incoming.try_recv() {
                Ok(pipe) => {
                    drop(incoming);
                    if let Some(result) = self.classify_received_pipe(pipe, &status) {
                        return result;
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(Error::closed()),
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            tokio::select! {
                biased;
                changed = status.changed() => {
                    drop(incoming);
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
                pipe = incoming.recv() => {
                    let pipe = pipe.ok_or_else(Error::closed)?;
                    drop(incoming);
                    if let Some(result) = self.classify_received_pipe(pipe, &status) {
                        return result;
                    }
                }
            }
        }
    }

    /// Removes this desired Listener without closing sibling handles or Pipes
    /// that the application already accepted.
    pub async fn close(&self) -> Result<()> {
        self.inner.detach_listener(&self.state);
        self.drain_unaccepted().await
    }
}

impl ListenerStatusSubscription {
    /// Returns the latest status and marks its version as observed.
    #[must_use]
    pub fn current(&mut self) -> ListenerStatus {
        *self.status.borrow_and_update()
    }

    /// Waits for a newer version and returns the latest value.
    ///
    /// Intermediate transitions may be skipped. `Closed` is still returned as
    /// a status; `None` means the Listener state itself has been dropped.
    pub async fn changed(&mut self) -> Option<ListenerStatus> {
        self.status.changed().await.ok()?;
        Some(*self.status.borrow_and_update())
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.inner.drop_listener(&self.state);
    }
}

impl Listener {
    fn classify_received_pipe(
        &self,
        pipe: Pipe,
        status: &watch::Receiver<ListenerStatus>,
    ) -> Option<Result<Pipe>> {
        // The final ACTIVE + non-terminal observation is accept's success
        // linearization point. A later session/peer failure is observed by
        // Pipe I/O, just like a socket may close immediately after accept.
        // Copied out so the watch read guard is released before
        // `blocked_error` takes the state lock (see `listen` for the order).
        let observed = *status.borrow();
        match observed {
            ListenerStatus::Active if !pipe.is_terminal() => Some(Ok(pipe)),
            ListenerStatus::Active => {
                drop(pipe);
                None
            }
            ListenerStatus::Blocked => {
                drop(pipe);
                Some(Err(self.state.blocked_error()))
            }
            ListenerStatus::Closed => {
                drop(pipe);
                Some(Err(Error::closed()))
            }
            ListenerStatus::Registering | ListenerStatus::Suspended => {
                drop(pipe);
                None
            }
        }
    }

    async fn drain_unaccepted(&self) -> Result<()> {
        let operation = async {
            self.state.drain_unaccepted(true).await;
        };
        timeout(self.inner.config.operation_timeout, operation)
            .await
            .map_err(|_| Error::deadline(PeerObservation::MaybeObserved))
    }
}
