//! The shared Relay handle: connect, dial, listen and status. The Listener
//! half of the API stays in the parent module.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak, atomic::AtomicU64},
};

use relaygate_protocol::{BearerToken, PipeId};
use tokio::{
    sync::{Notify, mpsc, oneshot, watch},
    time::{sleep_until, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{
    Listener, ListenerStatus, RelayCommand,
    runtime::relay_supervisor,
    state::{ListenerLifecycle, ListenerRuntime, ListenerState, RelayInner},
};
use crate::{
    AccessAction, AccessTokenRequest, AccessTokenSource, Config, Destination, Error, ErrorCode,
    PeerObservation, Pipe, Result,
    lifetime::RuntimeLifetime,
    resource::RelayResources,
    session::{ReconnectBackoff, establish_initial},
};

/// Current state of the shared Relay session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelayStatus {
    /// A current HELLO/WELCOME transport session is installed.
    Active,
    /// No current session is installed and managed reconnect is running.
    Reconnecting,
    /// The runtime is terminal and will not reconnect.
    Closed,
}

/// Coalescing view of Relay status; intermediate transitions may be skipped.
pub struct RelayStatusSubscription {
    status: watch::Receiver<RelayStatus>,
}

/// A shared application session to one RelayGate Gateway.
///
/// A Relay manages reconnect and republishes each desired [`Listener`]. Pipes
/// remain session-scoped and are never replayed across reconnects.
#[derive(Clone)]
pub struct Relay {
    inner: Arc<RelayInner>,
    _lifetime: Arc<RuntimeLifetime>,
}

struct DialGuard {
    cancellations: mpsc::UnboundedSender<PipeId>,
    pipe_id: PipeId,
    armed: bool,
}

impl Drop for DialGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cancellations.send(self.pipe_id);
        }
    }
}

struct ListenGuard {
    inner: Weak<RelayInner>,
    state: Arc<ListenerState>,
    armed: bool,
}

impl Drop for ListenGuard {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            inner.terminate_initial_listener(
                &self.state,
                ErrorCode::Cancelled,
                "listen operation was cancelled",
            );
        }
    }
}

impl Relay {
    /// Connects the initial shared Relay session and starts managed
    /// reconnection for every desired Listener handle.
    pub async fn connect(config: Config) -> Result<Self> {
        config.validate()?;
        let established = establish_initial(&config).await?;
        let (current, _) = watch::channel(None);
        let (status, _) = watch::channel(RelayStatus::Active);
        let cancel = CancellationToken::new();
        let lifetime = Arc::new(RuntimeLifetime::new(cancel.clone()));
        let inner = Arc::new(RelayInner {
            resources: RelayResources::new(config.resource_limits),
            reconnect_degraded: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            desired_settlement_calls: AtomicU64::new(0),
            republish_retry_epoch: Arc::new(AtomicU64::new(0)),
            republish_backoff: Arc::new(StdMutex::new(ReconnectBackoff::new(
                config.reconnect_initial,
                config.reconnect_maximum,
            ))),
            config,
            desired: StdMutex::new(HashMap::new()),
            current,
            status,
            last_error: StdMutex::new(None),
            reconcile: Arc::new(Notify::new()),
            cancel,
            lifetime: Arc::downgrade(&lifetime),
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(relay_supervisor(
            Arc::clone(&inner),
            established,
            Some(ready_tx),
        ));
        ready_rx.await.map_err(|_| Error::closed())?;
        Ok(Self {
            inner,
            _lifetime: lifetime,
        })
    }

    #[cfg(test)]
    pub(crate) fn desired_settlement_calls(&self) -> u64 {
        self.inner
            .desired_settlement_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Creates one desired Listener for a destination and waits until its initial
    /// Gateway-local binding is active.
    pub async fn listen(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Listener> {
        let deadline = self.inner.config.operation_deadline()?;
        let limits = self.inner.config.resource_limits;
        let (incoming_tx, incoming_rx) = mpsc::channel(limits.max_pending_pipes_per_listener);
        let (status, _) = watch::channel(ListenerStatus::Registering);
        let state = Arc::new(ListenerState {
            destination: destination.clone(),
            access_token_source,
            status,
            incoming_tx,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
            initial_deadline: deadline,
            runtime: StdMutex::new(ListenerRuntime::new(ListenerLifecycle::Pending)),
            live_pipe_slots: self
                .inner
                .resources
                .listener_slots(limits.max_live_pipes_per_listener),
        });
        {
            let mut desired = self.inner.desired.lock().map_err(|_| {
                self.inner.cancel.cancel();
                Error::new(
                    ErrorCode::Internal,
                    PeerObservation::NotObserved,
                    "Listener registry lock is poisoned",
                )
            })?;
            if desired.contains_key(&destination) {
                return Err(Error::new(
                    ErrorCode::AlreadyExists,
                    PeerObservation::NotObserved,
                    "a non-closed Listener already owns this destination in the Relay",
                ));
            }
            desired.insert(destination, Arc::clone(&state));
        }

        let mut guard = ListenGuard {
            inner: Arc::downgrade(&self.inner),
            state: Arc::clone(&state),
            armed: true,
        };
        self.inner.reconcile.notify_one();

        let mut status = state.status.subscribe();
        loop {
            // Copy the status out first: a `match` on `*status.borrow()` keeps
            // the watch read guard alive through the arms, and the state
            // methods called below publish through that watch while holding
            // the state lock, so matching on the guard would invert the order.
            let observed = *status.borrow();
            match observed {
                ListenerStatus::Active => {
                    if !state.promote_returned() {
                        continue;
                    }
                    guard.armed = false;
                    return Ok(Listener {
                        inner: Arc::clone(&self.inner),
                        _lifetime: Arc::clone(&self._lifetime),
                        state,
                    });
                }
                ListenerStatus::Blocked => {
                    return Err(state.last_error().unwrap_or_else(|| {
                        Error::new(
                            ErrorCode::PermissionDenied,
                            PeerObservation::NotObserved,
                            "Listener registration is blocked",
                        )
                    }));
                }
                ListenerStatus::Closed => {
                    return Err(state.last_error().unwrap_or_else(Error::closed));
                }
                ListenerStatus::Registering | ListenerStatus::Suspended => {}
            }
            tokio::select! {
                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                _ = sleep_until(deadline) => {
                    match self.inner.expire_initial_listener(
                        &state,
                        ErrorCode::DeadlineExceeded,
                        "operation deadline exceeded",
                    ) {
                        Some(error) => return Err(error),
                        None => continue,
                    }
                }
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
            }
        }
    }

    /// Opens one Pipe to a destination. A committed dial is never replayed.
    pub async fn dial(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Pipe> {
        crate::observability::observe("dial", self.dial_inner(destination, access_token_source))
            .await
    }

    async fn dial_inner(
        &self,
        destination: Destination,
        access_token_source: AccessTokenSource,
    ) -> Result<Pipe> {
        let deadline = self.inner.config.operation_deadline()?;
        let mut current = self.inner.current.subscribe();
        let mut supplied_access_token: Option<BearerToken> = None;
        loop {
            if self.inner.cancel.is_cancelled() {
                return Err(Error::closed());
            }
            let session = current.borrow().clone();
            if let Some(session) = session {
                let access_token = match supplied_access_token.as_ref() {
                    Some(access_token) => access_token.clone(),
                    None => {
                        let access_token = access_token_source
                            .supply_with_retry(
                                AccessTokenRequest {
                                    action: AccessAction::Dial,
                                    destination: destination.clone(),
                                },
                                deadline,
                                ReconnectBackoff::new(
                                    self.inner.config.reconnect_initial,
                                    self.inner.config.reconnect_maximum,
                                ),
                                &self.inner.cancel,
                            )
                            .await?;
                        supplied_access_token = Some(access_token.clone());
                        access_token
                    }
                };
                let resources = self.inner.resources.try_reserve_outgoing()?;
                // The lock spans the command send on purpose: the Gateway
                // rejects a DIAL whose ConnectionId is not above every id it
                // has seen on the session (SPEC 005 DIAL-001/DIAL-002), so ids
                // must reach the session loop in allocation order. An atomic
                // counter would let two concurrent dials enqueue out of order.
                let mut next_connection_id =
                    timeout_at(deadline, session.next_connection_id.lock())
                        .await
                        .map_err(|_| Error::deadline(PeerObservation::NotObserved))?;
                let connection_id = *next_connection_id;
                *next_connection_id = connection_id.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::ResourceExhausted,
                        PeerObservation::NotObserved,
                        "RelaySession exhausted ConnectionId space",
                    )
                })?;
                let pipe_id = PipeId::new(session.id, connection_id);
                let (response_tx, response_rx) = oneshot::channel();
                let committed = timeout_at(
                    deadline,
                    session.commands.send(RelayCommand::Dial {
                        connection_id,
                        destination: destination.clone(),
                        access_token: access_token.clone(),
                        response: response_tx,
                        resources,
                    }),
                )
                .await;
                drop(next_connection_id);
                match committed {
                    Ok(Ok(())) => {
                        let mut guard = DialGuard {
                            cancellations: session.cancellations.clone(),
                            pipe_id,
                            armed: true,
                        };
                        return match timeout_at(deadline, response_rx).await {
                            Ok(Ok(result)) => {
                                guard.armed = false;
                                result
                            }
                            Ok(Err(_)) => {
                                guard.armed = false;
                                Err(Error::maybe_observed(
                                    "RelaySession ended after DIAL commit",
                                ))
                            }
                            Err(_) => {
                                session.cancel.cancel();
                                Err(Error::transport_deadline("DIAL response deadline exceeded")
                                    .with_observation(PeerObservation::MaybeObserved))
                            }
                        };
                    }
                    Ok(Err(_)) => {
                        if current
                            .borrow()
                            .as_ref()
                            .is_some_and(|active| Arc::ptr_eq(active, &session))
                        {
                            tokio::select! {
                                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                                _ = sleep_until(deadline) => {
                                    return Err(Error::transport_deadline("DIAL deadline exceeded while waiting for a RelaySession"));
                                }
                                changed = current.changed() => {
                                    if changed.is_err() { return Err(Error::closed()); }
                                }
                            }
                        }
                    }
                    Err(_) => return Err(Error::deadline(PeerObservation::NotObserved)),
                }
                continue;
            }

            tokio::select! {
                _ = self.inner.cancel.cancelled() => return Err(Error::closed()),
                _ = sleep_until(deadline) => {
                    return Err(Error::transport_deadline("DIAL deadline exceeded while waiting for a RelaySession"));
                }
                changed = current.changed() => {
                    if changed.is_err() { return Err(Error::closed()); }
                }
            }
        }
    }

    /// Stops managed reconnection and closes all desired Listener handles.
    pub fn close(&self) {
        self.inner.cancel.cancel();
        self.inner.close_all();
    }

    /// Returns the latest shared Relay session status.
    #[must_use]
    pub fn status(&self) -> RelayStatus {
        *self.inner.status.borrow()
    }

    /// Returns the latest session failure or failed reconnect attempt, if any.
    ///
    /// The error is cleared when a new session becomes active or the Relay is
    /// closed. The session failure is recorded before publishing Reconnecting;
    /// later failed attempts replace it. This is a snapshot, not an error history.
    #[must_use]
    pub fn last_error(&self) -> Option<Error> {
        self.inner.relay_error()
    }

    /// Subscribes to coalesced Relay status changes.
    #[must_use]
    pub fn subscribe_status(&self) -> RelayStatusSubscription {
        RelayStatusSubscription {
            status: self.inner.status.subscribe(),
        }
    }

    /// Waits until a current Relay session is active.
    ///
    /// Returns [`ErrorCode::Cancelled`] when the Relay has already closed.
    pub async fn wait_ready(&self) -> Result<()> {
        let mut status = self.inner.status.subscribe();
        let mut current = self.inner.current.subscribe();
        loop {
            match *status.borrow() {
                RelayStatus::Active if current.borrow().is_some() => return Ok(()),
                RelayStatus::Active => {}
                RelayStatus::Closed => return Err(Error::closed()),
                RelayStatus::Reconnecting => {}
            }
            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
                changed = current.changed() => {
                    if changed.is_err() {
                        return Err(Error::closed());
                    }
                }
            }
        }
    }
}

impl RelayStatusSubscription {
    /// Returns the latest status and marks its version as observed.
    #[must_use]
    pub fn current(&mut self) -> RelayStatus {
        *self.status.borrow_and_update()
    }

    /// Waits for a newer version and returns the latest value.
    ///
    /// Intermediate transitions may be skipped. `Closed` is still returned as
    /// a status; `None` means the Relay runtime itself has been dropped.
    pub async fn changed(&mut self) -> Option<RelayStatus> {
        self.status.changed().await.ok()?;
        Some(*self.status.borrow_and_update())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, time::Duration};

    use futures_util::{SinkExt, StreamExt};
    use relaygate_protocol::{BindingId, DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, SessionId};
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};
    use tokio_util::codec::Framed;

    use super::{Relay, RelayStatus};
    use crate::{AccessToken, AccessTokenSource, Config, Destination, ErrorCode};

    type TestResult<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

    #[tokio::test]
    async fn connect_returns_after_initial_current_session_is_installed() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            if !matches!(
                transport.next().await.ok_or("SDK closed before HELLO")??,
                Frame::Hello
            ) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            transport
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            let _ = shutdown_rx.await;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let relay = Relay::connect(Config::new_insecure_for_tests(address.to_string())).await?;
        assert_eq!(relay.status(), RelayStatus::Active);
        assert!(relay.inner.current.borrow().is_some());
        timeout(Duration::from_secs(1), relay.wait_ready()).await??;

        relay.close();
        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_destination_listen_returns_already_exists() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut transport = Framed::new(stream, FrameCodec::new(DEFAULT_MAX_FRAME_LEN));
            if !matches!(
                transport.next().await.ok_or("SDK closed before HELLO")??,
                Frame::Hello
            ) {
                return Err::<(), Box<dyn StdError + Send + Sync>>(
                    "SDK first frame was not HELLO".into(),
                );
            }
            transport
                .send(Frame::Welcome {
                    session_id: SessionId::new(),
                })
                .await?;
            let request_id = match transport
                .next()
                .await
                .ok_or("SDK closed before PUBLISH")??
            {
                Frame::Publish { request_id, .. } => request_id,
                _ => return Err("SDK did not send PUBLISH".into()),
            };
            transport
                .send(Frame::Published {
                    request_id,
                    binding_id: BindingId::new(),
                })
                .await?;
            let _ = shutdown_rx.await;
            Ok::<(), Box<dyn StdError + Send + Sync>>(())
        });

        let relay = Relay::connect(Config::new_insecure_for_tests(address.to_string())).await?;
        let destination: Destination = "test/duplicate-listener".parse()?;
        let first = relay
            .listen(
                destination.clone(),
                AccessTokenSource::static_token(AccessToken::new("first-token")?),
            )
            .await?;
        let duplicate = relay
            .listen(
                destination,
                AccessTokenSource::static_token(AccessToken::new("second-token")?),
            )
            .await;
        let error = match duplicate {
            Ok(_) => return Err("duplicate listen unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::AlreadyExists);
        assert_eq!(first.status(), super::ListenerStatus::Active);

        relay.close();
        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }
}
