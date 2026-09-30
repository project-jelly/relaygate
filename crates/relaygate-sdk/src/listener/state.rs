use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::{
    sync::{Notify, Semaphore, mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{ListenerStatus, RelaySession, RelayStatus};
use crate::{
    AccessTokenSource, Config, Destination, Error, ErrorCode, PeerObservation, Pipe,
    lifetime::RuntimeLifetime, resource::RelayResources, session::ReconnectBackoff,
};

pub(super) struct RelayInner {
    pub(super) config: Config,
    pub(super) desired: StdMutex<HashMap<Destination, Arc<ListenerState>>>,
    pub(super) current: watch::Sender<Option<Arc<RelaySession>>>,
    pub(super) status: watch::Sender<RelayStatus>,
    pub(super) last_error: StdMutex<Option<Error>>,
    pub(super) reconcile: Arc<Notify>,
    pub(super) cancel: CancellationToken,
    pub(super) lifetime: Weak<RuntimeLifetime>,
    pub(super) resources: RelayResources,
    pub(super) republish_retry_epoch: Arc<AtomicU64>,
    pub(super) republish_backoff: Arc<StdMutex<ReconnectBackoff>>,
    pub(super) reconnect_degraded: AtomicBool,
    #[cfg(test)]
    pub(super) desired_settlement_calls: AtomicU64,
}

pub(super) struct ListenerState {
    pub(super) destination: Destination,
    pub(super) access_token_source: AccessTokenSource,
    pub(super) status: watch::Sender<ListenerStatus>,
    pub(super) incoming_tx: mpsc::Sender<Pipe>,
    pub(super) incoming_rx: tokio::sync::Mutex<mpsc::Receiver<Pipe>>,
    pub(super) initial_deadline: Instant,
    /// The one logical state machine behind this Listener. Status is published
    /// to `status` while this lock is held, so a reader that takes the lock
    /// sees a status no older than the lifecycle it observes.
    pub(super) runtime: StdMutex<ListenerRuntime>,
    pub(super) live_pipe_slots: Arc<Semaphore>,
}

pub(super) struct ListenerRuntime {
    pub(super) lifecycle: ListenerLifecycle,
    registration_progress: RegistrationProgress,
    pub(super) last_error: Option<Error>,
}

#[derive(Clone, Copy, Default)]
enum RegistrationProgress {
    #[default]
    Waiting,
    TokenSource,
    Committed,
}

impl ListenerRuntime {
    pub(super) const fn new(lifecycle: ListenerLifecycle) -> Self {
        Self {
            lifecycle,
            registration_progress: RegistrationProgress::Waiting,
            last_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ListenerLifecycle {
    Pending,
    Returned,
    Terminal,
}

impl RelayInner {
    pub(super) fn republish_retry_is_ready(&self) -> bool {
        self.republish_retry_epoch.load(Ordering::Acquire) & 1 == 0
    }

    pub(super) fn schedule_reconcile(&self) {
        let Ok(ready_epoch) =
            self.republish_retry_epoch
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                    (epoch & 1 == 0).then(|| epoch.wrapping_add(1))
                })
        else {
            return;
        };
        let scheduled_epoch = ready_epoch.wrapping_add(1);
        let delay = match self.republish_backoff.lock() {
            Ok(mut backoff) => backoff.next_delay(),
            Err(_) => {
                let _ = self.republish_retry_epoch.compare_exchange(
                    scheduled_epoch,
                    scheduled_epoch.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.cancel.cancel();
                return;
            }
        };
        let cancel = self.cancel.clone();
        let reconcile = Arc::clone(&self.reconcile);
        let epoch = Arc::clone(&self.republish_retry_epoch);
        tokio::spawn(async move {
            let should_notify = tokio::select! {
                _ = cancel.cancelled() => false,
                _ = tokio::time::sleep(delay) => true,
            };
            let current = epoch.compare_exchange(
                scheduled_epoch,
                scheduled_epoch.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            if should_notify && current.is_ok() {
                reconcile.notify_one();
            }
        });
    }

    pub(super) fn reset_republish_backoff(&self) {
        match self.republish_backoff.lock() {
            Ok(mut backoff) => {
                backoff.reset();
                let _ = self.republish_retry_epoch.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |epoch| {
                        Some(if epoch & 1 == 1 {
                            epoch.wrapping_add(1)
                        } else {
                            epoch
                        })
                    },
                );
            }
            Err(_) => self.cancel.cancel(),
        }
    }

    pub(super) fn desired_settlement(&self) -> DesiredSettlement {
        #[cfg(test)]
        self.desired_settlement_calls
            .fetch_add(1, Ordering::Relaxed);
        self.desired
            .lock()
            .map_or(DesiredSettlement::Pending, |desired| {
                let mut degraded = self.reconnect_degraded.load(Ordering::Acquire);
                for state in desired.values().filter(|state| state.was_returned()) {
                    match *state.status.borrow() {
                        ListenerStatus::Active => {}
                        ListenerStatus::Blocked => degraded = true,
                        ListenerStatus::Registering
                        | ListenerStatus::Suspended
                        | ListenerStatus::Closed => return DesiredSettlement::Pending,
                    }
                }
                if degraded {
                    DesiredSettlement::Degraded
                } else {
                    DesiredSettlement::Recovered
                }
            })
    }

    pub(super) fn mark_reconnect_degraded(&self) {
        self.reconnect_degraded.store(true, Ordering::Release);
    }

    pub(super) fn clear_reconnect_degraded(&self) {
        self.reconnect_degraded.store(false, Ordering::Release);
    }

    pub(super) fn set_relay_status(&self, status: RelayStatus) {
        if self.cancel.is_cancelled() || matches!(status, RelayStatus::Active | RelayStatus::Closed)
        {
            self.set_relay_error(None);
        }
        let mut previous = status;
        let mut applied = status;
        let changed = self.status.send_if_modified(|current| {
            previous = *current;
            applied = if self.cancel.is_cancelled() {
                RelayStatus::Closed
            } else {
                status
            };
            if *current == RelayStatus::Closed || *current == applied {
                return false;
            }
            *current = applied;
            true
        });
        if changed {
            tracing::debug!(
                component = "sdk",
                event = "sdk.relay.status_changed",
                previous = ?previous,
                status = ?applied,
                "Relay status changed"
            );
        }
    }

    pub(super) fn set_relay_error(&self, error: Option<Error>) {
        let mut last_error = self
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *last_error = if self.cancel.is_cancelled() {
            None
        } else {
            error
        };
    }

    pub(super) fn relay_error(&self) -> Option<Error> {
        self.last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn detach_listener(&self, state: &Arc<ListenerState>) {
        state.close(None);
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.destination)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.destination);
            self.reconcile.notify_one();
        }
    }

    pub(super) fn drop_listener(&self, state: &Arc<ListenerState>) {
        self.detach_listener(state);
    }

    pub(super) fn close_all(&self) {
        self.set_relay_status(RelayStatus::Closed);
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => poisoned.into_inner(),
        };
        let states = desired.drain().map(|(_, state)| state).collect::<Vec<_>>();
        for state in states {
            state.close(None);
        }
        self.reconcile.notify_one();
    }

    pub(super) fn fail_initial_listener(&self, state: &Arc<ListenerState>, error: Error) {
        if !state.fail_initial(error) {
            return;
        }
        self.remove_terminal_listener(state);
    }

    pub(super) fn terminate_initial_listener(
        &self,
        state: &Arc<ListenerState>,
        code: ErrorCode,
        message: &str,
    ) -> Error {
        let error = state
            .terminate_initial_operation(code, message, false)
            .or_else(|| state.last_error())
            .unwrap_or_else(|| Error::new(code, PeerObservation::NotObserved, message));
        self.remove_terminal_listener(state);
        error
    }

    /// Deadline expiry for an initial listen. Returns `None` when the
    /// registration became ACTIVE first (or already settled), so the caller
    /// re-reads the status instead of discarding a confirmed Binding.
    pub(super) fn expire_initial_listener(
        &self,
        state: &Arc<ListenerState>,
        code: ErrorCode,
        message: &str,
    ) -> Option<Error> {
        let error = state.terminate_initial_operation(code, message, true)?;
        self.remove_terminal_listener(state);
        Some(error)
    }

    pub(super) fn remove_terminal_listener(&self, state: &Arc<ListenerState>) {
        let mut desired = match self.desired.lock() {
            Ok(desired) => desired,
            Err(poisoned) => {
                self.cancel.cancel();
                poisoned.into_inner()
            }
        };
        if desired
            .get(&state.destination)
            .is_some_and(|current| Arc::ptr_eq(current, state))
        {
            desired.remove(&state.destination);
        }
        self.reconcile.notify_one();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DesiredSettlement {
    Recovered,
    Degraded,
    Pending,
}

impl ListenerState {
    /// Locks the logical state. A poisoned lock means a holder panicked
    /// mid-transition; the state is still the last consistent value written
    /// under the lock, so it is recovered rather than treated as absent.
    fn runtime(&self) -> std::sync::MutexGuard<'_, ListenerRuntime> {
        self.runtime.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Publishes `status` (unless already Closed) and records `error` as the
    /// last error, under the caller's lock so status and lifecycle move
    /// together.
    fn publish(&self, runtime: &mut ListenerRuntime, status: ListenerStatus, error: Option<Error>) {
        let mut previous = status;
        let applied = self.status.send_if_modified(|current| {
            previous = *current;
            if *current == ListenerStatus::Closed {
                return false;
            }
            *current = status;
            true
        });
        if !applied {
            return;
        }
        if previous != status {
            if let Some(error) = error.as_ref() {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener.status_changed",
                    destination = %self.destination,
                    previous = ?previous,
                    status = ?status,
                    error_code = ?error.code(),
                    observation = ?error.observation(),
                    "Listener status changed"
                );
            } else {
                tracing::debug!(
                    component = "sdk",
                    event = "sdk.listener.status_changed",
                    destination = %self.destination,
                    previous = ?previous,
                    status = ?status,
                    "Listener status changed"
                );
            }
        }
        runtime.last_error = error;
    }

    pub(super) fn set_status(&self, status: ListenerStatus, error: Option<Error>) {
        let mut runtime = self.runtime();
        self.publish(&mut runtime, status, error);
    }

    pub(super) fn last_error(&self) -> Option<Error> {
        self.runtime().last_error.clone()
    }

    #[cfg(test)]
    pub(super) fn lifecycle(&self) -> ListenerLifecycle {
        self.runtime().lifecycle
    }

    pub(super) fn blocked_error(&self) -> Error {
        self.last_error().unwrap_or_else(|| {
            Error::new(
                ErrorCode::PermissionDenied,
                PeerObservation::NotObserved,
                "Listener registration is blocked",
            )
        })
    }

    pub(super) fn was_returned(&self) -> bool {
        self.runtime().lifecycle == ListenerLifecycle::Returned
    }

    pub(super) fn promote_returned(&self) -> bool {
        let mut runtime = self.runtime();
        match runtime.lifecycle {
            ListenerLifecycle::Pending => {
                runtime.lifecycle = ListenerLifecycle::Returned;
                true
            }
            ListenerLifecycle::Returned => true,
            ListenerLifecycle::Terminal => false,
        }
    }

    fn fail_initial(&self, error: Error) -> bool {
        let mut runtime = self.runtime();
        if runtime.lifecycle != ListenerLifecycle::Pending {
            return false;
        }
        runtime.lifecycle = ListenerLifecycle::Terminal;
        runtime.registration_progress = RegistrationProgress::Waiting;
        self.publish(&mut runtime, ListenerStatus::Closed, Some(error));
        true
    }

    pub(super) fn suspend_or_fail_initial(
        &self,
        recovery_error: Error,
        initial_error: Error,
    ) -> bool {
        let mut runtime = self.runtime();
        match runtime.lifecycle {
            ListenerLifecycle::Pending => {
                runtime.lifecycle = ListenerLifecycle::Terminal;
                runtime.registration_progress = RegistrationProgress::Waiting;
                self.publish(&mut runtime, ListenerStatus::Closed, Some(initial_error));
                true
            }
            ListenerLifecycle::Returned => {
                runtime.registration_progress = RegistrationProgress::Waiting;
                self.publish(
                    &mut runtime,
                    ListenerStatus::Suspended,
                    Some(recovery_error),
                );
                false
            }
            ListenerLifecycle::Terminal => false,
        }
    }

    pub(super) fn handle_precommit_session_end(&self, recovery_error: Error) {
        let mut runtime = self.runtime();
        runtime.registration_progress = RegistrationProgress::Waiting;
        match runtime.lifecycle {
            ListenerLifecycle::Pending => {
                self.publish(&mut runtime, ListenerStatus::Registering, None);
            }
            ListenerLifecycle::Returned => {
                self.publish(
                    &mut runtime,
                    ListenerStatus::Suspended,
                    Some(recovery_error),
                );
            }
            ListenerLifecycle::Terminal => {}
        }
    }

    pub(super) fn block(&self, error: Error) {
        let mut runtime = self.runtime();
        if runtime.lifecycle == ListenerLifecycle::Terminal {
            return;
        }
        runtime.lifecycle = ListenerLifecycle::Terminal;
        runtime.registration_progress = RegistrationProgress::Waiting;
        self.publish(&mut runtime, ListenerStatus::Blocked, Some(error));
    }

    /// Publishes ACTIVE under the state lock, which is what lets
    /// `terminate_initial_operation` trust the status it reads under the same
    /// lock.
    pub(super) fn activate(&self) -> bool {
        let mut runtime = self.runtime();
        if runtime.lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        runtime.registration_progress = RegistrationProgress::Waiting;
        self.publish(&mut runtime, ListenerStatus::Active, None);
        true
    }

    pub(super) fn close(&self, error: Option<Error>) {
        let mut runtime = self.runtime();
        if runtime.lifecycle == ListenerLifecycle::Terminal
            && *self.status.borrow() == ListenerStatus::Closed
        {
            return;
        }
        runtime.lifecycle = ListenerLifecycle::Terminal;
        runtime.registration_progress = RegistrationProgress::Waiting;
        self.publish(&mut runtime, ListenerStatus::Closed, error);
    }

    pub(super) fn begin_token_supply(&self) -> bool {
        let mut runtime = self.runtime();
        if runtime.lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        runtime.registration_progress = RegistrationProgress::TokenSource;
        true
    }

    pub(super) fn begin_registration_commit(&self) -> bool {
        let mut runtime = self.runtime();
        if runtime.lifecycle == ListenerLifecycle::Terminal {
            return false;
        }
        runtime.registration_progress = RegistrationProgress::Committed;
        self.publish(&mut runtime, ListenerStatus::Registering, None);
        true
    }

    pub(super) fn finish_registration_attempt(&self) {
        self.runtime().registration_progress = RegistrationProgress::Waiting;
    }

    fn terminate_initial_operation(
        &self,
        code: ErrorCode,
        message: &str,
        keep_active: bool,
    ) -> Option<Error> {
        let mut runtime = self.runtime();
        if runtime.lifecycle != ListenerLifecycle::Pending {
            return None;
        }
        // `activate` publishes ACTIVE under this same lock.
        if keep_active && *self.status.borrow() == ListenerStatus::Active {
            return None;
        }
        let progress = std::mem::take(&mut runtime.registration_progress);
        let observation = if matches!(progress, RegistrationProgress::Committed) {
            PeerObservation::MaybeObserved
        } else {
            PeerObservation::NotObserved
        };
        runtime.lifecycle = ListenerLifecycle::Terminal;
        let error = if code == ErrorCode::DeadlineExceeded {
            match progress {
                RegistrationProgress::TokenSource => Error::token_source_deadline(),
                RegistrationProgress::Waiting => Error::transport_deadline(
                    "PUBLISH deadline exceeded while waiting for a RelaySession",
                ),
                RegistrationProgress::Committed => {
                    Error::transport_deadline("PUBLISH response deadline exceeded")
                        .with_observation(observation)
                }
            }
        } else {
            Error::new(code, observation, message)
        };
        self.publish(&mut runtime, ListenerStatus::Closed, Some(error.clone()));
        Some(error)
    }

    /// Drops terminal queued Pipes without waiting for an application accept.
    ///
    /// A busy receiver means an accept call owns the queue and will either
    /// consume the terminal entry or release the lane before the next Offer.
    /// Live entries are reinserted in FIFO order while the receiver lock keeps
    /// application accepts from observing the temporary compaction.
    pub(super) fn try_compact_terminal_queue(&self) -> bool {
        let Ok(mut incoming) = self.incoming_rx.try_lock() else {
            return true;
        };
        let mut live = Vec::new();
        while let Ok(pipe) = incoming.try_recv() {
            if pipe.is_terminal() {
                drop(pipe);
            } else {
                live.push(pipe);
            }
        }
        for pipe in live {
            if self.incoming_tx.try_send(pipe).is_err() {
                return false;
            }
        }
        true
    }

    pub(super) async fn drain_unaccepted(&self, close: bool) {
        let mut incoming = self.incoming_rx.lock().await;
        if close {
            incoming.close();
        }
        while let Ok(pipe) = incoming.try_recv() {
            drop(pipe);
        }
    }
}

pub(super) fn is_current_desired(inner: &RelayInner, state: &Arc<ListenerState>) -> bool {
    match inner.desired.lock() {
        Ok(desired) => desired
            .get(&state.destination)
            .is_some_and(|current| Arc::ptr_eq(current, state)),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            false
        }
    }
}

#[cfg(test)]
mod tests;
