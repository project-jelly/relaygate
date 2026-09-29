use std::{collections::HashMap, sync::Arc};

use futures_util::FutureExt;
use relaygate_protocol::Frame;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{PendingRegistration, RelaySessionState};
use crate::{
    AccessAction, AccessTokenRequest, Destination, Error, ErrorCode, PeerObservation,
    listener::{ListenerState, ListenerStatus, RelayInner, is_current_desired},
    session::{EstablishedSession, send_bounded},
};

pub(super) async fn reconcile_registrations(
    inner: &RelayInner,
    established: &mut EstablishedSession,
    session: &mut RelaySessionState,
    session_cancel: &CancellationToken,
) -> crate::Result<()> {
    let Some(desired) = snapshot_desired_by_destination(inner) else {
        return Err(Error::new(
            ErrorCode::Internal,
            PeerObservation::NotObserved,
            "Listener registry lock is poisoned",
        ));
    };
    let abandoned_committed_registration = session.pending.values().any(|pending| {
        pending.committed
            && (!desired
                .get(&pending.state.destination)
                .is_some_and(|current| Arc::ptr_eq(current, &pending.state))
                || *pending.state.status.borrow() == ListenerStatus::Closed)
    });
    if abandoned_committed_registration {
        return Err(Error::new(
            ErrorCode::Cancelled,
            PeerObservation::NotObserved,
            "RelaySession ended to discard a cancelled committed PUBLISH",
        ));
    }
    let registered_destinations = session.registrations.keys().cloned().collect::<Vec<_>>();
    for destination in registered_destinations {
        let stale = session
            .registrations
            .get(&destination)
            .is_some_and(|registration| {
                !desired
                    .get(&destination)
                    .is_some_and(|current| Arc::ptr_eq(current, &registration.state))
                    || *registration.state.status.borrow() == ListenerStatus::Closed
            });
        if !stale {
            continue;
        }
        let Some(registration) = session.registrations.remove(&destination) else {
            continue;
        };
        let Some(request_id) = session.next_request_id() else {
            return Err(Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "RelaySession exhausted request IDs",
            ));
        };
        send_bounded(
            &mut established.transport,
            Frame::Unpublish {
                request_id,
                binding_id: registration.binding_id,
            },
            inner.config.operation_timeout,
            session_cancel,
        )
        .await?;
    }

    for state in desired.values() {
        let status = *state.status.borrow();
        if !is_current_desired(inner, state) {
            continue;
        }
        if matches!(status, ListenerStatus::Blocked | ListenerStatus::Closed)
            || (status == ListenerStatus::Suspended && !inner.republish_retry_is_ready())
            || session.registrations.contains_key(&state.destination)
            || session
                .pending_by_destination
                .contains_key(&state.destination)
        {
            continue;
        }
        let deadline = if state.was_returned() {
            match inner.config.operation_deadline() {
                Ok(deadline) => deadline,
                Err(error) => {
                    state.block(error);
                    state.drain_unaccepted(true).await;
                    continue;
                }
            }
        } else {
            state.initial_deadline
        };
        if deadline <= Instant::now() {
            if state.was_returned() {
                state.set_status(
                    ListenerStatus::Suspended,
                    Some(Error::transport_deadline(
                        "PUBLISH deadline exceeded while waiting for a RelaySession",
                    )),
                );
            } else {
                inner.fail_initial_listener(
                    state,
                    Error::transport_deadline(
                        "PUBLISH deadline exceeded while waiting for a RelaySession",
                    ),
                );
            }
            continue;
        }
        let Some(request_id) = session.next_request_id() else {
            let error = Error::new(
                ErrorCode::ResourceExhausted,
                PeerObservation::NotObserved,
                "RelaySession exhausted request IDs",
            );
            if state.was_returned() {
                state.block(error);
                state.drain_unaccepted(true).await;
            } else {
                inner.fail_initial_listener(state, error);
            }
            continue;
        };
        if !state.begin_token_supply() {
            continue;
        }
        session.pending.insert(
            request_id,
            PendingRegistration {
                state: Arc::clone(state),
                committed: false,
                deadline,
            },
        );
        session
            .pending_by_destination
            .insert(state.destination.clone(), request_id);
        let source = state.access_token_source.clone();
        let destination = state.destination.clone();
        session.token_supplies.push(
            async move {
                let result = timeout_at(
                    deadline,
                    source.supply(AccessTokenRequest {
                        action: AccessAction::Publish,
                        destination,
                    }),
                )
                .await
                .map_err(|_| Error::token_source_deadline())
                .and_then(|result| result);
                (request_id, result)
            }
            .boxed(),
        );
    }

    if inner.cancel.is_cancelled() {
        Err(Error::closed())
    } else {
        Ok(())
    }
}

pub(super) async fn commit_registration_token(
    request_id: u64,
    token: crate::Result<relaygate_protocol::BearerToken>,
    inner: &RelayInner,
    established: &mut EstablishedSession,
    session: &mut RelaySessionState,
    session_cancel: &CancellationToken,
) -> crate::Result<()> {
    let Some(mut pending) = session.pending.remove(&request_id) else {
        return Ok(());
    };
    let state = Arc::clone(&pending.state);
    session.pending_by_destination.remove(&state.destination);
    if !is_current_desired(inner, &state) || *state.status.borrow() == ListenerStatus::Closed {
        return Ok(());
    }
    let token = match token {
        Ok(token) if pending.deadline > Instant::now() => token,
        Ok(_) => {
            handle_token_source_error(inner, &state, Error::token_source_deadline());
            return Ok(());
        }
        Err(error) => {
            handle_token_source_error(inner, &state, error);
            return Ok(());
        }
    };
    if !state.begin_registration_commit() {
        return Ok(());
    }
    pending.committed = true;
    let deadline = pending.deadline;
    session.pending.insert(request_id, pending);
    session
        .pending_by_destination
        .insert(state.destination.clone(), request_id);
    send_bounded(
        &mut established.transport,
        Frame::Publish {
            request_id,
            destination: state.destination.clone(),
            access_token: token,
        },
        deadline
            .saturating_duration_since(Instant::now())
            .min(inner.config.operation_timeout),
        session_cancel,
    )
    .await
}

fn handle_token_source_error(inner: &RelayInner, state: &Arc<ListenerState>, error: Error) {
    if !state.was_returned() {
        inner.fail_initial_listener(state, error);
        return;
    }
    state.set_status(ListenerStatus::Suspended, Some(error));
    inner.schedule_reconcile();
}

fn snapshot_desired_by_destination(
    inner: &RelayInner,
) -> Option<HashMap<Destination, Arc<ListenerState>>> {
    match inner.desired.lock() {
        Ok(desired) => Some(
            desired
                .iter()
                .map(|(destination, state)| (destination.clone(), Arc::clone(state)))
                .collect(),
        ),
        Err(_) => {
            tracing::error!(
                component = "sdk",
                event = "sdk.listener_registry.lock_poisoned",
                "Listener desired registry lock is poisoned; stopping runtime"
            );
            inner.cancel.cancel();
            None
        }
    }
}

pub(super) async fn wait_for_registration_deadline(deadline: Option<(u64, Instant)>) {
    if let Some((_, deadline)) = deadline {
        sleep_until(deadline).await;
    }
}
