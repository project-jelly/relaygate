use relaygate_protocol::SessionId;

use super::RelaySessionState;
use crate::{
    Error, PeerObservation,
    listener::{ListenerStatus, is_current_desired},
};

pub(super) async fn cleanup_relay_session(
    session_id: SessionId,
    inner: &crate::listener::RelayInner,
    state: RelaySessionState,
    timed_out_request: Option<u64>,
    registration_succeeded: bool,
    failure: Error,
) -> bool {
    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ended",
        session_id = %session_id.as_uuid(),
        pending_registrations = state.pending.len(),
        active_registrations = state.registrations.len(),
        pending_dials = state.pending_dials.len(),
        live_pipes = state.pipes.len(),
        registration_timed_out = timed_out_request.is_some(),
        error_code = ?failure.code(),
        error_origin = ?failure.origin(),
        "Relay session ended"
    );
    for pending in state.pending_dials.into_values() {
        let _ = pending.response.send(Err(failure
            .clone()
            .with_observation(PeerObservation::MaybeObserved)));
    }
    // Fail every Pipe first. Once the Listener status leaves ACTIVE, pending
    // accept calls release the receiver lane and the old session queue can be
    // drained before this function permits a replacement session to start.
    for pipe in state.pipes.values() {
        pipe.state.fail(
            failure
                .clone()
                .with_observation(PeerObservation::NotObserved),
        );
    }
    let mut queues_to_drain = Vec::new();
    for pending in state.pending.into_values() {
        if *pending.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((pending.state, true));
            continue;
        }
        if pending.committed {
            let recovery_error = failure
                .clone()
                .with_observation(PeerObservation::MaybeObserved);
            let initial_error = recovery_error.clone();
            if pending
                .state
                .suspend_or_fail_initial(recovery_error, initial_error)
            {
                inner.remove_terminal_listener(&pending.state);
            }
        } else if is_current_desired(inner, &pending.state) {
            pending.state.handle_precommit_session_end(
                failure
                    .clone()
                    .with_observation(PeerObservation::NotObserved),
            );
        }
        let close_queue = matches!(
            *pending.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((pending.state, close_queue));
    }
    for (_, registration) in state.registrations {
        if *registration.state.status.borrow() == ListenerStatus::Closed {
            queues_to_drain.push((registration.state, true));
            continue;
        }
        if registration.state.suspend_or_fail_initial(
            failure
                .clone()
                .with_observation(PeerObservation::NotObserved),
            failure.clone().with_observation(PeerObservation::Observed),
        ) {
            inner.remove_terminal_listener(&registration.state);
        }
        let close_queue = matches!(
            *registration.state.status.borrow(),
            ListenerStatus::Blocked | ListenerStatus::Closed
        );
        queues_to_drain.push((registration.state, close_queue));
    }
    for (listener, close_queue) in queues_to_drain {
        listener.drain_unaccepted(close_queue).await;
    }
    registration_succeeded
}
