use std::time::Instant;

use relaygate_protocol::{
    BindingId, Destination, ErrorCode, Frame, PeerObservation, PipeId, SessionId,
};
use relaygate_route_table::BindingSet;

use crate::peer::{OpenIdentity, PeerStreamKey};

use super::{
    GatewayAction, GatewayState, PeerDelivery, PipeEndpoint, PipeEntry, PipePhase,
    RemoteOpenAttempt, RemoteOpenPhase, observe_dial_result,
};

/// How a peer or route event relates to the current remote OPEN attempt.
#[derive(Debug, Clone, Copy)]
enum AttemptMatch {
    Resolving,
    /// The peer OPEN was requested; no stream key is bound yet.
    Starting {
        binding_id: BindingId,
    },
    /// The peer OPEN committed on the key the event names.
    Awaiting {
        binding_id: BindingId,
    },
    /// The peer OPEN committed on a different key than the event names.
    Stale,
    /// The peer OPEN committed and the event named no key to check.
    Committed,
}

impl GatewayState {
    /// Answers "which phase is this attempt in, from the point of view of an
    /// event on `key`?" once, so every entry point agrees on what matches.
    fn match_attempt(
        &self,
        open_identity: OpenIdentity,
        key: Option<PeerStreamKey>,
    ) -> Option<(PipeId, AttemptMatch)> {
        let attempt = self.remote_open_attempts.get(&open_identity)?;
        let matched = match attempt.phase {
            RemoteOpenPhase::Resolving => AttemptMatch::Resolving,
            RemoteOpenPhase::StartingPeer { binding_id } => AttemptMatch::Starting { binding_id },
            RemoteOpenPhase::AwaitingPeer {
                key: current_key,
                binding_id,
            } => match key {
                None => AttemptMatch::Committed,
                Some(key) if key == current_key => AttemptMatch::Awaiting { binding_id },
                Some(_) => AttemptMatch::Stale,
            },
        };
        Some((attempt.pipe_id, matched))
    }

    pub(crate) fn route_resolved(
        &mut self,
        open_identity: OpenIdentity,
        bindings: BindingSet,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.remote_open_attempts.get(&open_identity) else {
            return Vec::new();
        };
        if !matches!(attempt.phase, RemoteOpenPhase::Resolving) {
            return Vec::new();
        }
        let destination = attempt.destination.clone();
        let pipe_id = attempt.pipe_id;
        let started_at = attempt.started_at;
        let Some(projection) = bindings
            .entries()
            .iter()
            .find(|projection| {
                let identity = projection.identity();
                Some(identity.gateway_id()) != self.gateway_id
                    || identity.relay_session_id().as_uuid()
                        != pipe_id.origin_session_id().as_uuid()
            })
            .cloned()
        else {
            return self.fail_remote_attempt(
                open_identity,
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "only the dialing Relay publishes this Destination",
            );
        };
        if projection.destination() != &destination {
            return self.fail_remote_attempt(
                open_identity,
                ErrorCode::FailedPrecondition,
                PeerObservation::NotObserved,
                "RouteTable returned a projection for a different Destination",
            );
        }

        let binding_identity = projection.identity();
        let relay_session_id = SessionId::from_uuid(binding_identity.relay_session_id().as_uuid());
        let binding_id = BindingId::from_uuid(binding_identity.binding_id().as_uuid());
        if Some(binding_identity.gateway_id()) == self.gateway_id {
            let _ = self.take_remote_attempt(open_identity);
            let Some(binding) = self
                .registry
                .exact(relay_session_id, binding_id, &destination)
            else {
                observe_dial_result(Some(started_at), Some(ErrorCode::Unavailable));
                return self.open_failed(
                    pipe_id.origin_session_id(),
                    pipe_id.connection_id(),
                    ErrorCode::Unavailable,
                    PeerObservation::NotObserved,
                    "selected local Binding is stale",
                );
            };
            return self.offer_local_at(pipe_id, binding, Instant::now(), Some(started_at));
        }

        // Nothing between the guard above and here touches the attempt map.
        let Some(attempt) = self.remote_open_attempts.get_mut(&open_identity) else {
            return Vec::new();
        };
        attempt.phase = RemoteOpenPhase::StartingPeer { binding_id };
        vec![GatewayAction::OpenPeer {
            open_identity,
            gateway_id: binding_identity.gateway_id(),
            gateway_locator: projection.gateway_locator().clone(),
            destination,
            relay_session_id,
            binding_id,
        }]
    }

    pub(crate) fn route_failed(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        message: &str,
    ) -> Vec<GatewayAction> {
        if !matches!(
            self.match_attempt(open_identity, None),
            Some((_, AttemptMatch::Resolving))
        ) {
            return Vec::new();
        }
        let (code, message) = sdk_dependency_failure(code, message);
        self.fail_remote_attempt(open_identity, code, PeerObservation::NotObserved, message)
    }

    pub(crate) fn peer_open_committed(
        &mut self,
        open_identity: OpenIdentity,
        key: PeerStreamKey,
    ) -> Vec<GatewayAction> {
        let Some((pipe_id, matched)) = self.match_attempt(open_identity, Some(key)) else {
            if self.active_peer_opens.get(&open_identity) == Some(&key) {
                return Vec::new();
            }
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                PipeId::new(
                    open_identity.origin_session(),
                    open_identity.connection_id(),
                ),
                ErrorCode::Cancelled,
                "Open attempt ended before the peer OPEN committed",
            );
        };
        let binding_id = match matched {
            AttemptMatch::Starting { binding_id } => binding_id,
            AttemptMatch::Awaiting { .. } => return Vec::new(),
            AttemptMatch::Resolving | AttemptMatch::Stale | AttemptMatch::Committed => {
                return self.endpoint_reset(
                    PipeEndpoint::Peer(key),
                    pipe_id,
                    ErrorCode::ProtocolError,
                    "peer OPEN committed in an invalid local phase",
                );
            }
        };
        // The value scan covers the window in which a key is reserved by an
        // in-flight OPEN but not yet indexed in `peer_pipes`; it is O(active
        // opens), which the remote dial admission limit bounds.
        if self.peer_pipes.contains_key(&key)
            || self
                .active_peer_opens
                .values()
                .any(|current| *current == key)
            || self.active_peer_opens.contains_key(&open_identity)
        {
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                pipe_id,
                ErrorCode::ProtocolError,
                "peer stream identity is already active",
            );
        }
        let previous = self.active_peer_opens.insert(open_identity, key);
        debug_assert!(previous.is_none());
        if let Some(attempt) = self.remote_open_attempts.get_mut(&open_identity) {
            attempt.phase = RemoteOpenPhase::AwaitingPeer { key, binding_id };
        }
        Vec::new()
    }

    pub(crate) fn peer_open_commit_failed(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        if !matches!(
            self.match_attempt(open_identity, None),
            Some((_, AttemptMatch::Starting { .. }))
        ) {
            return Vec::new();
        }
        let (code, message) = sdk_dependency_failure(code, message);
        self.fail_remote_attempt(open_identity, code, observation, message)
    }

    pub(crate) fn receive_peer_open(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        destination: Destination,
        relay_session_id: SessionId,
        binding_id: BindingId,
    ) -> Vec<GatewayAction> {
        self.receive_peer_open_at(
            key,
            open_identity,
            destination,
            relay_session_id,
            binding_id,
            Instant::now(),
        )
    }

    pub(crate) fn receive_peer_open_at(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        destination: Destination,
        relay_session_id: SessionId,
        binding_id: BindingId,
        now: Instant,
    ) -> Vec<GatewayAction> {
        if self.draining {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::Unavailable,
                    observation: PeerObservation::NotObserved,
                    message: "Gateway is draining".to_owned(),
                }
                .into(),
            ];
        }
        if open_identity.entry_gateway() != key.peer_gateway_id() {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::PermissionDenied,
                    observation: PeerObservation::NotObserved,
                    message: "peer OPEN identity does not match the authenticated Gateway"
                        .to_owned(),
                }
                .into(),
            ];
        }
        if self.peer_pipes.contains_key(&key)
            || self.active_peer_opens.contains_key(&open_identity)
            || self.remote_open_attempts.contains_key(&open_identity)
        {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::AlreadyExists,
                    observation: PeerObservation::NotObserved,
                    message: "peer OPEN identity is already active".to_owned(),
                }
                .into(),
            ];
        }
        if self.live_pipe_count() >= self.limits.max_live_pipes || self.pending_capacity_reached() {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::ResourceExhausted,
                    observation: PeerObservation::NotObserved,
                    message: "Gateway Pipe limit reached".to_owned(),
                }
                .into(),
            ];
        }
        let listener_is_live = self.sessions.contains_key(&relay_session_id);
        let Some(binding) = self
            .registry
            .exact(relay_session_id, binding_id, &destination)
            .filter(|_| listener_is_live)
        else {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::Unavailable,
                    observation: PeerObservation::NotObserved,
                    message: "selected Binding is no longer current".to_owned(),
                }
                .into(),
            ];
        };

        let pipe_id = PipeId::new(
            open_identity.origin_session(),
            open_identity.connection_id(),
        );
        if self.pipes.contains_key(&pipe_id) {
            return vec![
                PeerDelivery::Failed {
                    key,
                    code: ErrorCode::AlreadyExists,
                    observation: PeerObservation::NotObserved,
                    message: "Pipe identity is already active".to_owned(),
                }
                .into(),
            ];
        }
        self.insert_offer(
            pipe_id,
            PipeEntry {
                dialer: PipeEndpoint::Peer(key),
                acceptor: PipeEndpoint::Sdk(relay_session_id),
                binding_id: binding.id,
                open_identity: Some(open_identity),
                phase: PipePhase::Offered,
                offered_at: now,
                open_started_at: None,
                dialer_finished: false,
                acceptor_finished: false,
            },
        );
        self.send_to(
            relay_session_id,
            Frame::Offer {
                pipe_id,
                binding_id,
                destination,
            },
        )
    }

    pub(crate) fn peer_opened(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
    ) -> Vec<GatewayAction> {
        self.peer_opened_at(key, open_identity, Instant::now())
    }

    pub(crate) fn peer_opened_at(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        now: Instant,
    ) -> Vec<GatewayAction> {
        let Some((pipe_id, matched)) = self.match_attempt(open_identity, Some(key)) else {
            if self.active_peer_opens.get(&open_identity) == Some(&key)
                && self.peer_pipes.contains_key(&key)
            {
                return Vec::new();
            }
            return self.endpoint_reset(
                PipeEndpoint::Peer(key),
                PipeId::new(
                    open_identity.origin_session(),
                    open_identity.connection_id(),
                ),
                ErrorCode::Cancelled,
                "late peer OPENED cannot recreate an ended attempt",
            );
        };
        let binding_id = match matched {
            AttemptMatch::Starting { binding_id } | AttemptMatch::Awaiting { binding_id } => {
                binding_id
            }
            AttemptMatch::Resolving | AttemptMatch::Stale | AttemptMatch::Committed => {
                return self.endpoint_reset(
                    PipeEndpoint::Peer(key),
                    pipe_id,
                    ErrorCode::ProtocolError,
                    "peer OPENED does not match the current attempt",
                );
            }
        };
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        let dialer = attempt.pipe_id.origin_session_id();
        let dialer_is_live = self.sessions.contains_key(&dialer);
        if !dialer_is_live || self.live_pipe_count() >= self.limits.max_live_pipes {
            let code = if dialer_is_live {
                ErrorCode::ResourceExhausted
            } else {
                ErrorCode::Cancelled
            };
            let mut actions = self.endpoint_reset(
                PipeEndpoint::Peer(key),
                attempt.pipe_id,
                code,
                "peer OPENED after the local endpoint became unavailable",
            );
            if dialer_is_live {
                observe_dial_result(Some(attempt.started_at), Some(code));
                actions.extend(self.open_failed(
                    dialer,
                    attempt.pipe_id.connection_id(),
                    code,
                    PeerObservation::MaybeObserved,
                    "Gateway live Pipe limit reached during remote admission",
                ));
            } else {
                observe_dial_result(Some(attempt.started_at), Some(ErrorCode::Cancelled));
            }
            return actions;
        }

        observe_dial_result(Some(attempt.started_at), None);
        self.insert_open(
            attempt.pipe_id,
            PipeEntry {
                dialer: PipeEndpoint::Sdk(dialer),
                acceptor: PipeEndpoint::Peer(key),
                binding_id,
                open_identity: Some(open_identity),
                phase: PipePhase::Open,
                offered_at: now,
                open_started_at: None,
                dialer_finished: false,
                acceptor_finished: false,
            },
        );
        self.send_to(
            dialer,
            Frame::Opened {
                pipe_id: attempt.pipe_id,
            },
        )
    }

    pub(crate) fn peer_open_failed(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        if !self.peer_event_matches_attempt(open_identity, key) {
            return Vec::new();
        }
        let (code, message) = sdk_dependency_failure(code, message);
        self.fail_remote_attempt(open_identity, code, observation, message)
    }

    pub(crate) fn peer_transport_lost_stream(
        &mut self,
        key: PeerStreamKey,
        open_identity: OpenIdentity,
        observation: PeerObservation,
    ) -> Vec<GatewayAction> {
        if self.peer_event_matches_attempt(open_identity, key) {
            return self.fail_remote_attempt(
                open_identity,
                ErrorCode::Unavailable,
                observation,
                "PeerTransport was lost during remote OPEN",
            );
        }

        let Some(pipe_id) = self.peer_pipes.get(&key).copied() else {
            return Vec::new();
        };
        let exact = self
            .pipes
            .get(&pipe_id)
            .is_some_and(|pipe| pipe.open_identity == Some(open_identity));
        if !exact {
            return Vec::new();
        }
        let Some(pipe) = self.remove_pipe(pipe_id) else {
            return Vec::new();
        };
        [pipe.dialer, pipe.acceptor]
            .into_iter()
            .filter_map(PipeEndpoint::sdk_session)
            .filter_map(|session_id| {
                self.to(
                    session_id,
                    Frame::Reset {
                        pipe_id,
                        code: ErrorCode::Unavailable,
                        message: "PeerTransport was lost".to_owned(),
                    },
                )
                .map(GatewayAction::SendSdkFrame)
            })
            .collect()
    }

    pub(super) fn cancel_remote_attempt(
        &mut self,
        dialer: SessionId,
        pipe_id: PipeId,
    ) -> Vec<GatewayAction> {
        let Some(gateway_id) = self.gateway_id else {
            return Vec::new();
        };
        if pipe_id.origin_session_id() != dialer {
            return Vec::new();
        }
        let open_identity = OpenIdentity::new(gateway_id, dialer, pipe_id.connection_id());
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        observe_dial_result(Some(attempt.started_at), Some(ErrorCode::Cancelled));
        match attempt.phase {
            RemoteOpenPhase::Resolving => Vec::new(),
            RemoteOpenPhase::StartingPeer { .. } => {
                vec![GatewayAction::CancelPeerOpen { open_identity }]
            }
            RemoteOpenPhase::AwaitingPeer { key, .. } => self.endpoint_reset(
                PipeEndpoint::Peer(key),
                pipe_id,
                ErrorCode::Cancelled,
                "Dialer cancelled the remote OPEN",
            ),
        }
    }

    /// A peer failure on `key` belongs to the attempt while the peer OPEN is
    /// starting or has committed on that same key.
    fn peer_event_matches_attempt(&self, open_identity: OpenIdentity, key: PeerStreamKey) -> bool {
        matches!(
            self.match_attempt(open_identity, Some(key)),
            Some((
                _,
                AttemptMatch::Starting { .. } | AttemptMatch::Awaiting { .. }
            ))
        )
    }

    fn fail_remote_attempt(
        &mut self,
        open_identity: OpenIdentity,
        code: ErrorCode,
        observation: PeerObservation,
        message: &str,
    ) -> Vec<GatewayAction> {
        let Some(attempt) = self.take_remote_attempt(open_identity) else {
            return Vec::new();
        };
        observe_dial_result(Some(attempt.started_at), Some(code));
        self.open_failed(
            attempt.pipe_id.origin_session_id(),
            attempt.pipe_id.connection_id(),
            code,
            observation,
            message,
        )
    }

    fn take_remote_attempt(&mut self, open_identity: OpenIdentity) -> Option<RemoteOpenAttempt> {
        let attempt = self.remote_open_attempts.remove(&open_identity)?;
        self.active_peer_opens.remove(&open_identity);
        Some(attempt)
    }
}

// Operation JWTs were already checked before RouteTable/peer admission. Their
// infrastructure credentials cannot be repaired by an application token refresh.
fn sdk_dependency_failure(code: ErrorCode, message: &str) -> (ErrorCode, &str) {
    match code {
        ErrorCode::Unauthenticated | ErrorCode::PermissionDenied => {
            tracing::warn!(
                component = "gateway",
                event = "gateway.dependency.authorization_failed",
                error_code = ?code,
                "Gateway internal dependency authentication or authorization failed"
            );
            (
                ErrorCode::Internal,
                "Gateway internal dependency authentication or authorization failed",
            )
        }
        _ => (code, message),
    }
}
