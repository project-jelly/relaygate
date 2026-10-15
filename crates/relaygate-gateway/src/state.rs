use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use relaygate_protocol::{BindingId, Destination, ErrorCode, Frame, PipeId, SessionId};
use relaygate_route_table::GatewayId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    GatewaySnapshot,
    authorization::{ControlOperation, VerifiedAuthorization},
    peer::{OpenIdentity, PeerStreamKey},
    rate_limit::TokenBucket,
    registry::LocalRegistry,
};

mod action;
mod control_admission;
#[cfg(test)]
mod control_admission_tests;
mod delivery;
#[cfg(test)]
mod delivery_tests;
#[cfg(test)]
mod observation_tests;
mod opening;
mod pipe;
mod registration;
mod remote;
#[cfg(test)]
mod remote_tests;
mod session;
#[cfg(test)]
mod tests;

pub(crate) use action::{GatewayAction, PeerDelivery, ProtocolViolation};
pub(crate) use delivery::{Delivery, DeliveryFailure, SdkWriterItem, TerminalBatchDelivery};

#[derive(Debug, Clone)]
struct SessionEntry {
    sender: mpsc::Sender<SdkWriterItem>,
    cancellation: CancellationToken,
    highest_connection_id: Option<u64>,
    control_rate: TokenBucket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipePhase {
    Offered,
    Open,
}

#[derive(Debug, Clone)]
struct PipeEntry {
    dialer: PipeEndpoint,
    acceptor: PipeEndpoint,
    binding_id: BindingId,
    open_identity: Option<OpenIdentity>,
    phase: PipePhase,
    offered_at: Instant,
    open_started_at: Option<Instant>,
    dialer_finished: bool,
    acceptor_finished: bool,
}

impl PipeEntry {
    fn ensure_sdk_owner(
        &self,
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    ) -> Result<(), ProtocolViolation> {
        if self.dialer == PipeEndpoint::Sdk(sender) || self.acceptor == PipeEndpoint::Sdk(sender) {
            return Ok(());
        }
        Err(ProtocolViolation::PipeOwnership {
            sender,
            pipe_id,
            frame_name,
        })
    }

    fn ensure_sdk_acceptor(
        &self,
        sender: SessionId,
        pipe_id: PipeId,
        frame_name: &'static str,
    ) -> Result<(), ProtocolViolation> {
        if self.acceptor == PipeEndpoint::Sdk(sender) {
            return Ok(());
        }
        Err(ProtocolViolation::PipeOwnership {
            sender,
            pipe_id,
            frame_name,
        })
    }

    fn peer_key(&self) -> Option<PeerStreamKey> {
        match (self.dialer, self.acceptor) {
            (PipeEndpoint::Peer(key), _) | (_, PipeEndpoint::Peer(key)) => Some(key),
            (PipeEndpoint::Sdk(_), PipeEndpoint::Sdk(_)) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipeEndpoint {
    Sdk(SessionId),
    Peer(PeerStreamKey),
}

impl PipeEndpoint {
    const fn sdk_session(self) -> Option<SessionId> {
        match self {
            Self::Sdk(session_id) => Some(session_id),
            Self::Peer(_) => None,
        }
    }

    const fn peer_key(self) -> Option<PeerStreamKey> {
        match self {
            Self::Sdk(_) => None,
            Self::Peer(key) => Some(key),
        }
    }
}

#[derive(Debug, Clone)]
struct RemoteOpenAttempt {
    pipe_id: PipeId,
    destination: Destination,
    started_at: Instant,
    phase: RemoteOpenPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteOpenPhase {
    Resolving,
    StartingPeer {
        binding_id: BindingId,
    },
    AwaitingPeer {
        key: PeerStreamKey,
        binding_id: BindingId,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GatewayLimits {
    pub(crate) control_rate_per_second: usize,
    pub(crate) control_burst: usize,
    pub(crate) session_control_rate_per_second: usize,
    pub(crate) session_control_burst: usize,
    pub(crate) max_sessions: usize,
    pub(crate) max_bindings: usize,
    pub(crate) max_pending_offers: usize,
    pub(crate) max_remote_dial_attempts: usize,
    pub(crate) max_live_pipes: usize,
    pub(crate) offer_timeout: Duration,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            max_sessions: crate::config::DEFAULT_MAX_SESSIONS,
            max_bindings: crate::config::DEFAULT_MAX_BINDINGS,
            max_pending_offers: crate::config::DEFAULT_MAX_PENDING_OFFERS,
            max_remote_dial_attempts: crate::config::DEFAULT_MAX_REMOTE_DIAL_ATTEMPTS,
            max_live_pipes: crate::config::DEFAULT_MAX_LIVE_PIPES,
            offer_timeout: crate::config::DEFAULT_OFFER_TIMEOUT,
            control_rate_per_second: crate::config::DEFAULT_CONTROL_RATE_PER_SECOND,
            control_burst: crate::config::DEFAULT_CONTROL_BURST,
            session_control_rate_per_second: crate::config::DEFAULT_SESSION_CONTROL_RATE_PER_SECOND,
            session_control_burst: crate::config::DEFAULT_SESSION_CONTROL_BURST,
        }
    }
}

#[derive(Debug)]
pub(crate) struct GatewayState {
    sessions: HashMap<SessionId, SessionEntry>,
    registry: LocalRegistry,
    pipes: HashMap<PipeId, PipeEntry>,
    peer_pipes: HashMap<PeerStreamKey, PipeId>,
    active_peer_opens: HashMap<OpenIdentity, PeerStreamKey>,
    remote_open_attempts: HashMap<OpenIdentity, RemoteOpenAttempt>,
    pending_offer_count: usize,
    live_pipe_count: usize,
    originated_pipe_count: usize,
    draining: bool,
    gateway_id: Option<GatewayId>,
    limits: GatewayLimits,
    control_rate: TokenBucket,
}

impl GatewayState {
    pub(crate) fn new(limits: GatewayLimits) -> Self {
        Self::build(limits, None)
    }

    pub(crate) fn new_distributed(limits: GatewayLimits, gateway_id: GatewayId) -> Self {
        Self::build(limits, Some(gateway_id))
    }

    fn build(limits: GatewayLimits, gateway_id: Option<GatewayId>) -> Self {
        Self {
            sessions: HashMap::new(),
            registry: LocalRegistry::default(),
            pipes: HashMap::new(),
            peer_pipes: HashMap::new(),
            active_peer_opens: HashMap::new(),
            remote_open_attempts: HashMap::new(),
            pending_offer_count: 0,
            live_pipe_count: 0,
            originated_pipe_count: 0,
            draining: false,
            gateway_id,
            limits,
            control_rate: TokenBucket::new(
                limits.control_rate_per_second,
                limits.control_burst,
                Instant::now(),
            ),
        }
    }

    pub(crate) fn handle(
        &mut self,
        session_id: SessionId,
        frame: Frame,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        let frame = match ControlOperation::take(frame) {
            Ok((operation, _)) => {
                if let Some(actions) =
                    self.prepare_authorization(session_id, &operation, Instant::now())
                {
                    return Ok(actions);
                }
                return Ok(self.authorization_failed(
                    session_id,
                    &operation,
                    ErrorCode::Unauthenticated,
                ));
            }
            Err(frame) => frame,
        };
        self.apply_frame(session_id, frame)
    }

    #[cfg(test)]
    pub(crate) fn handle_at(
        &mut self,
        session_id: SessionId,
        frame: Frame,
        now: Instant,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        match ControlOperation::take(frame) {
            Ok((mut operation, _)) => {
                if let ControlOperation::Dial { started_at, .. } = &mut operation {
                    *started_at = now;
                }
                Ok(self
                    .prepare_authorization(session_id, &operation, now)
                    .unwrap_or_else(|| self.apply_control_at(session_id, operation, now)))
            }
            Err(frame) => self.apply_frame(session_id, frame),
        }
    }

    fn apply_frame(
        &mut self,
        session_id: SessionId,
        frame: Frame,
    ) -> Result<Vec<GatewayAction>, ProtocolViolation> {
        if !self.sessions.contains_key(&session_id) {
            return Ok(Vec::new());
        }

        let actions = match frame {
            Frame::Unpublish {
                request_id,
                binding_id,
            } => self.unpublish(session_id, request_id, binding_id),
            Frame::OfferAccepted { pipe_id } => {
                Self::send_actions(self.offer_accepted(session_id, pipe_id)?)
            }
            Frame::OfferRejected {
                pipe_id,
                code,
                message,
            } => Self::send_actions(self.offer_rejected(session_id, pipe_id, code, message)?),
            Frame::Data { pipe_id, payload } => {
                Self::send_actions(self.data(session_id, pipe_id, payload)?)
            }
            Frame::Fin { pipe_id } => Self::send_actions(self.fin(session_id, pipe_id)?),
            Frame::Close { pipe_id } => Self::send_actions(self.close(session_id, pipe_id)?),
            Frame::Reset {
                pipe_id,
                code,
                message,
            } => Self::send_actions(self.reset(session_id, pipe_id, code, message)?),
            Frame::Cancel { pipe_id } => Self::send_actions(self.cancel(session_id, pipe_id)?),
            Frame::Ping { nonce } => self.send_to(session_id, Frame::Pong { nonce }),
            Frame::Pong { .. } => Vec::new(),
            Frame::Hello
            | Frame::Publish { .. }
            | Frame::Dial { .. }
            | Frame::Welcome { .. }
            | Frame::SessionRejected { .. }
            | Frame::Published { .. }
            | Frame::PublishFailed { .. }
            | Frame::Unpublished { .. }
            | Frame::Offer { .. }
            | Frame::Opened { .. }
            | Frame::DialFailed { .. } => Vec::new(),
        };
        Ok(actions)
    }

    pub(crate) fn prepare_authorization(
        &mut self,
        session_id: SessionId,
        operation: &ControlOperation,
        now: Instant,
    ) -> Option<Vec<GatewayAction>> {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            return Some(Vec::new());
        };
        if let ControlOperation::Dial { connection_id, .. } = operation {
            observe_dial_request();
            if session
                .highest_connection_id
                .is_some_and(|highest| *connection_id <= highest)
            {
                return Some(self.authorization_failed(
                    session_id,
                    operation,
                    ErrorCode::ProtocolError,
                ));
            }
            session.highest_connection_id = Some(*connection_id);
        }
        if self.draining {
            return Some(self.authorization_failed(session_id, operation, ErrorCode::Unavailable));
        }
        (!self.admit_control(session_id, operation.name(), now))
            .then(|| self.authorization_failed(session_id, operation, ErrorCode::ResourceExhausted))
    }

    pub(crate) fn commit_authorized(
        &mut self,
        session_id: SessionId,
        operation: ControlOperation,
        verified: VerifiedAuthorization,
        now: Instant,
    ) -> Vec<GatewayAction> {
        if !self.sessions.contains_key(&session_id) {
            if let ControlOperation::Dial { started_at, .. } = operation {
                observe_dial_result(Some(started_at), Some(ErrorCode::Cancelled));
            }
            return Vec::new();
        }
        if !verified.authorizes(&operation, now.into()) {
            return self.authorization_failed(session_id, &operation, ErrorCode::Unauthenticated);
        }
        self.apply_control_at(session_id, operation, now)
    }

    fn apply_control_at(
        &mut self,
        session_id: SessionId,
        operation: ControlOperation,
        now: Instant,
    ) -> Vec<GatewayAction> {
        match operation {
            ControlOperation::Publish {
                request_id,
                destination,
            } => self.publish(session_id, request_id, destination),
            ControlOperation::Dial {
                connection_id,
                destination,
                started_at,
            } => self.dial(session_id, connection_id, destination, now, started_at),
        }
    }

    pub(crate) fn authorization_failed(
        &self,
        session_id: SessionId,
        operation: &ControlOperation,
        code: ErrorCode,
    ) -> Vec<GatewayAction> {
        if let ControlOperation::Dial { started_at, .. } = operation {
            observe_dial_result(Some(*started_at), Some(code));
        } else {
            metrics::counter!(
                "relaygate_gateway_publish_results_total",
                "outcome" => "error",
                "code" => code.metric_name(),
            )
            .increment(1);
        }
        self.send_to(session_id, operation.failure(code))
    }

    pub(crate) fn snapshot(&self) -> GatewaySnapshot {
        let mut snapshot = GatewaySnapshot::from_parts(
            self.sessions.len(),
            self.registry.binding_count(),
            self.pending_offer_count,
            self.live_pipe_count,
            self.draining,
        );
        snapshot.remote_open_attempts = self.remote_open_attempts.len();
        snapshot.originated_pipes = self.originated_pipe_count;
        snapshot.max_sessions = self.limits.max_sessions;
        snapshot.max_bindings = self.limits.max_bindings;
        snapshot.max_pending_offers = self.limits.max_pending_offers;
        snapshot.max_remote_dial_attempts = self.limits.max_remote_dial_attempts;
        snapshot.max_live_pipes = self.limits.max_live_pipes;
        snapshot
    }

    #[cfg(test)]
    fn pipe_count(&self) -> usize {
        self.pipes.len()
    }

    fn pending_offer_count(&self) -> usize {
        self.pending_offer_count
    }

    fn live_pipe_count(&self) -> usize {
        self.live_pipe_count
    }

    pub(crate) fn begin_draining(&mut self) -> Vec<GatewayAction> {
        if self.draining {
            return Vec::new();
        }
        self.draining = true;
        self.sessions
            .keys()
            .map(|session_id| GatewayAction::PublishRegistration {
                session_id: *session_id,
                bindings: Vec::new(),
            })
            .collect()
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.pending_offer_count == 0
            && self.live_pipe_count == 0
            && self.remote_open_attempts.is_empty()
    }

    fn insert_offer(&mut self, pipe_id: PipeId, pipe: PipeEntry) {
        debug_assert_eq!(pipe.phase, PipePhase::Offered);
        self.index_peer_pipe(pipe_id, &pipe);
        let previous = self.pipes.insert(pipe_id, pipe);
        debug_assert!(previous.is_none());
        self.pending_offer_count += 1;
    }

    fn remove_pipe(&mut self, pipe_id: PipeId) -> Option<PipeEntry> {
        let pipe = self.pipes.remove(&pipe_id)?;
        if let Some(peer_key) = pipe.peer_key() {
            self.peer_pipes.remove(&peer_key);
        }
        if let Some(open_identity) = pipe.open_identity {
            self.active_peer_opens.remove(&open_identity);
        }
        match pipe.phase {
            PipePhase::Offered => self.pending_offer_count -= 1,
            PipePhase::Open => {
                self.live_pipe_count -= 1;
                if matches!(pipe.dialer, PipeEndpoint::Sdk(_)) {
                    self.originated_pipe_count -= 1;
                }
            }
        }
        Some(pipe)
    }

    fn promote_offer(&mut self, pipe_id: PipeId) -> Option<&mut PipeEntry> {
        let pipe = self.pipes.get_mut(&pipe_id)?;
        if pipe.phase != PipePhase::Offered {
            return None;
        }
        pipe.phase = PipePhase::Open;
        self.pending_offer_count -= 1;
        self.live_pipe_count += 1;
        if matches!(pipe.dialer, PipeEndpoint::Sdk(_)) {
            self.originated_pipe_count += 1;
        }
        Some(pipe)
    }

    fn insert_open(&mut self, pipe_id: PipeId, pipe: PipeEntry) {
        debug_assert_eq!(pipe.phase, PipePhase::Open);
        self.index_peer_pipe(pipe_id, &pipe);
        if matches!(pipe.dialer, PipeEndpoint::Sdk(_)) {
            self.originated_pipe_count += 1;
        }
        let previous = self.pipes.insert(pipe_id, pipe);
        debug_assert!(previous.is_none());
        self.live_pipe_count += 1;
    }

    fn index_peer_pipe(&mut self, pipe_id: PipeId, pipe: &PipeEntry) {
        let Some(peer_key) = pipe.peer_key() else {
            return;
        };
        let previous = self.peer_pipes.insert(peer_key, pipe_id);
        debug_assert!(previous.is_none());
        if let Some(open_identity) = pipe.open_identity {
            let previous = self.active_peer_opens.insert(open_identity, peer_key);
            debug_assert!(previous.is_none());
        }
    }

    fn send_actions<T>(deliveries: Vec<T>) -> Vec<GatewayAction>
    where
        T: Into<GatewayAction>,
    {
        deliveries.into_iter().map(Into::into).collect()
    }

    fn registration_publication(&self, session_id: SessionId) -> GatewayAction {
        let bindings = if self.draining {
            Vec::new()
        } else {
            self.registry.bindings_for_session(session_id)
        };
        GatewayAction::PublishRegistration {
            session_id,
            bindings,
        }
    }
}

fn observe_dial_request() {
    metrics::counter!("relaygate_gateway_dial_requests_total").increment(1);
}

pub(crate) fn observe_dial_result(started_at: Option<Instant>, code: Option<ErrorCode>) {
    let Some(started_at) = started_at else {
        return;
    };
    let class = dial_result_class(code);
    let (outcome, code) = match code {
        None => ("success", "ok"),
        Some(ErrorCode::Cancelled) => ("cancelled", "cancelled"),
        Some(code) => ("error", code.metric_name()),
    };
    metrics::counter!(
        "relaygate_gateway_dial_results_total",
        "outcome" => outcome,
        "code" => code,
        "class" => class
    )
    .increment(1);
    metrics::histogram!(
        "relaygate_gateway_dial_duration_seconds",
        "outcome" => outcome
    )
    .record(started_at.elapsed().as_secs_f64());
}

fn dial_result_class(code: Option<ErrorCode>) -> &'static str {
    match code {
        None => "success",
        Some(ErrorCode::Cancelled) => "cancelled",
        Some(ErrorCode::ResourceExhausted) => "capacity",
        Some(ErrorCode::Unavailable | ErrorCode::DeadlineExceeded) => "availability",
        Some(ErrorCode::Internal) => "internal",
        Some(
            ErrorCode::InvalidArgument
            | ErrorCode::Unauthenticated
            | ErrorCode::PermissionDenied
            | ErrorCode::NotFound
            | ErrorCode::FailedPrecondition
            | ErrorCode::ProtocolError
            | ErrorCode::AlreadyExists,
        ) => "request",
    }
}
