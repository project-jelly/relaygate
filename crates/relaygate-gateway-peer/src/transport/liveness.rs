use std::time::Duration;

use tokio::time::Instant;

use crate::{
    frame::PeerFrame,
    identity::{PeerTransportId, StreamEndpoint},
};

#[derive(Debug)]
pub(super) struct TransportLiveness {
    heartbeat_idle_interval: Duration,
    heartbeat_response_timeout: Duration,
    idle_retirement_timeout: Duration,
    last_inbound: Instant,
    pending: Option<PendingHeartbeat>,
    zero_stream_since: Option<Instant>,
    next_nonce: u64,
}

#[derive(Debug)]
struct PendingHeartbeat {
    nonce: u64,
    sent_at: Instant,
    deadline: Instant,
}

#[derive(Debug)]
pub(super) enum LivenessAction {
    Ping(PeerFrame),
    HeartbeatTimeout,
    IdleRetired,
}

impl TransportLiveness {
    pub(super) fn new(
        heartbeat_idle_interval: Duration,
        heartbeat_response_timeout: Duration,
        idle_retirement_timeout: Duration,
    ) -> Self {
        Self {
            heartbeat_idle_interval,
            heartbeat_response_timeout,
            idle_retirement_timeout,
            last_inbound: Instant::now(),
            pending: None,
            zero_stream_since: None,
            next_nonce: 1,
        }
    }

    pub(super) fn sync_stream_state(&mut self, is_empty: bool) {
        match (is_empty, self.zero_stream_since) {
            (true, None) => {
                self.zero_stream_since = Some(Instant::now());
                self.pending = None;
            }
            (false, Some(_)) => {
                self.zero_stream_since = None;
                self.last_inbound = Instant::now();
            }
            _ => {}
        }
    }

    pub(super) fn observe_inbound(&mut self, frame: &PeerFrame) -> Option<Duration> {
        let round_trip = match (&self.pending, frame) {
            (Some(pending), PeerFrame::Pong { nonce })
                if pending.nonce == *nonce && Instant::now() < pending.deadline =>
            {
                Some(pending.sent_at.elapsed())
            }
            (Some(_), _) | (None, PeerFrame::Pong { .. }) => return None,
            (
                None,
                PeerFrame::Ping { .. }
                | PeerFrame::Open { .. }
                | PeerFrame::Opened { .. }
                | PeerFrame::Failed { .. }
                | PeerFrame::Data { .. }
                | PeerFrame::Fin { .. }
                | PeerFrame::Close { .. }
                | PeerFrame::Reset { .. },
            ) => None,
            (_, PeerFrame::Hello(_))
            | (_, PeerFrame::Welcome(_))
            | (_, PeerFrame::HandshakeRejected { .. }) => return None,
        };
        self.last_inbound = Instant::now();
        self.pending = None;
        round_trip
    }

    pub(super) fn response_timed_out(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= Instant::now())
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        if let Some(zero_stream_since) = self.zero_stream_since {
            return Some(zero_stream_since + self.idle_retirement_timeout);
        }
        Some(self.pending.as_ref().map_or(
            self.last_inbound + self.heartbeat_idle_interval,
            |pending| pending.deadline,
        ))
    }

    pub(super) fn on_deadline(
        &mut self,
        now: Instant,
        is_empty: bool,
        command_queue_empty: bool,
    ) -> Option<LivenessAction> {
        if is_empty {
            if self
                .zero_stream_since
                .is_some_and(|since| since + self.idle_retirement_timeout <= now)
            {
                if command_queue_empty {
                    return Some(LivenessAction::IdleRetired);
                }
                self.zero_stream_since = Some(now);
            }
            return None;
        }
        if let Some(pending) = &self.pending {
            return (pending.deadline <= now).then_some(LivenessAction::HeartbeatTimeout);
        }
        if self.last_inbound + self.heartbeat_idle_interval > now {
            return None;
        }
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1).max(1);
        self.pending = Some(PendingHeartbeat {
            nonce,
            sent_at: now,
            deadline: now + self.heartbeat_response_timeout,
        });
        Some(LivenessAction::Ping(PeerFrame::Ping { nonce }))
    }

    pub(super) fn mark_probe_committed(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let now = Instant::now();
            pending.sent_at = now;
            pending.deadline = now + self.heartbeat_response_timeout;
        }
    }
}

pub(super) fn staggered_interval(
    interval: Duration,
    peer_transport_id: PeerTransportId,
    endpoint: StreamEndpoint,
) -> Duration {
    let salt = match endpoint {
        StreamEndpoint::Dialer => 0x50_u8,
        StreamEndpoint::Acceptor => 0x51_u8,
    };
    crate::jitter::staggered(interval, peer_transport_id.as_uuid(), salt)
}

#[cfg(test)]
mod tests {
    use super::{LivenessAction, TransportLiveness, staggered_interval};
    use crate::{
        frame::PeerFrame,
        identity::{PeerTransportId, StreamEndpoint},
    };
    use std::time::Duration;
    use tokio::time::Instant;

    fn liveness() -> TransportLiveness {
        TransportLiveness::new(
            Duration::from_secs(60),
            Duration::from_secs(20),
            Duration::from_secs(300),
        )
    }

    #[test]
    fn zero_stream_idle_retirement_waits_then_closes() {
        let mut liveness = liveness();

        liveness.sync_stream_state(true);
        assert!(liveness.on_deadline(Instant::now(), true, true).is_none());

        liveness.zero_stream_since = Some(Instant::now() - Duration::from_secs(300));
        assert!(matches!(
            liveness.on_deadline(Instant::now(), true, true),
            Some(LivenessAction::IdleRetired)
        ));
    }

    #[test]
    fn active_inbound_activity_does_not_clear_pending_heartbeat() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);

        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));
        assert!(liveness.pending.is_some());

        assert!(
            liveness
                .observe_inbound(&PeerFrame::Data {
                    stream_id: crate::identity::StreamId::from_raw(0),
                    payload: bytes::Bytes::from_static(b"x"),
                })
                .is_none()
        );
        assert!(liveness.pending.is_some());
    }

    #[test]
    fn unmatched_pong_does_not_clear_pending_heartbeat() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(_))
        ));

        assert!(
            liveness
                .observe_inbound(&PeerFrame::Pong { nonce: 999 })
                .is_none()
        );
        assert!(liveness.pending.is_some());
    }

    #[test]
    fn matching_pong_clears_pending_heartbeat() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));

        assert!(
            liveness
                .observe_inbound(&PeerFrame::Pong { nonce: 1 })
                .is_some()
        );

        assert!(liveness.pending.is_none());
    }

    #[test]
    fn late_matching_pong_does_not_clear_pending_heartbeat() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));
        if let Some(pending) = liveness.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        assert!(
            liveness
                .observe_inbound(&PeerFrame::Pong { nonce: 1 })
                .is_none()
        );

        assert!(liveness.pending.is_some());
        assert!(liveness.response_timed_out());
    }

    #[test]
    fn pending_probe_timeout_returns_heartbeat_timeout() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(_))
        ));
        let deadline = liveness.next_deadline().unwrap_or_else(Instant::now);

        assert!(matches!(
            liveness.on_deadline(deadline, false, true),
            Some(LivenessAction::HeartbeatTimeout)
        ));
    }

    #[test]
    fn probe_commit_starts_full_response_window() {
        let mut liveness = liveness();
        liveness.last_inbound = Instant::now() - Duration::from_secs(60);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));
        if let Some(pending) = liveness.pending.as_mut() {
            pending.deadline = Instant::now() - Duration::from_millis(1);
        }

        let committed_at = Instant::now();
        liveness.mark_probe_committed();

        assert!(!liveness.response_timed_out());
        assert!(
            liveness.next_deadline().unwrap_or_else(Instant::now)
                >= committed_at + liveness.heartbeat_response_timeout
        );
    }

    #[test]
    fn earlier_open_deadline_does_not_trigger_lifecycle_action() {
        let mut liveness = liveness();

        assert!(liveness.on_deadline(Instant::now(), false, true).is_none());
        assert!(liveness.pending.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn earlier_open_deadline_preserves_pending_probe_and_matching_pong() {
        let mut liveness = liveness();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));
        liveness.mark_probe_committed();
        let original_deadline = liveness.next_deadline();

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(liveness.on_deadline(Instant::now(), false, true).is_none());
        assert_eq!(liveness.next_deadline(), original_deadline);
        assert_eq!(liveness.pending.as_ref().map(|probe| probe.nonce), Some(1));
        assert!(
            liveness
                .observe_inbound(&PeerFrame::Pong { nonce: 1 })
                .is_some()
        );
        assert!(liveness.pending.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_open_deadlines_do_not_extend_pending_probe_timeout() {
        let mut liveness = liveness();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::Ping(PeerFrame::Ping { nonce: 1 }))
        ));
        liveness.mark_probe_committed();
        let original_deadline = liveness.next_deadline();

        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(5)).await;
            assert!(liveness.on_deadline(Instant::now(), false, true).is_none());
            assert_eq!(liveness.next_deadline(), original_deadline);
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(Some(Instant::now()), original_deadline);
        assert!(matches!(
            liveness.on_deadline(Instant::now(), false, true),
            Some(LivenessAction::HeartbeatTimeout)
        ));
    }

    #[test]
    fn queued_command_defers_idle_retirement_at_expiry() {
        let mut liveness = liveness();
        liveness.zero_stream_since = Some(Instant::now() - Duration::from_secs(300));

        assert!(liveness.on_deadline(Instant::now(), true, false).is_none());
        assert!(liveness.zero_stream_since.is_some());
    }

    #[test]
    fn new_stream_cancels_zero_stream_retirement() {
        let mut liveness = liveness();
        liveness.zero_stream_since = Some(Instant::now() - Duration::from_secs(300));

        liveness.sync_stream_state(false);

        assert!(liveness.zero_stream_since.is_none());
        assert!(liveness.on_deadline(Instant::now(), false, true).is_none());
    }

    #[test]
    fn heartbeat_interval_is_deterministically_staggered_within_ten_percent() {
        let interval = Duration::from_secs(60);
        let transport_id = PeerTransportId::new();
        let dialer = staggered_interval(interval, transport_id, StreamEndpoint::Dialer);
        let acceptor = staggered_interval(interval, transport_id, StreamEndpoint::Acceptor);

        assert!(dialer >= Duration::from_secs(54));
        assert!(dialer <= Duration::from_secs(66));
        assert!(acceptor >= Duration::from_secs(54));
        assert!(acceptor <= Duration::from_secs(66));
        assert_eq!(
            dialer,
            staggered_interval(interval, transport_id, StreamEndpoint::Dialer)
        );
    }
}
