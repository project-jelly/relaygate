use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relaygate_protocol::{DEFAULT_MAX_FRAME_LEN, Frame, FrameCodec, SessionId};
use relaygate_transport::BoxedIo;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::{Config, Error, ErrorCode, ErrorOrigin, PeerObservation, Result};

mod outbound;

pub(crate) use outbound::{FrameCommit, SessionOutbound, session_outbound_channel};

pub(crate) type WireTransport = Framed<BoxedIo, FrameCodec>;

const SDK_FRAME_INITIAL_CAPACITY: usize = 2 * 1024;
const SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY: usize = 8 * 1024;

pub(crate) struct EstablishedSession {
    pub(crate) id: SessionId,
    pub(crate) transport: WireTransport,
}

pub(crate) async fn establish_initial(config: &Config) -> Result<EstablishedSession> {
    // Observe the whole initial connection: its outer deadline must resolve
    // before the metric guard, rather than dropping an observed inner attempt.
    crate::observability::observe("session_connect", async {
        crate::retry::retry_precommit(
            config.connect_deadline()?,
            ReconnectBackoff::new(config.reconnect_initial, config.reconnect_maximum),
            &CancellationToken::new(),
            Error::transport_deadline("Gateway connection or handshake deadline exceeded"),
            || establish_inner(config),
        )
        .await
    })
    .await
}

pub(crate) async fn establish(config: &Config) -> Result<EstablishedSession> {
    crate::observability::observe("session_connect", async {
        timeout(config.connect_timeout, establish_inner(config))
            .await
            .map_err(|_| {
                Error::new(
                    ErrorCode::DeadlineExceeded,
                    PeerObservation::NotObserved,
                    "Gateway connection or handshake deadline exceeded",
                )
                .with_origin(ErrorOrigin::Transport)
            })?
    })
    .await
}

async fn establish_inner(config: &Config) -> Result<EstablishedSession> {
    let stream = config.transport.connect().await?;
    let mut transport = Framed::with_capacity(
        stream,
        FrameCodec::new(DEFAULT_MAX_FRAME_LEN),
        SDK_FRAME_INITIAL_CAPACITY,
    );
    transport.set_backpressure_boundary(SDK_FRAME_WRITE_BACKPRESSURE_BOUNDARY);
    transport
        .send(Frame::Hello)
        .await
        .map_err(|error| Error::from_protocol(error, "session HELLO failed"))?;
    let frame = transport
        .next()
        .await
        .ok_or_else(|| Error::transport_unavailable("Gateway closed before WELCOME"))?
        .map_err(|error| Error::from_protocol(error, "WELCOME decode failed"))?;
    let session_id = match frame {
        Frame::Welcome { session_id } => session_id,
        Frame::SessionRejected { code, message } => {
            return Err(Error::from_gateway(
                code,
                PeerObservation::NotObserved,
                message,
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorCode::ProtocolError,
                PeerObservation::NotObserved,
                "first Gateway response was not WELCOME",
            )
            .with_origin(ErrorOrigin::Transport));
        }
    };
    tracing::debug!(
        component = "sdk",
        event = "sdk.session.ready",
        session_id = %session_id.as_uuid(),
        "SDK session is ready"
    );
    Ok(EstablishedSession {
        id: session_id,
        transport,
    })
}

/// One session's bounded, cancellable frame sender.
pub(crate) struct SessionLink<'a> {
    transport: &'a mut WireTransport,
    timeout: Duration,
    cancel: &'a CancellationToken,
}

impl<'a> SessionLink<'a> {
    pub(crate) fn new(
        transport: &'a mut WireTransport,
        timeout: Duration,
        cancel: &'a CancellationToken,
    ) -> Self {
        Self {
            transport,
            timeout,
            cancel,
        }
    }

    pub(crate) async fn send(&mut self, frame: Frame) -> Result<()> {
        send_bounded(self.transport, frame, self.timeout, self.cancel).await
    }
}

pub(crate) async fn send_bounded(
    transport: &mut WireTransport,
    frame: Frame,
    duration: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Error::new(ErrorCode::Cancelled, PeerObservation::NotObserved, "RelaySession frame write was cancelled")),
        result = timeout(duration, transport.send(frame)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(Error::from_protocol(error, "RelaySession frame write failed")),
                Err(_) => Err(Error::transport_deadline("RelaySession frame write deadline exceeded")),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SessionHeartbeat {
    idle_timeout: Duration,
    response_timeout: Duration,
    last_inbound: Instant,
    pending: Option<PendingHeartbeat>,
    next_nonce: u64,
}

#[derive(Debug)]
struct PendingHeartbeat {
    nonce: u64,
    deadline: Instant,
}

impl SessionHeartbeat {
    pub(crate) fn new(config: &Config, session_id: SessionId, salt: u8) -> Self {
        Self {
            idle_timeout: jittered_duration(config.heartbeat_idle_interval, session_id, salt),
            response_timeout: config.heartbeat_response_timeout,
            last_inbound: Instant::now(),
            pending: None,
            next_nonce: 1,
        }
    }

    pub(crate) fn observe_inbound(&mut self, frame: &Frame) {
        match (&self.pending, frame) {
            (Some(pending), Frame::Pong { nonce })
                if pending.nonce == *nonce && Instant::now() < pending.deadline => {}
            (Some(_), _) | (None, Frame::Pong { .. }) => return,
            (None, Frame::Hello)
            | (None, Frame::Welcome { .. })
            | (None, Frame::SessionRejected { .. })
            | (None, Frame::Publish { .. })
            | (None, Frame::Published { .. })
            | (None, Frame::PublishFailed { .. })
            | (None, Frame::Unpublish { .. })
            | (None, Frame::Unpublished { .. })
            | (None, Frame::Dial { .. })
            | (None, Frame::Offer { .. })
            | (None, Frame::OfferAccepted { .. })
            | (None, Frame::OfferRejected { .. })
            | (None, Frame::Opened { .. })
            | (None, Frame::DialFailed { .. })
            | (None, Frame::Data { .. })
            | (None, Frame::Fin { .. })
            | (None, Frame::Close { .. })
            | (None, Frame::Reset { .. })
            | (None, Frame::Ping { .. })
            | (None, Frame::Cancel { .. }) => {}
        }
        self.last_inbound = Instant::now();
        self.pending = None;
    }

    pub(crate) fn response_timed_out(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= Instant::now())
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        self.pending.as_ref().map_or_else(
            || {
                self.last_inbound
                    .checked_add(self.idle_timeout)
                    .unwrap_or(self.last_inbound)
            },
            |pending| pending.deadline,
        )
    }

    pub(crate) fn on_deadline(&mut self) -> Option<Frame> {
        let now = Instant::now();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.deadline <= now)
        {
            return None;
        }
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1).max(1);
        let deadline = now.checked_add(self.response_timeout)?;
        self.pending = Some(PendingHeartbeat { nonce, deadline });
        Some(Frame::Ping { nonce })
    }

    pub(crate) fn mark_probe_committed(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            let now = Instant::now();
            pending.deadline = now.checked_add(self.response_timeout).unwrap_or(now);
        }
    }
}

pub(crate) async fn wait_for_heartbeat(deadline: Instant) {
    sleep_until(deadline).await;
}

fn jittered_duration(duration: Duration, session_id: SessionId, salt: u8) -> Duration {
    let mut hash = u64::from(salt);
    for byte in session_id.as_uuid().as_bytes() {
        hash = hash.wrapping_mul(16_777_619) ^ u64::from(*byte);
    }
    let offset_per_mille = (hash % 201) as i128 - 100;
    let factor_per_mille = 1_000_i128 + offset_per_mille;
    let nanos = duration.as_nanos().saturating_mul(factor_per_mille as u128) / 1_000;
    Duration::from_nanos(nanos.try_into().unwrap_or(u64::MAX)).max(Duration::from_millis(1))
}

pub(crate) struct ReconnectBackoff {
    initial: Duration,
    current: Duration,
    maximum: Duration,
    entropy: u64,
}

impl ReconnectBackoff {
    pub(crate) fn new(initial: Duration, maximum: Duration) -> Self {
        let seed = SessionId::new();
        let mut entropy = 14_695_981_039_346_656_037_u64;
        for byte in seed.as_uuid().as_bytes() {
            entropy = entropy.wrapping_mul(1_099_511_628_211) ^ u64::from(*byte);
        }
        Self {
            initial,
            current: initial,
            maximum,
            entropy,
        }
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        self.entropy ^= self.entropy << 13;
        self.entropy ^= self.entropy >> 7;
        self.entropy ^= self.entropy << 17;

        let base_nanos = self.current.as_nanos();
        let floor_nanos = base_nanos.saturating_mul(2) / 3;
        let jitter_nanos = (base_nanos - floor_nanos).saturating_mul(u128::from(self.entropy))
            / u128::from(u64::MAX);
        let delay = duration_from_nanos(floor_nanos.saturating_add(jitter_nanos));
        self.current = self.current.saturating_mul(2).min(self.maximum);
        delay
    }

    pub(crate) fn reset(&mut self) {
        self.current = self.initial;
    }

    #[cfg(test)]
    pub(crate) const fn current_delay(&self) -> Duration {
        self.current
    }
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = (nanos / NANOS_PER_SECOND).min(u128::from(u64::MAX));
    let subsecond_nanos = if seconds == u128::from(u64::MAX) {
        999_999_999
    } else {
        nanos % NANOS_PER_SECOND
    };
    Duration::new(seconds as u64, subsecond_nanos as u32).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests;
