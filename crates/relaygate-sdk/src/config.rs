use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::{Error, ErrorCode, GatewayTransportConfig, PeerObservation, Result};

const MIB: usize = 1024 * 1024;

/// Process-local resource bounds for one [`crate::Relay`].
///
/// These limits protect a cooperative SDK process. Gateway admission remains
/// the authoritative cluster-side limit for untrusted clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    pub(crate) max_pending_pipes_per_listener: usize,
    pub(crate) max_live_pipes_per_listener: usize,
    pub(crate) max_live_pipes_per_relay: usize,
    pub(crate) max_buffered_frames_per_pipe: usize,
    pub(crate) max_buffered_bytes_per_pipe: usize,
    pub(crate) max_buffered_bytes_per_relay: usize,
}

impl ResourceLimits {
    /// Sets the number of incoming Pipes that may wait for one Listener.
    #[must_use]
    pub const fn with_max_pending_pipes_per_listener(mut self, maximum: usize) -> Self {
        self.max_pending_pipes_per_listener = maximum;
        self
    }

    /// Sets the number of live Pipes owned by one Listener.
    #[must_use]
    pub const fn with_max_live_pipes_per_listener(mut self, maximum: usize) -> Self {
        self.max_live_pipes_per_listener = maximum;
        self
    }

    /// Sets the total number of live Pipes owned by one Relay.
    #[must_use]
    pub const fn with_max_live_pipes_per_relay(mut self, maximum: usize) -> Self {
        self.max_live_pipes_per_relay = maximum;
        self
    }

    /// Sets the maximum number of inbound frames buffered by one Pipe.
    #[must_use]
    pub const fn with_max_buffered_frames_per_pipe(mut self, maximum: usize) -> Self {
        self.max_buffered_frames_per_pipe = maximum;
        self
    }

    /// Sets the maximum number of inbound payload bytes buffered by one Pipe.
    #[must_use]
    pub const fn with_max_buffered_bytes_per_pipe(mut self, maximum: usize) -> Self {
        self.max_buffered_bytes_per_pipe = maximum;
        self
    }

    /// Sets the total inbound payload bytes buffered by one Relay.
    #[must_use]
    pub const fn with_max_buffered_bytes_per_relay(mut self, maximum: usize) -> Self {
        self.max_buffered_bytes_per_relay = maximum;
        self
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_pending_pipes_per_listener: 64,
            max_live_pipes_per_listener: 10_000,
            max_live_pipes_per_relay: 20_000,
            max_buffered_frames_per_pipe: 64,
            max_buffered_bytes_per_pipe: MIB,
            max_buffered_bytes_per_relay: 64 * MIB,
        }
    }
}

/// Runtime limits, TLS identity and reconnect policy for one Relay.
#[derive(Clone)]
pub struct Config {
    pub(crate) transport: GatewayTransportConfig,
    pub(crate) connect_timeout: Duration,
    pub(crate) operation_timeout: Duration,
    pub(crate) heartbeat_idle_interval: Duration,
    pub(crate) heartbeat_response_timeout: Duration,
    pub(crate) reconnect_initial: Duration,
    pub(crate) reconnect_maximum: Duration,
    pub(crate) outbound_capacity: usize,
    pub(crate) resource_limits: ResourceLimits,
}

impl Config {
    /// Connects to `host:port` or `tls://host:port` using public CA trust.
    /// `tcp://host:port` explicitly selects unencrypted transport, including
    /// operation access tokens. TLS failures never fall back to plaintext.
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self> {
        Ok(Self::new_with_transport(
            GatewayTransportConfig::from_endpoint(endpoint.as_ref())?,
        ))
    }

    /// Replaces public trust with a private CA; the endpoint still supplies
    /// the server name. Requires [`Config::new`]; explicit transports configure
    /// their CA through `ClientTlsConfig` to preserve custom identity settings.
    /// Not applicable to plaintext endpoints.
    pub fn with_ca_certificate(mut self, pem: &[u8]) -> Result<Self> {
        self.transport = self.transport.with_ca_certificate(pem)?;
        Ok(self)
    }

    /// Creates a Relay configuration with an explicit Gateway transport.
    #[must_use]
    pub fn with_transport(transport: GatewayTransportConfig) -> Self {
        Self::new_with_transport(transport)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "insecure-test-transport"))]
    #[must_use]
    pub fn new_insecure_for_tests(gateway_addr: impl Into<String>) -> Self {
        Self::new_with_transport(GatewayTransportConfig::insecure_tcp(gateway_addr))
    }

    fn new_with_transport(transport: GatewayTransportConfig) -> Self {
        Self {
            transport,
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            heartbeat_idle_interval: Duration::from_secs(60),
            heartbeat_response_timeout: Duration::from_secs(20),
            reconnect_initial: Duration::from_millis(100),
            reconnect_maximum: Duration::from_secs(5),
            outbound_capacity: 256,
            resource_limits: ResourceLimits::default(),
        }
    }

    /// Sets the total deadline for initial connection, including retries.
    /// Managed reconnect uses the same limit for each connection attempt.
    #[must_use]
    pub const fn with_connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    /// Sets the deadline for one `listen` or `dial` control operation.
    #[must_use]
    pub const fn with_operation_timeout(mut self, value: Duration) -> Self {
        self.operation_timeout = value;
        self
    }

    /// Configures transport liveness probing for the SDK-Gateway session.
    ///
    /// The SDK sends `PING` after `idle_interval` without valid inbound
    /// activity and closes the whole session if the matching `PONG` is not
    /// received within `response_timeout`. Pipe read idleness is not a failure.
    #[must_use]
    pub const fn with_heartbeat(
        mut self,
        idle_interval: Duration,
        response_timeout: Duration,
    ) -> Self {
        self.heartbeat_idle_interval = idle_interval;
        self.heartbeat_response_timeout = response_timeout;
        self
    }

    /// Sets the initial and maximum reconnect and precommit retry delays.
    ///
    /// Configuration validation requires both values to be positive and the
    /// maximum to be at least the initial delay.
    #[must_use]
    pub const fn with_reconnect_backoff(mut self, initial: Duration, maximum: Duration) -> Self {
        self.reconnect_initial = initial;
        self.reconnect_maximum = maximum;
        self
    }

    /// Sets the bounded number of frames queued for the Gateway writer.
    #[must_use]
    pub const fn with_outbound_capacity(mut self, value: usize) -> Self {
        self.outbound_capacity = value;
        self
    }

    /// Replaces the process-local resource limits for this Relay.
    #[must_use]
    pub const fn with_resource_limits(mut self, limits: ResourceLimits) -> Self {
        self.resource_limits = limits;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.transport.validate()?;
        if self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.heartbeat_idle_interval.is_zero()
            || self.heartbeat_response_timeout.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_maximum < self.reconnect_initial
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "timeouts and reconnect backoff must be positive and ordered",
            ));
        }
        for (name, duration) in [
            ("connect_timeout", self.connect_timeout),
            ("operation_timeout", self.operation_timeout),
            ("heartbeat_idle_interval", self.heartbeat_idle_interval),
            (
                "heartbeat_response_timeout",
                self.heartbeat_response_timeout,
            ),
            ("reconnect_initial", self.reconnect_initial),
            ("reconnect_maximum", self.reconnect_maximum),
        ] {
            deadline_from_now(name, duration)?;
        }
        let limits = self.resource_limits;
        if self.outbound_capacity == 0 {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "outbound capacity must be positive",
            ));
        }
        if limits.max_pending_pipes_per_listener == 0
            || limits.max_live_pipes_per_listener == 0
            || limits.max_live_pipes_per_relay == 0
            || limits.max_buffered_frames_per_pipe == 0
            || limits.max_buffered_bytes_per_pipe == 0
            || limits.max_buffered_bytes_per_relay == 0
            || limits.max_live_pipes_per_listener > limits.max_live_pipes_per_relay
            || limits.max_buffered_bytes_per_pipe > limits.max_buffered_bytes_per_relay
            || limits.max_pending_pipes_per_listener > Semaphore::MAX_PERMITS
            || limits.max_live_pipes_per_relay > Semaphore::MAX_PERMITS
            || limits.max_buffered_frames_per_pipe > Semaphore::MAX_PERMITS
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                PeerObservation::NotObserved,
                "SDK resource limits must be positive, ordered, and within runtime bounds",
            ));
        }
        Ok(())
    }

    pub(crate) fn operation_deadline(&self) -> Result<Instant> {
        deadline_from_now("operation_timeout", self.operation_timeout)
    }

    pub(crate) fn connect_deadline(&self) -> Result<Instant> {
        deadline_from_now("connect_timeout", self.connect_timeout)
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("transport", &self.transport)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("heartbeat_idle_interval", &self.heartbeat_idle_interval)
            .field(
                "heartbeat_response_timeout",
                &self.heartbeat_response_timeout,
            )
            .field("reconnect_initial", &self.reconnect_initial)
            .field("reconnect_maximum", &self.reconnect_maximum)
            .field("outbound_capacity", &self.outbound_capacity)
            .field("resource_limits", &self.resource_limits)
            .finish()
    }
}

fn deadline_from_now(name: &str, duration: Duration) -> Result<Instant> {
    Instant::now().checked_add(duration).ok_or_else(|| {
        Error::new(
            ErrorCode::InvalidArgument,
            PeerObservation::NotObserved,
            format!("{name} is too large to form a monotonic deadline"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_limits_are_positive_and_hierarchically_ordered() {
        let valid = Config::new_insecure_for_tests("127.0.0.1:1");
        assert!(valid.validate().is_ok());

        let zero = valid
            .clone()
            .with_resource_limits(ResourceLimits::default().with_max_buffered_bytes_per_pipe(0));
        assert!(zero.validate().is_err());

        let inverted = valid.with_resource_limits(
            ResourceLimits::default()
                .with_max_live_pipes_per_listener(3)
                .with_max_live_pipes_per_relay(2),
        );
        assert!(inverted.validate().is_err());

        for limits in [
            ResourceLimits::default()
                .with_max_pending_pipes_per_listener(Semaphore::MAX_PERMITS + 1),
            ResourceLimits::default().with_max_buffered_frames_per_pipe(Semaphore::MAX_PERMITS + 1),
        ] {
            assert!(
                Config::new_insecure_for_tests("127.0.0.1:1")
                    .with_resource_limits(limits)
                    .validate()
                    .is_err()
            );
        }
    }
}
