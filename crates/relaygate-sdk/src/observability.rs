use std::time::Instant;

mod operation;
pub(crate) use operation::observe;
#[cfg(test)]
mod connection_tests;
#[cfg(test)]
mod contract_tests;

// Tracing callsite interest is process-global even with a thread-local subscriber.
#[cfg(test)]
pub(crate) static RECONNECT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) struct ReconnectEpisode {
    started_at: Instant,
    attempts: u64,
    active: metrics::Gauge,
    outcome: &'static str,
}

impl ReconnectEpisode {
    pub(crate) fn start() -> Self {
        tracing::info!(
            component = "sdk",
            event = "sdk.session.reconnect_started",
            "SDK session reconnect episode started"
        );
        let active = metrics::gauge!("relaygate_sdk_reconnect_in_progress");
        active.increment(1.0);
        Self {
            started_at: Instant::now(),
            attempts: 0,
            active,
            outcome: "aborted",
        }
    }

    pub(crate) fn record_attempt(&mut self, outcome: &'static str) {
        self.attempts = self.attempts.saturating_add(1);
        metrics::counter!(
            "relaygate_sdk_reconnect_attempts_total",
            "outcome" => outcome
        )
        .increment(1);
    }

    pub(crate) fn recover(self) {
        self.settle(
            "recovered",
            "sdk.session.reconnect_recovered",
            "SDK session reconnect episode recovered",
        );
    }

    pub(crate) fn degrade(self) {
        self.settle(
            "degraded",
            "sdk.session.reconnect_degraded",
            "SDK session reconnect episode settled with blocked listeners",
        );
    }

    /// Both settled outcomes record the recovery histogram; `closed` does not.
    fn settle(mut self, outcome: &'static str, event: &'static str, message: &'static str) {
        self.outcome = outcome;
        let elapsed = self.started_at.elapsed();
        metrics::histogram!("relaygate_sdk_reconnect_duration_seconds")
            .record(elapsed.as_secs_f64());
        tracing::info!(
            component = "sdk",
            event,
            attempts = self.attempts,
            downtime_ms = elapsed.as_millis(),
            message
        );
    }

    pub(crate) fn close(mut self) {
        self.outcome = "closed";
        let elapsed = self.started_at.elapsed();
        tracing::info!(
            component = "sdk",
            event = "sdk.session.reconnect_closed",
            attempts = self.attempts,
            downtime_ms = elapsed.as_millis(),
            "SDK session reconnect episode closed with its runtime"
        );
    }
}

impl Drop for ReconnectEpisode {
    fn drop(&mut self) {
        self.active.decrement(1.0);
        metrics::counter!("relaygate_sdk_reconnect_episodes_total", "outcome" => self.outcome)
            .increment(1);
        metrics::histogram!("relaygate_sdk_reconnect_episode_duration_seconds", "outcome" => self.outcome)
            .record(self.started_at.elapsed().as_secs_f64());
    }
}

pub(crate) fn close_reconnect_episode(episode: &mut Option<ReconnectEpisode>) {
    if let Some(episode) = episode.take() {
        episode.close();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[test]
    fn reconnect_episode_records_recovery_and_runtime_close()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = RECONNECT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let logs = Arc::new(Mutex::new(Vec::new()));
        let writer_logs = Arc::clone(&logs);
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || BufferWriter(Arc::clone(&writer_logs)))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            metrics::with_local_recorder(&recorder, || {
                let mut episode = ReconnectEpisode::start();
                episode.record_attempt("error");
                episode.record_attempt("success");
                episode.recover();

                let mut closed = Some(ReconnectEpisode::start());
                if let Some(episode) = closed.as_mut() {
                    episode.record_attempt("error");
                }
                close_reconnect_episode(&mut closed);

                let mut degraded = ReconnectEpisode::start();
                degraded.record_attempt("success");
                degraded.degrade();
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let attempt_values = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                (key.key().name() == "relaygate_sdk_reconnect_attempts_total").then_some(value)
            })
            .collect::<Vec<_>>();
        assert_eq!(attempt_values.len(), 2);
        assert_eq!(
            attempt_values
                .iter()
                .map(|value| match value {
                    DebugValue::Counter(value) => *value,
                    _ => 0,
                })
                .sum::<u64>(),
            4
        );

        let durations = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                (key.key().name() == "relaygate_sdk_reconnect_duration_seconds").then_some(value)
            })
            .collect::<Vec<_>>();
        assert_eq!(durations.len(), 1);
        assert!(matches!(durations[0], DebugValue::Histogram(values) if values.len() == 2));

        let logs = match logs.lock() {
            Ok(logs) => logs.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let logs = String::from_utf8(logs)?;
        assert_eq!(logs.matches("sdk.session.reconnect_started").count(), 3);
        assert_eq!(logs.matches("sdk.session.reconnect_recovered").count(), 1);
        assert_eq!(logs.matches("sdk.session.reconnect_degraded").count(), 1);
        assert_eq!(logs.matches("sdk.session.reconnect_closed").count(), 1);
        assert!(logs.contains("\"attempts\":2"));
        Ok(())
    }

    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            match self.0.lock() {
                Ok(mut bytes) => bytes.extend_from_slice(buffer),
                Err(poisoned) => poisoned.into_inner().extend_from_slice(buffer),
            }
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
