use std::time::Instant;

use relaygate_protocol::SessionRole;

pub(crate) struct ReconnectEpisode {
    role: &'static str,
    started_at: Instant,
    attempts: u64,
}

impl ReconnectEpisode {
    pub(crate) fn start(role: SessionRole) -> Self {
        let role = role_name(role);
        tracing::info!(
            component = "sdk",
            event = "sdk.session.reconnect_started",
            role,
            "SDK session reconnect episode started"
        );
        Self {
            role,
            started_at: Instant::now(),
            attempts: 0,
        }
    }

    pub(crate) fn record_attempt(&mut self, outcome: &'static str) {
        self.attempts = self.attempts.saturating_add(1);
        metrics::counter!(
            "relaygate_sdk_reconnect_attempts_total",
            "role" => self.role,
            "outcome" => outcome
        )
        .increment(1);
    }

    pub(crate) fn recover(self) {
        let elapsed = self.started_at.elapsed();
        metrics::histogram!(
            "relaygate_sdk_reconnect_duration_seconds",
            "role" => self.role
        )
        .record(elapsed.as_secs_f64());
        tracing::info!(
            component = "sdk",
            event = "sdk.session.reconnect_recovered",
            role = self.role,
            attempts = self.attempts,
            downtime_ms = elapsed.as_millis(),
            "SDK session reconnect episode recovered"
        );
    }

    pub(crate) fn close(self) {
        let elapsed = self.started_at.elapsed();
        tracing::info!(
            component = "sdk",
            event = "sdk.session.reconnect_closed",
            role = self.role,
            attempts = self.attempts,
            downtime_ms = elapsed.as_millis(),
            "SDK session reconnect episode closed with its runtime"
        );
    }
}

pub(crate) fn close_reconnect_episode(episode: &mut Option<ReconnectEpisode>) {
    if let Some(episode) = episode.take() {
        episode.close();
    }
}

const fn role_name(role: SessionRole) -> &'static str {
    match role {
        SessionRole::Connector => "connector",
        SessionRole::Listener => "listener",
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
                let mut episode = ReconnectEpisode::start(SessionRole::Listener);
                episode.record_attempt("error");
                episode.record_attempt("success");
                episode.recover();

                let mut closed = Some(ReconnectEpisode::start(SessionRole::Connector));
                if let Some(episode) = closed.as_mut() {
                    episode.record_attempt("error");
                }
                close_reconnect_episode(&mut closed);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let attempt_values = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                (key.key().name() == "relaygate_sdk_reconnect_attempts_total").then_some(value)
            })
            .collect::<Vec<_>>();
        assert_eq!(attempt_values.len(), 3);
        assert!(
            attempt_values
                .iter()
                .all(|value| matches!(value, DebugValue::Counter(1)))
        );

        let durations = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                (key.key().name() == "relaygate_sdk_reconnect_duration_seconds").then_some(value)
            })
            .collect::<Vec<_>>();
        assert_eq!(durations.len(), 1);
        assert!(matches!(durations[0], DebugValue::Histogram(values) if values.len() == 1));

        let logs = match logs.lock() {
            Ok(logs) => logs.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let logs = String::from_utf8(logs)?;
        assert_eq!(logs.matches("sdk.session.reconnect_started").count(), 2);
        assert_eq!(logs.matches("sdk.session.reconnect_recovered").count(), 1);
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
