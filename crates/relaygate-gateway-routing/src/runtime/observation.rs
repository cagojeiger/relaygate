use std::time::Instant;

use relaygate_route_table::ShardId;
use relaygate_route_table_transport::TransportError;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DependencyState {
    Starting,
    Ready,
    Degraded,
    Terminal,
}

impl DependencyState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Terminal => "terminal",
        }
    }
}

pub(super) struct RouteDependencyObservation {
    state: DependencyState,
    degraded_at: Option<Instant>,
    connect_attempts: u64,
}

impl RouteDependencyObservation {
    pub(super) fn new() -> Self {
        Self {
            state: DependencyState::Starting,
            degraded_at: None,
            connect_attempts: 0,
        }
    }

    pub(super) fn connect_failed(&mut self, shard_id: &ShardId, error: &TransportError) {
        observe_connection_attempt("error", error.code().metric_name());
        self.connect_attempts = self.connect_attempts.saturating_add(1);
        if self.state == DependencyState::Degraded {
            tracing::debug!(
                component = "gateway",
                event = "gateway.route_dependency.retry_failed",
                shard_id = %shard_id,
                attempt = self.connect_attempts,
                error_code = error.code().metric_name(),
                "Gateway RouteTable reconnect attempt failed"
            );
            return;
        }
        self.degrade(shard_id, "connect_failed", Some(error));
        self.connect_attempts = 1;
    }

    pub(super) fn connection_lost(&mut self, shard_id: &ShardId, error: &TransportError) {
        self.degrade(shard_id, "connection_lost", Some(error));
    }

    pub(super) fn ready(&mut self, shard_id: &ShardId) {
        observe_connection_attempt("success", "ok");
        self.connect_attempts = self.connect_attempts.saturating_add(1);
        let previous = self.state;
        if previous == DependencyState::Ready {
            return;
        }
        self.transition(DependencyState::Ready);
        if let Some(started_at) = self.degraded_at.take() {
            let elapsed = started_at.elapsed();
            metrics::histogram!("relaygate_gateway_route_recovery_duration_seconds")
                .record(elapsed.as_secs_f64());
            tracing::info!(
                component = "gateway",
                event = "gateway.route_dependency.recovered",
                shard_id = %shard_id,
                attempts = self.connect_attempts,
                downtime_ms = elapsed.as_millis(),
                "Gateway RouteTable dependency recovered"
            );
        } else {
            tracing::info!(
                component = "gateway",
                event = "gateway.route_dependency.ready",
                shard_id = %shard_id,
                "Gateway RouteTable dependency is ready"
            );
        }
        self.connect_attempts = 0;
    }

    pub(super) fn terminal(&mut self, shard_id: &ShardId, error: &TransportError) {
        if self.state == DependencyState::Terminal {
            return;
        }
        self.transition(DependencyState::Terminal);
        tracing::error!(
            component = "gateway",
            event = "gateway.route_dependency.terminal",
            shard_id = %shard_id,
            error_code = error.code().metric_name(),
            "Gateway RouteTable dependency entered a terminal state"
        );
    }

    pub(super) fn connect_terminal(&mut self, shard_id: &ShardId, error: &TransportError) {
        observe_connection_attempt("error", error.code().metric_name());
        self.terminal(shard_id, error);
    }

    fn degrade(
        &mut self,
        shard_id: &ShardId,
        reason: &'static str,
        error: Option<&TransportError>,
    ) {
        if self.state == DependencyState::Degraded {
            return;
        }
        self.degraded_at = Some(Instant::now());
        self.connect_attempts = 0;
        self.transition(DependencyState::Degraded);
        tracing::warn!(
            component = "gateway",
            event = "gateway.route_dependency.degraded",
            shard_id = %shard_id,
            reason,
            error_code = error.map(|error| error.code().metric_name()),
            "Gateway RouteTable dependency degraded"
        );
    }

    fn transition(&mut self, current: DependencyState) {
        let previous = self.state;
        self.state = current;
        metrics::counter!(
            "relaygate_gateway_route_dependency_transitions_total",
            "previous" => previous.as_str(),
            "current" => current.as_str()
        )
        .increment(1);
    }
}

fn observe_connection_attempt(outcome: &'static str, code: &'static str) {
    metrics::counter!(
        "relaygate_gateway_route_connection_attempts_total",
        "outcome" => outcome,
        "code" => code
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use relaygate_route_table::RouteTableError;

    use super::*;

    #[test]
    fn repeated_failures_form_one_transition_and_one_recovery_duration()
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
        let shard_id = ShardId::new("rt-0")?;
        let error = TransportError::from(RouteTableError::NotFound);

        tracing::subscriber::with_default(subscriber, || {
            metrics::with_local_recorder(&recorder, || {
                let mut observation = RouteDependencyObservation::new();
                observation.connect_failed(&shard_id, &error);
                observation.connect_failed(&shard_id, &error);
                observation.ready(&shard_id);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let transitions = snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "relaygate_gateway_route_dependency_transitions_total"
            })
            .collect::<Vec<_>>();
        assert_eq!(transitions.len(), 2);
        assert!(
            transitions
                .iter()
                .all(|(_, _, _, value)| matches!(value, DebugValue::Counter(1)))
        );
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_recovery_duration_seconds"
                && matches!(value, DebugValue::Histogram(values) if values.len() == 1)
        }));
        let attempts = snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "relaygate_gateway_route_connection_attempts_total"
            })
            .collect::<Vec<_>>();
        assert_eq!(attempts.len(), 2);
        assert!(attempts.iter().any(|(key, _, _, value)| {
            key.key()
                .labels()
                .any(|label| label.key() == "outcome" && label.value() == "error")
                && matches!(value, DebugValue::Counter(2))
        }));
        assert!(attempts.iter().any(|(key, _, _, value)| {
            key.key()
                .labels()
                .any(|label| label.key() == "outcome" && label.value() == "success")
                && matches!(value, DebugValue::Counter(1))
        }));
        let logs = match logs.lock() {
            Ok(logs) => logs.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let logs = String::from_utf8(logs)?;
        assert_eq!(logs.matches("gateway.route_dependency.degraded").count(), 1);
        assert_eq!(
            logs.matches("gateway.route_dependency.retry_failed")
                .count(),
            1
        );
        assert_eq!(
            logs.matches("gateway.route_dependency.recovered").count(),
            1
        );
        assert!(logs.contains("\"attempts\":3"));
        Ok(())
    }

    #[test]
    fn terminal_connection_failure_records_attempt_and_transition()
    -> Result<(), Box<dyn std::error::Error>> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let shard_id = ShardId::new("rt-0")?;
        let error = TransportError::from(RouteTableError::FailedPrecondition(
            "generation mismatch".to_owned(),
        ));

        metrics::with_local_recorder(&recorder, || {
            RouteDependencyObservation::new().connect_terminal(&shard_id, &error);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_connection_attempts_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "error")
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "code" && label.value() == "failed_precondition")
                && matches!(value, DebugValue::Counter(1))
        }));
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_route_dependency_transitions_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "previous" && label.value() == "starting")
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "current" && label.value() == "terminal")
                && matches!(value, DebugValue::Counter(1))
        }));
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
