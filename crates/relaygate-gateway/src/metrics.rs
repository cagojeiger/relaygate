use std::time::Duration;

#[derive(Clone, Copy)]
pub(crate) enum HeartbeatTransport {
    Sdk,
    Peer,
}

impl HeartbeatTransport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sdk => "sdk",
            Self::Peer => "peer",
        }
    }
}

pub(crate) fn observe_heartbeat_round_trip(transport: HeartbeatTransport, round_trip: Duration) {
    metrics::histogram!(
        "relaygate_gateway_heartbeat_duration_seconds",
        "transport" => transport.as_str()
    )
    .record(round_trip.as_secs_f64());
}

pub(crate) fn observe_heartbeat_timeout(transport: HeartbeatTransport) {
    metrics::counter!(
        "relaygate_gateway_heartbeat_timeouts_total",
        "transport" => transport.as_str()
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use metrics_util::{debugging::DebugValue, debugging::DebuggingRecorder};

    use super::*;

    #[test]
    fn heartbeat_metrics_use_only_the_bounded_transport_label() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            observe_heartbeat_round_trip(HeartbeatTransport::Sdk, Duration::from_millis(7));
            observe_heartbeat_round_trip(HeartbeatTransport::Peer, Duration::from_millis(3));
            observe_heartbeat_timeout(HeartbeatTransport::Sdk);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_heartbeat_duration_seconds"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "transport" && label.value() == "sdk")
                && matches!(value, DebugValue::Histogram(values) if values.len() == 1)
        }));
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_heartbeat_duration_seconds"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "transport" && label.value() == "peer")
                && matches!(value, DebugValue::Histogram(values) if values.len() == 1)
        }));
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_gateway_heartbeat_timeouts_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "transport" && label.value() == "sdk")
                && matches!(value, DebugValue::Counter(1))
        }));
    }
}
