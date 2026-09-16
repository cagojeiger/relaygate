use std::time::Duration;

/// SDK-session heartbeat metrics; the peer crate emits the same series with
/// `transport = "peer"` for its own transport. `transport` is the bounded
/// label set of SPEC 008 OBS-006/OBS-007.
pub(crate) fn observe_sdk_heartbeat_round_trip(round_trip: Duration) {
    metrics::histogram!(
        "relaygate_gateway_heartbeat_duration_seconds",
        "transport" => "sdk"
    )
    .record(round_trip.as_secs_f64());
}

pub(crate) fn observe_sdk_heartbeat_timeout() {
    metrics::counter!(
        "relaygate_gateway_heartbeat_timeouts_total",
        "transport" => "sdk"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use metrics_util::{debugging::DebugValue, debugging::DebuggingRecorder};

    use super::*;

    #[test]
    fn sdk_heartbeat_metrics_carry_the_sdk_transport_label() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            observe_sdk_heartbeat_round_trip(Duration::from_millis(7));
            observe_sdk_heartbeat_timeout();
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let labelled = |name: &str| {
            snapshot.iter().find(|(key, _, _, _)| {
                key.key().name() == name
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "transport" && label.value() == "sdk")
            })
        };
        assert!(matches!(
            labelled("relaygate_gateway_heartbeat_duration_seconds"),
            Some((_, _, _, DebugValue::Histogram(values))) if values.len() == 1
        ));
        assert!(matches!(
            labelled("relaygate_gateway_heartbeat_timeouts_total"),
            Some((_, _, _, DebugValue::Counter(1)))
        ));
    }
}
