use futures_util::FutureExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;
use crate::{Error, ErrorCode, PeerObservation};

#[test]
fn reconnect_gauge_tracks_overlapping_episodes_and_all_exit_paths() {
    let _guard = RECONNECT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut snapshots = Vec::new();
    metrics::with_local_recorder(&recorder, || {
        let first = ReconnectEpisode::start();
        let second = ReconnectEpisode::start();
        snapshots.push(snapshotter.snapshot().into_vec());
        first.recover();
        snapshots.push(snapshotter.snapshot().into_vec());
        second.close();
        snapshots.push(snapshotter.snapshot().into_vec());
        let abandoned = ReconnectEpisode::start();
        snapshots.push(snapshotter.snapshot().into_vec());
        drop(abandoned);
        snapshots.push(snapshotter.snapshot().into_vec());
        let degraded = ReconnectEpisode::start();
        snapshots.push(snapshotter.snapshot().into_vec());
        degraded.degrade();
        snapshots.push(snapshotter.snapshot().into_vec());
    });
    // DebuggingRecorder drains even gauges on snapshot; reconstruct the sampled increments.
    let mut active = 0.0;
    for (snapshot, expected) in snapshots.iter().zip([2.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]) {
        let delta = snapshot.iter().find_map(|(key, _, _, value)| {
            if key.key().name() == "relaygate_sdk_reconnect_in_progress"
                && let DebugValue::Gauge(value) = value
            {
                Some(value.into_inner())
            } else {
                None
            }
        });
        assert!(delta.is_some());
        active += delta.unwrap_or_default();
        assert_eq!(active, expected);
    }
    for outcome in ["recovered", "closed", "aborted", "degraded"] {
        assert!(snapshots.iter().flatten().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_reconnect_episodes_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == outcome)
                && matches!(value, DebugValue::Counter(1))
        }));
    }
}

#[test]
fn sdk_operation_metrics_include_errors_and_dropped_polled_futures() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        assert!(matches!(
            observe("dial", async { Ok(()) }).now_or_never(),
            Some(Ok(()))
        ));
        let error = Error::new(
            ErrorCode::NotFound,
            PeerObservation::NotObserved,
            "secret-marker",
        );
        assert!(matches!(
            observe::<()>("dial", async { Err(error) }).now_or_never(),
            Some(Err(_))
        ));
        let mut pending = Box::pin(observe::<()>("dial", std::future::pending()));
        assert!(pending.as_mut().now_or_never().is_none());
        drop(pending);
        // A future never polled did not start an operation.
        drop(observe::<()>("dial", std::future::pending()));
    });
    let snapshot = snapshotter.snapshot().into_vec();
    for outcome in ["success", "error", "cancelled"] {
        assert!(snapshot.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_operation_results_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == outcome)
                && matches!(value, DebugValue::Counter(1))
        }));
    }
    for (key, _, _, _) in snapshot {
        assert!(
            key.key()
                .labels()
                .all(|l| matches!(l.key(), "operation" | "outcome" | "code"))
        );
        assert!(
            key.key()
                .labels()
                .all(|l| !l.value().contains("secret-marker"))
        );
    }
}
