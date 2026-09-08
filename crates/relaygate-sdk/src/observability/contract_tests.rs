use futures_util::FutureExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use super::*;
use crate::{Error, ErrorCode, PeerObservation};

fn active(snapshotter: &Snapshotter) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            if key.key().name() == "relaygate_sdk_reconnect_in_progress" {
                if let DebugValue::Gauge(value) = value {
                    return Some(value.into_inner());
                }
            }
            None
        })
        .unwrap_or(-1.0)
}

#[test]
fn reconnect_gauge_tracks_overlapping_episodes_and_all_exit_paths() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let first = ReconnectEpisode::start();
        let second = ReconnectEpisode::start();
        assert_eq!(active(&snapshotter), 2.0);
        first.recover();
        assert_eq!(active(&snapshotter), 1.0);
        second.close();
        assert_eq!(active(&snapshotter), 0.0);
        let abandoned = ReconnectEpisode::start();
        assert_eq!(active(&snapshotter), 1.0);
        drop(abandoned);
        assert_eq!(active(&snapshotter), 0.0);
    });
    for outcome in ["recovered", "closed", "aborted"] {
        assert!(
            snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .any(|(key, _, _, value)| {
                    key.key().name() == "relaygate_sdk_reconnect_episodes_total"
                        && key
                            .key()
                            .labels()
                            .any(|l| l.key() == "outcome" && l.value() == outcome)
                        && matches!(value, DebugValue::Counter(1))
                })
        );
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
