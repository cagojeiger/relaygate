use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;

#[test]
fn byte_reservations_return_capacity_on_every_drop_path() -> Result<(), Box<dyn std::error::Error>>
{
    let budget = Arc::new(ByteBudget::new(8, false));
    let first = budget.try_reserve(5, ResourceLimitKind::PipeBufferedBytes)?;
    assert_eq!(budget.used(), 5);
    assert!(
        budget
            .try_reserve(4, ResourceLimitKind::PipeBufferedBytes)
            .is_err()
    );
    assert_eq!(budget.used(), 5);
    drop(first);
    assert_eq!(budget.used(), 0);
    assert!(
        budget
            .try_reserve(8, ResourceLimitKind::PipeBufferedBytes)
            .is_ok()
    );
    Ok(())
}

#[test]
fn live_pipe_slots_are_shared_by_incoming_and_outgoing_pipes()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = ResourceLimits {
        max_live_pipes_per_listener: 1,
        max_live_pipes_per_relay: 2,
        ..ResourceLimits::default()
    };
    let resources = RelayResources::new(limits);
    let listener = resources.listener_slots(1);
    let incoming = resources.try_reserve_incoming(&listener)?;
    assert!(resources.try_reserve_incoming(&listener).is_err());
    let outgoing = resources.try_reserve_outgoing()?;
    assert!(resources.try_reserve_outgoing().is_err());

    drop(incoming);
    assert!(resources.try_reserve_incoming(&listener).is_ok());
    drop(outgoing);
    Ok(())
}

#[test]
fn relay_byte_budget_is_shared_and_released_across_pipes() -> Result<(), Box<dyn std::error::Error>>
{
    let limits = ResourceLimits {
        max_live_pipes_per_relay: 2,
        max_buffered_bytes_per_pipe: 6,
        max_buffered_bytes_per_relay: 6,
        ..ResourceLimits::default()
    };
    let resources = RelayResources::new(limits);
    let first = resources.pipe_resources(resources.try_reserve_outgoing()?, 6);
    let second = resources.pipe_resources(resources.try_reserve_outgoing()?, 6);

    let first_bytes = first.try_reserve_buffered(4)?;
    let error = second
        .try_reserve_buffered(3)
        .err()
        .ok_or("Relay byte budget accepted aggregate excess")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);

    drop(first_bytes);
    assert!(second.try_reserve_buffered(3).is_ok());
    Ok(())
}

#[test]
fn resource_metrics_report_bounded_limits_usage_rejections_and_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let mut snapshots = Vec::new();
    metrics::with_local_recorder(&recorder, || -> Result<(), Error> {
        let limits = ResourceLimits {
            max_live_pipes_per_relay: 2,
            max_buffered_bytes_per_pipe: 6,
            max_buffered_bytes_per_relay: 6,
            ..ResourceLimits::default()
        };
        let resources = RelayResources::new(limits);
        let pipe = resources.pipe_resources(resources.try_reserve_outgoing()?, 6);
        let bytes = pipe.try_reserve_buffered(4)?;
        assert!(pipe.try_reserve_buffered(3).is_err());
        snapshots.push(snapshotter.snapshot().into_vec());

        drop(bytes);
        drop(pipe);
        drop(resources);
        snapshots.push(snapshotter.snapshot().into_vec());
        Ok(())
    })?;

    let first = &snapshots[0];
    for (name, expected) in [("live_pipes", 2.0), ("buffered_bytes", 6.0)] {
        assert!(first.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_resource_limit"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "resource" && label.value() == name)
                && matches!(value, DebugValue::Gauge(value) if value.into_inner() == expected)
        }));
    }
    for (name, expected) in [("live_pipes", 1.0), ("buffered_bytes", 4.0)] {
        assert!(first.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_resource_used"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "resource" && label.value() == name)
                && matches!(value, DebugValue::Gauge(value) if value.into_inner() == expected)
        }));
    }
    assert!(first.iter().any(|(key, _, _, value)| {
        key.key().name() == "relaygate_sdk_resource_rejections_total"
            && key
                .key()
                .labels()
                .any(|label| label.key() == "resource" && label.value() == "pipe_buffered_bytes")
            && matches!(value, DebugValue::Counter(1))
    }));

    let second = &snapshots[1];
    for (name, expected) in [("live_pipes", -1.0), ("buffered_bytes", -4.0)] {
        assert!(second.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_resource_used"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "resource" && label.value() == name)
                && matches!(value, DebugValue::Gauge(value) if value.into_inner() == expected)
        }));
    }
    for (name, expected) in [("live_pipes", -2.0), ("buffered_bytes", -6.0)] {
        assert!(second.iter().any(|(key, _, _, value)| {
            key.key().name() == "relaygate_sdk_resource_limit"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "resource" && label.value() == name)
                && matches!(value, DebugValue::Gauge(value) if value.into_inner() == expected)
        }));
    }
    Ok(())
}
