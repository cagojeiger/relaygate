use super::*;

#[test]
fn dial_metric_classes_separate_request_capacity_and_availability() {
    for (code, class) in [
        (None, "success"),
        (Some(ErrorCode::NotFound), "request"),
        (Some(ErrorCode::ProtocolError), "request"),
        (Some(ErrorCode::ResourceExhausted), "capacity"),
        (Some(ErrorCode::Unavailable), "availability"),
        (Some(ErrorCode::DeadlineExceeded), "availability"),
        (Some(ErrorCode::Internal), "internal"),
        (Some(ErrorCode::Cancelled), "cancelled"),
    ] {
        assert_eq!(dial_result_class(code), class);
    }
}

#[test]
fn snapshot_reports_configured_limits_without_allocating_capacity() {
    let state = GatewayState::new(GatewayLimits {
        max_sessions: 7,
        max_bindings: 11,
        max_pending_offers: 13,
        max_remote_dial_attempts: 3,
        max_live_pipes: 17,
        ..GatewayLimits::default()
    });
    let snapshot = state.snapshot();
    assert_eq!(
        (
            snapshot.max_sessions,
            snapshot.max_bindings,
            snapshot.max_pending_offers,
            snapshot.max_remote_dial_attempts,
            snapshot.max_live_pipes
        ),
        (7, 11, 13, 3, 17)
    );
    assert_eq!(
        (
            snapshot.sessions,
            snapshot.pending_offers,
            snapshot.live_pipes,
            snapshot.originated_pipes
        ),
        (0, 0, 0, 0)
    );
}
