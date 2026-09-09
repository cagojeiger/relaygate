use super::*;
use std::time::Duration;

fn rejection() -> Frame {
    Frame::DialFailed {
        connection_id: 1,
        code: ErrorCode::ResourceExhausted,
        observation: PeerObservation::NotObserved,
        message: "capacity".into(),
    }
}

#[test]
fn only_single_local_unobserved_capacity_rejections_wait() {
    use crate::state::{GatewayLimits, GatewayState};
    let mut state = GatewayState::new(GatewayLimits {
        session_control_rate_per_second: 1,
        session_control_burst: 1,
        ..GatewayLimits::default()
    });
    let (sender, _receiver) = mpsc::channel(1);
    let session = state.add_session(sender, CancellationToken::new()).unwrap();
    let now = std::time::Instant::now();
    let dial = |connection_id| Frame::Dial {
        connection_id,
        destination_id: relaygate_protocol::DestinationId::new(),
    };
    let first = state.handle_at(session, dial(1), now).unwrap();
    assert!(!is_local_rejection(&first, session));
    let mut actions = state.handle_at(session, dial(2), now).unwrap();
    assert!(is_local_rejection(&actions, session));
    assert!(!is_local_rejection(&actions, SessionId::new()));
    assert!(!is_local_rejection(&[], session));
    actions.push(actions[0].clone());
    assert!(!is_local_rejection(&actions, session));
    actions.pop();
    let GatewayAction::SendSdkFrame(delivery) = &mut actions[0] else {
        unreachable!()
    };
    let Frame::DialFailed { observation, .. } = &mut delivery.frame else {
        unreachable!()
    };
    *observation = PeerObservation::MaybeObserved;
    assert!(!is_local_rejection(&actions, session));
    let published = state
        .handle_at(
            session,
            Frame::Publish {
                request_id: 1,
                destination_id: relaygate_protocol::DestinationId::new(),
            },
            now,
        )
        .unwrap();
    assert!(is_local_rejection(&published, session));
}

#[tokio::test(start_paused = true)]
async fn full_queue_waits_for_drain_without_cancelling_session() {
    let (sender, mut receiver) = mpsc::channel(1);
    sender.send(rejection()).await.unwrap();
    let cancellation = CancellationToken::new();
    let pending = send_rejection(
        &sender,
        rejection(),
        &cancellation,
        Instant::now() + Duration::from_secs(5),
    );
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    receiver.recv().await.unwrap();
    pending.await.unwrap();
    assert!(receiver.recv().await.is_some());
    assert!(!cancellation.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn stalled_queue_is_bounded_by_existing_liveness_deadline() {
    let (sender, _receiver) = mpsc::channel(1);
    sender.send(rejection()).await.unwrap();
    let cancellation = CancellationToken::new();
    let start = Instant::now();
    assert!(
        send_rejection(
            &sender,
            rejection(),
            &cancellation,
            start + Duration::from_secs(5)
        )
        .await
        .is_err()
    );
    assert_eq!(Instant::now() - start, Duration::from_secs(5));
    assert_eq!(sender.capacity(), 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_closed_queue_stop_wait_without_detached_send() {
    let (sender, receiver) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(
        send_rejection(
            &sender,
            rejection(),
            &cancellation,
            Instant::now() + Duration::from_secs(5)
        )
        .await
        .is_err()
    );
    assert_eq!(sender.capacity(), 1);
    drop(receiver);
    assert!(
        send_rejection(
            &sender,
            rejection(),
            &CancellationToken::new(),
            Instant::now() + Duration::from_secs(5)
        )
        .await
        .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn blocked_session_does_not_block_sibling_response() {
    let (blocked, _receiver) = mpsc::channel(1);
    blocked.send(rejection()).await.unwrap();
    let (sibling, mut receiver) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let pending = send_rejection(&blocked, rejection(), &cancellation, deadline);
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    send_rejection(&sibling, rejection(), &cancellation, deadline)
        .await
        .unwrap();
    assert!(receiver.recv().await.is_some());
    assert!(futures_util::poll!(&mut pending).is_pending());
}
