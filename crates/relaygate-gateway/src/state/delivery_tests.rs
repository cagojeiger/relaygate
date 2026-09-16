//! Unit tests for the writer-queue handoff rules in `state::delivery`. They
//! need only a bounded channel and a cancellation token; the session-level
//! consequences (state removal, sibling preservation) stay in
//! `gateway::tests` and `gateway::effects::tests`.
use std::error::Error;

use relaygate_protocol::{BindingId, ErrorCode, Frame, PeerObservation, PipeId, SessionId};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Delivery, DeliveryFailure, SdkWriterItem};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Lane {
    sender: mpsc::Sender<SdkWriterItem>,
    receiver: mpsc::Receiver<SdkWriterItem>,
    cancellation: CancellationToken,
}

fn lane(capacity: usize) -> Lane {
    let (sender, receiver) = mpsc::channel(capacity);
    Lane {
        sender,
        receiver,
        cancellation: CancellationToken::new(),
    }
}

impl Lane {
    fn delivery(&self, target: SessionId, frame: Frame) -> Delivery {
        Delivery {
            target,
            frame,
            sender: self.sender.clone(),
            cancellation: self.cancellation.clone(),
        }
    }

    fn fill(&self) -> TestResult {
        self.sender
            .try_send(SdkWriterItem::Single(Frame::Ping { nonce: 0 }))?;
        Ok(())
    }
}

fn session(value: u128) -> SessionId {
    SessionId::from_uuid(Uuid::from_u128(value))
}

fn offer(pipe_id: PipeId) -> TestResult<Frame> {
    Ok(Frame::Offer {
        pipe_id,
        binding_id: BindingId::from_uuid(Uuid::from_u128(7)),
        destination: "tests/delivery".parse()?,
    })
}

fn reset(pipe_id: PipeId) -> Frame {
    Frame::Reset {
        pipe_id,
        code: ErrorCode::Unavailable,
        message: "lost".to_owned(),
    }
}

fn dial_failed(connection_id: u64) -> Frame {
    Frame::DialFailed {
        connection_id,
        code: ErrorCode::Unavailable,
        observation: PeerObservation::NotObserved,
        message: "lost".to_owned(),
    }
}

#[test]
fn a_single_frame_is_queued_and_reports_no_failure() -> TestResult {
    let mut lane = lane(1);
    let target = session(1);
    assert_eq!(
        lane.delivery(target, Frame::Pong { nonce: 5 }).deliver(),
        None
    );
    assert!(matches!(
        lane.receiver.try_recv()?,
        SdkWriterItem::Single(Frame::Pong { nonce: 5 })
    ));
    assert!(!lane.cancellation.is_cancelled());
    Ok(())
}

#[test]
fn an_offer_on_a_full_queue_fails_only_that_offer() -> TestResult {
    let mut lane = lane(1);
    lane.fill()?;
    let target = session(1);
    let pipe_id = PipeId::new(session(2), 1);
    assert_eq!(
        lane.delivery(target, offer(pipe_id)?).deliver(),
        Some(DeliveryFailure::OfferQueueFull {
            acceptor: target,
            pipe_id,
        })
    );
    assert!(!lane.cancellation.is_cancelled());
    assert!(matches!(
        lane.receiver.try_recv()?,
        SdkWriterItem::Single(Frame::Ping { nonce: 0 })
    ));
    assert!(lane.receiver.try_recv().is_err());
    Ok(())
}

#[test]
fn a_non_offer_frame_on_a_full_queue_cancels_the_session() -> TestResult {
    let mut lane = lane(1);
    lane.fill()?;
    let target = session(1);
    assert_eq!(
        lane.delivery(target, Frame::Pong { nonce: 5 }).deliver(),
        Some(DeliveryFailure::SessionUnavailable(target))
    );
    assert!(lane.cancellation.is_cancelled());
    assert!(matches!(
        lane.receiver.try_recv()?,
        SdkWriterItem::Single(Frame::Ping { nonce: 0 })
    ));
    assert!(lane.receiver.try_recv().is_err());
    Ok(())
}

#[test]
fn an_offer_on_a_closed_queue_is_a_session_failure_not_an_offer_failure() -> TestResult {
    let lane = lane(1);
    let target = session(1);
    let delivery = lane.delivery(target, offer(PipeId::new(session(2), 1))?);
    drop(lane.receiver);
    assert_eq!(
        delivery.deliver(),
        Some(DeliveryFailure::SessionUnavailable(target))
    );
    assert!(lane.cancellation.is_cancelled());
    Ok(())
}

#[test]
fn only_reset_and_dial_failed_for_one_target_join_a_terminal_batch() -> TestResult {
    let mut lane = lane(4);
    let target = session(1);
    let other = session(2);
    let pipe_id = PipeId::new(other, 1);

    assert!(
        lane.delivery(target, Frame::Pong { nonce: 1 })
            .into_terminal_batch()
            .is_err()
    );
    let mut batch = lane
        .delivery(target, reset(pipe_id))
        .into_terminal_batch()
        .map_err(|_| "RESET did not start a terminal batch")?;
    batch
        .push(lane.delivery(target, dial_failed(3)))
        .map_err(|_| "DIAL_FAILED for the same target was refused")?;
    assert!(
        batch
            .push(lane.delivery(target, Frame::Close { pipe_id }))
            .is_err()
    );
    assert!(batch.push(lane.delivery(other, reset(pipe_id))).is_err());
    assert_eq!(batch.frames().len(), 2);

    assert_eq!(batch.deliver(), None);
    let SdkWriterItem::TerminalBatch(frames) = lane.receiver.try_recv()? else {
        return Err("writer did not receive one terminal batch".into());
    };
    assert_eq!(frames.len(), 2);
    assert!(matches!(frames[0], Frame::Reset { .. }));
    assert!(matches!(frames[1], Frame::DialFailed { .. }));
    assert!(!lane.cancellation.is_cancelled());
    Ok(())
}

#[test]
fn a_full_queue_rejects_the_whole_terminal_batch_and_cancels() -> TestResult {
    let mut lane = lane(1);
    lane.fill()?;
    let target = session(1);
    let pipe_id = PipeId::new(session(2), 1);
    let mut batch = lane
        .delivery(target, reset(pipe_id))
        .into_terminal_batch()
        .map_err(|_| "RESET did not start a terminal batch")?;
    batch
        .push(lane.delivery(target, dial_failed(3)))
        .map_err(|_| "DIAL_FAILED for the same target was refused")?;

    assert_eq!(
        batch.deliver(),
        Some(DeliveryFailure::SessionUnavailable(target))
    );
    assert!(lane.cancellation.is_cancelled());
    assert!(matches!(
        lane.receiver.try_recv()?,
        SdkWriterItem::Single(Frame::Ping { nonce: 0 })
    ));
    assert!(lane.receiver.try_recv().is_err());
    Ok(())
}
