use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use relaygate_protocol::Frame;
use tokio::sync::{
    Notify,
    futures::OwnedNotified,
    mpsc::{self, error::TrySendError},
};

pub(crate) struct SessionOutbound {
    sender: mpsc::Sender<Frame>,
    capacity: Arc<Notify>,
    waiter: Option<Pin<Box<OwnedNotified>>>,
}

pub(crate) struct SessionOutboundReceiver {
    receiver: mpsc::Receiver<Frame>,
    capacity: Arc<Notify>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameCommit {
    Sent,
    Skipped,
}

pub(crate) fn session_outbound_channel(
    capacity: usize,
) -> (SessionOutbound, SessionOutboundReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    let capacity = Arc::new(Notify::new());
    (
        SessionOutbound {
            sender,
            capacity: Arc::clone(&capacity),
            waiter: None,
        },
        SessionOutboundReceiver { receiver, capacity },
    )
}

impl Clone for SessionOutbound {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: Arc::clone(&self.capacity),
            waiter: None,
        }
    }
}

impl SessionOutbound {
    pub(crate) fn cancel_wait(&mut self) {
        self.waiter = None;
    }

    pub(crate) fn poll_send(
        &mut self,
        context: &mut Context<'_>,
        make_frame: impl FnOnce() -> Option<Frame>,
    ) -> Poll<Result<FrameCommit, ()>> {
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.waiter = None;
                let commit = commit_frame(permit, make_frame);
                if commit == FrameCommit::Skipped {
                    self.capacity.notify_waiters();
                }
                return Poll::Ready(Ok(commit));
            }
            Err(TrySendError::Closed(())) => {
                self.waiter = None;
                return Poll::Ready(Err(()));
            }
            Err(TrySendError::Full(())) => {}
        }

        if self.waiter.is_none() {
            self.waiter = Some(Box::pin(Arc::clone(&self.capacity).notified_owned()));
        }
        let notified = self
            .waiter
            .as_mut()
            .is_some_and(|waiter| waiter.as_mut().poll(context).is_ready());

        match self.sender.try_reserve() {
            Ok(permit) => {
                self.waiter = None;
                let commit = commit_frame(permit, make_frame);
                if commit == FrameCommit::Skipped {
                    self.capacity.notify_waiters();
                }
                Poll::Ready(Ok(commit))
            }
            Err(TrySendError::Closed(())) => {
                self.waiter = None;
                Poll::Ready(Err(()))
            }
            Err(TrySendError::Full(())) => {
                if notified {
                    self.waiter = None;
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<Frame>> {
        self.sender.send(frame).await
    }
}

fn commit_frame(
    permit: mpsc::Permit<'_, Frame>,
    make_frame: impl FnOnce() -> Option<Frame>,
) -> FrameCommit {
    let Some(frame) = make_frame() else {
        return FrameCommit::Skipped;
    };
    permit.send(frame);
    FrameCommit::Sent
}

impl SessionOutboundReceiver {
    pub(crate) async fn recv(&mut self) -> Option<Frame> {
        let frame = self.receiver.recv().await;
        if frame.is_some() {
            self.capacity.notify_waiters();
        }
        frame
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<Frame, mpsc::error::TryRecvError> {
        let frame = self.receiver.try_recv();
        if frame.is_ok() {
            self.capacity.notify_waiters();
        }
        frame
    }
}

impl Drop for SessionOutboundReceiver {
    fn drop(&mut self) {
        self.capacity.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use futures_util::future::poll_fn;
    use relaygate_protocol::Frame;
    use tokio::time::{Duration, timeout};

    use super::{FrameCommit, session_outbound_channel};

    #[tokio::test]
    async fn skipped_commit_releases_capacity_and_wakes_another_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut first, mut receiver) = session_outbound_channel(1);
        let mut second = first.clone();
        first.send(Frame::Ping { nonce: 1 }).await?;

        let pending =
            poll_fn(|context| second.poll_send(context, || Some(Frame::Pong { nonce: 2 })));
        tokio::pin!(pending);
        assert!(
            timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );

        assert_eq!(
            receiver.receiver.recv().await,
            Some(Frame::Ping { nonce: 1 })
        );
        assert_eq!(
            poll_fn(|context| first.poll_send(context, || None)).await,
            Ok(FrameCommit::Skipped)
        );
        assert_eq!(
            timeout(Duration::from_secs(1), &mut pending).await?,
            Ok(FrameCommit::Sent)
        );
        assert_eq!(receiver.recv().await, Some(Frame::Pong { nonce: 2 }));
        Ok(())
    }
}
