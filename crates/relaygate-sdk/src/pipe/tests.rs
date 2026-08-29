use relaygate_protocol::{PipeId, SessionId};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::PipeState;
use crate::{Error, ErrorCode};

#[tokio::test]
async fn dropped_pipe_uses_the_current_pipe_terminal_lane() -> Result<(), Box<dyn std::error::Error>>
{
    let (outbound, _receiver) = mpsc::channel(1);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 2);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    drop(pipe);

    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn cancelled_close_remains_drop_safe() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = mpsc::channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 3);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    assert!(
        timeout(Duration::from_millis(10), pipe.close())
            .await
            .is_err()
    );
    drop(pipe);

    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    Ok(())
}

#[tokio::test]
async fn remote_failure_wins_while_close_waits_for_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = mpsc::channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 4);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let close = pipe.close();
        tokio::pin!(close);
        assert!(
            timeout(Duration::from_millis(10), &mut close)
                .await
                .is_err()
        );

        assert!(state.fail(Error::unavailable("remote session failed")));
        assert!(receiver.recv().await.is_some());
        close.await?;
    }
    assert!(receiver.try_recv().is_err());

    let mut pipe = pipe;
    let mut buffer = [0_u8; 1];
    let error = pipe
        .read(&mut buffer)
        .await
        .err()
        .ok_or("remote failure was overwritten by local close")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn remote_failure_wins_while_write_waits_for_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = mpsc::channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 5);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("remote session failed");

    let write = pipe.write_all(b"blocked payload");
    tokio::pin!(write);
    assert!(
        timeout(Duration::from_millis(10), &mut write)
            .await
            .is_err()
    );

    assert!(state.fail(failure.clone()));
    assert!(receiver.recv().await.is_some());
    assert_eq!(write.await, Err(failure));
    assert!(receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn full_failure_is_not_masked_by_remote_fin() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = mpsc::channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 6);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.remote_fin();
    assert!(state.fail(Error::unavailable("session failed after remote FIN")));

    let mut buffer = [0_u8; 1];
    let error = pipe
        .read(&mut buffer)
        .await
        .err()
        .ok_or("remote FIN masked the later full failure")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}
