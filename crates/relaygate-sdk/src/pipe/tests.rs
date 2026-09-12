use std::{future::Future, task::Poll};

use bytes::Bytes;
use relaygate_protocol::{Frame, PipeId, SessionId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::PipeState;
use crate::{
    Error, ErrorCode, PeerObservation, config::ResourceLimits, resource::RelayResources,
    session::session_outbound_channel,
};

#[tokio::test]
async fn buffered_byte_capacity_is_returned_after_read_and_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 1);
    let (mut pipe, state) =
        PipeState::pair_with_resource_limits(pipe_id, outbound, 2, abandoned, 4, 6);

    state.push_data(Bytes::from_static(b"data"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    let error = state
        .push_data(Bytes::from_static(b"x"))
        .err()
        .ok_or("Pipe byte budget accepted excess DATA")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert_eq!(state.buffered_bytes(), (4, 4));

    let mut received = [0_u8; 4];
    pipe.read_exact(&mut received).await?;
    assert_eq!(&received, b"data");
    assert_eq!(state.buffered_bytes(), (0, 0));

    state.push_data(Bytes::from_static(b"more"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    drop(pipe);
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn partial_read_keeps_the_current_frame_charged_to_the_pipe_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 27);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.push_data(Bytes::from_static(b"ab"))?;
    let mut first = [0_u8; 1];
    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"a");

    let error = state
        .push_data(Bytes::from_static(b"c"))
        .err()
        .ok_or("Pipe frame budget ignored the partially read current frame")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);

    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"b");
    state.push_data(Bytes::from_static(b"c"))?;
    pipe.read_exact(&mut first).await?;
    assert_eq!(&first, b"c");
    Ok(())
}

#[tokio::test]
async fn empty_data_frame_releases_its_frame_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 28);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.push_data(Bytes::new())?;
    let mut byte = [0_u8; 1];
    state.push_data(Bytes::from_static(b"x"))?;
    pipe.read_exact(&mut byte).await?;
    assert_eq!(&byte, b"x");
    Ok(())
}

#[tokio::test]
async fn terminal_failure_releases_unread_buffer_capacity_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 29);
    let (_pipe, state) =
        PipeState::pair_with_resource_limits(pipe_id, outbound, 2, abandoned, 4, 4);

    state.push_data(Bytes::from_static(b"data"))?;
    assert_eq!(state.buffered_bytes(), (4, 4));
    assert!(state.fail(Error::unavailable("session failed")));
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn dropped_read_half_rejects_later_data_without_buffering()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 30);
    let (pipe, state) = PipeState::pair_with_resource_limits(pipe_id, outbound, 1, abandoned, 4, 4);
    let (reader, _writer) = pipe.into_split();

    drop(reader);
    let error = state
        .push_data(Bytes::from_static(b"data"))
        .err()
        .ok_or("dropped read half accepted later DATA")?;
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert_eq!(state.buffered_bytes(), (0, 0));
    Ok(())
}

#[test]
fn terminal_pipe_releases_its_live_slot_while_the_handle_is_retained()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = ResourceLimits {
        max_live_pipes_per_listener: 1,
        max_live_pipes_per_relay: 1,
        ..ResourceLimits::default()
    };
    let resources = RelayResources::new(limits);
    let pipe_resources = resources.pipe_resources(resources.try_reserve_outgoing()?, usize::MAX);
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 31);
    let (_retained_pipe, state) =
        PipeState::pair_inner(pipe_id, outbound, 1, abandoned, None, pipe_resources);

    assert!(resources.try_reserve_outgoing().is_err());
    assert!(state.fail(Error::unavailable("session failed")));
    assert!(resources.try_reserve_outgoing().is_ok());
    Ok(())
}

#[tokio::test]
async fn dropped_pipe_uses_the_current_pipe_terminal_lane() -> Result<(), Box<dyn std::error::Error>>
{
    let (outbound, _receiver) = session_outbound_channel(1);
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
    let (outbound, _receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 3);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

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
    let (outbound, mut receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 4);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

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

    let mut buffer = [0_u8; 1];
    let error = pipe
        .read_into(&mut buffer)
        .await
        .err()
        .ok_or("remote failure was overwritten by local close")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn remote_failure_wins_while_write_waits_for_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut receiver) = session_outbound_channel(1);
    outbound
        .send(relaygate_protocol::Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 5);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("remote session failed");

    let write = pipe.write_all_bytes(b"blocked payload");
    tokio::pin!(write);
    assert!(
        timeout(Duration::from_millis(10), &mut write)
            .await
            .is_err()
    );

    assert!(state.fail(failure.clone()));
    assert_eq!(
        timeout(Duration::from_secs(1), &mut write).await?,
        Err(failure)
    );
    assert_eq!(receiver.recv().await, Some(Frame::Ping { nonce: 1 }));
    assert!(receiver.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn full_failure_is_not_masked_by_remote_fin() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _receiver) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 6);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    state.remote_fin();
    assert!(state.fail(Error::unavailable("session failed after remote FIN")));

    let mut buffer = [0_u8; 1];
    let error = pipe
        .read_into(&mut buffer)
        .await
        .err()
        .ok_or("remote FIN masked the later full failure")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn pipe_implements_async_read_and_write_without_changing_frame_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 7);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    state.push_data(Bytes::from_static(b"inbound"))?;

    let mut inbound = [0_u8; 7];
    pipe.read_exact(&mut inbound).await?;
    assert_eq!(&inbound, b"inbound");

    pipe.write_all(b"outbound").await?;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"outbound"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn owned_halves_can_read_and_write_concurrently() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 8);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    let read_task = tokio::spawn(async move {
        let mut payload = [0_u8; 4];
        reader.read_exact(&mut payload).await?;
        Ok::<_, std::io::Error>(payload)
    });
    let write_task = tokio::spawn(async move { writer.write_all(b"pong").await });

    tokio::task::yield_now().await;
    state.push_data(Bytes::from_static(b"ping"))?;

    assert_eq!(read_task.await??, *b"ping");
    write_task.await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"pong"),
        })
    );
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn dropping_split_halves_signals_abandonment_only_after_the_last_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 9);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (reader, writer) = pipe.into_split();

    drop(writer);
    assert!(outbound_rx.try_recv().is_err(), "Drop must not send FIN");
    assert!(abandoned_rx.try_recv().is_err());

    drop(reader);
    assert_eq!(abandoned_rx.recv().await, Some(pipe_id));
    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn async_shutdown_sends_one_fin_and_keeps_the_read_half_alive()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 10);
    let (pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    writer.shutdown().await?;
    writer.shutdown().await?;
    assert_eq!(outbound_rx.recv().await, Some(Frame::Fin { pipe_id }));
    assert!(outbound_rx.try_recv().is_err());

    drop(writer);
    assert!(abandoned_rx.try_recv().is_err());
    state.push_data(Bytes::from_static(b"reply"))?;
    state.remote_fin();

    let mut payload = [0_u8; 5];
    reader.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"reply");
    assert_eq!(reader.read(&mut payload).await?, 0);
    drop(reader);

    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn fin_racing_after_empty_poll_does_not_discard_accepted_data()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 25);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let state_for_race = state.clone();
    pipe.reader
        .buffer
        .lock()
        .map_err(|_| "Pipe read buffer lock poisoned")?
        .after_inbound_pending = Some(Box::new(move || {
        assert!(
            state_for_race
                .push_data(Bytes::from_static(b"accepted-before-fin"))
                .is_ok(),
            "DATA before FIN must fit the inbound queue"
        );
        state_for_race.remote_fin();
    }));

    let mut payload = [0_u8; 19];
    pipe.read_exact(&mut payload).await?;
    assert_eq!(&payload, b"accepted-before-fin");
    assert_eq!(pipe.read(&mut payload).await?, 0);
    Ok(())
}

#[tokio::test]
async fn remote_fin_final_drain_does_not_treat_cooperative_pending_as_eof()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 26);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let state_for_race = state.clone();
    pipe.reader
        .buffer
        .lock()
        .map_err(|_| "Pipe read buffer lock poisoned")?
        .after_inbound_pending = Some(Box::new(move || {
        assert!(
            state_for_race
                .push_data(Bytes::from_static(b"accepted-before-fin"))
                .is_ok(),
            "DATA before FIN must fit the inbound queue"
        );
        state_for_race.remote_fin();
    }));

    let mut payload = [0_u8; 19];
    let count = std::future::poll_fn(|context| {
        for _ in 0..4096 {
            let mut consume = Box::pin(tokio::task::coop::consume_budget());
            if consume.as_mut().poll(context).is_pending() {
                return match pipe.reader.poll_read(&state, context, &mut payload) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => Poll::Ready(Err(Error::new(
                        ErrorCode::Internal,
                        PeerObservation::NotObserved,
                        "remote FIN final drain depends on cooperative budget",
                    ))),
                };
            }
        }
        Poll::Ready(Err(Error::new(
            ErrorCode::Internal,
            PeerObservation::NotObserved,
            "Tokio cooperative budget was not exhausted",
        )))
    })
    .await?;

    assert_eq!(count, payload.len());
    assert_eq!(&payload, b"accepted-before-fin");
    assert_eq!(pipe.read(&mut payload).await?, 0);
    Ok(())
}

#[tokio::test]
async fn write_half_close_terminates_the_split_read_half() -> Result<(), Box<dyn std::error::Error>>
{
    let (outbound, mut outbound_rx) = session_outbound_channel(2);
    let (abandoned, mut abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 13);
    let (pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let (mut reader, mut writer) = pipe.into_split();

    writer.close().await?;
    assert_eq!(outbound_rx.recv().await, Some(Frame::Close { pipe_id }));
    let write_error = writer
        .write_all_bytes(b"after-close")
        .await
        .err()
        .ok_or("closed Pipe write unexpectedly succeeded")?;
    assert_eq!(write_error.code(), ErrorCode::FailedPrecondition);

    let mut byte = [0_u8; 1];
    assert_eq!(reader.read(&mut byte).await?, 0);
    drop(writer);
    drop(reader);
    assert!(abandoned_rx.try_recv().is_err());
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn async_write_waits_for_the_existing_bounded_session_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 11);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let write = pipe.write_all(b"bounded");
        tokio::pin!(write);
        assert!(
            timeout(Duration::from_millis(10), &mut write)
                .await
                .is_err()
        );
        assert_eq!(outbound_rx.recv().await, Some(Frame::Ping { nonce: 1 }));
        write.await?;
    }

    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id,
            payload: Bytes::from_static(b"bounded"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_write_does_not_poison_shared_session_queue()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let session_id = SessionId::new();
    let pipe_a_id = PipeId::new(session_id, 21);
    let pipe_b_id = PipeId::new(session_id, 22);
    let (pipe_a, _state_a) = PipeState::pair(pipe_a_id, outbound.clone(), 1, abandoned.clone());
    let (mut pipe_b, _state_b) = PipeState::pair(pipe_b_id, outbound, 1, abandoned);
    let (_reader_a, mut writer_a) = pipe_a.into_split();

    {
        let pending = writer_a.write_all(b"cancelled-a");
        tokio::pin!(pending);
        assert!(
            timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
    }

    assert_eq!(outbound_rx.recv().await, Some(Frame::Ping { nonce: 1 }));
    timeout(Duration::from_secs(1), pipe_b.write_all(b"sibling-b")).await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id: pipe_b_id,
            payload: Bytes::from_static(b"sibling-b"),
        })
    );
    assert!(outbound_rx.try_recv().is_err());

    timeout(Duration::from_secs(1), writer_a.write_all(b"retry-a")).await??;
    assert_eq!(
        outbound_rx.recv().await,
        Some(Frame::Data {
            pipe_id: pipe_a_id,
            payload: Bytes::from_static(b"retry-a"),
        })
    );
    Ok(())
}

#[tokio::test]
async fn closed_session_outbound_wakes_a_pending_write() -> Result<(), Box<dyn std::error::Error>> {
    let (outbound, outbound_rx) = session_outbound_channel(1);
    outbound
        .send(Frame::Ping { nonce: 1 })
        .await
        .map_err(|_| std::io::Error::other("test queue unexpectedly closed before backpressure"))?;
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 23);
    let (mut pipe, _state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    let pending = pipe.write_all_bytes(b"pending");
    tokio::pin!(pending);
    assert!(
        timeout(Duration::from_millis(10), &mut pending)
            .await
            .is_err()
    );

    drop(outbound_rx);
    let error = timeout(Duration::from_secs(1), &mut pending)
        .await?
        .err()
        .ok_or("pending write unexpectedly succeeded after outbound receiver closed")?;
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.observation(), crate::PeerObservation::MaybeObserved);
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_read_can_be_retried_without_losing_data()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 24);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);

    {
        let mut cancelled_byte = [0_u8; 1];
        let pending = pipe.read(&mut cancelled_byte);
        tokio::pin!(pending);
        assert!(
            timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
    }

    state.push_data(Bytes::from_static(b"retry"))?;
    let mut payload = [0_u8; 5];
    timeout(Duration::from_secs(1), pipe.read_exact(&mut payload)).await??;
    assert_eq!(&payload, b"retry");
    Ok(())
}

#[tokio::test]
async fn async_io_errors_preserve_the_structured_sdk_error_as_their_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, _outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 12);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("session failed");
    assert!(state.fail(failure.clone()));

    assert_eq!(pipe.write_all_bytes(b"custom").await, Err(failure.clone()));

    let write_error = pipe
        .write_all(b"trait")
        .await
        .err()
        .ok_or("AsyncWrite unexpectedly succeeded")?;
    let write_source = write_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Error>())
        .ok_or("AsyncWrite error lost its SDK Error payload")?;
    assert_eq!(write_source, &failure);

    let mut byte = [0_u8; 1];
    let read_error = pipe
        .read(&mut byte)
        .await
        .err()
        .ok_or("AsyncRead unexpectedly succeeded")?;
    let read_source = read_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Error>())
        .ok_or("AsyncRead error lost its SDK Error payload")?;
    assert_eq!(read_source, &failure);
    Ok(())
}

#[tokio::test]
async fn async_shutdown_reports_terminal_failure_and_preserves_its_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let (outbound, mut outbound_rx) = session_outbound_channel(1);
    let (abandoned, _abandoned_rx) = mpsc::unbounded_channel();
    let pipe_id = PipeId::new(SessionId::new(), 14);
    let (mut pipe, state) = PipeState::pair(pipe_id, outbound, 1, abandoned);
    let failure = Error::unavailable("session failed before shutdown");
    assert!(state.fail(failure.clone()));

    let error = pipe
        .shutdown()
        .await
        .err()
        .ok_or("AsyncWrite shutdown masked terminal failure")?;
    let payload = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Error>())
        .ok_or("AsyncWrite shutdown lost its SDK Error payload")?;
    assert_eq!(payload, &failure);
    assert_eq!(pipe.shutdown_write().await, Err(failure));
    assert!(outbound_rx.try_recv().is_err());
    Ok(())
}
