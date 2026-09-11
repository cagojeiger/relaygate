use super::*;
use relaygate_protocol::{ErrorCode, PeerObservation};

#[tokio::test]
async fn rejection_burst_with_slow_reader_preserves_session_and_sibling() -> TestResult {
    let gateway = Gateway::new(
        GatewayConfig::new(authorization_config())
            .with_writer_queue_capacity(1)
            .with_session_control_rate_limit(1, 1),
    )?;
    let (client, cancel, task) = start_session(&gateway, 1024)?;
    let mut client = Framed::new(client, FrameCodec::default());
    client.send(Frame::Hello).await?;
    assert!(matches!(
        client.next().await,
        Some(Ok(Frame::Welcome { .. }))
    ));
    let (mut writer, mut reader) = client.split();
    let address = unique_address();
    let access_token = bearer_token(&address, TestAction::Dial);
    let producer = tokio::spawn(async move {
        for connection_id in 1..=512 {
            writer
                .send(Frame::Dial {
                    connection_id,
                    address: address.clone(),
                    access_token: access_token.clone(),
                })
                .await?;
        }
        Ok::<_, relaygate_protocol::ProtocolError>(writer)
    });
    // The small duplex output buffer and one-slot queue apply real write backpressure.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (sibling, sibling_cancel, sibling_task) = start_session(&gateway, 1024)?;
    let mut sibling = Framed::new(sibling, FrameCodec::default());
    sibling.send(Frame::Hello).await?;
    assert!(matches!(
        timeout(Duration::from_secs(1), sibling.next()).await?,
        Some(Ok(Frame::Welcome { .. }))
    ));
    sibling.send(Frame::Ping { nonce: 42 }).await?;
    assert!(matches!(
        timeout(Duration::from_secs(1), sibling.next()).await?,
        Some(Ok(Frame::Pong { nonce: 42 }))
    ));
    let mut rejected = 0;
    timeout(Duration::from_secs(5), async {
        for expected in 1..=512 {
            match reader.next().await {
                Some(Ok(Frame::DialFailed {
                    connection_id,
                    code,
                    observation,
                    ..
                })) => {
                    assert_eq!(connection_id, expected);
                    assert_eq!(observation, PeerObservation::NotObserved);
                    assert!(matches!(
                        code,
                        ErrorCode::NotFound | ErrorCode::ResourceExhausted
                    ));
                    rejected += usize::from(code == ErrorCode::ResourceExhausted);
                }
                other => return Err(format!("lost rejection response: {other:?}").into()),
            }
        }
        TestResult::Ok(())
    })
    .await??;
    assert!(rejected > 0);
    let mut writer = producer.await??;
    writer.send(Frame::Ping { nonce: 99 }).await?;
    assert!(matches!(
        timeout(Duration::from_secs(1), reader.next()).await?,
        Some(Ok(Frame::Pong { nonce: 99 }))
    ));
    assert_eq!(gateway.snapshot().sessions, 2);
    cancel.cancel();
    sibling_cancel.cancel();
    task.await??;
    sibling_task.await??;
    assert_empty(&gateway);
    Ok(())
}
