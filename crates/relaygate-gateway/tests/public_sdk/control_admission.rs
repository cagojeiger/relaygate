use super::*;

#[tokio::test]
async fn control_throttle_returns_request_errors_preserves_pipe_and_recovers() -> TestResult {
    let (address, shutdown, server) = start_gateway_with_config(
        GatewayConfig::new(CLUSTER_TOKEN).with_session_control_rate_limit(1, 1),
    )
    .await?;
    let result: TestResult = async {
        let config = Config::new_insecure_for_tests(address.to_string(), CLUSTER_TOKEN)
            .with_operation_timeout(Duration::from_secs(2));
        let receiver = Relay::connect(config.clone()).await?;
        let caller = Relay::connect(config).await?;
        let destination = DestinationId::new();
        let listener = receiver.listen(destination).await?;
        let (outgoing, incoming) = tokio::join!(caller.dial(destination), listener.accept());
        let mut outgoing = outgoing?;
        let mut incoming = incoming?;
        // Repeated operations tolerate a token refill but must eventually reject.
        timeout(Duration::from_secs(3), async {
            loop {
                match caller.dial(DestinationId::new()).await {
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::ResourceExhausted => {
                        assert_eq!(
                            error.observation(),
                            relaygate_sdk::PeerObservation::NotObserved
                        );
                        break;
                    }
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::NotFound => {}
                    other => return Err(format!("unexpected dial result: {other:?}").into()),
                }
            }
            TestResult::Ok(())
        })
        .await??;
        timeout(Duration::from_secs(3), async {
            loop {
                match receiver.listen(DestinationId::new()).await {
                    Err(error) if error.code() == relaygate_sdk::ErrorCode::ResourceExhausted => {
                        break;
                    }
                    Ok(extra) => extra.close(),
                    Err(error) => return Err(error.into()),
                }
            }
            TestResult::Ok(())
        })
        .await??;
        outgoing.write_all(b"ping").await?;
        let mut bytes = [0; 4];
        timeout(Duration::from_secs(1), incoming.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"ping");
        incoming.write_all(b"pong").await?;
        timeout(Duration::from_secs(1), outgoing.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"pong");
        sleep(Duration::from_millis(1100)).await;
        let (recovered, accepted) = tokio::join!(caller.dial(destination), listener.accept());
        drop((recovered?, accepted?));
        listener.close();
        caller.close();
        receiver.close();
        Ok(())
    }
    .await;
    shutdown.cancel();
    timeout(Duration::from_secs(3), server).await???;
    result
}
