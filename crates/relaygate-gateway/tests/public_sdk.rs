mod support;

use relaygate_sdk::{Config, Connector, ErrorCode, Listener, ListenerRuntime, Pipe};
use tokio::time::{Duration, Instant, sleep, timeout};

use support::{TestGateway, TestResult};

#[tokio::test]
async fn shared_client_id_open_reaches_surviving_listener_runtime() -> TestResult {
    let gateway = TestGateway::start(&[("echo.shared", "secret")]).await?;
    let config = Config::new(gateway.address.to_string())
        .with_operation_timeout(Duration::from_millis(500))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20));
    let first_runtime = ListenerRuntime::connect(config.clone()).await?;
    let second_runtime = ListenerRuntime::connect(config.clone()).await?;
    let first = first_runtime.listen("echo.shared", "secret").await?;
    let second = second_runtime.listen("echo.shared", "secret").await?;
    let connector = Connector::connect(config).await?;

    let opened = connector.open("echo.shared").await?;
    let (first_selected, accepted) = accept_one_of(&first, &second).await?;
    assert_no_queued_pipe(if first_selected { &second } else { &first }).await?;
    opened.close().await?;
    accepted.close().await?;

    if first_selected {
        first_runtime.close();
    } else {
        second_runtime.close();
    }
    let survivor = if first_selected { &second } else { &first };
    let opened_after_close = open_until_success(&connector, "echo.shared").await?;
    let accepted_after_close = timeout(Duration::from_secs(1), survivor.accept()).await??;
    opened_after_close.close().await?;
    accepted_after_close.close().await?;

    connector.close();
    first_runtime.close();
    second_runtime.close();
    gateway.stop().await?;
    Ok(())
}

async fn accept_one_of(first: &Listener, second: &Listener) -> TestResult<(bool, Pipe)> {
    timeout(Duration::from_secs(1), async {
        tokio::select! {
            pipe = first.accept() => pipe.map(|pipe| (true, pipe)),
            pipe = second.accept() => pipe.map(|pipe| (false, pipe)),
        }
    })
    .await?
    .map_err(Into::into)
}

async fn assert_no_queued_pipe(listener: &Listener) -> TestResult {
    match timeout(Duration::from_millis(30), listener.accept()).await {
        Err(_) => Ok(()),
        Ok(Ok(_)) => Err("unselected Listener unexpectedly accepted a Pipe".into()),
        Ok(Err(error)) => Err(error.into()),
    }
}

async fn open_until_success(connector: &Connector, client_id: &str) -> TestResult<Pipe> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match connector.open(client_id).await {
            Ok(pipe) => return Ok(pipe),
            Err(error)
                if matches!(
                    error.code(),
                    ErrorCode::NotFound
                        | ErrorCode::Unavailable
                        | ErrorCode::DeadlineExceeded
                ) && Instant::now() < deadline =>
            {
                sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
