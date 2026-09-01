#[allow(dead_code)]
mod support;

use relaygate_sdk::{Config, Connector, ListenerRuntime};
use tracing::subscriber::{NoSubscriber, set_global_default};

use support::{TestGateway, TestResult};

#[tokio::test]
async fn embedding_gateway_and_sdk_do_not_install_a_global_subscriber() -> TestResult {
    let gateway = TestGateway::start(&[("echo.alpha", "secret")]).await?;
    let config = Config::new(gateway.address.to_string());
    let listener_runtime = ListenerRuntime::connect(config.clone()).await?;
    let listener = listener_runtime.listen("echo.alpha", "secret").await?;
    let connector = Connector::connect(config).await?;

    connector.close();
    listener.close().await?;
    listener_runtime.close();
    gateway.stop().await?;

    set_global_default(NoSubscriber::default())
        .map_err(|_| "an embedded RelayGate library installed the global tracing subscriber")?;
    Ok(())
}
