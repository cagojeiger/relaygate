use std::{error::Error, time::Duration};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use relaygate_gateway::{Gateway, GatewayConfig};
use relaygate_sdk::{ClientTlsConfig, Config, DestinationId, ListenerStatus, Relay};
use relaygate_transport::ServerTlsConfig;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CLUSTER_TOKEN: &str = "v02-test-cluster-token";

#[tokio::test]
async fn sdk_gateway_path_uses_tls_before_cluster_admission() -> TestResult {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["relaygate.test".to_owned()])?;
    let certificate = cert.pem();
    let private_key = signing_key.serialize_pem();
    let gateway_tls =
        ServerTlsConfig::server_authenticated(certificate.as_bytes(), private_key.as_bytes())?;
    let client_tls =
        ClientTlsConfig::server_authenticated("relaygate.test", certificate.as_bytes())?;
    let config = GatewayConfig::new(CLUSTER_TOKEN).with_sdk_tls(gateway_tls);
    let (address, shutdown, server) = start_gateway_with_config(config).await?;

    let relay = Relay::connect(Config::new(
        address.to_string(),
        CLUSTER_TOKEN,
        client_tls.clone(),
    ))
    .await?;
    let rejected = Relay::connect(
        Config::new(address.to_string(), "wrong-token", client_tls)
            .with_connect_timeout(Duration::from_secs(1))
            .with_operation_timeout(Duration::from_secs(1)),
    )
    .await
    .err()
    .ok_or("invalid ClusterToken was admitted over TLS")?;
    assert_eq!(rejected.code(), relaygate_sdk::ErrorCode::Unauthenticated);

    relay.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn one_session_can_listen_dial_and_accept() -> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let config = Config::new_insecure_for_tests(address.to_string(), CLUSTER_TOKEN)
        .with_operation_timeout(Duration::from_secs(1))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50));
    let relay_a = Relay::connect(config.clone()).await?;
    let relay_b = Relay::connect(config).await?;
    let destination_a = DestinationId::new();
    let destination_b = DestinationId::new();
    let listener_a = relay_a.listen(destination_a).await?;
    let listener_b = relay_b.listen(destination_b).await?;

    let (dial_a_to_b, accepted_by_b, dial_b_to_a, accepted_by_a) =
        timeout(Duration::from_secs(2), async {
            tokio::join!(
                relay_a.dial(destination_b),
                listener_b.accept(),
                relay_b.dial(destination_a),
                listener_a.accept(),
            )
        })
        .await?;

    let mut dial_a_to_b = dial_a_to_b?;
    let mut accepted_by_b = accepted_by_b?;
    let mut dial_b_to_a = dial_b_to_a?;
    let mut accepted_by_a = accepted_by_a?;
    dial_a_to_b.write_all(b"a-to-b").await?;
    dial_a_to_b.shutdown().await?;
    dial_b_to_a.write_all(b"b-to-a").await?;
    dial_b_to_a.shutdown().await?;

    let mut from_a = Vec::new();
    let mut from_b = Vec::new();
    accepted_by_b.read_to_end(&mut from_a).await?;
    accepted_by_a.read_to_end(&mut from_b).await?;
    assert_eq!(from_a, b"a-to-b");
    assert_eq!(from_b, b"b-to-a");

    relay_a.close();
    relay_b.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn same_destination_selects_one_of_multiple_relays() -> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let config = Config::new_insecure_for_tests(address.to_string(), CLUSTER_TOKEN)
        .with_operation_timeout(Duration::from_secs(1));
    let first = Relay::connect(config.clone()).await?;
    let second = Relay::connect(config.clone()).await?;
    let caller = Relay::connect(config).await?;
    let destination = DestinationId::new();
    let first_listener = first.listen(destination).await?;
    let second_listener = second.listen(destination).await?;

    let dial = caller.dial(destination);
    let accepted = async {
        tokio::select! {
            pipe = first_listener.accept() => pipe,
            pipe = second_listener.accept() => pipe,
        }
    };
    let (dialed, accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(dial, accepted)
    })
    .await?;
    let mut dialed = dialed?;
    let mut accepted = accepted?;
    dialed.write_all(b"one binding").await?;
    dialed.shutdown().await?;
    let mut payload = Vec::new();
    accepted.read_to_end(&mut payload).await?;
    assert_eq!(payload, b"one binding");

    first.close();
    second.close();
    caller.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn session_admission_accepts_current_and_next_tokens_and_rejects_other_tokens() -> TestResult
{
    let config = GatewayConfig::new(CLUSTER_TOKEN).with_next_cluster_token("next-token");
    let (address, shutdown, server) = start_gateway_with_config(config).await?;

    let current = Relay::connect(Config::new_insecure_for_tests(
        address.to_string(),
        CLUSTER_TOKEN,
    ))
    .await?;
    let next = Relay::connect(Config::new_insecure_for_tests(
        address.to_string(),
        "next-token",
    ))
    .await?;
    let rejected = Relay::connect(Config::new_insecure_for_tests(
        address.to_string(),
        "wrong-token",
    ))
    .await
    .err()
    .ok_or("invalid ClusterToken was admitted")?;
    assert_eq!(rejected.code(), relaygate_sdk::ErrorCode::Unauthenticated);

    current.close();
    next.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn relay_cannot_dial_its_own_only_binding() -> TestResult {
    let (address, shutdown, server) = start_gateway().await?;
    let relay = Relay::connect(Config::new_insecure_for_tests(
        address.to_string(),
        CLUSTER_TOKEN,
    ))
    .await?;
    let destination = DestinationId::new();
    let _listener = relay.listen(destination).await?;

    let error = relay
        .dial(destination)
        .await
        .err()
        .ok_or("Relay unexpectedly dialed its own only Binding")?;
    assert_eq!(error.code(), relaygate_sdk::ErrorCode::FailedPrecondition);

    relay.close();
    shutdown.cancel();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn relay_reconnects_republishes_and_replaces_ended_pipes() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let first_gateway = Gateway::new(
        GatewayConfig::new(CLUSTER_TOKEN).with_drain_timeout(Duration::from_millis(20)),
    )?;
    let first_shutdown = CancellationToken::new();
    let serve_shutdown = first_shutdown.clone();
    let first_server =
        tokio::spawn(async move { first_gateway.serve(listener, serve_shutdown).await });

    let config = Config::new_insecure_for_tests(address.to_string(), CLUSTER_TOKEN)
        .with_operation_timeout(Duration::from_secs(2))
        .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50));
    let publisher = Relay::connect(config.clone()).await?;
    let caller = Relay::connect(config).await?;
    let destination = DestinationId::new();
    let publication = publisher.listen(destination).await?;

    let (old_dialed, old_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(caller.dial(destination), publication.accept())
    })
    .await?;
    let mut old_dialed = old_dialed?;
    let mut old_accepted = old_accepted?;

    first_shutdown.cancel();
    first_server.await??;
    let mut byte = [0_u8; 1];
    let caller_end = timeout(Duration::from_secs(1), old_dialed.read(&mut byte)).await?;
    let publisher_end = timeout(Duration::from_secs(1), old_accepted.read(&mut byte)).await?;
    assert!(
        matches!(caller_end, Ok(0) | Err(_)),
        "the old caller Pipe must terminate: {caller_end:?}"
    );
    assert!(
        matches!(publisher_end, Ok(0) | Err(_)),
        "the old published Pipe must terminate: {publisher_end:?}"
    );

    let listener = TcpListener::bind(address).await?;
    let second_gateway = Gateway::new(
        GatewayConfig::new(CLUSTER_TOKEN).with_drain_timeout(Duration::from_millis(20)),
    )?;
    let second_shutdown = CancellationToken::new();
    let serve_shutdown = second_shutdown.clone();
    let second_server =
        tokio::spawn(async move { second_gateway.serve(listener, serve_shutdown).await });

    timeout(Duration::from_secs(2), async {
        while publication.status() != ListenerStatus::Active {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    let (new_dialed, new_accepted) = timeout(Duration::from_secs(2), async {
        tokio::join!(caller.dial(destination), publication.accept())
    })
    .await?;
    let mut new_dialed = new_dialed?;
    let mut new_accepted = new_accepted?;
    new_dialed.write_all(b"after-reconnect").await?;
    new_dialed.shutdown().await?;
    let mut payload = Vec::new();
    new_accepted.read_to_end(&mut payload).await?;
    assert_eq!(payload, b"after-reconnect");

    publisher.close();
    caller.close();
    second_shutdown.cancel();
    second_server.await??;
    Ok(())
}

async fn start_gateway() -> TestResult<(
    std::net::SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
)> {
    start_gateway_with_config(GatewayConfig::new(CLUSTER_TOKEN)).await
}

async fn start_gateway_with_config(
    config: GatewayConfig,
) -> TestResult<(
    std::net::SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), relaygate_gateway::GatewayError>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gateway = Gateway::new(config.with_drain_timeout(Duration::from_millis(100)))?;
    let shutdown = CancellationToken::new();
    let serve_shutdown = shutdown.clone();
    let server = tokio::spawn(async move { gateway.serve(listener, serve_shutdown).await });
    Ok((address, shutdown, server))
}
