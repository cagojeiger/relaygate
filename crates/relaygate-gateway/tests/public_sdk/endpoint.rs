use super::*;

#[tokio::test]
async fn sdk_endpoint_private_ca_and_explicit_plaintext_connect() -> TestResult {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let pem = cert.pem();
    let tls = ServerTlsConfig::server_authenticated(
        pem.as_bytes(),
        signing_key.serialize_pem().as_bytes(),
    )?;
    let (address, shutdown, server) =
        start_gateway_with_config(GatewayConfig::new(CLUSTER_TOKEN).with_sdk_tls(tls)).await?;
    let endpoint = format!("localhost:{}", address.port());
    let relay = Relay::connect(
        Config::new(&endpoint)?
            .cluster_token(CLUSTER_TOKEN)
            .with_ca_certificate(pem.as_bytes())?,
    )
    .await?;
    relay.close();
    let untrusted = Relay::connect(
        Config::new(&endpoint)?
            .cluster_token(CLUSTER_TOKEN)
            .with_connect_timeout(Duration::from_secs(1)),
    )
    .await;
    assert!(untrusted.is_err());
    let wrong_name = Relay::connect(
        Config::new(address.to_string())?
            .cluster_token(CLUSTER_TOKEN)
            .with_ca_certificate(pem.as_bytes())?
            .with_connect_timeout(Duration::from_secs(1)),
    )
    .await;
    assert!(wrong_name.is_err());
    shutdown.cancel();
    server.await??;

    let (address, shutdown, server) = start_gateway().await?;
    let relay =
        Relay::connect(Config::new(format!("tcp://{address}"))?.cluster_token(CLUSTER_TOKEN))
            .await?;
    relay.close();
    // A TLS endpoint never retries this plaintext server with HELLO.
    assert!(
        Relay::connect(
            Config::new(address.to_string())?
                .cluster_token(CLUSTER_TOKEN)
                .with_connect_timeout(Duration::from_millis(100))
        )
        .await
        .is_err()
    );
    shutdown.cancel();
    server.await??;
    Ok(())
}
