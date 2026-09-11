use super::*;

#[test]
fn local_gateway_validates_explicit_transport_before_sdk_setup() -> Result<(), Box<dyn Error>> {
    for mode in ["", "tcp", "MTLS"] {
        let output = server_command()
            .env_remove("RELAYGATE_INSECURE_TEST_TRANSPORT")
            .env("RELAYGATE_INTERNAL_TRANSPORT", mode)
            .output()?;
        assert_unsuccessful_output(&output, "must be `mtls` or `plaintext`");
    }
    for mode in ["mtls", "plaintext"] {
        let output = server_command()
            .env("RELAYGATE_INTERNAL_TRANSPORT", mode)
            .output()?;
        assert_unsuccessful_output(
            &output,
            "cannot be combined with legacy test transport flags",
        );
    }
    Ok(())
}

#[test]
fn internal_plaintext_does_not_disable_sdk_tls() -> Result<(), Box<dyn Error>> {
    let output = server_command()
        .env_remove("RELAYGATE_INSECURE_TEST_TRANSPORT")
        .env("RELAYGATE_INTERNAL_TRANSPORT", "plaintext")
        .output()?;
    assert_unsuccessful_output(&output, "RELAYGATE_SDK_TLS_CERT_PATH is required");
    Ok(())
}

#[cfg(unix)]
#[test]
fn internal_transport_rejects_unknown_and_conflicting_modes_before_serving()
-> Result<(), Box<dyn Error>> {
    for mode in ["", "tcp", "MTLS"] {
        let output = server_command()
            .arg("route-table")
            .env_remove("RELAYGATE_INSECURE_TEST_TRANSPORT")
            .env("RELAYGATE_INTERNAL_TRANSPORT", mode)
            .output()?;
        assert_unsuccessful_output(&output, "must be `mtls` or `plaintext`");
    }
    let output = server_command()
        .arg("route-table")
        .env("RELAYGATE_INTERNAL_TRANSPORT", "plaintext")
        .output()?;
    assert_unsuccessful_output(
        &output,
        "cannot be combined with legacy test transport flags",
    );

    let artifact = ShardDirectoryArtifact::create()?;
    for mode in [None, Some("mtls")] {
        let mut command = server_command();
        command
            .arg("route-table")
            .env_remove("RELAYGATE_INSECURE_TEST_TRANSPORT")
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path());
        if let Some(mode) = mode {
            command.env("RELAYGATE_INTERNAL_TRANSPORT", mode);
        }
        assert_unsuccessful_output(
            &command.output()?,
            "RELAYGATE_INTERNAL_TLS_CA_PATH is required",
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_plaintext_route_table_admits_without_keys_or_certificates()
-> Result<(), Box<dyn Error>> {
    let address = unused_loopback_address()?;
    let artifact = ShardDirectoryArtifact::create()?;
    let mut server = ChildGuard::spawn(
        server_command()
            .arg("route-table")
            .env_remove("RELAYGATE_INSECURE_TEST_TRANSPORT")
            .env("RELAYGATE_INTERNAL_TRANSPORT", "plaintext")
            .env("RELAYGATE_RT_BIND_ADDR", &address)
            .env("RELAYGATE_RT_SHARD_DIRECTORY_PATH", artifact.path()),
    )?;
    let client = wait_until_route_table_ready(&address, GatewayId::new(), &mut server).await?;
    let directory = ShardDirectory::from_json_bytes(ShardDirectoryArtifact::BYTES)?;
    let error = client
        .resolve(directory.generation(), &DESTINATION_A.parse()?)
        .await;
    assert!(matches!(error, Err(error) if error.code() == RouteTableErrorCode::NotFound));

    Ok(())
}
