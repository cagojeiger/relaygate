use super::*;

#[test]
fn addresses_select_transport_and_identity() -> Result<()> {
    for (input, name, plaintext) in [
        ("example.com:443", "example.com", false),
        ("tls://example.com:443", "example.com", false),
        ("tcp://localhost:27420", "localhost", true),
        ("127.0.0.1:443", "127.0.0.1", false),
        ("tls://[::1]:443", "::1", false),
    ] {
        let (_, actual_name, actual_plaintext) = endpoint_parts(input)?;
        assert_eq!(actual_name, name);
        assert_eq!(actual_plaintext, plaintext);
        let config = GatewayTransportConfig::from_endpoint(input)?;
        assert_eq!(
            matches!(config.kind, GatewayTransport::InsecureTcp { .. }),
            plaintext
        );
    }
    Ok(())
}

#[test]
fn invalid_endpoints_are_rejected_without_echoing_input() -> Result<()> {
    for input in [
        "",
        "example.com",
        "example.com:0",
        "example.com:65536",
        "example.com:+443",
        "https://example.com:443",
        "tls://user:secret@example.com:443",
        "example.com:443/path",
        "example.com:443?x",
        "example.com:443#x",
        " example.com:443",
        "::1:443",
        "[bad]:443",
    ] {
        let error = match GatewayTransportConfig::from_endpoint(input) {
            Err(error) => error,
            Ok(_) => return Err(invalid_endpoint()),
        };
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!error.message().contains("secret"));
    }
    Ok(())
}

#[test]
fn endpoint_config_requires_token_and_redacts_it() -> Result<()> {
    let config = crate::Config::new("localhost:443")?;
    assert!(config.validate().is_err());
    let config = config.cluster_token("private-token");
    config.validate()?;
    assert!(!format!("{config:?}").contains("private-token"));
    assert!(
        crate::Config::new("tcp://localhost:80")?
            .with_ca_certificate(b"invalid")
            .is_err()
    );
    assert!(
        crate::Config::new("localhost:443")?
            .with_ca_certificate(b"invalid")
            .is_err()
    );
    Ok(())
}

#[test]
fn ca_helper_rejects_explicit_transport_instead_of_replacing_identity() -> Result<()> {
    let tls =
        ClientTlsConfig::with_webpki_roots("custom.example.com").map_err(|_| invalid_endpoint())?;
    let config = crate::Config::with_transport(
        "token",
        GatewayTransportConfig::tls_tcp("127.0.0.1:443", tls),
    );
    let error = config
        .with_ca_certificate(b"irrelevant")
        .err()
        .ok_or_else(invalid_endpoint)?;
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(error.message().contains("preserve its identity"));
    Ok(())
}
