# relaygate-token-issuer

Server-side helper for RelayGate operation tokens.

This crate serializes and signs RelayGate operation JWTs after an application
has already authenticated the caller and made an authorization decision. It
does not own user identity, policy evaluation, HTTP endpoints, private-key
storage, refresh tokens or key rotation.

Applications may use this crate in a backend token service that supplies
operation tokens to `relaygate-sdk`.

## Backend example

The backend must authenticate its caller and decide whether that caller may
perform the requested operation before it invokes `TokenIssuer`. Keep the
private key in an application-owned secret store and never send it to the SDK
or Gateway. Issue separate, short-lived exact tokens for publish and dial:

```no_run
use std::time::Duration;

use relaygate_destination::Destination;
use relaygate_token_issuer::{Action, TokenIssuer};

fn issue_after_policy_check(
    issuer: &TokenIssuer,
    authenticated_subject: &str,
    action: Action,
    destination: &Destination,
) -> Result<String, Box<dyn std::error::Error>> {
    // Replace this with the application's authorization policy. A denied
    // caller must never reach TokenIssuer.
    let allowed = !authenticated_subject.is_empty();
    if !allowed {
        return Err("operation is not authorized".into());
    }

    let token = issuer.issue_exact(action, destination, Duration::from_secs(60))?;
    Ok(token.into_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In production, load this value from the backend's secret provider.
    let private_key_pem = std::env::var("RELAYGATE_ES256_PRIVATE_KEY_PEM")?;
    let issuer = TokenIssuer::from_es256_pem(
        "https://auth.example.com",
        "relaygate",
        "operation-key-2026-09",
        private_key_pem,
    )?;

    let listener: Destination = "speech/transcriber.seoul".parse()?;
    let publish_token = issue_after_policy_check(
        &issuer,
        "listener-service",
        Action::Publish,
        &listener,
    )?;
    let dial_token = issue_after_policy_check(
        &issuer,
        "api-service",
        Action::Dial,
        &listener,
    )?;

    // Return only the appropriate compact token to the corresponding SDK
    // operation. The two grants are deliberately not interchangeable.
    assert_ne!(publish_token, dial_token);
    Ok(())
}
```

Authentication mechanisms, policy evaluation, endpoint design, private-key
storage, and key rotation are outside this crate's scope.

This crate is currently guarded with `publish = false`; the metadata and
package checks exist so crate archives can be validated before a future public
release decision.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
