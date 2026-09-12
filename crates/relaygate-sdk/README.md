# relaygate-sdk

Public Rust SDK for RelayGate applications.

This crate owns the application-facing `Relay`, `Listener`, `Pipe`,
configuration, resource-limit and status-observation APIs. It also owns managed
reconnect and Listener republish behavior. Gateway state types and raw protocol
watch channels are intentionally not exposed as the public SDK contract.

Use this crate when an application needs to publish a destination with a
`Listener` or dial a destination to obtain a byte-stream `Pipe`.

This crate is currently guarded with `publish = false`; the metadata and
package checks exist so crate archives can be validated before a future public
release decision.

## Transport and operation tokens

A bare `host:port` or `tls://host:port` uses public CA trust and verifies the
endpoint's DNS/IP identity. A private deployment can add a CA with
`Config::with_ca_certificate`. `tcp://host:port` explicitly selects plaintext;
it does not encrypt access tokens or Pipe data, and a TLS failure never falls
back to plaintext.

Every `listen` and `dial` supplies an application-issued operation token.
RelayGate does not issue, refresh, or persist these credentials. Production
applications should use `AccessTokenSource::dynamic` to fetch short-lived token
material from their own backend. The helper below uses an environment variable
only as a compileable stand-in for that backend boundary; do not hard-code raw
tokens in source.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
};

async fn token_from_application_backend(
    _request: AccessTokenRequest,
) -> Result<AccessToken, AccessTokenSourceError> {
    // Replace this stand-in with an authenticated call to your application backend.
    let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
        .map_err(|_| AccessTokenSourceError)?;
    AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
}

let tokens = AccessTokenSource::dynamic(token_from_application_backend);
# let _ = tokens;
```

## Publish and accept Pipes

`Relay::listen` waits for the initial Gateway-local binding. A returned
`Listener` is active and remains desired while the SDK reconnects. Status
subscriptions coalesce changes, so observers receive the latest state rather
than an audit log of every transition.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
    Config, Destination, ListenerStatus, Relay, RelayStatus,
};

# async fn token_from_application_backend(
#     _request: AccessTokenRequest,
# ) -> Result<AccessToken, AccessTokenSourceError> {
#     let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
#         .map_err(|_| AccessTokenSourceError)?;
#     AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
# }
# async fn provider() -> Result<(), Box<dyn std::error::Error>> {
let relay = Relay::connect(Config::new("relaygate.example.com:443")?).await?;
let mut relay_status = relay.subscribe_status();
assert_eq!(relay_status.current(), RelayStatus::Active);
let relay_observer = tokio::spawn(async move {
    while let Some(status) = relay_status.changed().await {
        eprintln!("Relay status: {status:?}");
        if status == RelayStatus::Closed {
            break;
        }
    }
});

let destination: Destination = "inference/stt.seoul".parse()?;
let tokens = AccessTokenSource::dynamic(token_from_application_backend);
let listener = relay.listen(destination, tokens).await?;
let mut listener_status = listener.subscribe_status();
assert_eq!(listener_status.current(), ListenerStatus::Active);
let listener_observer = tokio::spawn(async move {
    while let Some(status) = listener_status.changed().await {
        eprintln!("Listener status: {status:?}");
        if status == ListenerStatus::Closed {
            break;
        }
    }
});

let mut pipe = listener.accept().await?;
let mut request = [0_u8; 4096];
let received = pipe.read_into(&mut request).await?;
pipe.write_all_bytes(&request[..received]).await?;
pipe.shutdown_write().await?;

listener.close().await?;
relay.close();
let _ = listener_observer.await;
let _ = relay_observer.await;
# Ok(())
# }
```

## Dial a destination

`Relay::dial` opens one new opaque byte-stream `Pipe`. A committed dial and
Pipe payloads are never replayed by the SDK. After a session interruption,
`wait_ready` can wait for the managed Relay session to become active before the
application chooses whether to start a new operation.

```no_run
use relaygate_sdk::{
    AccessToken, AccessTokenRequest, AccessTokenSource, AccessTokenSourceError,
    Config, Destination, Relay, RelayStatus,
};

# async fn token_from_application_backend(
#     _request: AccessTokenRequest,
# ) -> Result<AccessToken, AccessTokenSourceError> {
#     let raw = std::env::var("RELAYGATE_ACCESS_TOKEN")
#         .map_err(|_| AccessTokenSourceError)?;
#     AccessToken::new(raw).map_err(|_| AccessTokenSourceError)
# }
# async fn dialer() -> Result<(), Box<dyn std::error::Error>> {
let relay = Relay::connect(Config::new("relaygate.example.com:443")?).await?;
let mut statuses = relay.subscribe_status();
assert_eq!(statuses.current(), RelayStatus::Active);
let observer = tokio::spawn(async move {
    while let Some(status) = statuses.changed().await {
        eprintln!("Relay status: {status:?}");
        if status == RelayStatus::Closed {
            break;
        }
    }
});

relay.wait_ready().await?;
let destination: Destination = "inference/stt.seoul".parse()?;
let tokens = AccessTokenSource::dynamic(token_from_application_backend);
let mut pipe = relay.dial(destination, tokens).await?;
pipe.write_all_bytes(b"transcribe this audio").await?;
pipe.shutdown_write().await?;

let mut response = [0_u8; 4096];
while pipe.read_into(&mut response).await? != 0 {
    // Process the opaque response bytes according to the application protocol.
}
relay.close();
let _ = observer.await;
# Ok(())
# }
```

`RelayStatusSubscription::current` and
`ListenerStatusSubscription::current` consume the current watch version. The
next `changed` call therefore waits for a newer state. A `None` result from
`changed` means the owning Relay or Listener runtime has terminated.

## License

Licensed under the Apache License, Version 2.0. The package includes the
workspace `LICENSE` file through Cargo's `license-file` metadata, so the full
license text is present inside the packaged crate archive.
