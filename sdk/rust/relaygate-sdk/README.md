# RelayGate Rust SDK

`relaygate-sdk` is the public Rust client for RelayGate's exact-target relay data plane.

```bash
cargo add relaygate-sdk@0.1.0
```

The public API exposes `Client`, `ManagedClient`, `Listener`, `Offer`, and `Pipe`. Generated protobuf and tonic types
remain private; server, control, peer, and Raft types are not part of this crate.

```rust,no_run
use relaygate_sdk::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(Config::new(
        "https://relay.example.com",
        "client-id",
        "api-key-id",
        "api-key",
    ))
    .await?;

    let mut pipe = client.open("/echo", "server").await?;
    pipe.send(b"hello".to_vec()).await?;
    let response = pipe.recv().await?;
    println!("{}", String::from_utf8_lossy(&response));

    pipe.close().await?;
    client.close().await;
    Ok(())
}
```

TLS is required by default. Plaintext is available only for loopback development endpoints through
`Config::with_insecure_local`.

`ManagedClient` can reconnect a fresh authenticated session and rebind current listener declarations. It never queues
or retries Opens, resumes Pipes, or replays payloads across a session boundary. `Pipe::send` success means the frame was
accepted by the local bounded stream writer, not acknowledged by the peer application.

Bind and Unbind rejections are typed, operation-local errors and leave the authenticated session usable. A
`PipePayloadRejected` response is terminal for that exact Pipe because payload frames have no acknowledgement ID; it does
not terminate the Client. Managed reconnect treats invalid arguments, authentication, permission, failed preconditions,
and protocol violations as permanent, while transient transport and availability failures enter bounded backoff.

The crate packages its protobuf build input under `proto/`. From the repository root, verify the release artifact and
the full Rust workspace with:

```bash
cargo package --locked --package relaygate-sdk --allow-dirty
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
