# RelayGate Rust SDK

`relaygate-sdk`는 RelayGate exact-target relay data plane의 public Rust client다.

```bash
cargo add relaygate-sdk@0.1.0
```

Public API는 `Client`, `ManagedClient`, `Listener`, `Offer`, `Pipe`를 노출한다. Generated protobuf/tonic type은 private이며 server, control, peer, Raft type은 crate에 포함하지 않는다.

```rust,no_run
use relaygate_sdk::{Config, ManagedClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ManagedClient::connect(Config::new(
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

TLS는 기본 필수다. Plaintext는 loopback development endpoint에서 `Config::with_insecure_local`을 명시한 경우만 허용한다.

권장 entry point는 `ManagedClient`다. Fresh authenticated session을 reconnect하고 current Listener declaration을 rebind한다. Application이 reconnect/redeclaration을 직접 소유할 때만 raw `Client`를 사용한다. `ManagedClient`는 session boundary를 넘어 Open을 queue/retry하거나 Pipe를 resume하거나 payload를 replay하지 않는다.

`Pipe::send`는 remote SDK가 exact `PayloadId`를 bounded receive queue에 넣은 뒤에만 성공한다. `DeliveryError`는 `NotSent`, `Rejected`, post-handoff `Unknown`을 구분하며 receipt는 application processing이나 durable commit을 증명하지 않는다.

Bind/Unbind rejection은 typed operation-local error이며 authenticated session은 계속 사용할 수 있다. Correlated payload rejection은 exact Pipe의 terminal이지만 Client는 종료하지 않는다. Managed reconnect는 invalid argument, authentication, permission, failed precondition, protocol violation을 permanent로 분류하고 transient transport/availability만 bounded backoff한다. Unknown/`UNSPECIFIED` response code와 foreign correlation은 protocol-fatal이다. `OpenError::DuplicateInFlight`와 `CloseError::NotOwned`는 distinct rejected-Open/non-owned-close 결과를 보존한다.

Crate는 protobuf build input을 `proto/` 아래에 package한다. Repository root에서 release artifact와 Rust workspace를 검증한다.

```bash
cargo package --locked --package relaygate-sdk --allow-dirty
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
