# RelayGate Rust 언어 SDK

`relaygate-sdk`는 RelayGate exact-target Relay 데이터 영역의 공개 Rust 클라이언트다.

```bash
cargo add relaygate-sdk@0.1.0
```

공개 API는 `Client`, `ManagedClient`, `Listener`, `Offer`, `Pipe`를 노출한다. 생성된 protobuf·tonic type은 비공개이며 서버, 제어, Peer, Raft type은 crate에 포함하지 않는다.

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

TLS는 기본 필수다. 평문 연결은 loopback 개발 endpoint에서 `Config::with_insecure_local`을 명시한 경우만 허용한다.

권장 시작점은 `ManagedClient`다. 새 인증 세션으로 재연결하고 현재 Listener 선언을 재바인딩한다. 애플리케이션이 재연결·재선언을 직접 소유할 때만 원시 `Client`를 사용한다. `ManagedClient`는 세션 경계를 넘어 Open을 대기열에 넣거나 재시도하지 않고 Pipe를 재개하거나 payload를 재생하지 않는다.

`Pipe::send`는 원격 SDK가 exact `PayloadId`를 상한이 있는 수신 대기열에 넣은 뒤에만 성공한다. `DeliveryError`는 `NotSent`, `Rejected`, 전달 이후 `Unknown`을 구분하며 전달 확인은 애플리케이션 처리나 영속 commit을 증명하지 않는다.

Bind/Unbind 거부는 형식이 있는 작업 범위 오류이며 인증된 세션은 계속 사용할 수 있다. 연관된 payload 거부는 exact Pipe를 종료하지만 Client는 종료하지 않는다. 관리형 재연결은 잘못된 인자, 인증, 권한, 사전 조건 실패, 프로토콜 위반을 영구 오류로 분류하고 일시적인 전송·가용성 오류만 상한이 있는 backoff를 적용한다. 알 수 없는 응답 코드, `UNSPECIFIED`, 다른 요청과의 연관은 프로토콜 종료 오류다. `OpenError::DuplicateInFlight`와 `CloseError::NotOwned`는 중복 진행 Open 거부와 소유하지 않은 Pipe 닫기를 별도 결과로 보존한다.

Crate는 protobuf 빌드 입력을 `proto/` 아래에 포함한다. 저장소 최상위 경로에서 릴리스 산출물과 Rust workspace를 검증한다.

```bash
cargo package --locked --package relaygate-sdk --allow-dirty
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
