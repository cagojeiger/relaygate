# RelayGate

RelayGate는 NAT 뒤의 Listener가 먼저 Gateway에 연결하고, Connector가 논리 주소인 `ClientId`로 opaque bidirectional `Pipe`를 여는 relay입니다.

```text
Connector SDK ── open(ClientId) ──► Gateway ◄── listen(ClientId, ClientKey) ── Listener SDK
              ◄════════════════ opaque bidirectional Pipe ═════════════════════►
```

Rust API에서 `Connector::connect(Config)`와 `ListenerRuntime::connect(Config)`는 각각
Gateway session을 만들고 관리한다. 논리적인 Pipe 연결은 `connector.open(ClientId)`가
시작하며, Listener application은 `listener.accept()`로 선택된 Pipe를 받는다.

`Pipe`는 Tokio의 `AsyncRead`와 `AsyncWrite`를 구현한다. 하나의 task에서 그대로 읽고
쓸 수 있고, 읽기와 쓰기를 독립 task에서 동시에 수행하려면 `into_split()`으로
`PipeReadHalf`와 `PipeWriteHalf`를 얻는다.

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};

let pipe = connector.open("echo.alpha").await?;
let (mut reader, mut writer) = pipe.into_split();

let receive = tokio::spawn(async move {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    Ok::<_, std::io::Error>(bytes)
});

writer.write_all(b"hello relaygate").await?;
writer.shutdown().await?;
let echoed = receive.await??;
```

`shutdown()`과 구조화된 `shutdown_write()`는 write 방향의 `FIN`이다. 한 half만 drop해도
반대 방향을 임의로 닫지 않으며, 마지막 public Pipe owner가 사라질 때 전체 Pipe를 한 번
정리한다. RelayGate 오류 세부 정보가 필요한 코드는 `read_into()`와
`write_all_bytes()` 구조화 메서드를 사용할 수 있다.

현재 구현 단계는 **단일 Gateway의 local Pipe와 RouteTable shard core**입니다.

- Rust workspace와 Rust public SDK
- memory-only `ListenerBinding` registry
- 하나의 Listener session 위에 여러 `ClientId`, 하나의 Connector session 위에 여러 Pipe
- `ClientId` 하나에 여러 live Listener binding을 허용하는 N:M 모델
- bounded queue와 `FIN` / `CLOSE` / `RESET`
- SDK session reconnect; 이미 전송된 `OPEN`과 기존 Pipe는 replay하지 않음
- exact-byte generation과 `sha256-modulo-v1`을 사용하는 immutable shard directory
- memory-only registration lease와 `Register` / `Update` / `KeepAlive` / `Deregister` / `Resolve`
- RouteTable network service, Gateway registration manager와 peer relay는 다음 단계

SDK는 자신의 Gateway session을 재연결하고 이미 반환된 Listener를 재등록한다. 끊어진
Pipe나 commit된 `open` operation은 자동 재시도하지 않으며, application은
`Error::is_retryable()`을 보수적 힌트로 삼아 새 `open(ClientId)` 여부를 결정한다.

## 로컬 실행

Docker Compose 한 번으로 Gateway, echo Listener, 검증 probe를 실행합니다.

```bash
docker compose up --build --abort-on-container-exit --exit-code-from probe
docker compose down --volumes --remove-orphans
```

Probe는 UTF-8 payload, 65,537-byte binary payload, 32개 동시 Pipe의 byte 일치 여부를 검사합니다. Compose의 ClientKey는 로컬 검증 전용이며 운영 credential이 아닙니다.

## Gateway 설정

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_BIND_ADDR` | `0.0.0.0:27420` | SDK session을 받을 주소 |
| `RELAYGATE_CLIENT_KEYS` | 빈 값 | 쉼표로 구분한 `ClientId=ClientKey` 등록 권한 |
| `RELAYGATE_LOG` | `info` | tracing filter |
| `RELAYGATE_WRITER_QUEUE_CAPACITY` | `128` | SDK session별 outbound frame 상한 |
| `RELAYGATE_MAX_FRAME_LEN` | `1048576` | frame당 최대 byte 수 |
| `RELAYGATE_MAX_SESSIONS` | `10000` | handshake 중인 연결을 포함한 session 상한 |
| `RELAYGATE_MAX_BINDINGS` | `100000` | local ListenerBinding 총 상한 |
| `RELAYGATE_MAX_PENDING_OFFERS` | `10000` | 응답 대기 중인 `OFFER` 총 상한 |
| `RELAYGATE_MAX_LIVE_PIPES` | `100000` | 열린 Pipe 총 상한 |
| `RELAYGATE_OFFER_TIMEOUT_MS` | `5000` | Listener의 `OFFER` 응답 기한 |

모든 상한과 timeout은 0보다 큰 정수여야 합니다. Gateway는 시작할 때 configured `ClientId`마다 `ClientKey` 하나를 로드하고 process 수명 동안 갱신하지 않습니다. ClientKey는 최초·recovery 등록 검증에만 사용하며 RelayGate가 발급하거나 영속화하지 않습니다.

## 검증

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## 구조

```text
crates/
├── relaygate-protocol/   # 내부 wire identifier, frame, codec
├── relaygate-gateway/    # session, local registry, OPEN, Pipe relay
├── relaygate-route-table/ # shard directory, lease, current mapping core
├── relaygate-sdk/        # public Connector, Listener, Pipe API
└── relaygate-server/     # process config, health, shutdown, wiring
examples/
├── echo-listener/
└── echo-probe/
```

아키텍처 결정은 [`docs/adr`](docs/adr/), 동작 계약은 [`docs/spec`](docs/spec/), 검증 범위는 [`docs/test`](docs/test/)를 기준으로 합니다.
