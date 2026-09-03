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

RelayGate는 **memory-only RouteTable과 Gateway 간 one-hop remote Pipe**를 제공한다.

- Rust workspace와 Rust public SDK
- `ClientId`와 live `ListenerBinding`의 N:M 관계를 보관하는 memory-only registry
- exact-byte generation과 `sha256-modulo-v1`으로 고정된 shard directory
- registration lease 기반 `Register` / `Update` / `KeepAlive` / `Deregister` / `Resolve`
- local-first `OPEN`과 exact Owner binding 재검증을 거치는 one-hop remote `OPEN`
- Gateway pair별 lazy shared `PeerTransport`와 Pipe별 multiplexed `RelayStream`
- bounded queue, activity-aware heartbeat, zero-stream idle retirement와 `FIN` / `CLOSE` / `RESET`
- SDK session 재연결과 current snapshot 재등록. commit된 `OPEN`과 기존 Pipe는 replay·reroute하지 않음

```text
Connector SDK ──► Entry Gateway ══ shared PeerTransport ══► Owner Gateway ◄── Listener SDK
                         │
                         └──── Resolve(ClientId) ──► RouteTable
                                      │
                                      └── BindingSet(owner locator) ──► Entry Gateway
```

SDK는 자신의 Gateway session을 재연결하고 이미 반환된 Listener를 재등록한다. 끊어진
Pipe나 commit된 `open` operation은 자동 재시도하지 않으며, application은
`Error::is_retryable()`을 보수적 힌트로 삼아 새 `open(ClientId)` 여부를 결정한다.

## 로컬 실행

Docker Compose 한 번으로 memory-only RouteTable shard 2대, Gateway 3대, Listener 3대와 검증
probe를 실행합니다.

```bash
docker compose up --build --abort-on-container-exit --exit-code-from topology-probe
docker compose down --volumes --remove-orphans
```

Probe는 local 3경로와 Gateway 사이의 directed remote 6경로, shared `ClientId` 경로, 경로별
32개 동시 Pipe와 byte 일치 여부를 검사합니다. Rust integration test는 shared `ClientId`의
BindingSet 2개와 exact-one 선택을 직접 확인합니다. CI는 추가로 Gateway B 재시작 중 A-C 기존
Pipe의 연속성, shard별 RT 중단 중 local/established Pipe와 다른 shard 유지, 해당 shard의 신규
remote open 실패, RT 재시작 뒤 current-state 재등록을 검사합니다. 두 RT와 세 Gateway는 동일한
exact-byte ShardDirectory artifact를 사용합니다. Compose의 ClientKey와 InternalGatewayKey는 로컬 검증 전용이며
운영 credential이 아닙니다. host에는 Gateway A의 SDK port `127.0.0.1:27420`만 노출하고 RT와
peer port는 Compose network 안에만 둡니다.

## Gateway 설정

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_BIND_ADDR` | `0.0.0.0:27420` | SDK session을 받을 주소 |
| `RELAYGATE_CLIENT_KEYS` | 빈 값 | 쉼표로 구분한 `ClientId=ClientKey` 등록 권한 |
| `RELAYGATE_LOG` | `info` | tracing filter |
| `RELAYGATE_LOG_FORMAT` | `text` | `text` 또는 `json` 로그 출력 |
| `RELAYGATE_STATS_INTERVAL_MS` | unset | 0보다 큰 millisecond. 설정하면 Gateway snapshot을 주기적으로 로그 출력 |
| `RELAYGATE_WRITER_QUEUE_CAPACITY` | `128` | SDK session별 outbound frame 상한 |
| `RELAYGATE_MAX_FRAME_LEN` | `1048576` | frame당 최대 byte 수 |
| `RELAYGATE_MAX_SESSIONS` | `10000` | handshake 중인 연결을 포함한 session 상한 |
| `RELAYGATE_MAX_BINDINGS` | `100000` | local ListenerBinding 총 상한 |
| `RELAYGATE_MAX_PENDING_OFFERS` | `10000` | 응답 대기 중인 `OFFER` 총 상한 |
| `RELAYGATE_MAX_LIVE_PIPES` | `100000` | 열린 Pipe 총 상한 |
| `RELAYGATE_OFFER_TIMEOUT_MS` | `5000` | Listener의 `OFFER` 응답 기한 |
| `RELAYGATE_SDK_HEARTBEAT_IDLE_MS` | `60000` | SDK-Gateway session에서 valid inbound activity 없이 heartbeat `PING`을 보내기 전 대기 시간 |
| `RELAYGATE_SDK_HEARTBEAT_TIMEOUT_MS` | `20000` | SDK-Gateway heartbeat `PING` commit 뒤 기한 내 matching `PONG` 대기 시간 |
| `RELAYGATE_RT_TRUSTED_LOCAL` | distributed mode에서 필수 | 로컬·CI용 plain-TCP RT·peer key adapter를 명시적으로 사용할 때 정확히 `true` |
| `RELAYGATE_RT_SHARD_DIRECTORY_PATH` | distributed mode에서 필수 | Gateway가 process 수명 동안 고정할 exact-byte ShardDirectory JSON artifact |
| `RELAYGATE_GATEWAY_NAME` | distributed mode에서 필수 | 이 Gateway를 고르는 stable configuration 이름 |
| `RELAYGATE_GATEWAY_LOCATOR` | distributed mode에서 필수 | 다른 Gateway가 peer listener에 연결할 주소 |
| `RELAYGATE_INTERNAL_GATEWAY_KEYS` | distributed mode에서 필수 | 쉼표로 구분한 전체 `GatewayName=InternalGatewayKey` local·CI allowlist. local 이름의 key를 자신의 handshake key로 사용 |
| `RELAYGATE_PEER_BIND_ADDR` | `0.0.0.0:27421` | Gateway peer connection을 받을 주소 |
| `RELAYGATE_PEER_HEARTBEAT_IDLE_MS` | `60000` | stream이 있는 PeerTransport에서 valid inbound activity 없이 heartbeat `PING`을 보내기 전 대기 시간 |
| `RELAYGATE_PEER_HEARTBEAT_TIMEOUT_MS` | `20000` | PeerTransport heartbeat `PING` commit 뒤 기한 내 matching `PONG` 대기 시간 |
| `RELAYGATE_PEER_IDLE_TIMEOUT_MS` | `300000` | stream 수가 0인 PeerTransport를 keepalive 없이 유지하는 최대 시간 |

모든 상한과 timeout은 0보다 큰 정수여야 합니다. Gateway는 시작할 때 configured `ClientId`마다 `ClientKey` 하나를 로드하고 process 수명 동안 갱신하지 않습니다. ClientKey는 최초·recovery 등록 검증에만 사용하며 RelayGate가 발급하거나 영속화하지 않습니다.

distributed 관련 변수가 모두 없으면 Gateway는 local-only mode로 시작합니다. 하나라도 있으면
필수 값을 모두 요구하며, RT가 아직 시작되지 않았더라도 Gateway는 local SDK session을 받고
manager가 background에서 bounded backoff로 연결합니다. local binding이 진실이고 RT에는 현재
complete snapshot만 publish하므로 RT 단절은 local binding이나 이미 열린 Pipe를 제거하지 않습니다.
remote OPEN은 Entry Gateway가 Resolve 결과에서 선택한 정확히 한 Owner로만 one hop 전달되며,
같은 Gateway pair의 current Pipe는 shared PeerTransport를 재사용합니다.

`relaygate-server`는 standalone process의 tracing subscriber를 설치합니다. SDK나 Gateway를 application에 직접 포함하면 application이 subscriber를 하나 설치해야 합니다. 구조화 로그는 `ClientKey`와 payload를 기록하지 않으며, snapshot은 현재 Gateway-local 상태일 뿐 전달 성공을 뜻하지 않습니다.

Gateway lifecycle을 자세히 볼 때는 예를 들어 `RELAYGATE_LOG=relaygate_server=info,relaygate_gateway=debug`를 사용합니다. SDK를 포함한 application은 자신의 subscriber filter에 `relaygate_sdk=debug`를 추가합니다.

## RouteTable 설정

Release image는 Gateway와 RouteTable을 분리한다. 로컬 Compose 검증 image는 echo 예제
바이너리까지 포함하지만, GHCR runtime image는 `relaygate-server`만 포함한다.

```text
ghcr.io/cagojeiger/relaygate-gateway:<version>      -> relaygate-server gateway
ghcr.io/cagojeiger/relaygate-route-table:<version>  -> relaygate-server route-table
```

개발 image에서 같은 `relaygate-server` image를 직접 쓴다면 `route-table` 역할로 실행합니다.

```bash
relaygate-server route-table
```

| 환경변수 | 기본값 | 의미 |
| --- | --- | --- |
| `RELAYGATE_RT_TRUSTED_LOCAL` | 필수 | 로컬·CI용 plain-TCP key adapter를 명시적으로 사용할 때 정확히 `true` |
| `RELAYGATE_RT_BIND_ADDR` | `127.0.0.1:27430` | Gateway 요청을 받을 주소. 기본값은 loopback 전용 |
| `RELAYGATE_RT_SHARD_DIRECTORY_PATH` | 필수 | exact-byte ShardDirectory JSON artifact 경로 |
| `RELAYGATE_RT_SHARD_ID` | `rt-0` | 이 process가 소유할 logical shard |
| `RELAYGATE_RT_LEASE_TTL_MS` | `30000` | registration lease TTL |
| `RELAYGATE_INTERNAL_GATEWAY_KEYS` | 필수 | 쉼표로 구분한 `GatewayName=InternalGatewayKey` local/CI allowlist |
| `RELAYGATE_RT_REQUEST_QUEUE_CAPACITY` | `128` | shard actor의 pending request 상한 |
| `RELAYGATE_RT_WRITER_QUEUE_CAPACITY` | `32` | TCP connection별 response queue 상한 |
| `RELAYGATE_RT_MAX_CONNECTIONS` | `1024` | 동시 Gateway connection 상한 |
| `RELAYGATE_RT_MAX_FRAME_LEN` | `1048576` | 내부 RT frame 최대 byte 수 |
| `RELAYGATE_RT_HANDSHAKE_TIMEOUT_MS` | `3000` | Gateway 인증 handshake 기한 |

RouteTable은 시작할 때 항상 빈 memory-only state이며 directory artifact와 shard를 process 수명 동안 고정합니다. `InternalGatewayKey` adapter와 plain TCP는 로컬·CI 검증용이며, 실수로 켜지지 않도록 `RELAYGATE_RT_TRUSTED_LOCAL=true`가 없으면 시작을 거부하고 활성화 시 경고를 기록합니다. 운영 channel identity와 기밀성은 배포 환경의 mTLS 또는 service identity 계층이 제공해야 합니다.

RouteTable shard는 replication이나 consensus 없이 stable logical endpoint 하나로 동작한다.
ShardDirectory는 process 수명 동안 바뀌지 않으며, 변경은 coordinated restart와 current-state
재등록으로 적용한다.

## 검증

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## 릴리스

RelayGate runtime image는 컴포넌트별로 릴리스한다.

| 파일 | Git tag | GHCR image |
| --- | --- | --- |
| `VERSION.gateway` | `gateway-vX.Y.Z` | `ghcr.io/cagojeiger/relaygate-gateway:X.Y.Z` |
| `VERSION.route-table` | `route-table-vX.Y.Z` | `ghcr.io/cagojeiger/relaygate-route-table:X.Y.Z` |

`main`에 해당 `VERSION.*` 변경이 들어오면 release workflow가 multi-arch image를 push하고
같은 컴포넌트의 `latest` tag와 GitHub Release를 갱신한다. 두 파일을 함께 바꾸면 두 image가
같이 릴리스되고, 하나만 바꾸면 해당 image만 릴리스된다. 각 GHCR package는 현재 `latest`가
가리키는 version을 포함해 semver release image 최근 20개를 유지한다.

릴리스 실행은 순서대로 대기하므로 연속된 version 변경도 생략하지 않는다. 이미 생성된 tag가
같은 commit을 가리키는 재실행은 허용하고, 다른 commit에서 같은 version을 재사용하는 것은 거절한다.
GitHub Release의 전역 `Latest` 표시는 사용하지 않고, image별 `latest` tag만 갱신한다.

`VERSION.gateway`와 `VERSION.route-table`은 배포 image 버전이며 서로 독립적이다. Rust
workspace version은 crate/API 버전이므로 image release version과 같은 수명주기를 강제하지 않는다.

## 구조

```text
crates/
├── relaygate-protocol/   # SDK-Gateway wire identifier, frame, codec
├── relaygate-gateway/    # session, local registry, OPEN, Pipe relay
├── relaygate-route-table/ # shard directory, lease, current mapping core
├── relaygate-route-table-transport/ # bounded internal TCP service/client
├── relaygate-sdk/        # public Connector, Listener, Pipe API
└── relaygate-server/     # process config, health, shutdown, wiring
examples/
├── echo-listener/
└── echo-probe/
```

아키텍처 결정은 [`docs/adr`](docs/adr/), 동작 계약은 [`docs/spec`](docs/spec/), 검증 범위는 [`docs/test`](docs/test/)를 기준으로 합니다.
