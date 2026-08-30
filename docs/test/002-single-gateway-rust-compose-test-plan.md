# TEST 002: 단일 Gateway Rust 회귀 profile

| 항목 | 값 |
| --- | --- |
| 상태 | 구현 완료, workspace CI 회귀 대상 |
| 목적 | RT와 peer relay 없이 SDK↔Gateway local Pipe 계약을 결정적으로 검증한다. |
| 상위 계약 | [SPEC 002](../spec/002-sdk-pipe-contract.md), [SPEC 003](../spec/003-listener-registration-contract.md), [SPEC 005](../spec/005-connection-establishment-contract.md), [SPEC 007](../spec/007-error-and-state-model.md), [SPEC 008](../spec/008-runtime-observability-contract.md) |
| 전체 matrix | [TEST 001](001-requirement-test-matrix.md) |
| 현재 Compose profile | [TEST 004](004-rt1-gw3-closed-loop-test-plan.md) |

이 파일명은 기존 링크 호환을 위해 유지한다. 현재 root `docker-compose.yml`은 RT1/GW3 profile이며,
단일 Gateway는 빠르고 결정적인 Rust unit·integration·process test가 소유한다.

## 범위

```text
Language       = Rust
Gateway count  = 1
RouteTable     = 없음
PeerTransport  = 없음
Persistence    = 없음
Data path      = local binding only
```

```text
Connector SDK ── open(ClientId) ──► Gateway ◄── listen(ClientId, ClientKey) ── Listener SDK
              ◄════════════════ opaque bidirectional Pipe ═════════════════════►
```

이 profile은 다음 폐루프를 검증한다.

```text
REGISTER
  -> ACTIVE local binding
  -> OPEN / OFFER / OFFER_ACCEPTED
  -> bidirectional Pipe
  -> FIN | CLOSE | RESET
  -> bounded cleanup
  -> SDK session 재연결과 returned Listener 재등록
```

RT lookup, lease, shard, Gateway 간 relay와 process-level 분산 장애는 [TEST 004](004-rt1-gw3-closed-loop-test-plan.md)가 소유한다.

## 공개 API 경계

```text
Connector::connect(Config)                    -> ConnectorSession
connector.open(ClientId)                      -> Pipe
ListenerRuntime::connect(Config)              -> ListenerSession
listener_runtime.listen(ClientId, ClientKey)  -> Listener
listener.accept()                             -> Pipe
Pipe                                          -> AsyncRead + AsyncWrite
Pipe::into_split()                            -> read half + write half
```

- SDK는 자기 Gateway session을 재연결한다.
- Listener SDK는 이미 반환된 Listener만 새 session identity로 재등록한다.
- 기존 Pipe, commit된 OPEN과 payload는 replay·reroute·resume하지 않는다.
- 한 open은 Listener 하나만 선택하며 같은 attempt의 sibling fallback은 없다.
- payload protocol, application 인증·인가와 업무 retry는 RelayGate 범위 밖이다.

## 검증 소유권

| 계층 | 현재 검증 |
| --- | --- |
| Gateway unit | registry index, N:M binding, OPEN/OFFER state, owner 검증, timeout, bounded queue, FIN/CLOSE/RESET, cleanup, current snapshot |
| SDK unit | managed reconnect, Listener recovery, Pipe I/O·half-close, operation deadline, error observation |
| Gateway integration | local Pipe, public SDK full duplex, 같은 ClientId의 surviving Listener 선택, disconnect·queue·foreign frame edge case |
| Server process | config validation, health check, structured log redaction, SIGTERM cleanup |
| Docker image/process topology | [TEST 004](004-rt1-gw3-closed-loop-test-plan.md)의 RT1/GW3 Compose profile |

핵심 executable regression은 다음과 같다.

| Test | 보장 |
| --- | --- |
| `local_pipe` | Listener queue admission 뒤 OPENED, opaque bytes와 정상 close |
| `public_sdk` | public API full duplex와 same-ClientId surviving Listener |
| `gateway_edges` | invalid key, session/offer loss, no fallback, role·owner 격리 |
| `relaygate-sdk` unit/integration | reconnect, returned Listener 재등록, Pipe lifecycle과 observation |
| `relaygate-server` process | boot, health, config error, secret-free logs와 graceful shutdown |

세부 상태·오류 조합의 canonical 목록은 이 문서에서 반복하지 않고 [TEST 001](001-requirement-test-matrix.md)을 따른다.

## 실행

```text
cargo test -p relaygate-sdk
cargo test -p relaygate-gateway --test local_pipe --test public_sdk --test gateway_edges
cargo test -p relaygate-server
```

workspace 최종 gate는 다음을 포함한다.

```text
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

## 완료 조건

1. local open은 RT·peer 호출 없이 exactly one Listener Pipe를 만든다.
2. terminal 결과는 한 번만 노출되고 닫힌 identity를 다시 활성화하지 않는다.
3. session 단절은 해당 session의 current attempt와 Pipe만 bounded cleanup한다.
4. managed reconnect는 returned Listener current set만 재등록하고 과거 operation을 replay하지 않는다.
5. 상태량은 current session, binding, attempt, Pipe와 configured queue bound에 비례한다.
