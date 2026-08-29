# TEST 002: 단일 Gateway Rust·Docker Compose 구현 profile

| 항목 | 값 |
| --- | --- |
| 상태 | Phase 1 local Pipe와 SDK↔Gateway closure 구현·검증 완료 (2026-08-29) |
| 목적 | 전체 구조를 한 번에 구현하지 않고 local Pipe 경로를 먼저 증명한다. |
| 상위 계약 | [SPEC 002](../spec/002-sdk-pipe-contract.md), [SPEC 003](../spec/003-listener-registration-contract.md), [SPEC 005](../spec/005-connection-establishment-contract.md), [SPEC 007](../spec/007-error-and-state-model.md) |
| 전체 검증 기준 | [TEST 001](001-requirement-test-matrix.md) |

이 문서는 ADR이나 SPEC을 바꾸지 않는다. 첫 구현 단계에서 검증할 subset과 실행 환경만 고정한다.

## 구현 profile

```text
Language       = Rust
Gateway count  = 1
RouteTable     = 없음
PeerTransport  = 없음
Persistence    = 없음
Runtime        = Docker Compose
Data path      = local binding only
```

Gateway가 하나면 모든 live `ListenerBinding`은 같은 Gateway의 `LocalRegistry`에 있다. 따라서 최초 구현은 RT 조회, shard, registration lease와 Gateway 간 peer relay를 포함하지 않는다.

```text
┌──────────────────── Docker Compose network ────────────────────┐
│                                                                │
│  listener-echo ── Listener SDK ─┐                               │
│                                 ▼                               │
│                           gateway × 1                           │
│                                 ▲                               │
│  probe ──────── Connector SDK ──┘                               │
│                                                                │
│  connect(ClientId) -> local binding -> Pipe -> echo -> close    │
└────────────────────────────────────────────────────────────────┘
```

첫 단계가 증명하는 것은 다음뿐이다.

```text
Listener 등록
  -> Gateway-local ACTIVE binding
  -> local connect
  -> bounded Listener queue
  -> bidirectional Pipe
  -> FIN / CLOSE / RESET
  -> cleanup 또는 새 session 재연결
```

Listener SDK는 per-command 재생 큐를 source of truth로 삼지 않는다. `ListenerRuntime`은 pending `ListenAttempt` reservation과 이미 반환된 `ClientId -> Listener handle` 복구 집합을 구분한다. shared ListenerSession actor는 current session의 pending/registered set과 비교하여 `REGISTER`와 `UNREGISTER`를 수렴시키되, session을 넘어 복구하는 대상은 반환된 Listener뿐이다.

```text
ListenerRuntime
  ├── pending ListenAttempt reservation
  └── returned Listener recovery set
              │ reconcile
              ▼
ListenerSession actor
        │
        ├── 최초 attempt REGISTER commit 뒤 실패 -> terminal, 새 session 이동 없음
        ├── returned Listener가 session에 없음   -> 새 identity로 REGISTER
        ├── recovery transient failure            -> bounded backoff
        ├── current set에서 제거된 registered     -> UNREGISTER
        └── pending 결과가 늦게 도착              -> state 부활 없이 cleanup
```

`Listener::close`는 desired에서 handle을 제거하고 신규 수신을 닫는다. 아직 `accept`되지 않은 queued Pipe는 drop되어 session actor가 Pipe `CLOSE`를 보낼 수 있지만, shared ListenerSession, sibling Listener handle과 이미 application이 accept한 Pipe는 닫지 않는다.

## Rust workspace와 crate 경계

루트에 하나의 `src/`를 두지 않는다. workspace 아래의 책임별 crate가 각자 `src/`와 `tests/`를 소유한다.

```text
relaygate/
├── Cargo.toml                         # workspace, 공통 dependency·lint·profile
├── Cargo.lock
├── rust-toolchain.toml
├── crates/
│   ├── relaygate-protocol/
│   │   ├── Cargo.toml
│   │   ├── src/                  # identity, frame, error, codec
│   │   └── tests/                # encode/decode, invalid frame
│   ├── relaygate-gateway/
│   │   ├── Cargo.toml
│   │   ├── src/                  # registry, session, opening, pipe
│   │   └── tests/                # state race, disconnect, cleanup
│   ├── relaygate-sdk/
│   │   ├── Cargo.toml
│   │   ├── src/                  # Connector, Listener, Pipe, reconnect runtime
│   │   └── tests/                # public API contract
│   └── relaygate-server/
│       ├── Cargo.toml
│       ├── src/                  # main, config, health, shutdown, wiring
│       └── tests/                # process boot and graceful shutdown
├── examples/
│   ├── echo-listener/
│   │   ├── Cargo.toml
│   │   └── src/main.rs
│   └── echo-probe/
│       ├── Cargo.toml
│       └── src/main.rs
├── deploy/docker/Dockerfile
└── docker-compose.yml
```

```text
relaygate-server ──► relaygate-gateway ──► relaygate-protocol
echo-listener   ─┐
echo-probe      ─┴──► relaygate-sdk     ──► relaygate-protocol
```

| Crate | 책임 | 금지 경계 |
| --- | --- | --- |
| `relaygate-protocol` | SDK–Gateway 간 identifier, frame, error code와 encoding | socket, session 소유, reconnect, routing policy |
| `relaygate-gateway` | local registry, Listener/Connector session, OPEN admission, Pipe relay와 bounded cleanup | SDK public API, process 설정·signal wiring |
| `relaygate-sdk` | public `Connector`·`Listener`·`Pipe` API와 managed reconnect | Gateway state 소유, RT/peer 로직, 내부 protocol type 노출 |
| `relaygate-server` | binary boot, config validation, tracing, health, graceful shutdown과 dependency wiring | registry·OPEN·Pipe 정책 |
| `echo-*` | 배포 사용자와 같이 public SDK만 사용하는 검증 program | Gateway/protocol crate 직접 참조 |

`relaygate-protocol`은 workspace-internal crate로 두고 SDK public signature에 직접 노출하지 않는다. `main.rs`는 구성과 lifecycle wiring만 하고 Gateway 상태 로직을 갖지 않는다. RT와 peer는 다음 phase가 시작될 때 실제 책임이 생기면 추가하며, 최초 workspace에 빈 placeholder crate를 만들지 않는다.

복잡한 행동 검증은 각 crate의 `tests/`에 두고, 작고 순수한 helper의 불변식만 해당 `src/` module의 unit test로 둔다. Compose E2E는 별도 shell script 대신 `echo-probe` executable의 exit code로 종료한다.

## Compose 구성

| Service | 역할 | 종료 조건 |
| --- | --- | --- |
| `gateway` | Listener/Connector session, local registry, local OPEN과 Pipe relay | test 종료 또는 fatal error |
| `listener-echo` | `echo.alpha`를 등록하고 받은 bytes를 그대로 반환 | session 종료 또는 test 종료 |
| `probe` | readiness 대기, connect·payload·종료 assertion 수행 | 모든 assertion 성공 시 exit 0 |

`probe`는 Gateway healthcheck만으로 Listener 등록 완료를 가정하지 않는다. 자신의 deadline 안에서 `connect`를 반복해 local binding 수렴을 기다리되, 이미 commit된 하나의 connect attempt를 replay하지 않고 매번 새 operation을 시작한다.

기본 smoke 실행의 목표 형태는 다음과 같다.

```text
docker compose up --build --abort-on-container-exit --exit-code-from probe
docker compose down --volumes --remove-orphans
```

Compose E2E는 실제 container build, DNS, process startup, healthcheck와 TCP-level disconnect를 확인한다. 세부 상태 경쟁은 Docker timing에 의존시키지 않고 Rust integration test에서 결정적으로 주입한다.

## 테스트 계층

```text
빠르고 결정적
    │
    ├── Rust unit test
    │     순수 state, registry, identity, buffer
    │
    ├── Rust integration test
    │     실제 async session + fault injection
    │
    └── Docker Compose E2E
          실제 image + process + network
느리고 실제 환경에 가까움
```

| 계층 | 주로 증명하는 것 | 증명하지 않는 것 |
| --- | --- | --- |
| Unit | state transition, 중복 방지, index 일관성, bounded 구조 | socket과 container 동작 |
| Integration | SDK-Gateway protocol, race, disconnect, backpressure, cleanup | image와 Compose wiring |
| Compose E2E | build·startup·health·실제 echo·process restart | 모든 상태 조합의 exhaustive coverage |

## 필수 테스트

### Unit

| Test ID | 시나리오 | 기대 결과 |
| --- | --- | --- |
| `SG-U-01` | 같은 Listener runtime에서 동일 ClientId handle 동시 생성 | 하나만 성공하고 나머지는 `ALREADY_EXISTS`; binding과 queue 하나 |
| `SG-U-02` | unregister 뒤 같은 ClientId 재등록 | 이전 `BindingId`를 재사용하지 않고 새 binding 하나 생성 |
| `SG-U-03` | local registry add/remove를 각 index에서 조회 | BindingId, ClientId, ListenerSessionId index가 같은 live set을 나타냄 |
| `SG-U-04` | OPEN admission과 binding remove를 동시에 실행 | 먼저 확정된 순서 하나만 적용; Pipe 하나 또는 `NOT_OBSERVED` 실패 |
| `SG-U-05` | queue와 buffer capacity 소진 | 기존 item을 버리지 않고 backpressure 또는 `RESOURCE_EXHAUSTED` |
| `SG-U-06` | FIN, duplicate FIN, CLOSE, RESET과 FIN 뒤 DATA | 방향별 EOF, idempotent terminal, protocol 위반은 해당 Pipe만 RESET |
| `SG-U-07` | terminal frame과 unknown identity의 늦은 도착 | state 재생성 없음; 다른 live Pipe 불변 |
| `SG-U-08` | 한 session에서 실패 OPEN 10,000개 뒤 낮은 ConnectionId 재사용 | Gateway는 scalar high-watermark 하나만 유지하고 terminal history를 누적하지 않으며 낮은 ID를 무시 |
| `SG-U-09` | outbound capacity를 기다리는 close·drop과 remote FIN·failure 경쟁 | terminal·abandonment lane은 current Pipe당 한 번만 신호하고 먼저 확정된 전체 종료 의미를 유지하며 방향 FIN이 뒤의 full failure를 EOF로 가리지 않음 |

### Integration

| Test ID | 시나리오 | 기대 결과 |
| --- | --- | --- |
| `SG-I-01` | valid ClientKey로 Listener 등록 | local binding `ACTIVE`; RT 없이 등록 성공 |
| `SG-I-02` | invalid ClientKey로 Listener 등록 | `UNAUTHENTICATED`; binding과 queue 없음 |
| `SG-I-03` | Connector connect 뒤 Listener queue 확인 | queue 적재 뒤에만 connect 성공과 `OBSERVED` |
| `SG-I-04` | binary payload 양방향 전송 | byte 순서와 값 보존; message boundary 가정 없음 |
| `SG-I-05` | 서로 다른 두 ListenerSession이 같은 ClientId 등록 | 두 binding 공존; 한 connect는 한 Listener에만 전달되고 fan-out 없음 |
| `SG-I-06` | Listener queue full 상태에서 추가 connect | 기존 Pipe 보존; 새 operation만 대기 또는 명시적 실패 |
| `SG-I-07` | Connector request commit 전 session 단절 | deadline 전 새 session 준비를 기다리거나 terminal 취소 |
| `SG-I-08` | Connector request commit 후 OPENED 확인 전 단절 | `MAYBE_OBSERVED`; 새 session으로 자동 replay 없음 |
| `SG-I-09` | ListenerSession 단절 | 소유 binding과 Pipe 제거; 다른 session과 binding 유지 |
| `SG-I-10` | Gateway connection 복구 | SDK가 새 session identity로 재연결하고 이미 반환된 Listener만 재등록; pending 최초 attempt와 과거 Pipe 복구 없음 |
| `SG-I-11` | concurrent close·cancel·OPENED | 외부 terminal 결과 하나; provisional state와 buffer bounded cleanup |
| `SG-I-12` | `outbound_capacity=1`에서 여러 returned Listener 재연결 | command queue 크기와 무관하게 모든 live returned Listener가 새 session에 재등록 |
| `SG-I-13` | Listener close와 queued·pending accept 경쟁 | close가 먼저 확정되면 미수락 Pipe를 반환하지 않고 sibling handle·accepted Pipe는 유지 |
| `SG-I-14` | Gateway OFFER 무응답 deadline | selected ListenerSession 전체와 소유 binding·Pipe 제거; 다른 ListenerSession 유지; 늦은 accept로 state 부활 없음 |
| `SG-I-15` | commit된 Connector OPEN terminal 응답 무응답 | `MAYBE_OBSERVED`; current ConnectorSession과 소유 Pipe 종료; 새 session identity; 자동 replay 없음 |
| `SG-I-16` | public SDK owner drop과 explicit close | clone 하나의 drop은 유지; live Pipe는 runtime을 유지; 마지막 owner drop 또는 explicit close는 transport 종료와 reconnect 중단 |
| `SG-I-17` | SDK transport write 정체 | cancellation 또는 configured deadline 안에 해당 session 종료와 bounded cleanup |
| `SG-I-18` | Listener queue full의 명시적 OFFER_REJECTED | 해당 attempt만 `RESOURCE_EXHAUSTED`; ListenerSession, sibling binding과 기존 Pipe 유지 |
| `SG-I-19` | periodic heartbeat 없는 idle session | idle 자체로 닫히지 않으며 active operation 또는 transport event에서만 failure-on-use 수렴 |
| `SG-I-20` | commit된 Listener REGISTER terminal 응답 무응답 또는 명시적 transient 실패 | 무응답은 current ListenerSession 전체 종료; 최초 ListenAttempt는 두 경우 모두 terminal 실패하고 reservation 제거; 새 session에는 이미 반환된 Listener만 새 request identity로 재등록하고 그 recovery transient 실패만 bounded backoff; silent old binding은 OFFER timeout으로 정리 |

### Server process integration

| Test ID | 시나리오 | 기대 결과 |
| --- | --- | --- |
| `SG-P-01` | 유효한 설정으로 process 부팅 뒤 protocol health check와 SIGTERM | health check 성공; SIGTERM 뒤 deadline 안에 exit 0 |
| `SG-P-02` | 알 수 없는 command, 누락·초과된 `check` 인자 | non-zero exit와 사용 가능한 오류 메시지 |
| `SG-P-03` | 잘못된 ClientKey 형식과 0인 queue capacity | socket을 열기 전에 non-zero exit와 설정 항목을 식별하는 오류 메시지 |

### Docker Compose E2E

| Test ID | 시나리오 | 기대 결과 |
| --- | --- | --- |
| `SG-E-01` | clean build와 startup | gateway healthy, listener 등록, probe exit 0 |
| `SG-E-02` | `hello relaygate` echo | 입력과 출력 byte가 동일 |
| `SG-E-03` | binary 및 frame boundary를 넘는 payload | byte 손실·중복·재정렬 없음 |
| `SG-E-04` | 여러 동시 Pipe | Pipe별 데이터 격리, configured memory bound 유지 |
| `SG-E-05` | 완료된 probe 뒤 Gateway process restart와 새 probe | Listener 재등록 뒤 새로운 connect와 echo 성공; restart 당시 live Pipe 실패는 SDK integration test가 검증 |
| `SG-E-06` | Compose 종료 | process가 deadline 안에 종료되고 volume·stale container 의존성 없음 |

## 테스트 데이터

| 이름 | 값 | 목적 |
| --- | --- | --- |
| valid ClientId | `echo.alpha` | 기본 등록과 연결 |
| shared ClientId | `echo.shared` | N:M local binding |
| valid ClientKey | Compose 전용 비밀값 `dev-echo-alpha-v1` | 등록 권한 성공 |
| invalid ClientKey | `invalid` | 인증 실패 |
| small payload | UTF-8 `hello relaygate` | 기본 echo |
| binary payload | `00 01 7f 80 fe ff` 반복 | opaque bytes 보존 |
| boundary payload | deterministic 65,537 bytes | frame/message boundary 비의존성 |
| concurrency | 32 Pipes | 기본 multiplex·격리와 bounded resource smoke |
| queue capacity | 2 | backpressure와 full queue를 작게 재현 |

ClientKey는 Compose test environment에만 두고 repository에 운영 credential을 저장하지 않는다. random payload 대신 deterministic seed를 기록해 실패를 재현할 수 있게 한다.

## CI gate

```text
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
docker compose up --build --abort-on-container-exit --exit-code-from probe
docker compose up --build -d --wait gateway listener-echo
docker compose run --rm probe
docker compose restart gateway
docker compose up -d --wait gateway
docker compose run --rm probe
docker compose down --volumes --remove-orphans
```

CI는 `probe` exit code를 최종 E2E 결과로 사용하고 실패 시 Gateway, Listener와 probe log를 보존한다. Compose test는 persistent volume, host network와 Docker socket mount를 사용하지 않는다.

## 완료 기준

1. `SG-U-*`, `SG-I-*`, `SG-P-*`, `SG-E-*`가 모두 통과한다.
2. 한 connect는 정확히 한 Listener queue와 Pipe만 만든다.
3. 실패·취소·단절 뒤 live state와 buffer는 configured bound 안에 제거된다.
4. SDK reconnect가 이전 connect, Pipe 또는 payload를 replay하지 않는다.
5. `docker compose up` 한 실행으로 clean build부터 echo assertion까지 재현할 수 있다.

## 검증 기록

2026-08-29 기준 Phase 1 구현은 다음 명령으로 통과했다.

```text
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
docker compose config
docker compose up --build --abort-on-container-exit --exit-code-from probe
docker compose down --volumes --remove-orphans
```

Rust test 66개가 통과했다: Gateway 26개, protocol 4개, SDK 31개, server 5개다. 여기에는 실패 OPEN 10,000개 뒤 scalar high-watermark만 남는지, `outbound_capacity=1`보다 많은 returned Listener가 재연결 뒤 전부 등록되는지, terminal 경쟁에서 최초 결과가 유지되고 방향 FIN이 full failure를 가리지 않는지, Listener close가 이긴 queued·pending accept가 Pipe를 반환하지 않는지, `BLOCKED` 전 admission을 마친 queued Pipe만 유지되는지 확인하는 회귀 테스트가 포함된다. `relaygate-server` process integration test는 정상 부팅·protocol health check·SIGTERM 정상 종료와 잘못된 CLI·환경 설정의 non-zero exit를 확인한다.

Compose probe는 UTF-8 payload, deterministic 65,537-byte payload와 32 concurrent Pipe echo를 확인한다. 완료된 probe 뒤 Gateway를 재시작하고 새 probe가 성공하는 것을 확인했으며, 종료 뒤 persistent volume이나 stale container를 남기지 않았다. 재시작 순간의 live Pipe failure는 Compose가 아니라 결정적인 SDK integration test가 검증한다.

`SG-I-14`~`SG-I-20`도 integration test로 검증했다. unanswered `OFFER`는 선택된 ListenerSession 전체만 닫고 sibling session을 보존한다. commit된 `OPEN`·`REGISTER` timeout은 각각 current SDK session 전체를 닫고 과거 operation을 replay하지 않는다. 모든 SDK transport send는 cancellation과 configured deadline에 종속된다. 마지막 public owner가 사라지면 supervisor가 종료되지만 live Pipe가 남아 있으면 I/O 수명은 유지된다. 명시적 `OFFER_REJECTED`, idle session 무heartbeat, Pipe read idle과 Listener recovery의 transient retry도 각각 독립적으로 검증했다.

## 다음 단계로 미루는 것

```text
Phase 1  Gateway × 1, local Pipe              <- 이 문서
Phase 2  RouteTable × 1, registration/Resolve
Phase 3  Gateway × 2, one-hop PeerTransport
Phase 4  RT shard × N, immutable directory
Phase 5  replication/failover는 별도 결정
```

- RouteTable schema, lease, restart와 stale mapping
- ShardDirectory와 generation mismatch
- Gateway 간 PeerTransport와 StreamId multiplexing
- peer identity와 one-hop failure isolation
- RT replication, failover와 online directory 변경

## repository 경계 정합성

Repository 작업 지침은 production runtime과 public SDK를 Rust workspace가 소유하고, 책임별 crate가 각자 `src/`와 `tests/`를 소유하는 경계로 갱신되었다. 이 profile의 language와 module 배치는 repository 지침과 일치한다.
