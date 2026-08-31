# TEST 004: RT 1개와 Gateway 3개 closed-loop 구현 profile

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 목적 | RT 1개 아래 Gateway 3개가 local, Entry, Owner 역할을 모두 수행하는 최소 분산 구성을 검증한다. |
| 기준 | [SPEC 004](../spec/004-route-table-contract.md), [SPEC 005](../spec/005-connection-establishment-contract.md), [SPEC 006](../spec/006-peer-relay-contract.md), [SPEC 007](../spec/007-error-and-state-model.md), [TEST 001](001-requirement-test-matrix.md) |

이 문서는 새 동작 규칙을 정의하지 않는다. SPEC 요구사항과 TEST 001 시나리오를
`RT x 1`, `Gateway x 3`, Rust SDK, Docker Compose 실행 profile로 연결한다.

현재 in-process 통합 검증은 RT 1개와 Gateway 3개에서 local 3경로, directed remote 6경로,
N:M binding 단일 선택, pair별 shared PeerTransport, 양방향 bytes, RT 단절 뒤 기존 Pipe 지속과
terminal cleanup을 결정적으로 증명한다. CI의 Docker Compose profile은 실제 process에서 GW-B
restart, RT outage와 READY-empty restart 뒤 current-state 복구를 검증한다.

RT sharding 증설, shard directory 교체와 online reconfiguration은 이번 profile의 목표가 아니다.
운영 중 후속 절차로 다룬다.

## 구현 profile

```text
Language       = Rust
RouteTable     = 1 process, memory-only, READY-empty restart
Gateway count  = 3
SDK            = public Rust SDK only
Persistence    = 없음
Runtime        = Docker Compose
Data path      = local shortcut 또는 one-hop peer relay
Internal auth  = Gateway별 static key allowlist, local/CI only
Liveness       = SDK-Gateway heartbeat, active PeerTransport heartbeat
Idle cleanup   = zero-stream PeerTransport idle retirement
```

```text
                         control plane
                   Register / Update / Resolve
                              │
                              ▼
                         ┌────────┐
                         │ RT-0   │
                         │ memory │
                         └───┬────┘
                             │ BindingSet
      ┌──────────────────────┼──────────────────────┐
      │                      │                      │
      ▼                      ▼                      ▼
  Gateway A ═══════════ Gateway B ═══════════ Gateway C
      ╚══════════════════════╩══════════════════════╝
          shared one-hop PeerTransport, pair-local

Connector SDK -> Entry Gateway -> Owner Gateway -> Listener SDK
                                  hop = 1
```

RT는 `ClientId -> current BindingSet`만 제공한다. payload, established Pipe,
application authentication, delivery acknowledgement, replay와 retry는 RT를 통과하지 않는다.

## Compose topology

| Service | 역할 | 주요 검증 |
| --- | --- | --- |
| `rt-0` | memory-only RouteTable service | `Register`, `Update`, `KeepAlive`, `Deregister`, `Resolve`, restart 뒤 READY-empty |
| `gateway-a` | Entry와 Owner Gateway | local `echo.a`, remote B/C 연결, peer pair A-B/A-C |
| `gateway-b` | Entry와 Owner Gateway | local `echo.b`, remote A/C 연결, restart 대상 |
| `gateway-c` | Entry와 Owner Gateway | local `echo.c`, remote A/B 연결, continuity 대상 |
| `listener-a` | `echo.a` Listener SDK | A local binding과 remote Owner path |
| `listener-b` | `echo.b`, `echo.shared` Listener SDK | N:M BindingSet의 한 binding |
| `listener-c` | `echo.c`, `echo.shared` Listener SDK | N:M BindingSet의 다른 binding |
| `topology-probe` | Connector SDK probe | local, remote, concurrency, half-close, byte equality |
| `continuity-ac` | long-lived A -> C Pipe probe | GW-B restart와 RT outage가 A-C Pipe에 전파되지 않음 |

`echo.shared`는 B와 C의 Listener가 같은 `ClientId`를 동시에 제공하는 N:M 검증 대상이다.
Compose probe는 public SDK만 사용한다. RT 내부 table 직접 검사는 Rust integration test가 수행한다.

## 필수 검증 경로

| ID | 경로 | 기대 결과 | TEST 001 연결 |
| --- | --- | --- | --- |
| `G3-PATH-01` | A -> A, B -> B, C -> C | local binding hit은 RT와 peer를 쓰지 않고 Pipe 하나를 연다. | `T-OPEN-01`, `T-PEER-01` |
| `G3-PATH-02` | A -> B, B -> A, A -> C, C -> A, B -> C, C -> B | remote path는 selected Owner Gateway 하나만 경유하고 bytes가 보존된다. | `T-OPEN-04`, `T-PEER-01`, `T-SDK-04`, `T-SDK-06` |
| `G3-PATH-03` | 각 local/directed remote path에서 동시 Pipe 32개 | remote Pipe마다 peer TCP를 만들지 않고 pair의 shared PeerTransport 위 RelayStream을 사용한다. | `T-PEER-02`, `T-PEER-04` |
| `G3-PATH-04` | A-B, A-C, B-C opposite-direction open | cross-dial에서도 방향별 slot 최대 하나와 충돌 없는 StreamId를 유지한다. | `T-PEER-02`, `T-PEER-03` |
| `G3-PATH-05` | `echo.shared` open | 하나의 open은 B 또는 C의 Listener 하나에만 전달되고 fan-out하지 않는다. | `T-TERM-03`, `T-OPEN-02`, `T-EDGE-12` |

이 profile은 각 경로의 UTF-8 payload와 deterministic 65,537-byte binary payload의 byte
equality·half-close를 Compose에서, 양방향 bytes와 `FIN`, `CLOSE`, `RESET` 상태 전이를 Rust
integration·regression test에서 검증한다. message boundary는 검증하지 않는다.

## Rust integration profile

Docker timing에 의존하면 불안정한 순서와 state cleanup은 Rust integration test에서 결정적으로
검증한다.

| ID | 시나리오 | 기대 결과 | TEST 001 연결 |
| --- | --- | --- | --- |
| `G3-I-RT-01` | RT service round-trip | core와 같은 `Register`, `Update`, `KeepAlive`, `Deregister`, `Resolve` 결과, bounded queue와 oversized Resolve의 명시적 `RESOURCE_EXHAUSTED` | `T-RT-01` ~ `T-RT-05` |
| `G3-I-RT-02` | READY RT에 mapping 없음 | `NOT_FOUND`; RT down의 `UNAVAILABLE`과 구분 | `T-RT-04`, `T-ERR-03` |
| `G3-I-AUTH-01` | unknown name·잘못된 key 또는 인증 뒤 다른 runtime owner·pair·direction 주장 | credential/name 실패는 `UNAUTHENTICATED`, authenticated claim mismatch는 `PERMISSION_DENIED`; RT·peer state 없음, valid connection만 fresh runtime identity에 결합 | `T-RT-01`, `T-PEER-02`, `T-EDGE-36` |
| `G3-I-REG-01` | A/B/C 동시 registration | Gateway별 registration 격리, `echo.shared`의 BindingSet 2개 | `T-REG-01`, `T-REG-05`, `T-EDGE-01` |
| `G3-I-REG-02` | RT restart 뒤 current snapshot 재등록 | 과거 mutation replay 없이 새 lease와 current snapshot으로만 복구 | `T-REG-06`, `T-RT-05`, `T-EDGE-06` |
| `G3-I-REG-03` | `echo.shared`의 B registration 제거 | RT BindingSet이 C 하나로 수렴하고 application의 새 open은 C에 정확히 하나의 Pipe를 만든다. 제거된 B로 same-attempt fallback하지 않는다. | `T-REG-04`, `T-RT-04`, `T-OPEN-03` |
| `G3-I-OPEN-01` | Resolve 뒤 selected binding 제거 | Owner revalidation 실패, `UNAVAILABLE`, `NOT_OBSERVED`, same-attempt fallback 없음 | `T-OPEN-03`, `T-EDGE-07` |
| `G3-I-OPEN-02` | peer OPEN commit 전 실패 | Pipe 없음, `NOT_OBSERVED`, candidate cleanup | `T-OPEN-17`, `T-PEER-09`, `T-ERR-06` |
| `G3-I-OPEN-03` | peer OPEN commit 뒤 terminal 결과 유실, local OPENING 중 RESET 또는 stream-local protocol violation | `FAILED(code, MAYBE_OBSERVED)`, current attempt cleanup, same-attempt replay/reroute 없음 | `T-OPEN-17`, `T-PEER-05`, `T-PEER-09`, `T-ERR-06` |
| `G3-I-OPEN-04` | OPENING remote attempt cancel과 OPENED 경쟁 | pre-commit state 제거, post-commit `RESET(CANCELLED)`, late result no-op, 별도 peer CANCEL 없음 | `T-OPEN-17`, `T-EDGE-34` |
| `G3-I-OPEN-05` | local hit과 같은 ClientId의 반복 remote open | local hit은 RT·peer 호출 0회, local miss는 attempt마다 Resolve 정확히 1회와 Owner one hop. Resolve 결과 cache·fallback·reroute·replay 없음 | `T-OPEN-01`, `T-OPEN-06`, `T-OPEN-10`, `T-PEER-01` |
| `G3-I-PEER-00` | 한 endpoint의 concurrent StreamId 할당과 OPEN commit | counter 순서와 writer commit 순서 일치, 실패한 counter 비재사용 | `T-PEER-03`, `T-EDGE-35` |
| `G3-I-PEER-01` | A-B transport loss | A-B stream과 Pipe만 terminal, A-C/B-C state 불변 | `T-PEER-06`, `T-EDGE-14` |
| `G3-I-PEER-02` | 한 RelayStream reset | sibling stream과 PeerTransport 유지 | `T-PEER-05`, `T-ERR-05` |
| `G3-I-PEER-03` | 같은 transport의 ConnectorSession S1 종료, RESET writer commit 성공·실패 | 성공 시 S1 current stream만 닫고 sibling 유지, 실패 시 transport close로 해당 transport stream 전체 cleanup. 반대 slot·RT mapping·binding 유지 | `T-OPEN-09`, `T-PEER-08`, `T-STATE-CONNECTOR`, `T-EDGE-14` |
| `G3-I-PEER-04` | capacity 1 PeerEvent queue 포화와 receiver 종료 | 실행 중에는 cyclic wait 없이 각각 `RESOURCE_EXHAUSTED`/`UNAVAILABLE` fail-closed와 count 0 수렴. 정상 shutdown과 경쟁한 Full/Closed는 새 장애가 아님 | `T-PEER-04`, `T-STATE-TRANSPORT` |
| `G3-I-PEER-05` | active PeerTransport heartbeat timeout | 해당 transport의 stream과 Pipe만 terminal cleanup하고 반대 방향 transport, 다른 pair, RT mapping과 Listener binding을 유지 | `T-PEER-10`, `T-STATE-TRANSPORT`, `T-EDGE-38` |
| `G3-I-PEER-06` | zero-stream PeerTransport idle retirement와 재사용 경쟁 | stream 수 0에서는 keepalive를 보내지 않고 retirement timeout 뒤 정상 종료한다. timeout 전 새 stream이 재사용하면 timer가 취소된다. | `T-PEER-10`, `T-STATE-TRANSPORT`, `T-EDGE-38` |
| `G3-I-STATE-01` | unmapped remote open 실패 뒤 mapped remote open 성공·종료를 100회 반복 | 매 cycle은 새 application operation이며 마지막 snapshot의 pending offer, Pipe, remote attempt, connecting transport와 stream이 baseline으로 수렴한다. ConnectionId·StreamId는 재사용하지 않고 scalar high-watermark 외에 terminal history를 누적하지 않는다. | `T-TERM-07`, `T-OPEN-09`, `T-STATE-PAIR` |

integration test는 RT table, Gateway snapshot, peer pool snapshot을 직접 관찰할 수 있다. public
SDK에는 RT, Gateway, peer 내부 타입을 노출하지 않는다.

## Docker Compose CI 순서

Compose는 실제 container build, DNS, process startup, healthcheck, TCP disconnect와 restart를
검증한다. 정밀한 frame ordering은 integration test 책임이다.

```text
1. docker compose up --build -d --wait rt-0 gateway-a gateway-b gateway-c listener-a listener-b listener-c continuity-ac
2. docker compose run --rm --no-deps topology-probe relaygate-echo-probe matrix
3. docker compose exec -T continuity-ac relaygate-echo-probe continuity-check
4. docker compose restart gateway-b
5. docker compose up -d --wait --no-deps gateway-b
6. docker compose exec -T continuity-ac relaygate-echo-probe continuity-check
7. docker compose run --rm --no-deps topology-probe relaygate-echo-probe wait-client echo.b
8. docker compose run --rm --no-deps topology-probe relaygate-echo-probe matrix
9. docker compose stop rt-0
10. docker compose run --rm --no-deps topology-probe relaygate-echo-probe expect-rt-unavailable
11. docker compose exec -T continuity-ac relaygate-echo-probe continuity-check
12. docker compose up -d rt-0
13. docker compose run --rm --no-deps topology-probe relaygate-echo-probe wait-client echo.a
14. docker compose run --rm --no-deps topology-probe relaygate-echo-probe wait-client echo.b
15. docker compose run --rm --no-deps topology-probe relaygate-echo-probe wait-client echo.c
16. docker compose run --rm --no-deps topology-probe relaygate-echo-probe matrix
17. docker compose exec -T continuity-ac relaygate-echo-probe continuity-check
18. docker compose down --volumes --remove-orphans
```

`matrix`는 local 3경로, remote 6방향, 경로별 65,537-byte payload, path별 32 concurrent Pipe,
cross-dial과 public SDK를 통한 `echo.shared` 도달을 검증한다. `echo.shared`의 BindingSet 2개와
exact-one 선택·no fan-out은 Rust integration snapshot이 결정적으로 검증한다. `continuity-ac`는
A에서 C로 열린 기존 Pipe가 GW-B restart와 RT outage 중에도 freshness deadline 안에서 계속
왕복하는지 확인한다.

`expect-rt-unavailable`에서는 local 3경로는 계속 성공하고 신규 remote 6방향만
`UNAVAILABLE` terminal 결과인지 확인한다. RT 재시작 뒤 `matrix`의 bounded retry는 A/B/C의
current snapshot publication을 기다리되 이전 attempt를 replay하지 않고 매번 새 `open`으로
확인한다. configured deadline 안에 수렴하지 않으면 실패한다.

`wait-client`는 full matrix 전에 특정 `ClientId`를 모든 Gateway entry에서 한 번씩 열어 RT
publication과 Owner Gateway 도달성이 수렴했는지 확인한다. 이 단계는 새 semantic을 정의하지
않고, full matrix 실패 원인이 재시작 직후 publication 수렴 지연인지 실제 relay 경로 장애인지
분리하기 위한 Compose 검증 harness다.

READY-empty의 `NOT_FOUND`는 Compose race로 검증하지 않는다. RT service integration test에서
deterministic하게 검증한다.

## Acceptance criteria

| ID | 계층 | 통과 조건 |
| --- | --- | --- |
| `AC-G3-RT-01` | integration | RT network operation이 core state와 같은 결과를 내고 restart 뒤 READY-empty가 된다. |
| `AC-G3-AUTH-01` | integration | local/CI key allowlist가 RT와 peer connection identity를 fresh GatewayId에 결합하고 impersonation이 state를 만들지 않는다. |
| `AC-G3-REG-01` | integration | A/B/C registration과 `echo.shared` BindingSet이 Gateway별로 격리되고 한 registration 제거 뒤 남은 binding으로 새 open이 성공한다. |
| `AC-G3-OPEN-01` | integration/Compose | local 3경로와 remote 6방향이 모두 Pipe 하나로 성공한다. |
| `AC-G3-OPEN-02` | integration | stale mapping은 `UNAVAILABLE`, `NOT_OBSERVED`로 끝나고 같은 attempt fallback이 없다. |
| `AC-G3-PEER-01` | integration/Compose | peer pair별 shared transport가 재사용되고 RelayStream이 Pipe 단위로 분리된다. |
| `AC-G3-PEER-02` | integration | 한 peer pair 장애가 다른 pair, RT mapping, sibling stream으로 전파되지 않는다. |
| `AC-G3-PEER-03` | integration | concurrent StreamId 할당·commit 순서와 ConnectorSession/cancel의 RESET cleanup이 deterministic하다. RESET writer commit 실패는 해당 transport close로 수렴한다. |
| `AC-G3-PEER-04` | integration | active PeerTransport heartbeat timeout과 zero-stream idle retirement가 서로 다른 cleanup scope로 닫힌다. |
| `AC-G3-PIPE-01` | integration/Compose | local/remote `DATA`, `FIN`, `CLOSE`, `RESET` 의미가 동일하다. |
| `AC-G3-FAIL-01` | Compose | GW-B restart 중 A-C established Pipe가 유지되고 B 재등록 뒤 matrix가 다시 성공한다. |
| `AC-G3-FAIL-02` | Compose | RT outage 중 established Pipe는 유지되고 신규 remote open은 `UNAVAILABLE`로 terminal 실패한다. |
| `AC-G3-FAIL-03` | integration/Compose | RT restart 뒤 Gateway가 current local snapshot으로만 재등록하여 신규 open이 복구된다. |
| `AC-G3-STATE-01` | integration | 반복 실패 뒤 transient count와 buffer가 baseline으로 돌아가며 과거 operation 수에 비례한 누적 state가 없다. |
| `AC-G3-SDK-01` | regression | public Rust SDK 사용 패턴과 single-Gateway profile의 기존 보장이 유지된다. |

## 제외 범위

이 profile은 다음을 완료 조건으로 삼지 않는다.

| 제외 | 이유 |
| --- | --- |
| RT HA, replica, consensus | 이번 profile은 RT process 1개의 memory-only 동작과 restart 후 재구성만 검증한다. |
| RT persistence | current state만 재등록한다는 SPEC 004/007의 범위를 유지한다. |
| RT shard 2개 이상 E2E | sharding authority는 core/integration에서 검증하고 process-level multi-shard는 후속 profile로 둔다. |
| RT sharding 운영 증설 | 이번 profile은 RT 1개 운영 준비를 기준으로 하며 shard 증설은 운영 중 후속 절차로 정한다. |
| Kubernetes, Helm, mTLS | 배포와 production identity adapter는 runtime profile 밖이다. |
| delivery acknowledgement, replay, resume | RelayGate는 opaque Pipe만 제공하고 payload 의미와 업무 retry를 소유하지 않는다. |
| selection 품질, load balancing | 한 attempt가 후보 하나를 선택한다는 정확성만 검증한다. |

## 완료 기준

1. `AC-G3-*`가 모두 통과한다.
2. `TEST 001`의 RT, registration, open, peer, pipe, error/state 핵심 시나리오가 이 profile의
   integration 또는 Compose test에 연결된다.
3. 모든 실패 attempt는 하나의 terminal result로 끝나고 usable Pipe를 남기지 않는다.
4. established Pipe는 RT outage, RT restart와 무관하게 RT를 필요로 하지 않는다.
5. 상태량은 current live session, binding, lease, open attempt, Pipe, PeerTransport와
   RelayStream/OpenIdentity 수에 비례하며 Owner에 remote ConnectorSession history를 남기지 않는다.
6. 실패 로그나 snapshot에 `ClientKey`, `InternalGatewayKey`와 payload가 남지 않는다.
