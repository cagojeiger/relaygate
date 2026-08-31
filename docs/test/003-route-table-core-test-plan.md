# TEST 003: RouteTable core 구현 profile

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 실행 | RouteTable shard core 구현 |
| 기준 | [SPEC 004](../spec/004-route-table-contract.md), [TEST 001](001-requirement-test-matrix.md) |

## 범위

```text
ShardDirectory JSON bytes
        │ load + generation + authority
        ▼
RouteTableShard
  ├── RouteIndex
  ├── RegistrationIndex
  └── ExpiryIndex
        │
        └── Register / Update / KeepAlive / Deregister / Resolve
```

이 단계는 버리지 않는 shard-local 상태 머신을 구현한다. Gateway registration manager,
RT network service, peer relay, replication, persistence와 rolling directory 변경은 포함하지
않는다.

## 검증 경계

| 대상 | 반드시 검증 | 이 단계에서 주장하지 않음 |
| --- | --- | --- |
| directory | exact bytes generation, JSON validation, deterministic authority | online 변경, service discovery |
| lease | 발급, duplicate Register, revision, keepalive, deregister, expiry | Gateway retry/backoff |
| mapping | complete snapshot replace, N:M Resolve, registration 격리 | binding selection, reachability |
| memory | active lease당 expiry record 하나, tombstone/history 없음 | process RSS 수치 |
| failure | generation·scope·caller mismatch와 stale lease가 state 불변 | network timeout과 RPC error mapping |

서로 다른 invalid condition이 한 요청에 함께 있어도 SPEC 004의 validation order를 따른다.
특히 인증·caller mismatch는 generation, lease와 mapping 존재 여부보다 먼저 실패하여 내부
state를 결과로 누출하거나 expiry를 유발하지 않아야 한다. caller·generation·scope 검증을
통과한 뒤에는 monotonic time에 도달한 lease를 먼저 만료시키며, 이후 stale lease·revision
오류는 그 expiry 외의 state를 변경하지 않는다.

## 결정적 시간

core operation과 expiry driver는 RT process가 관찰한 monotonic instant를 받는다. 테스트는
wall-clock sleep을 사용하지 않고 instant를 전진시켜 다음 순서를 고정한다.

```text
Register(L1) ── Update(1) ── KeepAlive ── expiry
       │                                │
       └──── late L1 operation ─────────┘  state 생성 금지
```

## TEST 001 대응

| Test ID | executable core 범위 |
| --- | --- |
| `T-RT-01` | directory, generation, authority, caller와 shard scope |
| `T-RT-02` | Register와 atomic snapshot revision |
| `T-RT-03` | KeepAlive, Deregister, expiry와 stale lease |
| `T-RT-04` | live BindingSet Resolve와 READY-empty `NOT_FOUND` |
| `T-RT-05` | 새 instance의 READY-empty state와 current snapshot 재구성 |
| `T-RT-07` | forward/reverse index 격리와 bounded state count |

`T-RT-05`의 process-down `UNAVAILABLE`, `T-RT-06`의 existing Pipe 독립성, `T-RT-07`의
Gateway lookup behavior는 RT service와 Gateway integration 단계에서 완성한다.
`T-RT-01`의 unauthenticated channel rejection도 service adapter가 소유하며, core는 adapter가
검증한 caller와 mutation owner의 불일치를 `PERMISSION_DENIED`로 검증한다.

## 완료 조건

1. 동일 artifact bytes와 `ClientId` bytes는 항상 동일 generation과 shard를 만든다.
2. 실패한 operation 전후의 lease, deadline, revision, mapping과 expiry count가 같다.
3. 한 registration의 replace·deregister·expiry가 다른 registration mapping을 바꾸지 않는다.
4. current state count는 active lease와 current mapping 수에만 비례한다.
5. restart는 새 empty core instance로 검증하며 과거 state를 복원하지 않는다.
