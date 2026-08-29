# SPEC 003: Gateway local registry와 Listener publication 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 002](../adr/002-application-protocol-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 005](../adr/005-soft-state-registration-lifecycle.md) |
| 용어 | [SPEC 001](001-terminology-and-object-model.md) |
| RT 계약 | [SPEC 004](004-route-table-contract.md) |
| 상태와 오류 | [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 Gateway가 `ListenerSession`과 `ListenerBinding`을 local truth로 관리하고, 현재 binding을 session-shard snapshot으로 RT에 게시하는 계약을 정의한다. binding 선택과 Pipe 수립은 이 문서의 범위가 아니다.

## Gateway-local 모델

```text
ListenerSession
  └── RegistrationSet
        ├── ClientId A ── BindingId A
        ├── ClientId B ── BindingId B
        └── ClientId C ── BindingId C

LocalRegistry
  ├── BindingId  -> ListenerBinding
  ├── ClientId   -> Set<BindingId>
  └── SessionId  -> Set<BindingId>
```

하나의 `ListenerBinding` record를 여러 index가 참조한다. `ClientId` index는 local connect 후보 조회에, `ListenerSessionId` index는 session 종료 시 일괄 cleanup에 사용한다.

```text
ClientId A
  ├── Session 1 / Binding 1
  ├── Session 2 / Binding 2
  └── Session 3 / Binding 3
```

같은 Listener handle이 재전송하거나 desired state를 다시 선언하여 같은 session에서 같은 `ClientId`의 현재 live 등록이 반복되면 같은 `BindingId`를 가리킨다. 이는 한 SDK runtime에서 같은 `ClientId`의 Listener handle을 여러 개 만드는 것을 허용하지 않는다. 제거된 binding은 terminal이며 같은 `ClientId`를 다시 등록하면 새 `BindingId`를 만든다.

## Publication 모델

```text
PublicationManager

ShardDirectoryGeneration = process 시작 시 고정

(ListenerSessionId, ShardId)
  └── PublicationState
        ├── current LeaseId 0..1
        ├── last PublicationRevision
        ├── ABSENT | SYNCED | UNSYNCED
        └── current BindingId set
```

하나의 session이 여러 `ClientId`를 등록하면 deterministic hash에 따라 여러 shard snapshot으로 나뉠 수 있다.

```text
Session 1
  ├── Shard 0 snapshot = {Binding A, Binding C}
  └── Shard 1 snapshot = {Binding B}
```

각 snapshot은 해당 session-shard가 현재 소유한 complete set이다. Gateway는 과거 mutation을 replay하지 않는다.

## 등록 요구사항

| ID | 요구사항 |
| --- | --- |
| `REG-001` | `ListenerSession`은 Listener SDK와 Gateway 사이의 현재 live session이며, Gateway가 소유하는 local truth다. |
| `REG-002` | 하나의 `ListenerSession`은 `0..N`개의 `ClientId`를 `RegistrationSet`으로 등록할 수 있다. |
| `REG-003` | 하나의 `ClientId`는 서로 다른 `0..N`개의 live `ListenerSession`에 동시에 등록될 수 있다. 다른 session의 기존 등록은 충돌이 아니다. |
| `REG-004` | 하나의 existing Listener handle이 같은 session의 같은 current `ClientId` 등록을 재전송하거나 재선언하면 동일 `BindingId`를 가리키는 idempotent 결과여야 한다. 이는 여러 handle 생성을 의미하지 않으며, 제거 후 재등록은 새 `BindingId`를 만들어야 한다. |
| `REG-005` | Gateway는 등록 요청마다 `ClientId`와 `ClientKey`의 등록 권한을 확인해야 한다. 검증 실패 시 local binding과 RT projection을 만들지 않는다. |
| `REG-006` | `ClientKey`는 해당 `ClientId`의 등록 권한만 증명한다. peer 인증·인가, payload 의미 또는 application 처리 권한을 증명하지 않는다. |
| `REG-007` | Gateway는 권한이 확인되면 재사용하지 않은 `BindingId`의 local binding을 `ACTIVE`로 atomic하게 설치하고 해당 publication scope를 `UNSYNCED`로 만들어 authority shard의 next snapshot에 projection을 포함해야 한다. |
| `REG-008` | local binding 설치 뒤 최초 등록 성공을 보고하고 신규 Pipe의 local 목적지로 사용할 수 있다. RT snapshot 수락은 scope를 `SYNCED`로 만들지만 local binding의 활성 여부를 결정하지 않으며, 등록 성공은 remote discovery 완료를 보장하지 않는다. |

## Publication과 cleanup 요구사항

| ID | 요구사항 |
| --- | --- |
| `REG-009` | 명시적 등록 해제는 해당 binding을 local registry에서 즉시 제거하고 다음 current snapshot에서 제외한다. 같은 `ClientId`의 다른 binding에는 영향을 주지 않는다. |
| `REG-010` | publication scope는 `(GatewayId, ListenerSessionId, ShardId)`이고 scope마다 current lease는 최대 하나다. Gateway는 scope를 처음 게시하거나 ended lease를 대체할 때 재사용하지 않은 새 `LeaseId`를 만들며 local binding마다 독립 lease를 만들지 않는다. lease를 교체해도 scope revision은 다시 시작하지 않는다. |
| `REG-011` | 한 session이 여러 shard에 걸치면 Gateway는 shard마다 독립된 `LeaseId`, revision과 sync 상태를 관리해야 한다. 한 shard의 실패는 다른 shard publication이나 local binding을 변경하지 않는다. |
| `REG-012` | Gateway는 한 publication scope의 snapshot update를 직렬화하고 current set 또는 lease incarnation이 바뀔 때 scope의 revision을 증가시켜야 한다. current lease가 없어져도 revision watermark는 `ListenerSession` 종료 전까지 유지한다. |
| `REG-013` | `ListenerSession` 종료 시 Gateway는 해당 session의 local binding과 publication state를 모두 제거하고 각 current lease를 best-effort로 release해야 한다. release 유실이나 늦은 publication이 있어도 마지막으로 수락된 publish 또는 refresh 뒤 더 이상 operation이 도착하지 않으면 lease expiry가 projection을 제거해야 한다. |
| `REG-014` | 등록 권한 상실이 관찰되면 영향받은 local binding을 제거하고 해당 Listener handle을 `BLOCKED`로 만들어 신규 Pipe admission을 중단해야 한다. 변경된 current snapshot은 영향받은 projection을 제외해야 한다. binding 제거 전에 Listener queue에 적재되었거나 application에 accept된 Pipe는 `ClientKey` 폐기만으로 닫지 않으며 자신의 기존 Pipe lifecycle을 따른다. |
| `REG-015` | RT 연결 상실, stale lease 또는 publication 실패가 발생하면 해당 scope를 `UNSYNCED`로 만들되 `ACTIVE` local binding과 ListenerSession을 제거해서는 안 된다. transient failure만 같은 process generation으로 자동 재시도할 수 있다. |
| `REG-016` | compatible RT를 다시 사용할 수 있으면 Gateway는 과거 mutation이 아니라 현재 제거되지 않은 `ACTIVE` local binding으로 snapshot을 다시 만들고 게시해야 한다. 성공하면 local binding을 바꾸지 않고 해당 scope만 `SYNCED`로 만든다. |
| `REG-017` | scope가 `UNSYNCED`인 동안 같은 Gateway의 local shortcut은 사용할 수 있지만 remote discovery는 RT에 새 snapshot이 수락되기 전까지 보장하지 않는다. |
| `REG-018` | Gateway는 process 시작 시 고정한 `ShardDirectoryGeneration`과 shard directory, local registry 및 자신의 publication 상태만 유지해야 하며 RT 전체 table 또는 remote `Resolve` 결과를 publication 복구의 source로 사용해서는 안 된다. |
| `REG-019` | Gateway는 모든 publication operation에 자신의 `ShardDirectoryGeneration`을 보내야 한다. RT가 generation mismatch를 `FAILED_PRECONDITION`으로 거절하면 scope를 `UNSYNCED`로 유지하고 local binding을 제거하지 않아야 하며, 같은 process configuration으로 자동 재시도해서는 안 된다. |
| `REG-020` | Gateway는 종료한 scope의 `LeaseId`로 새 publication operation을 만들거나 재사용해서는 안 된다. 이미 전송된 늦은 `PublishCurrent`가 release·expiry 뒤 수락되고 그 뒤 일치하는 늦은 `Refresh`까지 수락되어도 local `ListenerSession`이나 binding을 다시 만들지 않으며, stale projection은 Owner Gateway의 binding 재검증과 lease expiry로 격리해야 한다. |
| `REG-021` | RT transport identity 검증 실패 또는 authenticated Gateway identity와 `PublicationScope.GatewayId` mismatch는 해당 scope를 `UNSYNCED`로 유지하고 local binding을 제거하지 않아야 한다. Gateway는 deployment trust 또는 authorization configuration이 바뀌기 전 같은 실패를 자동 재시도해서는 안 된다. |

## 관찰 가능한 실패

| 상황 | Gateway-local 결과 | RT 결과 |
| --- | --- | --- |
| `ClientKey` 검증 실패 | binding 없음 | snapshot 변경 없음 |
| 최초 snapshot publish 불가 | binding `ACTIVE`, scope `UNSYNCED`, local 등록 성공 | projection 없음 또는 미확정, remote discovery 보장 없음 |
| 활성화 뒤 publication 상실 | binding `ACTIVE`, scope `UNSYNCED` | remote discovery 일시 중단 가능 |
| ShardDirectory generation mismatch | binding `ACTIVE`, scope `UNSYNCED`, process restart 필요 | operation 거절, state 불변 |
| RT transport identity 또는 GatewayId 검증 실패 | binding `ACTIVE`, scope `UNSYNCED`, trust configuration 변경 필요 | operation 거절, state 불변 |
| 명시적 binding 해제 publish 실패 | local binding 즉시 제거 | stale projection은 owner 재검증으로 거절되고 replace 또는 expiry로 제거 |
| ListenerSession 단절 | 해당 session의 local binding 제거 | release 또는 expiry로 해당 scope projection만 제거 |
| Release 또는 expiry 뒤 늦은 Publish | local session과 binding은 다시 생기지 않음 | projection이 일시적으로 다시 보일 수 있으나 Owner가 거절하고 마지막 늦은 갱신 뒤 expiry로 제거 |
| 등록 권한 폐기 | 영향받은 handle `BLOCKED`, 신규 admission 중단, 기존 queued·accepted Pipe 유지 | 새 snapshot에서 제외, 실패 시 expiry 보조 |

## 불변식

1. 등록 관계는 `ClientId N:M ListenerSession`이고, 성공한 개별 Pipe는 `Connector 1:1 Listener`다.
2. `LocalRegistry`의 `BindingId`, `ClientId`, `ListenerSessionId` index는 같은 live binding set을 나타내야 한다.
3. RT publication의 replace, release, expiry와 restart는 Gateway-local binding을 직접 제거하지 않는다.
4. 한 session 또는 shard의 failure는 다른 session이나 shard의 local binding과 publication을 제거하지 않는다.
5. Gateway는 removed binding과 ended lease identity를 새 operation에 재사용하지 않으며, live session의 scope revision은 lease 공백 뒤에도 뒤로 가지 않는다. 이미 전송된 늦은 operation의 RT 재관찰은 새 identity 발급이 아니다.
6. `ClientKey`, application payload와 remote RT 전체 mapping은 Gateway publication state에 저장하지 않는다.
7. Gateway process의 `ShardDirectoryGeneration`과 shard directory는 process 수명 동안 바뀌지 않는다.
8. RT의 stale projection은 local binding truth를 되살리지 못하고 `OPEN` 성공을 만들기 전에 Owner Gateway에서 거절되어야 한다.
9. `ClientKey` 상태는 신규 binding과 Pipe admission에만 적용하며 admission을 마친 Pipe의 application 권한이나 종료를 소급해 결정하지 않는다.
10. RT publication의 claimed `GatewayId`는 authenticated transport identity와 일치해야 하며 identity 또는 authorization 실패는 local binding truth를 제거하지 않는다.
11. local binding의 활성 여부와 RT publication sync 상태는 독립적이다. `UNSYNCED`는 local admission을 막지 않으며 remote discovery만 약화시킨다.

## 이 계약에서 정하지 않는 것

- public Listener registration API signature
- online 또는 rolling shard directory 변경
- lease lifetime, refresh interval과 retry backoff 수치
- wire format과 transport
