# SPEC 004: RouteTable schema와 interface 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 005](../adr/005-soft-state-registration-lifecycle.md) |
| 용어 | [SPEC 001](001-terminology-and-object-model.md) |
| 상태와 오류 | [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 logical RT shard의 memory-only schema와 `PublishCurrent`, `Refresh`, `Release`, `Resolve` 동작을 정의한다. binding 선택, Pipe 수립, RT replication과 failover는 범위가 아니다.

## 논리 interface

```text
RouteTable {
  PublishCurrent(PublicationSnapshot) -> PublicationAck
  Refresh(ShardDirectoryGeneration, PublicationScope, LeaseId) -> PublicationAck
  Release(ShardDirectoryGeneration, PublicationScope, LeaseId) -> success
  Resolve(ShardDirectoryGeneration, ClientId)                  -> BindingSet
}
```

`PublicationSnapshot`은 `ShardDirectoryGeneration`을 포함한다. 따라서 모든 RouteTable operation은 caller와 RT process의 generation을 명시적으로 비교할 수 있다.

`PublicationAck`는 수락된 `LeaseId`, `PublicationRevision`과 Gateway가 다음 refresh를 예약할 수 있는 lease 정보를 나타낸다. RT의 절대 clock 값이나 구현 timer identity를 외부 identity로 사용하지 않는다.

## Shard directory와 authority

```text
ShardDirectory artifact bytes
  ├── format version
  ├── authority hash rule = sha256-modulo-v1
  └── ordered shard records = [(ShardId, stable logical RT endpoint), ...]

ShardDirectoryGeneration = SHA-256(exact artifact bytes)

digest = SHA-256(exact UTF-8 bytes of ClientId)
index  = unsigned-big-endian(digest[0..8]) mod shard_count
Authority(ClientId) = ordered shard records[index]
```

artifact에는 운영자가 입력하는 generation field를 두지 않는다. loader가 exact bytes에서 generation을 계산한 뒤 parsed directory와 함께 process 수명 동안 고정한다. 공백이나 record 순서를 포함해 artifact bytes가 달라지면 generation을 다시 계산하므로 의미가 같은 configuration도 bytes가 다르면 호환되지 않을 수 있다. 이는 canonicalization이나 암묵적 configuration 합치기보다 동일 artifact 배포를 요구하는 선택이다.

generation 비교는 SHA-256의 collision resistance를 전제로 한다. 의도적인 hash collision 방어는 이 계약의 범위가 아니다.

`ClientId`는 정규화, case folding 또는 locale 변환 없이 protocol이 받은 exact UTF-8 bytes로 hash한다. shard list는 artifact에 선언된 순서를 사용하며 비어 있거나 중복 `ShardId`가 있거나 stable logical endpoint가 정확히 하나가 아닌 record는 invalid configuration이다. 하나의 endpoint가 내부적으로 어떤 process 주소나 service discovery를 사용하는지는 deployment detail이지만, 서로 독립적으로 쓰이는 여러 RT instance를 한 record에 넣어서는 안 된다.

## shard-local schema

```text
Authority(ShardDirectoryGeneration, ClientId) = exactly 1 logical shard

ConfiguredGeneration
  -> process 수명 동안 불변

RouteIndex
  ClientId
    -> Map<(GatewayId, ListenerSessionId, BindingId), BindingProjection>

PublicationIndex
  PublicationScope
    -> {
         LeaseId
         PublicationRevision
         Deadline
         ProjectionIdentity Set
       }

ExpiryIndex
  (PublicationScope, LeaseId) -> Deadline
```

`RouteIndex`는 `Resolve(ShardDirectoryGeneration, ClientId)`를 위한 forward index다. `PublicationIndex`는 snapshot replace, release와 expiry가 해당 scope의 projection만 제거하기 위한 reverse index다. 하나의 canonical projection record를 두 index가 참조할 수 있지만 외부에서 관찰되는 의미는 동일해야 한다.

`PublicationIndex`와 `ExpiryIndex`에는 current lease만 둔다. release, expiry 또는 restart 뒤 과거 `LeaseId`, revision과 projection에 대한 tombstone이나 high-watermark를 보관하지 않는다.

`ConnectorSessionId`와 `ConnectionId`는 연결 수립 상태이며 RT schema와 interface에 포함되지 않는다.

```text
ClientId A
  ├── Projection 1 -> GW-1
  └── Projection 2 -> GW-2

Lease 1
  ├── Projection 1
  └── Projection 3
```

## shard 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-001` | 같은 `ShardDirectoryGeneration`에서 같은 `ClientId`는 항상 같은 하나의 logical shard로 결정되어야 한다. |
| `RT-002` | `PublishCurrent` snapshot의 모든 `ClientId`와 `Resolve(ShardDirectoryGeneration, ClientId)`는 대상 `ClientId`의 authority shard에서 처리되어야 한다. |
| `RT-003` | 각 shard는 자신이 authority인 `ClientId` projection만 저장한다. 한 `ListenerSession`이 여러 shard에 걸치면 각 shard는 자기 부분집합만 저장한다. |
| `RT-004` | shard partition은 mapping 용량을 분산한다. logical shard의 replica 수와 failover 보장은 이 계약에 포함되지 않는다. |
| `RT-005` | RT process는 시작 시 하나의 `ShardDirectoryGeneration`을 고정해야 한다. 모든 operation은 같은 generation일 때만 처리하며 mismatch는 `FAILED_PRECONDITION`으로 끝내고 projection, lease, revision과 deadline을 읽기 결과로 반환하거나 변경해서는 안 된다. |
| `RT-006` | `ShardDirectoryGeneration`은 generation field가 없는 exact directory artifact bytes의 SHA-256이어야 한다. loader는 artifact의 format version, `sha256-modulo-v1` 규칙, ordered shard records, unique `ShardId`와 각 record의 exactly-one stable logical endpoint를 검증해야 하며 process 시작 뒤 directory와 generation을 바꾸어서는 안 된다. |
| `RT-007` | `sha256-modulo-v1` authority는 `ClientId`의 exact UTF-8 bytes를 SHA-256으로 계산하고 첫 8 bytes를 unsigned big-endian integer로 해석한 뒤 ordered shard count로 modulo하여 하나의 shard를 선택해야 한다. 빈 shard list는 invalid configuration이다. |
| `RT-008` | 모든 RT operation은 배포 환경이 identity와 integrity를 보장하는 internal channel에서만 처리해야 한다. `PublishCurrent`, `Refresh`, `Release`의 authenticated Gateway identity는 `PublicationScope.GatewayId`와 일치해야 하며 message가 주장하는 `GatewayId`만 신뢰해서는 안 된다. 인증·일치 검증 실패는 state를 읽거나 변경하지 않고 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`로 끝나야 한다. |

## PublishCurrent 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-010` | `PublishCurrent`는 하나의 `PublicationScope`와 그 shard에 속하는 하나 이상의 complete current projection을 포함해야 한다. scope 또는 authority가 섞인 snapshot은 `INVALID_ARGUMENT`이다. |
| `RT-011` | scope에 current lease가 없으면 RT는 request가 제공한 새 `LeaseId`의 snapshot을 atomic하게 수락하고 해당 projection을 `RouteIndex`에 추가해야 한다. |
| `RT-012` | current lease와 같은 `LeaseId`의 더 높은 revision은 기존 scope projection set을 atomic하게 새 complete set으로 교체해야 한다. 생략된 projection은 제거한다. |
| `RT-013` | 같은 `LeaseId`와 같은 revision의 동일 snapshot 반복은 idempotent success여야 한다. 같은 revision의 다른 내용은 `FAILED_PRECONDITION`이며 state를 바꾸지 않는다. |
| `RT-014` | current revision보다 낮은 revision은 `LeaseId`와 관계없이 `FAILED_PRECONDITION`이며 current projection, deadline과 revision을 바꾸지 않는다. |
| `RT-015` | current scope에 다른 `LeaseId`와 더 높은 revision이 도착하면 RT는 이를 새 lease incarnation으로 atomic하게 교체해야 한다. 같거나 낮은 revision의 다른 `LeaseId`는 stale 또는 competing lease로 거절한다. |
| `RT-016` | snapshot replace는 같은 scope의 projection만 변경하고 같은 `ClientId`의 다른 scope projection이나 다른 shard state를 변경해서는 안 된다. |
| `RT-017` | scope에 current lease가 없으면 RT는 valid `PublishCurrent`가 새 publication인지 release·expiry 뒤 늦게 도착한 publication인지 구분하지 않고 `RT-011`대로 수락해야 한다. RT는 이를 구분하기 위한 tombstone, 종료 lease history 또는 revision high-watermark를 저장해서는 안 된다. |

scope에 더 이상 binding이 없으면 Gateway는 빈 snapshot을 유지하지 않고 `Release`한다.

## Refresh, Release와 Expire 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-020` | `Refresh`는 요청한 scope의 current `LeaseId`가 일치할 때만 deadline을 연장해야 하며 projection set이나 revision을 변경해서는 안 된다. |
| `RT-021` | 없는, expired 또는 stale `LeaseId`의 `Refresh`는 lease나 projection을 만들지 않고 `FAILED_PRECONDITION`으로 끝나야 한다. |
| `RT-022` | `Release`는 일치하는 current lease와 그 projection subset만 제거해야 한다. 이미 없는 같은 lease의 반복 release는 idempotent success이고, 다른 current `LeaseId`를 제거해서는 안 된다. |
| `RT-023` | deadline이 지나면 shard는 만료 시점에도 같은 `LeaseId`가 current인지 확인하고 일치하는 projection subset만 제거해야 한다. |
| `RT-024` | 반복 refresh는 live lease당 expiry record를 무제한 누적시켜서는 안 된다. timer와 cleanup state는 현재 live lease 수에 비례해야 한다. |
| `RT-025` | release·expiry 뒤 늦은 `PublishCurrent`가 같은 lease를 다시 current로 만들 수 있고 그 뒤 도착한 일치 `Refresh`가 deadline을 연장할 수 있다. 마지막으로 수락한 publish 또는 refresh 뒤 더 이상 operation이 도착하지 않으면 한 lease lifetime 안에 해당 lease와 projection을 제거해야 한다. |

## Resolve 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-030` | `Resolve(ShardDirectoryGeneration, ClientId)`는 요청 시점에 해당 shard가 보유한 모든 live projection을 하나의 `BindingSet` snapshot으로 반환한다. |
| `RT-031` | `Resolve`는 projection을 선택하거나 정렬 우선순위, affinity, weight 또는 health 품질을 결정하지 않는다. |
| `RT-032` | current snapshot에서 제거되었거나 release, expiry 또는 restart로 사라진 projection은 결과에 포함하지 않는다. 이후 늦은 `PublishCurrent`가 수락되면 그 projection은 새 current soft-state observation으로 다시 포함될 수 있다. |
| `RT-033` | live projection은 최근 수락한 publication을 뜻하며 실제 Gateway 또는 Listener 도달 가능성을 보장하지 않는다. Owner Gateway가 OPEN 시점에 binding identity를 다시 확인한다. |
| `RT-034` | `READY` shard에 live projection이 없으면 `NOT_FOUND`로 끝난다. shard를 사용할 수 없는 `UNAVAILABLE`과 구분해야 한다. |

## 재시작과 가용성

| ID | 요구사항 |
| --- | --- |
| `RT-040` | RT shard는 memory-only이며 process 시작 시 configured `ShardDirectoryGeneration`의 빈 상태다. 이전 projection, lease, revision, Pipe 또는 payload를 복원하지 않는다. |
| `RT-041` | RT가 요청을 처리할 수 없는 동안 publication operation과 `Resolve`는 `UNAVAILABLE`로 관찰되어야 한다. 이를 빈 `BindingSet`으로 응답해서는 안 된다. |
| `RT-042` | 재시작한 shard는 같은 generation의 live Gateway가 current local binding으로 새 snapshot을 게시하여 구성한다. generation을 바꾸는 coordinated restart에서도 과거 mutation이나 종료된 session을 replay하지 않는다. |
| `RT-043` | shard가 `READY`지만 current snapshot이 아직 도착하지 않은 수렴 구간에는 `Resolve`가 `NOT_FOUND`가 될 수 있다. |
| `RT-044` | established Pipe는 RT mapping과 독립적이다. RT 중단, restart, replace, release 또는 expiry만으로 기존 Pipe를 종료하지 않는다. |

## Gateway와 구현 경계

| ID | 요구사항 |
| --- | --- |
| `RT-050` | Gateway는 remote `Resolve` 결과를 이후 신규 연결의 routing authority로 사용하는 core mapping cache를 두지 않는다. local binding이 없는 신규 연결은 current authority shard를 조회한다. |
| `RT-051` | RT는 `BindingSet`만 반환한다. 어느 binding으로 Pipe를 열지는 connection establishment 계약이 결정한다. |
| `RT-052` | `Resolve`, snapshot replace와 lease cleanup은 전체 RT table scan을 요구해서는 안 된다. 구현은 `ClientId` forward index와 publication reverse index에 상응하는 경계를 제공해야 한다. |
| `RT-053` | 한 shard의 multi-index mutation은 하나의 순서로 atomic하게 적용되어 partial projection이나 orphan index를 외부에 노출해서는 안 된다. |
| `RT-054` | RT는 `ConnectorSessionId`, `ConnectionId`, connect attempt 또는 Pipe 상태를 저장하지 않아야 한다. ConnectorSession의 생성·단절은 RT mapping을 직접 변경하지 않는다. |

## 불변식

1. process의 `ShardDirectoryGeneration`은 불변이며 다른 generation의 operation은 state를 관찰하거나 변경하지 못한다.
2. 같은 generation에서 `ClientId`의 logical shard authority는 정확히 하나다.
3. `ClientId`의 live projection 수는 `0..N`이다.
4. 하나의 publication scope에는 current lease가 최대 하나다.
5. 같은 scope의 낮거나 충돌하는 revision은 current snapshot을 바꾸지 못하고, 더 높은 revision만 current lease와 snapshot을 교체할 수 있다.
6. 한 scope의 replace, release 또는 expiry는 다른 scope와 Gateway-local binding에 영향을 주지 않는다.
7. RT state 크기는 현재 live projection과 lease 수에 비례하며 과거 mutation 수에 비례하지 않는다.
8. RT는 ConnectorSession, connect attempt, payload, binding selection과 established Pipe lifecycle에 참여하지 않는다.
9. release와 expiry는 hard fence가 아니다. 늦은 publication이 stale projection을 잠시 다시 만들 수 있지만 Owner Gateway의 binding revalidation 없이는 Pipe를 열 수 없다.
10. RT mutation의 `GatewayId`는 authenticated transport identity와 일치해야 하며 claimed identifier만으로 publication authority를 얻을 수 없다.

## 이 계약에서 정하지 않는 것

- online 또는 rolling shard directory 변경 절차
- logical shard replication, consensus와 failover
- binding selection policy
- lease 시간, refresh 주기와 clock tolerance의 수치
- wire format과 transport
- internal channel identity와 integrity를 제공하는 TLS, mTLS 또는 service-mesh 구현
