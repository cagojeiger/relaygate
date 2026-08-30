# SPEC 004: RouteTable schema와 registration lease 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 005](../adr/005-soft-state-registration-lifecycle.md) |
| 용어 | [SPEC 001](001-terminology-and-object-model.md) |
| 상태와 오류 | [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 logical RT shard의 memory-only schema와 `Register`, `Update`, `KeepAlive`, `Deregister`, `Resolve` 동작을 정의한다. binding 선택, Pipe 수립, RT replication과 failover는 범위가 아니다.

## 논리 interface

```text
AuthenticatedGatewayId = internal channel이 검증한 caller identity

RouteTable {
  Register(AuthenticatedGatewayId,
           ShardDirectoryGeneration, RegistrationKey) -> RegistrationAck
  Update(AuthenticatedGatewayId,
         ShardDirectoryGeneration, RegistrationKey,
         LeaseId, RegistrationRevision, MappingSnapshot) -> RegistrationAck
  KeepAlive(AuthenticatedGatewayId,
            ShardDirectoryGeneration, RegistrationKey, LeaseId) -> RegistrationAck
  Deregister(AuthenticatedGatewayId,
             ShardDirectoryGeneration, RegistrationKey, LeaseId) -> success
  Resolve(AuthenticatedGatewayId,
          ShardDirectoryGeneration, ClientId) -> BindingSet
}
```

```text
RegistrationKey = (GatewayId, ListenerSessionId, ShardId)

MappingSnapshot
  └── 같은 RegistrationKey와 shard에 속하는 complete current MappingEntry set
```

`Register`만 새 registration lease를 만들 수 있고 `LeaseId`는 RT가 발급한다. `Update`와 `KeepAlive`는 RT가 현재 보유한 active `LeaseId`에만 적용된다. 종료되거나 알려지지 않은 lease의 operation은 registration이나 mapping을 만들지 않는다.

`AuthenticatedGatewayId`는 request body가 주장한 값이 아니라 RT service adapter가 internal channel에서 검증한 identity다. core는 mutation의 `RegistrationKey.GatewayId`와 이 값을 다시 비교한다. `Resolve`도 authenticated internal operation이지만 특정 registration 소유권을 요구하지 않는다.

최초 local/CI runtime profile의 service adapter는 Gateway별 `GatewayName -> InternalGatewayKey`
allowlist를 startup configuration으로 고정한다. handshake의 name과 key를 constant-time으로
검증한 뒤 그 connection이 제시한 fresh runtime `GatewayId`를 해당 authenticated identity에
결합한다. 다른 Gateway의 key로 이름을 주장하거나 결합된 `GatewayId`와 mutation owner가
다르면 state를 만들지 않는다. `InternalGatewayKey`는 로그와 protocol state에 남기지 않는다.
이 adapter는 plain TCP의 confidentiality나 production-grade channel integrity를 보장하지 않으며,
실제 배포의 mTLS 또는 service identity adapter를 대체하지 않는다.

`RegistrationAck`는 active `LeaseId`, 최근 수락된 `RegistrationRevision 0..1`과 응답 시점의 `LeaseTtlRemaining`을 나타낸다. 첫 snapshot 전에는 revision이 없다. `LeaseTtlRemaining`은 Gateway가 keepalive를 예약하기 위한 상대 duration이며 RT의 절대 clock 값이나 구현 timer identity를 외부 identity로 사용하지 않는다.

core의 lease 계산은 RT process가 제공하는 monotonic time만 사용한다. wire caller는 시간을 보내지 않으며, wall clock 변경은 lease 순서나 expiry를 바꾸지 않는다.

operation 검증은 정보 노출과 결과 차이를 막기 위해 다음 순서를 따른다.

```text
1. internal channel identity 인증
2. mutation caller와 RegistrationKey.GatewayId 일치
3. ShardDirectoryGeneration 일치
4. shard authority와 request/snapshot scope
5. monotonic time에 도달한 lease expiry 적용
6. active LeaseId
7. RegistrationRevision과 snapshot equality
8. state read 결과 반환 또는 atomic mutation
```

1~4단계가 실패하면 뒤 단계의 존재 여부를 관찰 가능한 결과로 노출하지 않고 expiry도
진행하지 않는다. 5단계의 expiry는 요청 mutation이 아니라 monotonic time으로 이미 끝난
lease를 제거하는 state transition이다. 그 뒤 lease나 revision 검사가 실패하면 요청 자체는
추가 state를 변경하지 않는다. `Resolve`에는 registration owner 비교와 lease·revision 검사가
없지만 나머지 순서는 같다.

## Shard directory와 authority

```text
ShardDirectory artifact bytes
  ├── format version
  ├── authority hash rule = sha256-modulo-v1
  └── ordered shard records = [(ShardId, ShardEndpoint), ...]

ShardDirectoryGeneration = SHA-256(exact artifact bytes)

digest = SHA-256(exact UTF-8 bytes of ClientId)
index  = unsigned-big-endian(digest[0..8]) mod shard_count
Authority(ClientId) = ordered shard records[index]
```

최초 artifact 형식은 UTF-8 JSON이다. generation은 parse 전 input bytes 전체에서 계산한다.

```json
{
  "format_version": 1,
  "authority_hash": "sha256-modulo-v1",
  "shards": [
    { "id": "rt-0", "endpoint": "rt-0:27430" },
    { "id": "rt-1", "endpoint": "rt-1:27430" }
  ]
}
```

`format_version`, `authority_hash`, `shards`, 각 record의 `id`와 `endpoint`만 허용한다. identifier와 endpoint는 non-empty string이어야 하며 `ShardId`는 중복될 수 없다. `shards` array 순서가 authority modulo의 순서다. endpoint string의 해석과 실제 service discovery는 deployment adapter가 소유한다.

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
    -> Map<(GatewayId, ListenerSessionId, BindingId), MappingEntry>

RegistrationIndex
  RegistrationKey
    -> {
         LeaseId
         RegistrationRevision 0..1
         Deadline
         MappingIdentity Set
       }

ExpiryIndex
  (RegistrationKey, LeaseId) -> Deadline
```

`RouteIndex`는 `Resolve(AuthenticatedGatewayId, ShardDirectoryGeneration, ClientId)`를 위한 forward index다. `RegistrationIndex`는 complete snapshot replace, deregister와 expiry가 해당 registration의 mapping만 제거하기 위한 reverse index다. 하나의 canonical mapping record를 두 index가 참조할 수 있지만 외부에서 관찰되는 의미는 동일해야 한다.

`RegistrationIndex`와 `ExpiryIndex`에는 active lease만 둔다. `Deregister`, expiry 또는 restart 뒤 과거 `LeaseId`, revision과 mapping에 대한 tombstone이나 high-watermark를 보관하지 않는다. 안전성은 과거 lease를 기억하는 대신 새 state 생성과 active lease 갱신을 분리하여 얻는다.

`ConnectorSessionId`와 `ConnectionId`는 연결 수립 상태이며 RT schema와 interface에 포함되지 않는다.

```text
ClientId A
  ├── MappingEntry 1 -> GW-1
  └── MappingEntry 2 -> GW-2

Registration 1
  ├── MappingEntry 1
  └── MappingEntry 3
```

## shard 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-001` | 같은 `ShardDirectoryGeneration`에서 같은 `ClientId`는 항상 같은 하나의 logical shard로 결정되어야 한다. |
| `RT-002` | `Register`, `Update`, `KeepAlive`, `Deregister`와 `Resolve`는 대상 `RegistrationKey.ShardId` 또는 `ClientId`의 authority shard에서 처리되어야 한다. |
| `RT-003` | 각 shard는 자신이 authority인 `ClientId` mapping만 저장한다. 한 `ListenerSession`이 여러 shard에 걸치면 각 shard는 자기 부분집합만 저장한다. |
| `RT-004` | shard partition은 mapping 용량을 분산한다. logical shard의 replica 수와 failover 보장은 이 계약에 포함되지 않는다. |
| `RT-005` | RT process는 시작 시 하나의 `ShardDirectoryGeneration`을 고정해야 한다. 모든 operation은 같은 generation일 때만 처리하며 mismatch는 `FAILED_PRECONDITION`으로 끝내고 mapping, lease, revision과 deadline을 읽기 결과로 반환하거나 변경해서는 안 된다. |
| `RT-006` | `ShardDirectoryGeneration`은 generation field가 없는 exact UTF-8 JSON directory artifact bytes의 SHA-256이어야 한다. loader는 unknown field를 거절하고 artifact의 format version, `sha256-modulo-v1` 규칙, ordered shard records, non-empty unique `ShardId`와 각 record의 non-empty exactly-one stable logical endpoint를 검증해야 하며 process 시작 뒤 directory와 generation을 바꾸어서는 안 된다. |
| `RT-007` | `sha256-modulo-v1` authority는 `ClientId`의 exact UTF-8 bytes를 SHA-256으로 계산하고 첫 8 bytes를 unsigned big-endian integer로 해석한 뒤 ordered shard count로 modulo하여 하나의 shard를 선택해야 한다. 빈 shard list는 invalid configuration이다. |
| `RT-008` | 모든 RT operation은 배포 환경이 identity와 integrity를 보장하는 internal channel에서만 처리해야 한다. authenticated Gateway identity는 `RegistrationKey.GatewayId`와 일치해야 하며 message가 주장하는 `GatewayId`만 신뢰해서는 안 된다. 인증·일치 검증 실패는 state를 읽거나 변경하지 않고 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`로 끝나야 한다. |
| `RT-009` | 최초 local/CI adapter는 startup configuration의 Gateway별 `InternalGatewayKey`를 constant-time으로 검증하고 성공한 connection의 fresh runtime `GatewayId`에 identity를 결합해야 한다. name/key 실패는 `UNAUTHENTICATED`, 결합된 identity와 operation owner mismatch는 `PERMISSION_DENIED`이며 state를 읽거나 만들지 않는다. key를 로그·mapping·lease에 기록하거나 claimed `GatewayId`만으로 인증해서는 안 된다. 이 adapter의 plain TCP를 production confidentiality 또는 integrity 보장으로 표현해서는 안 된다. |

## Register와 Update 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-010` | `Register`는 하나의 valid `RegistrationKey`만 받으며 mapping을 함께 만들지 않는다. key의 shard authority가 다르거나 scope가 섞이면 `INVALID_ARGUMENT`이다. |
| `RT-011` | key에 active registration이 없으면 RT는 재사용하지 않는 새 opaque `LeaseId`를 발급하고 mapping이 없는 registration을 atomic하게 만들어야 한다. |
| `RT-012` | 같은 authenticated Gateway가 같은 active `RegistrationKey`를 다시 `Register`하면 mapping, revision과 deadline을 변경하지 않고 current lease를 반환하는 idempotent success여야 한다. |
| `RT-013` | `Update`는 active `LeaseId`와 일치하고 같은 shard에 속하는 하나 이상의 complete current `MappingEntry`를 포함해야 한다. 없는, expired 또는 다른 current lease의 `Update`는 mapping이나 registration을 만들지 않고 `FAILED_PRECONDITION`으로 끝나야 한다. |
| `RT-014` | active lease의 첫 `Update`는 `RegistrationRevision = 1`이어야 한다. 이후 current revision보다 strictly greater인 `Update`는 기존 registration mapping set을 atomic하게 새 complete set으로 교체하고 생략된 mapping을 제거한다. current snapshot과 새 snapshot에 함께 있는 `MappingIdentity`는 동일한 `ClientId`와 `GatewayLocator`를 유지해야 하며, binding destination이나 locator가 바뀌면 새 `BindingId`가 필요하다. revision은 lease 안에서만 유효하며 `Update`는 lease deadline을 연장하지 않는다. |
| `RT-015` | 같은 active lease와 같은 revision의 동일 snapshot 반복은 idempotent success여야 한다. 같은 revision의 다른 내용과 낮은 revision은 `FAILED_PRECONDITION`이며 state와 deadline을 바꾸지 않는다. |
| `RT-016` | snapshot replace는 같은 `RegistrationKey`의 mapping만 변경하고 같은 `ClientId`의 다른 registration mapping이나 다른 shard state를 변경해서는 안 된다. |
| `RT-017` | `Deregister`, expiry 또는 restart로 lease가 사라진 뒤 도착한 과거 `Update`는 새 registration 또는 mapping을 만들 수 없다. 새 mapping을 만들려면 `Register`로 RT가 발급한 새 lease를 얻은 뒤 현재 snapshot을 `Update`해야 한다. |

registration에 더 이상 binding이 없으면 Gateway는 빈 snapshot을 유지하지 않고 `Deregister`한다.

## KeepAlive, Deregister와 Expire 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-020` | `KeepAlive`는 요청한 `RegistrationKey`의 active `LeaseId`가 일치할 때만 deadline을 연장해야 하며 mapping set이나 revision을 변경해서는 안 된다. |
| `RT-021` | 없는, expired 또는 stale `LeaseId`의 `KeepAlive`는 lease나 mapping을 만들지 않고 `FAILED_PRECONDITION`으로 끝나야 한다. |
| `RT-022` | `Deregister`는 일치하는 active lease와 그 mapping subset만 제거해야 한다. registration이 이미 없으면 idempotent success다. 같은 key에 다른 active `LeaseId`가 있으면 `FAILED_PRECONDITION`이며 그 lease와 mapping을 변경해서는 안 된다. |
| `RT-023` | monotonic time이 deadline에 도달하거나 지나면 shard는 만료 시점에도 같은 `LeaseId`가 active인지 확인하고 일치하는 mapping subset만 제거해야 한다. |
| `RT-024` | 반복 `KeepAlive`는 live lease당 expiry record를 무제한 누적시켜서는 안 된다. timer와 cleanup state는 현재 active lease 수에 비례해야 한다. |
| `RT-025` | 종료된 lease의 늦은 `Update`, `KeepAlive`와 `Deregister`는 이후 만들어진 registration과 mapping을 변경하지 못한다. RT는 이를 위해 tombstone을 저장하지 않고 active lease identity 일치만 검사한다. |

## Resolve 요구사항

| ID | 요구사항 |
| --- | --- |
| `RT-030` | `Resolve(AuthenticatedGatewayId, ShardDirectoryGeneration, ClientId)`는 요청 시점에 해당 shard가 active lease 아래 보유한 모든 live mapping을 하나의 `BindingSet` snapshot으로 반환한다. |
| `RT-031` | `Resolve`는 mapping을 선택하거나 정렬 우선순위, affinity, weight 또는 health 품질을 결정하지 않는다. |
| `RT-032` | current snapshot에서 제거되었거나 deregister, expiry 또는 restart로 사라진 mapping은 결과에 포함하지 않는다. 종료된 lease의 늦은 operation으로 다시 포함되어서는 안 된다. |
| `RT-033` | live mapping은 최근 수락한 registration update를 뜻하며 실제 Gateway 또는 Listener 도달 가능성을 보장하지 않는다. Owner Gateway가 `OPEN` 시점에 binding identity를 다시 확인한다. |
| `RT-034` | `READY` shard에 live mapping이 없으면 `NOT_FOUND`로 끝난다. shard를 사용할 수 없는 `UNAVAILABLE`과 구분해야 한다. |

## 재시작과 가용성

| ID | 요구사항 |
| --- | --- |
| `RT-040` | RT shard는 memory-only이며 process 시작 시 configured `ShardDirectoryGeneration`의 빈 상태다. 이전 mapping, lease, revision, Pipe 또는 payload를 복원하지 않는다. |
| `RT-041` | RT가 요청을 처리할 수 없는 동안 registration operation과 `Resolve`는 `UNAVAILABLE`로 관찰되어야 한다. 이를 빈 `BindingSet`으로 응답해서는 안 된다. |
| `RT-042` | 재시작한 shard는 같은 generation의 live Gateway가 새 lease를 `Register`하고 current local binding snapshot을 `Update`하여 구성한다. 과거 mutation이나 종료된 session을 replay하지 않는다. |
| `RT-043` | shard가 `READY`지만 current snapshot이 아직 도착하지 않은 수렴 구간에는 `Resolve`가 `NOT_FOUND`가 될 수 있다. |
| `RT-044` | established Pipe는 RT mapping과 독립적이다. RT 중단, restart, replace, deregister 또는 expiry만으로 기존 Pipe를 종료하지 않는다. |

## Gateway와 구현 경계

| ID | 요구사항 |
| --- | --- |
| `RT-050` | Gateway는 remote `Resolve` 결과를 이후 신규 연결의 routing authority로 사용하는 core mapping cache를 두지 않는다. local binding이 없는 신규 연결은 current authority shard를 조회한다. |
| `RT-051` | RT는 `BindingSet`만 반환한다. 어느 binding으로 Pipe를 열지는 connection establishment 계약이 결정한다. |
| `RT-052` | `Resolve`, snapshot replace와 lease cleanup은 전체 RT table scan을 요구해서는 안 된다. 구현은 `ClientId` forward index와 registration reverse index에 상응하는 경계를 제공해야 한다. |
| `RT-053` | 한 shard의 multi-index mutation은 하나의 순서로 atomic하게 적용되어 partial mapping이나 orphan index를 외부에 노출해서는 안 된다. |
| `RT-054` | RT는 `ConnectorSessionId`, `ConnectionId`, open attempt 또는 Pipe 상태를 저장하지 않아야 한다. ConnectorSession의 생성·단절은 RT mapping을 직접 변경하지 않는다. |

## 불변식

1. process의 `ShardDirectoryGeneration`은 불변이며 다른 generation의 operation은 state를 관찰하거나 변경하지 못한다.
2. 같은 generation에서 `ClientId`의 logical shard authority는 정확히 하나다.
3. `ClientId`의 live mapping 수는 `0..N`이다.
4. 하나의 `RegistrationKey`에는 active lease가 최대 하나다.
5. 새 lease는 `Register`만 만들고 RT가 `LeaseId`를 발급한다.
6. `Update`와 `KeepAlive`는 active lease identity가 일치할 때만 state를 변경한다.
7. 한 registration의 replace, deregister 또는 expiry는 다른 registration과 Gateway-local binding에 영향을 주지 않는다.
8. RT state 크기는 현재 live mapping과 active lease 수에 비례하며 과거 mutation 수에 비례하지 않는다.
9. RT는 ConnectorSession, open attempt, payload, binding selection과 established Pipe lifecycle에 참여하지 않는다.
10. 종료된 lease의 operation은 mapping을 되살리지 못하며, Owner Gateway는 active mapping도 `OPEN` 시점에 재검증한다.
11. RT mutation의 `GatewayId`는 authenticated transport identity와 일치해야 하며 claimed identifier만으로 registration authority를 얻을 수 없다.

## 이 계약에서 정하지 않는 것

- online 또는 rolling shard directory 변경 절차
- logical shard replication, consensus와 failover
- binding selection policy
- lease 시간, keepalive 주기와 clock tolerance의 수치
- wire format과 transport
- internal channel identity와 integrity를 제공하는 TLS, mTLS 또는 service-mesh 구현
- local/CI `InternalGatewayKey`를 production credential로 배포하는 방법
