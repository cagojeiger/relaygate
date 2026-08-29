# SPEC 001: 용어와 객체 모델

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 001](../adr/001-relayed-pipe-responsibility-boundary.md), [ADR 002](../adr/002-application-protocol-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 005](../adr/005-soft-state-registration-lifecycle.md), [ADR 006](../adr/006-one-hop-peer-multiplexing.md) |

이 문서는 RelayGate의 공통 용어, identity, 소유 관계와 cardinality를 정의한다. 등록, publication, route 조회, 연결 수립과 오류는 각 후속 SPEC이 소유한다.

## 역할

```text
Connector application                              Listener application
         │ connect(ClientId)                       accept() │
         ▼                                                  ▼
    Connector ───────────── opaque bidirectional Pipe ── Listener
```

| 용어 | 정의 |
| --- | --- |
| `Connector` | `ClientId`를 지정하여 새 Pipe를 요청하는 SDK 역할 |
| `Connector SDK runtime` | Connector 호출들과 하나의 current `ConnectorSession`을 관리하는 SDK runtime |
| `ConnectorSession` | Connector SDK runtime과 Entry Gateway 사이의 한 번의 live connection incarnation |
| `ConnectorSessionId` | 하나의 `EntryGatewayId` 범위에서 한 live `ConnectorSession`을 구분하는 opaque identifier |
| `Listener SDK runtime` | Listener handle들과 하나의 current `ListenerSession`을 관리하는 SDK runtime |
| `Listener` | desired `ClientId` 하나로 들어오는 Pipe를 받는 애플리케이션용 SDK handle |
| `ListenerSession` | Listener SDK runtime이 Gateway에 현재 연결된 한 번의 live incarnation |
| `ClientId` | 위치가 아닌 non-empty UTF-8 logical destination identifier. identity 비교와 authority hash는 정규화하지 않은 exact bytes를 사용한다. |
| `ClientKey` | Listener가 해당 `ClientId`에 binding을 등록할 권한을 증명하는 credential |
| `BindingId` | 하나의 `ListenerSession` 안에서 모든 `ListenerBinding` incarnation을 lifetime 전체에 걸쳐 구분하는 재사용하지 않는 opaque identifier |
| `ListenerBinding` | 하나의 `ClientId`와 하나의 live `ListenerSession`을 연결하는 Gateway-local association |
| `BindingProjection` | authority shard가 resolve에 사용하는 하나의 `ListenerBinding`에 대한 shard-local soft-state view |
| `BindingSet` | 하나의 `ClientId`에 대해 authority shard가 가진 active `BindingProjection`의 집합 |
| `ShardId` | 현재 shard directory 안에서 logical RT shard를 구분하는 identifier |
| `ShardDirectoryGeneration` | generation 필드를 포함하지 않은 immutable shard directory artifact exact bytes의 SHA-256 digest |
| `ShardDirectory` | format version, authority hash 규칙과 순서가 있는 `ShardId`별 stable logical RT endpoint 하나를 담고 content hash generation이 파생되는 공통 불변 deployment configuration. route mapping 자체가 아니다. |
| `PublicationScope` | 한 Gateway session이 한 shard에 게시하는 부분집합의 scope인 `(GatewayId, ListenerSessionId, ShardId)` |
| `LeaseId` | 하나의 `PublicationScope`에서 한 publication lease incarnation을 구분하는 opaque identifier |
| `PublicationRevision` | 하나의 `PublicationScope` 안에서 lease 교체를 포함한 current snapshot의 순서를 구분하는 단조 증가 revision |
| `PublicationSnapshot` | 한 `PublicationScope`가 해당 revision에 소유하는 complete `BindingProjection` 집합 |
| `ListenerSessionId` | 하나의 `GatewayId` 범위에서 `ListenerSession` incarnation마다 새로 발급하고 재사용하지 않는 식별자 |
| `GatewayId` | Gateway runtime incarnation마다 새로 발급하고 이전 incarnation에서 재사용하지 않는 식별자 |
| `GatewayLocator` | `ListenerSession`을 현재 소유한 Gateway의 위치 식별자 |
| `ConnectionId` | 하나의 `ConnectorSession` 안에서 한 번의 Pipe 수립을 상관시키는 strictly increasing unsigned counter. 애플리케이션 message 또는 delivery ID가 아니다. |
| `Pipe` | 정확히 하나의 Connector와 하나의 Listener를 연결하는 opaque bidirectional byte stream |
| `PeerTransport` | Gateway pair 사이에서 여러 `RelayStream`을 운반하는 reusable bidirectional transport |
| `PeerTransportSlot` | 하나의 unordered Gateway pair 안에서 `DialerGatewayId`로 구분되는 방향별 transport 자리 |
| `PeerTransportId` | 하나의 dialer Gateway incarnation에서 peer transport candidate를 구분하는 식별자 |
| `RelayStream` | remote Pipe 하나를 Gateway 간 전달하는 logical bidirectional stream |
| `StreamId` | active `RelayStream`을 소유 `PeerTransport` 안에서 구분하는 unsigned 64-bit 식별자. 최하위 bit는 stream을 시작한 endpoint의 transport role이다. |

`EntryGatewayId`, `OwnerGatewayId`, `DialerGatewayId`와 `PeerGatewayId`는 서로 다른 identifier type이 아니라 해당 역할에 사용된 `GatewayId`를 뜻한다.

`Listener`는 애플리케이션이 닫을 때까지 desired `ClientId`를 유지하는 handle이다. 여러 Listener가 하나의 `ListenerSession`을 공유한다. 그 session은 Gateway 연결이 끊어지면 끝나며 재연결 시 새 incarnation으로 대체된다.

## 관계

```text
Connector SDK runtime 1
    └── current ConnectorSession 0..1
            └── ConnectionId 0..N
                    └── Connector Pipe 0..1

Listener SDK runtime 1
    ├── current ListenerSession 0..1
    │       └── ListenerBinding 0..N
    └── Listener handle 0..N
            ├── desired ClientId 1
            └── current ListenerBinding 0..1
        non-CLOSED handle의 desired ClientId는 runtime 안에서 unique

ClientId 1 ── ListenerBinding 0..N

ClientId *  ◄──── ListenerBinding ────►  * ListenerSession
ListenerBinding 1 ── BindingProjection 0..1

PublicationScope 1
    └── PublicationSnapshot 0..1
            └── BindingProjection 0..N

unordered Gateway pair 1
    ├── PeerTransportSlot(dialer A) 1 ── READY PeerTransport 0..1
    └── PeerTransportSlot(dialer B) 1 ── READY PeerTransport 0..1

PeerTransport 1 ── RelayStream 0..N ── remote Pipe 1

one connect
    └── one selected ListenerBinding
            └── Connector 1 ◄════ Pipe ════► 1 Listener
```

등록 관계는 many-to-many지만, 하나의 연결을 broadcast하거나 여러 Listener로 fan-out하지 않는다.

## 최소 schema

```text
BindingProjection {
  ClientId
  GatewayId
  ListenerSessionId
  BindingId
  GatewayLocator
}

PublicationSnapshot {
  ShardDirectoryGeneration
  GatewayId
  ListenerSessionId
  ShardId
  LeaseId
  PublicationRevision
  BindingProjection[]
}
```

snapshot의 모든 projection은 같은 `PublicationScope`에 속하고 그 `ClientId`의 authority는 같은 `ShardId`여야 한다. projection과 snapshot은 `ClientKey`, `ConnectorSessionId`, `ConnectionId`, application identity와 payload를 포함하지 않는다.

## 소유권과 수명

| 객체 | 소유자 | 수명 |
| --- | --- | --- |
| `ClientId`, `ClientKey` | external client configuration | runtime session과 독립적인 configuration lifetime |
| `Connector SDK runtime` | Connector application | runtime을 닫을 때까지 |
| `ConnectorSession`, `ConnectorSessionId` | Entry Gateway | Connector SDK와 맺은 한 번의 live 연결 동안 |
| `Listener SDK runtime` | Listener application | runtime을 닫을 때까지 |
| `Listener` | Listener application | 명시적으로 닫을 때까지 |
| `ListenerSession` | Owner Gateway | Listener SDK runtime과 맺은 한 번의 live 연결 동안 |
| `ListenerBinding`, `BindingId` | 하나의 `ListenerSession` | session 종료 또는 binding 제거까지 |
| `PublicationSnapshot`, `LeaseId` | Gateway publication manager와 authority RT shard | Gateway는 한 번 발급하고 재사용하지 않음. RT current observation은 release, expiry 또는 restart까지이며 늦은 Publish로 다시 관찰될 수 있음. |
| `BindingProjection` | authority RT shard | current snapshot에 포함된 동안 |
| `BindingSet` | 같은 `ClientId`의 active projection view | 원소의 추가·제거에 따라 계속 변함 |
| `ShardDirectory`, `ShardDirectoryGeneration` | deployment configuration | process 시작부터 종료까지 불변 |
| `GatewayId` | Gateway runtime | 한 번의 live runtime incarnation 동안 |
| `GatewayLocator` | deployment/network configuration | 해당 위치가 routable한 동안 |
| `ConnectionId` | 하나의 `ConnectorSession` | 수립 시도 시작부터 실패 또는 Pipe 종료까지 |
| `Pipe`의 각 endpoint | 각각 Connector/Listener application | close, transport 상실 또는 terminal failure까지 |
| `PeerTransport` | 참여하는 Gateway pair | transport close 또는 상실까지 |
| `PeerTransportSlot` | unordered Gateway pair | 참여 Gateway incarnation 중 하나가 종료될 때까지 |
| `PeerTransportId` | 하나의 peer transport candidate | candidate 생성부터 close까지 |
| `RelayStream`, `StreamId` | 하나의 `PeerTransport` | stream close 또는 소유 transport 종료까지 |

`ListenerBinding`은 이를 소유한 `ListenerSession`보다 오래 존재할 수 없다. binding은 새 Pipe를 찾는 데만 쓰이며, 제거된 binding 자체가 이미 수립된 Pipe를 이동하거나 재생하지 않는다.

## 식별자 규칙

`ClientId`, `BindingId`, `LeaseId`, `ConnectorSessionId`, `ListenerSessionId`, `GatewayId`, `GatewayLocator`와 `PeerTransportId`는 자신을 정의한 범위에서 opaque identifier다. 소비자는 equality 비교 외의 의미를 추론해서는 안 된다. 예외는 ConnectorSession 안에서만 순서를 비교하는 `ConnectionId` counter, 소유 protocol 안에서만 해석하는 `StreamId`의 initiator bit와 counter, directory load 시 검증하는 `ShardDirectoryGeneration`의 content digest, 같은 `PublicationScope` 안에서 순서를 비교하는 `PublicationRevision`이다.

- 문자열 구조, 정렬 순서 또는 발급 시간을 해석하지 않는다.
- 명시된 `ConnectionId`, `StreamId`와 `ShardDirectoryGeneration` 규칙 밖에서는 하나의 identifier에서 다른 identifier나 위치를 계산하지 않는다.
- 내부 identifier를 application identity, 인증 정보 또는 delivery ID로 사용하지 않는다.
- 같은 `GatewayLocator`를 새 Gateway incarnation이 재사용할 수 있으므로 `GatewayId`와 `GatewayLocator`를 서로 대체하지 않는다.

## 요구사항

- **`TERM-001`**: SDK와 protocol은 연결을 요청하는 역할을 `Connector`, 연결을 받는 역할을 `Listener`로 표현해야 한다.
- **`TERM-002`**: `ClientId`는 특정 Listener process, `ListenerSession` 또는 Gateway 위치와 동일시해서는 안 된다.
- **`TERM-003`**: `ClientKey`는 binding 등록 권한에만 사용하며 Pipe peer 인증이나 payload 권한을 의미해서는 안 된다.
- **`TERM-004`**: 하나의 Listener SDK runtime은 동시에 0개 또는 1개의 current `ListenerSession`을 가져야 한다. 각 `Listener` handle은 desired `ClientId` 하나와 그 shared session의 current binding 0개 또는 1개만 가리켜야 하며 session을 소유해서는 안 된다.
- **`TERM-005`**: 하나의 `ListenerBinding`은 정확히 하나의 `ClientId`, `BindingId`, `GatewayId`, `ListenerSession`과 `GatewayLocator`를 연결해야 한다. `BindingId`는 같은 `ListenerSession`의 서로 다른 `ClientId`와 제거된 incarnation을 포함해 session lifetime 동안 unique하고 재사용하지 않아야 한다.
- **`TERM-006`**: 같은 `(GatewayId, ListenerSessionId, ClientId)`에는 동시에 최대 하나의 live `ListenerBinding`만 존재해야 하며 제거 후 재등록은 재사용하지 않은 새 `BindingId`를 가져야 한다.
- **`TERM-007`**: 하나의 `ClientId`와 하나의 `ListenerSession`은 각각 0개 이상의 `ListenerBinding`에 참여할 수 있어야 한다.
- **`TERM-008`**: `BindingSet(ClientId)`은 그 `ClientId`의 active `BindingProjection`만 포함해야 하며 durable history로 취급해서는 안 된다.
- **`TERM-009`**: 하나의 connect는 후보가 몇 개이든 최대 하나의 `ListenerBinding`을 선택해야 한다.
- **`TERM-010`**: 성공한 connect는 정확히 하나의 Connector endpoint와 하나의 Listener endpoint로 이루어진 Pipe 하나를 만들어야 한다.
- **`TERM-011`**: `ListenerBinding`은 이를 소유한 `ListenerSession`보다 오래 존재해서는 안 된다. 새 live session은 같은 `GatewayId` 범위에서 이전에 사용하지 않은 `ListenerSessionId`를 가져야 한다.
- **`TERM-012`**: protocol identifier는 해당 protocol이 명시한 `ConnectionId` counter, `StreamId` role bit와 counter, `ShardDirectoryGeneration` content digest 외에는 opaque하게 취급해야 하며 application 의미를 부여해서는 안 된다.
- **`TERM-013`**: `GatewayId`는 한 Gateway incarnation의 identity이고 runtime 시작마다 이전 incarnation에서 사용하지 않은 새 값을 가져야 한다. `GatewayLocator`는 재사용 가능한 routable location이며 둘을 동일한 식별자로 취급해서는 안 된다.
- **`TERM-014`**: 하나의 `PeerTransport`는 0개 이상의 `RelayStream`을 운반할 수 있고, 각 `RelayStream`은 remote Pipe 하나에만 대응하며 소유 transport보다 오래 존재해서는 안 된다.
- **`TERM-015`**: 하나의 `BindingProjection`은 `(GatewayId, ListenerSessionId, BindingId)`로 식별되는 binding incarnation 하나를 나타내고 `ClientId`와 `GatewayLocator`를 포함해야 한다. 그 identity는 authority shard에 최대 하나의 active projection만 가져야 하며 `ClientKey`나 payload를 포함해서는 안 된다.
- **`TERM-016`**: peer transport candidate는 `(DialerGatewayId, PeerTransportId)`로 식별해야 한다. `PeerTransportId`는 해당 dialer Gateway incarnation 안에서 재사용하지 않는 opaque identity여야 하며 `GatewayLocator`나 연결 도착 순서에서 계산해서는 안 된다.
- **`TERM-017`**: 한 connect attempt는 `(EntryGatewayId, ConnectorSessionId, ConnectionId)`로 식별해야 한다. `ConnectorSessionId`는 해당 Entry Gateway incarnation 안에서 재사용하지 않아야 한다. Connector SDK는 session마다 증가하는 counter로 `ConnectionId`를 할당하고 전송 순서가 이전 값보다 커야 하며, Gateway는 remote high-watermark 하나로 낮거나 같은 값을 거절해야 한다.
- **`TERM-018`**: `ShardDirectory`는 각 `ShardId`의 stable logical RT endpoint를 정확히 하나만 포함하고 `ClientId`의 remote binding mapping을 포함해서는 안 된다. 서로 독립적으로 쓰이는 복수 endpoint를 한 logical shard record에 넣어서는 안 된다.
- **`TERM-019`**: 하나의 `PublicationScope`는 `(GatewayId, ListenerSessionId, ShardId)`이고 동시에 최대 하나의 current lease를 가져야 한다.
- **`TERM-020`**: `PublicationRevision`은 같은 `PublicationScope` 안에서 lease가 바뀌어도 단조 증가해야 하며, 낮은 revision이 높은 revision의 current state를 대체해서는 안 된다.
- **`TERM-021`**: Gateway는 release, expiry 관찰 또는 lease 교체 뒤 새 lease를 게시할 때 재사용하지 않은 새 `LeaseId`를 발급해야 한다. RT가 tombstone을 보관하지 않아 이미 발급했던 늦은 `PublishCurrent`를 다시 관찰하는 것은 Gateway의 새 lease 발급이나 identity 재사용으로 간주하지 않는다.
- **`TERM-022`**: Gateway는 자신이 소유한 local binding과 publication 상태만 보관하고 RT 전체 mapping이나 과거 `Resolve` 결과를 current routing authority로 보관해서는 안 된다.
- **`TERM-023`**: 하나의 Connector SDK runtime은 동시에 `0..1`개의 current `ConnectorSession`을 가져야 한다. 재연결은 이전 identity를 재사용하지 않는 새 `ConnectorSessionId`를 만들어야 한다.
- **`TERM-024`**: 모든 connect attempt와 Connector Pipe endpoint는 정확히 하나의 `ConnectorSession`에 속하고 그 session보다 오래 존재해서는 안 된다. `ConnectorSessionId`는 application peer identity, credential 또는 delivery identity가 아니다.
- **`TERM-025`**: 하나의 Listener SDK runtime에는 같은 `ClientId`를 desired destination으로 가진 non-`CLOSED` Listener handle이 동시에 최대 하나만 존재해야 한다. 이 제한은 다른 SDK runtime이나 `ListenerSession`이 같은 `ClientId`를 등록하는 N:M 관계를 제한하지 않는다.
- **`TERM-026`**: 하나의 unordered Gateway pair에는 `DialerGatewayId`별 `PeerTransportSlot`이 하나씩 있어야 한다. 각 slot은 `READY` PeerTransport를 최대 하나만 가지므로 pair 전체의 `READY` PeerTransport는 최대 두 개다.
- **`TERM-027`**: `ShardDirectoryGeneration`은 generation 필드를 포함하지 않은 immutable shard directory artifact의 exact bytes를 SHA-256으로 계산해야 한다. 같은 bytes는 같은 generation을 만들고 artifact bytes가 바뀌면 generation을 다시 계산해야 하며, Gateway와 RT process는 시작 시 검증한 generation과 directory를 종료할 때까지 바꾸어서는 안 된다.
- **`TERM-028`**: `PeerTransport`의 dialer는 initiator bit `0`, acceptor는 bit `1`을 가져야 한다. 각 endpoint가 새 stream을 시작할 때 `StreamId = (local_counter << 1) | initiator_bit`로 할당하며, 같은 transport 안에서 실패한 OPEN을 포함해 counter와 `StreamId`를 재사용하거나 wrap해서는 안 된다.
- **`TERM-029`**: `ClientId`는 non-empty valid UTF-8이어야 하고 equality와 authority hash에 exact bytes를 사용해야 한다. Unicode normalization, case folding 또는 locale 변환을 암묵적으로 적용해서는 안 된다.

## 이 SPEC에서 정하지 않는 것

- 등록, publication과 제거 절차
- binding selection policy
- online 또는 rolling shard directory 변경
- 위에서 정하지 않은 identifier encoding과 생성 방식
- 상태명, 오류 code, timeout과 retry 값
