# RelayGate 설계 문서

> 상태: Proposed. ADR을 먼저 확정하고, SPEC과 TEST는 ADR에서 파생한다.

```text
RelayGate = logical destination resolution
          + one-hop Pipe establishment
          + opaque bidirectional byte relay
```

## 전체 구조

```text
Listener SDK
     │ long-lived session + registration
     ▼
Owner Gateway × G
     │ Gateway-local ListenerBinding = truth
     │ PublishCurrent(session-shard snapshot)
     ▼
RouteTable shard × R
     │ immutable ShardDirectoryGeneration
     │ ClientId -> BindingSet<BindingProjection>
     │ Resolve(Generation, ClientId)
     ▼
Entry Gateway × G
     ├── local binding ─────────────────────► Listener SDK
     └── remote binding ─► Owner Gateway ──► Listener SDK
                            one hop

Connector SDK ── ConnectorSession + connect(ClientId) ──► Entry Gateway
```

```text
Authority(Generation, ClientId) = exactly 1 logical shard
Endpoint(Generation, ShardId)   = exactly 1 stable logical endpoint
Mappings(ClientId)              = 0..N live binding projections
Payload는 RouteTable을 통과하지 않는다.

Generation = SHA-256(exact ShardDirectory artifact bytes)
```

Gateway는 RT 전체 mapping을 복제하거나 구독하지 않는다. 자신이 소유한 current binding만
session-shard snapshot으로 게시하고, 원격 연결이 필요할 때 해당 `ClientId`의 `BindingSet`만
조회한다.

등록 관계와 실제 연결은 다르다.

```text
ClientId *  ◄──── ListenerBinding ────►  * ListenerSession

registration = many-to-many
actual Pipe  = Connector 1 : 1 Listener

one Listener SDK runtime
  └── ClientId별 non-CLOSED Listener handle 0..1

one unordered Gateway pair
  ├── A가 dial한 reusable PeerTransport 0..1
  └── B가 dial한 reusable PeerTransport 0..1

one PeerTransport
  ├── dialer StreamId   = 0, 2, 4, ...
  ├── acceptor StreamId = 1, 3, 5, ...
  └── FIN(one direction) | CLOSE(normal) | RESET(failure)
```

RT의 `Release`와 expiry는 tombstone을 남기는 hard fence가 아니다. 늦은 `PublishCurrent`가 stale projection을 잠시 다시 만들 수 있지만, Owner Gateway가 `BindingId`를 재검증하므로 잘못된 Pipe는 열리지 않는다. 마지막 늦은 갱신 뒤에는 lease expiry로 다시 제거된다.

## 책임

| 구성 요소 | 책임 |
| --- | --- |
| 애플리케이션 | payload protocol, peer 인증·인가, service 선택, aggregation, idempotency, 업무 retry |
| 배포 환경 | SDK가 접속하는 Gateway service identity와 channel integrity, Gateway 간 및 Gateway-RT 간 component identity와 integrity |
| SDK | `Connector`, `Listener`, `Pipe` 사용자 계약 |
| Gateway | Connector/Listener session과 local binding 소유, RT publication, binding 선택, Pipe 수립, local/one-hop relay |
| RouteTable shard | 한 `ShardDirectoryGeneration`의 현재 `ClientId -> BindingSet<BindingProjection>` mapping authority와 publication lease |

```text
ClientKey        = ClientId binding 등록 권한
application data = endpoint가 해석하고 보호하는 opaque payload
component trust  = 배포 환경이 보장하며 application peer 인증과 별개
```

## 문서 권위

```text
IETF 원문 -> RFC 참고 노트 -> ADR -> SPEC -> TEST
```

| 계층 | 질문 | 포함하는 내용 |
| --- | --- | --- |
| [`rfc/`](rfc/README.md) | 일반 네트워크 이론은 무엇인가? | 비규범적 한글 요약과 원문 링크 |
| `adr/` | RelayGate는 무엇을, 왜 선택했는가? | 장기 경계, 핵심 모델, 결과와 비용 |
| `spec/` | 외부에서 관찰되는 동작은 무엇인가? | 객체, interface, 상태, 오류, timeout, retry |
| `test/` | 계약을 어떻게 증명하는가? | SPEC requirement와 test 대응 |

같은 규칙을 여러 문서에서 다시 정의하지 않는다. 상태와 오류의 최종 권위는 SPEC이며, TEST는 새 규칙을 만들지 않는다.

## ADR

| 문서 | 결정 |
| --- | --- |
| [ADR 001](adr/001-relayed-pipe-responsibility-boundary.md) | RelayGate는 logical destination으로 opaque bidirectional Pipe를 수립한다. |
| [ADR 002](adr/002-application-protocol-boundary.md) | 등록 권한 밖의 application 의미와 보안은 endpoint가 소유한다. |
| [ADR 003](adr/003-client-id-listener-binding.md) | 전역 binding은 many-to-many이고 runtime 내부 동일 ClientId Listener 중복은 거절한다. |
| [ADR 004](adr/004-current-state-routing-topology.md) | `RouteTable`은 content-hash generation의 hash-sharded mapping authority다. |
| [ADR 005](adr/005-soft-state-registration-lifecycle.md) | Route mapping은 tombstone을 두지 않는 current soft state다. |
| [ADR 006](adr/006-one-hop-peer-multiplexing.md) | Gateway data plane은 initiator-bit StreamId를 쓰는 one-hop multiplexed relay다. |

## SPEC

| 문서 | 소유 계약 |
| --- | --- |
| [SPEC 001](spec/001-terminology-and-object-model.md) | 용어, 객체, identifier와 scope |
| [SPEC 002](spec/002-sdk-pipe-contract.md) | SDK와 Pipe 사용자 계약 |
| [SPEC 003](spec/003-listener-registration-contract.md) | Gateway local registry와 RT publication lifecycle |
| [SPEC 004](spec/004-route-table-contract.md) | RouteTable schema와 `PublishCurrent/Refresh/Release/Resolve` 계약 |
| [SPEC 005](spec/005-connection-establishment-contract.md) | local lookup, resolve, binding 선택과 Pipe 수립 계약 |
| [SPEC 006](spec/006-peer-relay-contract.md) | one-hop peer relay와 multiplexing 계약 |
| [SPEC 007](spec/007-error-and-state-model.md) | canonical 오류, 상태와 failure observation |

## TEST

| 문서 | 검증 범위 |
| --- | --- |
| [TEST 001](test/001-requirement-test-matrix.md) | 전체 SPEC requirement와 edge case 대응 |
| [TEST 002](test/002-single-gateway-rust-compose-test-plan.md) | 단일 Gateway Rust·Docker Compose 첫 구현 profile |

## 제외 범위

```text
payload interpretation       application peer authentication protocol
application aggregation      message storage
Pipe replay / resume         multi-hop routing
RT persistence               RT replication / consensus
online shard reconfiguration
TLS / mTLS / service-mesh 선택
implementation language      module layout
```
