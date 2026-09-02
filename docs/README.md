# RelayGate 설계 문서

이 디렉터리는 RelayGate의 결정, 관찰 가능한 계약과 검증 근거의 기준이다.

```text
RelayGate = logical destination resolution
          + one-hop Pipe establishment
          + opaque bidirectional byte relay
```

## 책임

| 구성 요소 | 책임 |
| --- | --- |
| 애플리케이션 | payload protocol, peer 인증·인가, service 선택, aggregation, idempotency, 업무 retry |
| 배포 환경 | SDK가 접속하는 Gateway service identity와 channel integrity, Gateway 간 및 Gateway-RT 간 component identity와 integrity |
| SDK | `Connector`, `Listener`, `Pipe` 사용자 계약 |
| Gateway | Connector/Listener session과 local binding 소유, RT registration, binding 선택, Pipe 수립, local/one-hop relay |
| RouteTable shard | 한 `ShardDirectoryGeneration`의 현재 `ClientId -> BindingSet<MappingEntry>` mapping authority와 registration lease |

```text
ClientKey        = startup config의 고정 ClientId binding 등록 권한
application data = endpoint가 해석하고 보호하는 opaque payload
component trust  = 배포 환경이 보장하며 application peer 인증과 별개
```

## Plane 대응

[RFC 7426](rfc/rfc-7426-sdn-architecture.md)의 용어로 책임을 다음처럼 구분한다.

| Plane 또는 interface | RelayGate에서의 범위 |
| --- | --- |
| Application Plane | SDK를 사용하는 application과 application 소유 protocol·정책 |
| Service interface | public SDK의 `Connector`, `Listener`, `Pipe` API |
| Control Plane | RT mapping과 Gateway의 registration, resolve, binding 선택 |
| Data / Forwarding Plane | established SDK-Gateway Pipe byte path, local Pipe와 one-hop peer byte relay |
| Operational Plane | live session, binding, Pipe, transport liveness와 current-state snapshot |
| Management Plane | process boot, config, deployment, health, logs와 metrics |

Plane은 책임을 설명하는 개념적 경계다. 하나의 process나 crate가 여러 plane의 기능을
포함할 수 있으며 plane마다 별도 process나 protocol을 요구하지 않는다.

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

같은 규칙을 여러 문서에서 다시 정의하지 않는다. 상태와 오류의 최종 권위는 SPEC이며,
TEST는 새 규칙을 만들지 않는다.

## 현재 구현과 검증 범위

현재 Rust 구현은 memory-only RouteTable 1개와 Gateway 3개의 one-hop profile에서 local/remote
Pipe 수립, RT 단절·재시작, Gateway 재시작과 current-state 재등록의 주요 경로를 CI로 검증한다.
[TEST 004](test/004-rt1-gw3-closed-loop-test-plan.md)가 이 profile을 정의한다.

이는 [TEST 001](test/001-requirement-test-matrix.md)의 모든 경쟁 조건과 조합이 실행으로 완전히
증명되었다는 뜻은 아니다. 전체 요구사항의 현재 실행 증거와 `partial` 범위는
[`001-executable-coverage.toml`](test/001-executable-coverage.toml)이 기준이다.

## ADR

| 문서 | 결정 |
| --- | --- |
| [ADR 001](adr/001-relayed-pipe-responsibility-boundary.md) | RelayGate는 logical destination으로 opaque bidirectional Pipe를 수립한다. |
| [ADR 002](adr/002-application-protocol-boundary.md) | 등록 권한 밖의 application 의미와 보안은 endpoint가 소유한다. |
| [ADR 003](adr/003-client-id-listener-binding.md) | 전역 binding은 many-to-many이고 runtime 내부 동일 ClientId Listener 중복은 거절한다. |
| [ADR 004](adr/004-current-state-routing-topology.md) | `RouteTable`은 content-hash generation의 hash-sharded mapping authority다. |
| [ADR 005](adr/005-soft-state-registration-lifecycle.md) | Route mapping은 active registration lease에 연결된 current soft state다. |
| [ADR 006](adr/006-one-hop-peer-multiplexing.md) | Gateway data plane은 initiator-bit StreamId를 쓰는 one-hop multiplexed relay다. |
| [ADR 007](adr/007-transport-liveness-and-idle-retirement.md) | transport liveness와 zero-stream idle retirement를 분리한다. |
| [ADR 008](adr/008-operational-health-boundaries.md) | process liveness, SDK admission readiness와 RT dependency health를 분리한다. |

## SPEC

| 문서 | 소유 계약 |
| --- | --- |
| [SPEC 001](spec/001-terminology-and-object-model.md) | 용어, 객체, identifier와 scope |
| [SPEC 002](spec/002-sdk-pipe-contract.md) | SDK와 Pipe 사용자 계약 |
| [SPEC 003](spec/003-listener-registration-contract.md) | Gateway local registry와 RT registration lifecycle |
| [SPEC 004](spec/004-route-table-contract.md) | RouteTable schema와 `Register/Update/KeepAlive/Deregister/Resolve` 계약 |
| [SPEC 005](spec/005-connection-establishment-contract.md) | local lookup, resolve, binding 선택과 Pipe 수립 계약 |
| [SPEC 006](spec/006-peer-relay-contract.md) | one-hop peer relay와 multiplexing 계약 |
| [SPEC 007](spec/007-error-and-state-model.md) | canonical 오류, 상태와 failure observation |
| [SPEC 008](spec/008-runtime-observability-contract.md) | runtime 로그, lifecycle event와 Gateway snapshot |

## TEST

| 문서 | 검증 범위 |
| --- | --- |
| [TEST 001](test/001-requirement-test-matrix.md) | 전체 SPEC requirement와 edge case 대응 |
| [TEST 001 실행 증거](test/001-executable-coverage.toml) | TEST 001 시나리오와 현재 Rust test의 기계 검증 가능한 연결 |
| [TEST 002](test/002-single-gateway-rust-compose-test-plan.md) | 단일 Gateway local Pipe Rust 회귀 profile |
| [TEST 003](test/003-route-table-core-test-plan.md) | memory-only RouteTable shard core 구현과 결정적 검증 profile |
| [TEST 004](test/004-rt1-gw3-closed-loop-test-plan.md) | RT 1개와 Gateway 3개의 one-hop closed-loop 구현 profile |

## 제외 범위

```text
payload interpretation       application peer authentication protocol
application aggregation      message storage
Pipe replay / resume         multi-hop routing
RT persistence               RT replication / consensus
online shard reconfiguration
TLS / mTLS / service-mesh 선택
implementation language      module layout
RT shard 증설 운영 절차
```
