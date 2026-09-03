# RelayGate 설계 문서

이 디렉터리는 RelayGate의 결정, 관찰 가능한 계약과 검증 근거의 기준이다.

```text
RelayGate = logical destination resolution
          + one-hop Pipe establishment
          + opaque bidirectional byte relay
```

## 현재 구조

```text
Listener SDK ── session / binding ──► Owner Gateway ── snapshot / lease ──► RouteTable shard
Connector SDK ── open(ClientId) ────► Entry Gateway ── Resolve ──────────► RouteTable shard

Connector Pipe ◄══ Entry Gateway [── Owner Gateway, 최대 one hop ──] ══► Listener Pipe
```

`RouteTable`은 control plane에만 참여한다. Established Pipe의 opaque bytes는 local Gateway
또는 하나의 peer hop을 통해 전달된다.

## 책임

| 구성 요소 | Plane | 책임 |
| --- | --- | --- |
| 애플리케이션 | Application | payload protocol, peer 인증·인가, service 선택, aggregation, idempotency, 업무 retry |
| 배포 환경 | Management | process config, Gateway service identity, channel integrity와 component identity |
| SDK | Service interface | `Connector`, `Listener`, `Pipe` 사용자 계약 |
| Gateway | Control + Data | session과 local binding 소유, RT registration·resolve, binding 선택, Pipe 수립과 local/one-hop relay |
| RouteTable shard | Control | 한 `ShardDirectoryGeneration`의 현재 `ClientId -> BindingSet<MappingEntry>` authority와 registration lease |
| runtime observation | Operational | live session, binding, Pipe, transport liveness와 current-state snapshot |

Plane은 [RFC 7426](rfc/rfc-7426-sdn-architecture.md)의 개념적 책임 경계이며 process 경계가 아니다.

## 보장 경계

| RelayGate가 보장하는 것 | RelayGate가 보장하지 않는 것 |
| --- | --- |
| logical destination으로 Pipe 수립 | payload 해석과 application peer 인증·인가 |
| bounded relay와 transport-level cleanup | message 저장, delivery acknowledgement와 업무 retry |
| active lease에서 파생된 memory-only current mapping | RT persistence, replication과 consensus |
| local 또는 최대 one-hop peer relay | 닫힌 Pipe의 replay·resume·reroute와 multi-hop routing |
| startup config의 `ClientKey`로 binding 등록 권한 확인 | key 발급·영속화·hot rotation과 channel security 구현 |

Channel identity와 integrity는 배포 환경이 제공한다.

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
| [TEST 002](test/002-single-gateway-rust-compose-test-plan.md) | 단일 Gateway local Pipe 회귀 검증 |
| [TEST 003](test/003-route-table-core-test-plan.md) | memory-only RouteTable shard core 검증 |
| [TEST 004](test/004-rt2-gw3-closed-loop-test-plan.md) | 독립 RT shard 2개와 Gateway 3개의 one-hop closed-loop 검증 |
| [TEST 005](test/005-helm-deployment-test-plan.md) | Kubernetes resource 렌더링과 Gateway/RT 배포 계약 검증 |

TEST 문서의 존재 자체는 완전한 실행 증명을 뜻하지 않는다. 시나리오별 증거 수준은
[`001-executable-coverage.toml`](test/001-executable-coverage.toml)의 `executable/partial/gap` 상태가 기준이다.
