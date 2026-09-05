# RelayGate 문서

ADR은 결정 이력을 보존하고, SPEC과 TEST는 0.2 현재 계약만 다음 세 층으로 분리합니다.

```text
ADR   왜 이 구조를 선택했는가
SPEC  구현이 반드시 지킬 상태·동작 계약
TEST  각 계약을 어떤 실행 증거로 검증하는가
```

## 현재 구조

```text
                       hash(DestinationId)
                               │
                               v
                     RouteTable shards × M
                     Destination -> BindingSet
                               ▲
                               │ mTLS control plane
                               │
Relay A ── TLS ──► GW A ═════ GW B ◄── TLS ── Relay B
 listen/dial +             mTLS one hop       listen/dial +
 Listener.accept                              Listener.accept

data plane: Relay A <== GW A [-- GW B --] ==> Relay B
```

```text
RelaySession 1 -> Listener 0..N
Destination  * <-> Binding <-> * RelaySession
dial 1회 -> eligible Binding 1개 -> Pipe 1개
```

`DestinationId`는 애플리케이션 소유 UUIDv4입니다. RT는 live Binding에서 파생된 현재 상태만
유지합니다. 세션이 끊기면 해당 Binding과 Pipe를 정리하고 SDK가 live Listener를 새 세션에 다시
등록합니다. 기존 Pipe와 payload는 복구하지 않습니다.

## 책임

| 구성요소 | 소유 책임 |
| --- | --- |
| SDK | Gateway TLS 검증, ClusterToken 제시, Relay 세션 재연결, Listener 재등록, Pipe API |
| Gateway | 세션 admission, local Binding, dial 선택, RT 등록/조회, one-hop relay, bounded cleanup |
| RouteTable | shard별 lease 기반 current `Destination -> BindingSet` |
| Transport | SDK TLS와 내부 mTLS 설정·handshake |
| Server | process config, dependency wiring, readiness, metric, shutdown |
| Application | Destination 생성·보관, Pipe 상대 인증·인가, payload 의미·재시도·필요한 E2E 보호 |
| Helm | RT/GW resource와 외부 Secret file 배선 |

## 현재 ADR

| ADR | 결정 |
| --- | --- |
| [004](adr/004-current-state-routing-topology.md) | current-state RouteTable을 hash shard로 나눈다. |
| [005](adr/005-soft-state-registration-lifecycle.md) | mapping은 lease가 갱신되는 동안만 존재한다. |
| [006](adr/006-one-hop-peer-multiplexing.md) | remote data path는 최대 one hop이며 PeerTransport를 재사용한다. |
| [007](adr/007-transport-liveness-and-idle-retirement.md) | heartbeat와 idle retirement로 transport 수명을 닫는다. |
| [008](adr/008-operational-health-boundaries.md) | readiness와 data-plane 성공을 구분한다. |
| [009](adr/009-bounded-gateway-drain-and-reconnect-jitter.md) | drain과 reconnect 폭주를 bounded하게 만든다. |
| [010](adr/010-symmetric-relay-session.md) | 하나의 Relay 세션이 송신과 수신을 모두 수행한다. |
| [011](adr/011-public-destination-access.md) | ClusterToken은 trust-domain admission만 담당한다. |
| [012](adr/012-deployment-transport-security.md) | SDK 구간은 TLS, 내부 구간은 mTLS를 사용한다. |
| [013](adr/013-application-owned-destination.md) | DestinationId는 application-owned UUIDv4다. |
| [014](adr/014-sdk-transport-and-l4-boundary.md) | SDK transport와 platform L4 진입점을 분리한다. |

ADR 001–003은 0.1 역할/권한 모델의 이력입니다. Pipe와 application 책임 경계는 유지되지만,
고정 Connector/Listener 역할, `ClientId`와 `ClientKey`는 현재 계약이 아닙니다. 현재 해석은
ADR 010–013과 SPEC을 따릅니다.

## SPEC

| SPEC | 계약 |
| --- | --- |
| [001](spec/001-terminology-and-object-model.md) | 용어, identity와 cardinality |
| [002](spec/002-sdk-pipe-contract.md) | public SDK, 재연결과 Pipe |
| [003](spec/003-destination-binding-contract.md) | Listener, Binding과 publication |
| [004](spec/004-route-table-contract.md) | memory-only shard와 lease |
| [005](spec/005-connection-establishment-contract.md) | dial, selection과 observation |
| [006](spec/006-peer-relay-contract.md) | one-hop multiplexing |
| [007](spec/007-error-and-state-model.md) | 오류와 canonical 상태 전이 |
| [008](spec/008-runtime-observability-contract.md) | TLS, 로그, metric과 probe |

## TEST

[TEST 001](test/001-requirement-test-matrix.md)이 requirement와 실행 증거의 canonical 대응표입니다.
Rust 검증, Compose, Helm render와 kind acceptance를 구분하며 정적 render만으로 runtime 보안을
입증하지 않습니다.

기술적 배경은 [RFC 요약](rfc/)에 있으며, RFC는 RelayGate 고유 정책을 대신 정의하지 않습니다.
