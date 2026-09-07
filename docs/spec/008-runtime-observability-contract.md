# SPEC 008: 전송 보안과 관측 계약

## 전송과 admission

```text
SDK <-> GW : TLS/TCP + server authentication + ClusterToken
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

- **`SEC-001`**: SDK는 명시적 custom CA 또는 bundled public Web PKI roots, 별도 server name과
  `relaygate/2` ALPN을 검증한 뒤에만 ClusterToken을 보낸다.
- **`SEC-002`**: Gateway는 current token 하나와 optional next token 하나만 허용한다.
- **`SEC-003`**: token 불일치는 SessionId/Binding/Pipe 없이 `UNAUTHENTICATED`로 끝난다.
- **`SEC-004`**: 내부 transport는 certificate, `relaygate/2` ALPN과 기존 logical identity를 모두 검증한다.
- **`SEC-005`**: TLS 실패는 평문 fallback을 하지 않는다.
- **`SEC-006`**: production server config는 certificate/key 경로를 요구한다.
- **`SEC-007`**: insecure transport는 명시적인 test-only config에서만 허용하며 Helm에는 노출하지 않는다.
- **`SEC-008`**: hop TLS는 application E2E 보호나 Pipe peer 인증이 아니다.
- **`SEC-009`**: public Relay API는 Gateway transport 종류와 독립적이며 0.2 구현은 명시적인 TLS/TCP 하나다.
- **`SEC-010`**: SDK-facing TLS와 내부 mTLS는 별도 Secret과 trust domain으로 운영할 수 있다.
- **`SEC-011`**: 외부 L4는 byte stream을 passthrough하며 Gateway가 SDK TLS를 종단한다.

## 로그

필수 lifecycle event:

```text
session admitted/rejected/removed
listener active/suspended/blocked/closed
dial result + code + observation
peer/RT connect, handshake, loss, recovery
drain start/deadline/complete
TLS handshake rejection
bounded queue/capacity rejection
```

DATA hot path에는 per-frame info 로그를 남기지 않습니다. 로그에는 credential, certificate private key,
payload와 무제한 error body를 넣지 않습니다.

## metric

지표는 합성된 단일 health 점수를 만들지 않고 장애 경계를 그대로 노출합니다.

| 질문 | 지표 | 의미 |
| --- | --- | --- |
| process가 scrape 가능한가 | Prometheus `up` | process/metrics endpoint 도달 가능성 |
| SDK admission이 열려 있는가 | `relaygate_gateway_draining` | `0`이면 신규 SDK work 허용, `1`이면 drain 중 |
| RT current state를 사용할 수 있는가 | `relaygate_gateway_route_dependency{state}` | `DISABLED`, `READY`, `DEGRADED`, `TERMINAL` one-hot 상태 |
| RT와 아직 수렴하지 않은 publication이 있는가 | `relaygate_gateway_route_registrations_unsynced` | Gateway가 관측한 미수렴 registration 수 |
| peer transport가 준비되었는가 | `relaygate_gateway_peer_transports_connecting`, `relaygate_gateway_peer_transports_ready` | 연결 중/재사용 가능한 transport 수 |
| 연결 생존성 판정이 실패하는가 | `relaygate_gateway_heartbeat_timeouts_total{transport}` | `sdk`, `peer` heartbeat timeout 누계 |

### RED와 latency

| 구간 | request/result | latency | 측정 경계 |
| --- | --- | --- | --- |
| SDK DIAL | `relaygate_gateway_dial_requests_total`, `relaygate_gateway_dial_results_total` | `relaygate_gateway_dial_duration_seconds` | Gateway가 DIAL을 수락한 시점부터 OPENED/실패/cancel까지 |
| publish | `relaygate_gateway_publish_results_total` | 없음 | publication의 terminal result |
| heartbeat | `relaygate_gateway_heartbeat_timeouts_total` | `relaygate_gateway_heartbeat_duration_seconds{transport}` | Gateway가 PING을 writer에 commit한 시점부터 일치하는 PONG까지 |
| GW가 체감한 RT | `relaygate_gateway_route_table_requests_total` | `relaygate_gateway_route_table_request_duration_seconds` | client queue 진입부터 RT 응답 또는 transport failure까지 |
| RT 내부 처리 | `relaygate_route_table_requests_total` | `relaygate_route_table_request_duration_seconds` | shard actor가 요청 처리를 시작한 시점부터 결과 생성까지 |
| peer lifecycle | `relaygate_gateway_peer_handshakes_total`, `relaygate_gateway_peer_transport_closed_total` | heartbeat latency 사용 | handshake 결과와 transport 종료 이유 |
| SDK reconnect | `relaygate_sdk_reconnect_attempts_total` | `relaygate_sdk_reconnect_duration_seconds` | reconnect episode 시작부터 session 복구까지 |

GW→RT client latency와 RT actor service latency의 차이는 local queue, socket과 network 비용입니다. heartbeat
latency는 liveness probe 표본이며 일반 DATA latency가 아닙니다. RelayGate는 Pipe payload와 application message
경계를 해석하지 않으므로 established Pipe의 DATA RTT를 core metric으로 추정하지 않습니다.

### USE와 current state

```text
Gateway: sessions, bindings, pending offers, live Pipes, remote DIAL attempts
Peer: connecting/ready transports, live streams
RouteTable: registrations, mappings, routes, expiry records
Saturation: writer queue rejection과 RESOURCE_EXHAUSTED terminal result
Recovery: reconnect duration, route dependency transition/recovery, lease expiry, drain
```

DestinationId, SessionId, BindingId, PipeId, Gateway 주소, credential과 자유형 error를 label로 쓰지 않습니다.
허용 label은 `operation`, `outcome`, `code`, `reason`, `state`, `direction`, `transport`처럼 값의 집합이
고정된 경우뿐입니다. identity가 필요한 분석은 bounded lifecycle log를 사용합니다.

## probe

| probe | 보장 | 보장하지 않음 |
| --- | --- | --- |
| startup/readiness `check` | TLS + ClusterToken HELLO/WELCOME | RT, Destination, Pipe, payload 성공 |
| liveness TCP | process가 socket을 수락할 수 있음 | control/data plane 정상 |
| topology test | local/one-hop/dial/Pipe byte 일치 | application 업무 성공 |
| Pipe latency probe | 고정된 payload/concurrency에서 established Pipe RTT 분포 | 모든 application payload의 latency |

- **`OBS-001`**: snapshot gauge는 event counter가 아니라 현재 값으로 갱신한다.
- **`OBS-002`**: cleanup 뒤 gauge는 baseline으로 돌아와야 한다.
- **`OBS-003`**: secret marker는 로그, metric과 error에 나타나지 않아야 한다.
- **`OBS-004`**: Helm annotation은 scrape endpoint만 노출하며 SDK/peer/RT port를 외부 공개하지 않는다.
- **`OBS-005`**: process, SDK admission, RT dependency와 peer health는 서로 독립된 지표로 관측한다.
- **`OBS-006`**: DIAL, GW→RT, RT actor와 heartbeat latency는 측정 경계가 다른 histogram으로 기록한다.
- **`OBS-007`**: SDK와 peer heartbeat timeout은 bounded `transport` label counter로 기록한다.
- **`OBS-008`**: established Pipe DATA RTT는 core metric으로 추정하지 않고 명시적인 latency probe로 측정한다.
