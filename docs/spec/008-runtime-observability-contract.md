# SPEC 008: 전송 보안과 관측 계약

## 전송과 admission

```text
SDK <-> GW : TLS/TCP + server authentication + ClusterToken
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

| ID | 계약 |
| --- | --- |
| `SEC-001` | SDK는 CA trust source, server name과 `relaygate/2` ALPN 검증 뒤 ClusterToken을 전송한다. |
| `SEC-002` | Gateway token set은 current 하나와 optional next 하나다. |
| `SEC-003` | token mismatch는 state 생성 전 `UNAUTHENTICATED`로 끝난다. |
| `SEC-004` | internal transport는 certificate, `relaygate/2` ALPN과 logical identity를 함께 검증한다. |
| `SEC-005` | TLS failure는 평문 fallback 없는 terminal connection failure이며 새 retry는 새 TLS connection으로 시작한다. |
| `SEC-006` | production server config는 certificate/key path를 필수로 가진다. |
| `SEC-007` | insecure transport의 scope는 explicit test-only config다. |
| `SEC-008` | hop TLS는 transport peer를 보호하고 application E2E/peer auth는 Pipe 위 application protocol이 담당한다. |
| `SEC-009` | public Relay API는 transport-independent이고 current adapter는 TLS/TCP다. |
| `SEC-010` | SDK edge와 internal mTLS는 독립 Secret·trust domain이다. |
| `SEC-011` | external L4는 byte stream passthrough, Gateway는 SDK TLS termination을 담당한다. |

## 로그

| category | lifecycle event |
| --- | --- |
| session | admitted, rejected, removed |
| Listener | active, suspended, blocked, closed |
| dial | result, code, observation |
| dependency | peer/RT connect, handshake, loss, recovery |
| shutdown | drain start, deadline, complete |
| protection | TLS rejection, queue/capacity rejection |

DATA hot path는 aggregate metric으로 관측합니다. Lifecycle log field는 bounded identifier와 outcome을
사용하고 credential, private key, payload와 free-form error body는 redaction합니다.

## metric

| 운영 질문 | metric | 판정 |
| --- | --- | --- |
| process scrape | Prometheus `up` | process/endpoint reachability |
| SDK admission | `relaygate_gateway_sdk_admission_ready`, `relaygate_gateway_draining` | non-draining + session capacity |
| RT dependency | `relaygate_gateway_route_dependency{state}` | `DISABLED/READY/DEGRADED/TERMINAL` one-hot |
| RT convergence | `relaygate_gateway_route_registrations_unsynced` | pending registration 수 |
| peer state | `relaygate_gateway_peer_transports_connecting`, `relaygate_gateway_peer_transports_ready` | connecting·reusable transport 수 |
| liveness failure | `relaygate_gateway_heartbeat_timeouts_total{transport}` | SDK/peer timeout 누계 |

### RED와 latency

| 구간 | metric | 측정 경계 |
| --- | --- | --- |
| SDK DIAL | `relaygate_gateway_dial_requests_total`, `relaygate_gateway_dial_results_total`, `relaygate_gateway_dial_duration_seconds` | DIAL admission → OPENED/failure/cancel |
| publish | `relaygate_gateway_publish_results_total` | terminal result counter |
| heartbeat | timeout counter + `relaygate_gateway_heartbeat_duration_seconds{transport}` | committed PING → matching PONG |
| Gateway→RT | `relaygate_gateway_route_table_requests_total`, `relaygate_gateway_route_table_request_duration_seconds` | client queue admission → response/failure |
| RT actor | `relaygate_route_table_requests_total`, `relaygate_route_table_request_duration_seconds` | actor service start → result |
| peer | `relaygate_gateway_peer_handshakes_total`, `relaygate_gateway_peer_transport_closed_total` | transport lifecycle outcome |
| SDK reconnect | `relaygate_sdk_reconnect_attempts_total`, `relaygate_sdk_reconnect_duration_seconds` | episode start → active session |

Gateway→RT와 RT actor latency의 차이는 client queue, socket과 network 비용입니다. Heartbeat는 liveness RTT,
Pipe latency probe는 고정 payload·concurrency의 application DATA RTT를 측정합니다.

### USE와 current state

| 영역 | 관측값 |
| --- | --- |
| Gateway | sessions, bindings, pending offers, live Pipes, remote DIAL attempts |
| Peer | connecting/ready transports, live streams |
| RouteTable | registrations, mappings, routes, expiry records |
| Saturation | writer rejection, `RESOURCE_EXHAUSTED` result |
| Recovery | reconnect duration, dependency transition, lease expiry, drain |

Metric label set은 `operation`, `outcome`, `code`, `reason`, `state`, `direction`, `transport`처럼 bounded
enumeration으로 구성합니다. Instance identity는 Prometheus target metadata, request identity는 lifecycle log가
담당합니다.

## probe

| probe | 판정 범위 | 상위 검증 |
| --- | --- | --- |
| startup/readiness `check` | TLS + ClusterToken `HELLO/WELCOME` | topology test가 RT·Destination·Pipe 검증 |
| liveness TCP | process socket reachability | health metric이 control/data plane 구분 |
| topology test | local/one-hop/dial/Pipe byte | application test가 업무 성공 검증 |
| Pipe latency probe | 고정 workload의 established Pipe RTT | application benchmark가 실제 payload 특성 검증 |

| ID | 계약 |
| --- | --- |
| `OBS-001` | snapshot gauge는 current value를 기록한다. |
| `OBS-002` | cleanup 뒤 gauge는 baseline으로 수렴한다. |
| `OBS-003` | secret marker의 log·metric·error cardinality는 0이다. |
| `OBS-004` | Helm scrape surface는 metrics endpoint로 한정된다. |
| `OBS-005` | process, SDK admission, RT dependency와 peer health를 독립 지표로 관측한다. |
| `OBS-006` | DIAL, GW→RT, RT actor와 heartbeat는 각 측정 경계의 histogram을 가진다. |
| `OBS-007` | SDK/peer heartbeat timeout은 bounded `transport` label counter다. |
| `OBS-008` | DATA RTT는 explicit established-Pipe latency probe가 측정한다. |
| `OBS-009` | SDK admission ready는 non-draining과 session semaphore capacity의 conjunction이다. |
