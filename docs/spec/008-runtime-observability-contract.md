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

DATA RTT와 payload goodput은 명시적으로 실행한 SDK probe로 측정합니다. Lifecycle log field는 bounded identifier와 outcome을
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
| GW DIAL | `relaygate_gateway_dial_requests_total`, `relaygate_gateway_dial_results_total`, `relaygate_gateway_dial_duration_seconds` | Gateway DIAL admission → OPENED/failure/cancel 결정 |
| SDK 접속·DIAL | `relaygate_sdk_operation_results_total`, `relaygate_sdk_operation_duration_seconds` | `session_connect`: transport·TLS·HELLO/WELCOME 1회; `dial`: API 진입 → Pipe 반환·실패·polled future 취소 |
| publish | `relaygate_gateway_publish_results_total` | terminal result counter |
| heartbeat | timeout counter + `relaygate_gateway_heartbeat_duration_seconds{transport}` | committed PING → matching PONG |
| Gateway→RT | `relaygate_gateway_route_table_requests_total`, `relaygate_gateway_route_table_request_duration_seconds` | client queue admission → response/failure |
| RT actor | `relaygate_route_table_requests_total`, `relaygate_route_table_request_duration_seconds` | actor service start → result |
| peer | `relaygate_gateway_peer_handshakes_total`, `relaygate_gateway_peer_transport_closed_total` | transport lifecycle outcome |
| SDK reconnect | `relaygate_sdk_reconnect_attempts_total`, `relaygate_sdk_reconnect_episode_duration_seconds{outcome}` | episode start → `recovered/closed/aborted`; recovered는 session + desired Listener 수렴 |
| SDK 미복구 | `relaygate_sdk_reconnect_in_progress` | process 내 진행 중 episode 수; 회복·close·task drop에서 감소 |

```text
SDK session_connect → SDK dial → established Pipe DATA RTT
                      └─ GW DIAL → 필요 시 GW→RT 요청
                                   └─ RT actor service
```

GW→RT와 RT actor histogram은 측정 구간과 표본이 다릅니다. 비교는 병목 후보를 좁히는 근거이며 두 p95의
차이를 network p95로 해석하지 않습니다. Heartbeat는 liveness RTT, DATA RTT는 payload 왕복입니다.
SDK dial 시간은 session·queue 대기를 포함하고 application의 여러 호출/retry 전체 시간은 application이 측정합니다.

| DIAL `class` | code |
| --- | --- |
| `success` | `ok` |
| `request` | `invalid_argument`, `unauthenticated`, `permission_denied`, `not_found`, `failed_precondition`, `protocol_error`, `already_exists` |
| `capacity` | `resource_exhausted` |
| `availability` | `unavailable`, `deadline_exceeded` |
| `internal` | `internal` |
| `cancelled` | `cancelled` |

분류는 결과 종류이며 장애 책임의 판정은 code·lifecycle log와 함께 합니다. 무요청 구간에는 결과 비율이 없습니다.

### USE와 current state

| 영역 | 관측값 |
| --- | --- |
| Gateway | SDK sessions, bindings, pending offers, GW-local Pipe states, remote DIAL attempts |
| 고유 Pipe | `relaygate_gateway_originated_pipes`: 호출 SDK의 GW에서 한 번 집계 |
| Peer | connecting/ready transport endpoints, stream endpoints; one-hop 양단 포함 |
| Capacity | `relaygate_gateway_resource_used/limit{resource}`: 현재 점유 / 설정 상한 |
| RouteTable | registrations, mappings, routes, expiry records |
| Saturation | writer rejection, `RESOURCE_EXHAUSTED` result |
| Recovery | reconnect 진행 개수·종료 시간, dependency transition, lease expiry, drain |

| `resource` | used | limit |
| --- | --- | --- |
| `sessions` | handshake 포함 SDK transport semaphore 점유 | max sessions |
| `bindings` | local bindings | max bindings |
| `pending_opens` | pending offers + remote attempts | max pending offers |
| `remote_dials` | remote attempts | max remote dial attempts |
| `pipes` | GW-local open Pipe states | max live Pipes |

설정 상한은 지속 가능한 처리량과 다릅니다. SDK session·Pipe 수의 사용자 수 환산은 application 모델이 정합니다.
`pending_opens`는 OFFER와 remote attempt의 공통 예산이고 `remote_dials`는 그 안의 remote 하위 예산입니다.
각 자원의 사용률은 해당 제약을 독립 판정하며 자원 간 사용률 합산으로 전체 여유를 계산하지 않습니다.
snapshot은 GW별 순간 관측이며 cluster 합계는 전역 원자적 값이 아닙니다.

Metric label set은 `operation`, `outcome`, `code`, `class`, `reason`, `resource`, `state`, `direction`, `transport`처럼 bounded
enumeration으로 구성합니다. Instance identity는 Prometheus target metadata, request identity는 lifecycle log가
담당합니다.

## 수집과 해석

| 대상 | 수집 계약 |
| --- | --- |
| GW·RT | Prometheus target의 `cluster`, `namespace`, `instance` label을 유지한다. Compose는 `compose/relaygate`를 사용한다. |
| SDK | application이 recorder와 scrape endpoint를 설치하고 같은 배포 범위를 나타내는 target label을 부여한다. |
| Kubernetes | cAdvisor의 Pod별 CPU·throttling·working set·RSS·network와 kube-state-metrics의 limit·replica를 사용한다. |
| 미수집 | No data로 표현한다. `up`은 발견된 target 기준이며 desired replica는 별도 비교한다. |
| 메모리 | logical state baseline 복귀와 반복 부하 뒤 working set/RSS 추세를 함께 관측한다. 실제 heap 소유권 분석은 별도 profile로 검증한다. |
| 네트워크 | Pod RX/TX는 relay 양단과 transport 비용을 포함한다. DATA probe의 echo payload goodput과 구분한다. |

SDK reconnect의 연속 비영 구간은 process에 미복구 episode가 존재한 시간입니다. 개별 episode의 최대 나이와는
다릅니다. 종료 histogram은 완료 표본이며 진행 중인 count와 함께 읽습니다.

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
| `OBS-010` | 대시보드의 runtime selector는 cluster·namespace 범위를 일관되게 적용한다. |
| `OBS-011` | 고유 Pipe와 GW-local Pipe 상태를 구분하며 resource used/limit의 집계 기준을 일치시킨다. |
| `OBS-012` | SDK 계측은 error·polled future 취소·reconnect 미완료와 종료를 구분한다. |
| `OBS-013` | 결과 분류와 gauge/rate 단위를 유지하고 실제 PromQL 기대값으로 검증한다. |
