# SPEC 008: 전송 보안과 관측 계약

## 전송과 admission

```text
SDK <-> GW : TLS/TCP + server authentication + ClusterToken
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

위 표는 기본값이다. `RELAYGATE_INTERNAL_TRANSPORT=plaintext`는 내부 두 구간만 평문 TCP로
실행하고 SDK TLS를 유지한다. 내부 plaintext는 인증과 전송 암호화가 없는 격리 테스트 모드다.

| ID | 계약 |
| --- | --- |
| `SEC-001` | SDK는 CA trust source, server name과 `relaygate/2` ALPN 검증 뒤 ClusterToken을 전송한다. |
| `SEC-002` | Gateway token set은 current 하나와 optional next 하나다. |
| `SEC-003` | token mismatch는 state 생성 전 `UNAUTHENTICATED`로 끝난다. |
| `SEC-004` | internal mTLS는 신뢰 CA·Gateway client DNS SAN·서버 DNS SAN·`relaygate/2` ALPN을 검증한다. logical identity는 incarnation/owner fencing에 사용한다. 명시적 plaintext는 무인증 logical handshake만 수행한다. |
| `SEC-005` | TLS failure는 평문 fallback 없는 terminal connection failure이며 새 retry는 새 TLS connection으로 시작한다. |
| `SEC-006` | SDK TLS와 internal mTLS config는 certificate/key path를 필수로 가진다. internal plaintext는 내부 certificate mount가 없다. |
| `SEC-007` | internal mode는 `mtls` 기본값 또는 명시적 `plaintext`다. unknown mode와 legacy test flag 혼용은 startup failure다. legacy 전체 평문은 test-only config다. |
| `SEC-008` | hop TLS는 transport peer를 보호하고 application E2E/peer auth는 Pipe 위 application protocol이 담당한다. |
| `SEC-009` | public Relay API는 transport-independent이고 current adapter는 TLS/TCP다. |
| `SEC-010` | SDK edge와 internal mTLS는 독립 Secret·trust domain이다. |
| `SEC-011` | external L4는 byte stream passthrough, Gateway는 SDK TLS termination을 담당한다. |
| `SEC-012` | SDK accept는 전체 transport slot과 별도 handshake slot을 TLS 전에 확보한다. handshake 상한 도달 시 새 socket을 닫고 기존 session을 유지한다. |
| `SEC-013` | 인증 전 frame payload 상한은 `min(max_frame_len, 65537)` bytes다. HELLO 교환은 읽기와 WELCOME/거절 쓰기를 합쳐 5초 이내 끝내고, 성공 뒤 일반 frame 한도로 전환하며 이미 읽은 다음 frame을 보존한다. |
| `SEC-014` | SDK 신규 socket은 slot·TLS 처리 전 GW-local token bucket을 통과한다. 초기 burst와 초당 refill을 제한하고 token 부족은 새 socket만 종료한다. clone은 같은 예산을 공유한다. |
| `SEC-015` | SDK `PUBLISH/DIAL`은 session별·GW 전체의 공유 제어 예산을 통과한 뒤 Binding 생성·조회·OFFER를 수행한다. 초과 요청은 `RESOURCE_EXHAUSTED`로 끝나며 DIAL observation은 `NOT_OBSERVED`다. |

### SDK handshake 보호

```text
accept → rate token → transport + handshake slot → TLS(5s) → HELLO/응답(5s)
                                                 ├─ 성공 → handshake slot 반환 → session
                                                 └─ 실패 → owned state + 모든 slot 반환
```

| 설정 | 기본값·의미 |
| --- | --- |
| `RELAYGATE_MAX_PENDING_HANDSHAKES` | 256; 실제 한도는 `min(설정값, MAX_SESSIONS)`. 0은 시작 실패 |
| `RELAYGATE_MAX_SESSIONS` | 10,000; handshake와 admitted session을 포함한 전체 transport 수 |
| `RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND` | 256; 초당 신규 admission token refill. 0은 시작 실패 |
| `RELAYGATE_SDK_CONNECTION_BURST` | 256; 초기·유휴 후 token 최대 보유량. 0은 시작 실패 |
| HELLO payload | u16 token 길이 2 bytes + 최대 65,535 bytes; 기존 wire 범위 유지 |

256은 초기 동시 handshake 보호 상한이며 처리량 보장 수치가 아니다. 기존 Relay의 managed reconnect는 SDK backoff로 분산한다.
초기 `Relay::connect`는 단일 시도이므로 admission 거절 뒤 재시도는 application이 결정한다. 운영 부하에 맞춰 상한을 조정한다.
포화 중에는 SDK readiness도 저하되며 readiness probe와 SDK listener에 들어오는 TCP probe가 같은 예산을 사용한다.
snapshot의 readiness 조회는 token을 소비하지 않는다. 기존 session은 유지하며 process liveness는 별도 TCP probe로 관측한다.
Token은 TLS 실패·인증 실패·slot 부족·연결 종료에도 반환하지 않고 시간 경과로만 보충한다.
임의의 t초 구간에서 rate gate 통과 수는 `burst + rate × t` 이하이며 gate 뒤의 실제 admission은 더 적을 수 있다.
기본값은 초기 보호 정책이며 처리량 보장이 아니다. 재접속 burst와 정상 접속 지연을 측정해 조정한다.
이 제한은 GW-local이다. GW 재시작은 burst를 초기화하고 replica 증가는 총 예산을 늘린다.
인증 후 제어 메시지는 아래의 별도 예산을 사용한다. 사용자별 quota·분산 DDoS, TCP accept 자체의 CPU와 상위 회선 포화는 별도 보호 경계다.
rate 거절은 `reason="rate_limit"` counter로 집계한다. 고빈도 거절마다 로그를 만들지 않는다.
인증 후 Pipe 전송 크기와 ClusterToken 계약은 유지한다.

### SDK 제어 요청 보호

```text
PUBLISH / DIAL → session token → Gateway token → 기존 처리
                     └─ 부족 ─────┴─ 부족 → 요청 실패
DATA / PING / OFFER 응답 / UNPUBLISH / CANCEL / FIN / CLOSE / RESET → 기존 처리
```

| 설정 | 기본값 |
| --- | --- |
| `RELAYGATE_CONTROL_RATE_PER_SECOND` / `RELAYGATE_CONTROL_BURST` | GW 전체 4,096/s · burst 4,096 |
| `RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND` / `RELAYGATE_SESSION_CONTROL_BURST` | session별 256/s · burst 256 |

두 operation은 같은 bucket을 공유한다. session token부터 소비하므로 session 예산이 없는 요청은 GW token을 소비하지 않는다.
소비한 token은 요청 결과·연결 종료와 무관하게 시간으로만 보충한다. 0·잘못된 설정은 시작 실패다.
session 종료는 해당 bucket을 제거하고 재연결은 새 session bucket을 만든다. GW bucket은 다른 session과 함께 유지한다.
거절은 registry 변경·RT Resolve·peer OPEN 전에 결정한다. DIAL의 ConnectionId fence는 거절 후에도 유지한다.
기존 Binding·Pipe와 정리 메시지는 이 제한 때문에 종료되거나 차단되지 않는다. 응답 writer 포화·transport loss는 기존 session cleanup 계약을 따른다.
초기 listen과 dial 실패의 새 시도는 application이 판단한다. 이미 반환된 Listener의 republish 실패는 SDK의 기존 transient 복구 경로를 따른다.
기본값은 보호 정책이며 운영 용량 보장이 아니다. 여러 session을 가진 사용자의 공정성, 거절·정리 frame 처리 CPU와 payload 대역폭 제한은 이 예산의 범위 밖이다.
SDK admission readiness는 새 transport 가능성을 유지해서 표현하고, 제어 예산 거절은 아래 counter로 별도 관측한다.

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
| SDK admission | `relaygate_gateway_sdk_admission_ready`, `relaygate_gateway_draining` | non-draining + transport·handshake capacity + rate token 여유 |
| SDK handshake 포화 | `relaygate_gateway_resource_used{resource="sdk_handshakes"}`, `relaygate_gateway_resource_limit{resource="sdk_handshakes"}` | TLS/HELLO 진행 수·상한 |
| SDK admission 거절 | `relaygate_gateway_sdk_transport_rejections_total{reason}` | `rate_limit` / `session_limit` / `handshake_limit` / `cluster_token`; rate 거절은 counter, 나머지 요청별 로그는 debug |
| SDK 제어 요청 거절 | `relaygate_gateway_control_rejections_total{operation,scope}` | `publish` / `dial` × `session` / `gateway`; 기존 operation RED에도 실패 집계 |
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

Metric label set은 `operation`, `outcome`, `code`, `class`, `reason`, `scope`, `resource`, `state`, `direction`, `transport`처럼 bounded
enumeration으로 구성합니다. Instance identity는 Prometheus target metadata, request identity는 lifecycle log가
담당합니다.

## 수집과 해석

### 운영 화면

```text
운영 개요: 준비율 · 세션/Pipe → 요청 · 지연 · 결과 · 용량
  ├─ GW·RT 진단: 연결/큐 → 라우팅/수렴 → Kubernetes
  └─ SDK 복구: 진행 중 재연결 ↔ 종료 시간, 접속/DIAL
```

| 화면 | 표시 |
| --- | --- |
| 운영 개요 | instant 숫자 4개 + 추이 4개; 자원 점유율은 선택 GW 중 자원별 최댓값 |
| GW·RT 진단 | 수집 가능률 + 접힌 진단 영역 3개; 개별 GW·RT·Pod 필터 |
| SDK 복구 | 애플리케이션 수집 전제와 미수집 표시, DATA RTT와 다른 시간 경계 |

시간·공통 필터를 유지하는 링크로 이동한다. 상세 숫자·비율의 해석은 아래 수집 계약을 따른다.
화면 구성은 [Design](../../DESIGN.md), 화면·쿼리 검증은 [TEST 006](../test/006-local-observability-test-plan.md)이 소유한다.

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
| `OBS-009` | SDK admission ready는 non-draining, transport·handshake slot 여유와 rate token 여유의 conjunction이다. |
| `OBS-010` | 대시보드의 runtime selector는 cluster·namespace 범위를 일관되게 적용한다. |
| `OBS-011` | 고유 Pipe와 GW-local Pipe 상태를 구분하며 resource used/limit의 집계 기준을 일치시킨다. |
| `OBS-012` | SDK 계측은 error·polled future 취소·reconnect 미완료와 종료를 구분한다. |
| `OBS-013` | 결과 분류와 gauge/rate 단위를 유지하고 실제 PromQL 기대값으로 검증한다. |
