# SPEC 008: 전송과 관측 계약

## 전송

```text
SDK <-> GW : TLS/TCP + server authentication + credential-free HELLO
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake
```

위 구성이 기본값입니다. `RELAYGATE_INTERNAL_TRANSPORT=plaintext`는 내부 두 구간만 평문 TCP로 실행하고
SDK TLS를 유지합니다. SDK edge는 독립적으로 `RELAYGATE_SDK_TRANSPORT=tls|plaintext`를 사용합니다(`tls`
기본). SDK Gateway endpoint의 `tcp://`는 plaintext에 대응하며 access token과 payload를 암호화하지 않습니다. Unknown mode,
명시적 mode와 legacy test flag 혼용은 시작 실패입니다. Readiness도 같은 mode를 사용합니다.

| ID | 계약 |
| --- | --- |
| `SEC-001` | TLS endpoint에서 SDK는 CA trust source, server name, SNI와 `relaygate/3` ALPN을 검증한 뒤 credential-free HELLO를 보낸다. |
| `SEC-002` | HELLO는 application identity·token·Destination을 포함하지 않고 WELCOME은 새 SessionId만 부여한다. |
| `SEC-003` | internal mTLS는 신뢰 CA·Gateway client DNS SAN·서버 DNS SAN·`relaygate/3` ALPN을 검증한다. Logical identity는 incarnation/owner fencing에 사용한다. |
| `SEC-004` | TLS failure는 평문 fallback 없는 terminal connection failure이며 새 retry는 새 TLS connection으로 시작한다. |
| `SEC-005` | Gateway SDK TLS와 internal mTLS는 서버 certificate/key path를 요구한다. 공인 TLS SDK client는 기본 roots를 사용한다. |
| `SEC-006` | internal mode는 `mtls` 기본값 또는 명시적 `plaintext`다. Plaintext는 인증과 전송 암호화가 없는 격리 테스트 mode다. |
| `SEC-007` | hop TLS는 transport peer를 보호하고 application E2E/peer auth는 Pipe 위 application protocol이 담당한다. |
| `SEC-008` | public Relay API는 transport-independent이고 endpoint는 TLS/TCP 기본 또는 명시적 TCP다. |
| `SEC-009` | SDK edge와 internal mTLS는 독립 Secret·trust domain이다. |
| `SEC-010` | internal plaintext는 SDK edge TLS와 operation authorization을 비활성화하지 않는다. |
| `SEC-011` | external L4는 byte stream passthrough이고 Gateway가 SDK TLS를 종료한다. |
| `SEC-012` | SDK accept는 전체 transport slot과 별도 handshake slot을 TLS 전에 확보한다. Handshake 상한 도달 시 새 socket을 닫고 기존 session을 유지한다. |
| `SEC-013` | HELLO payload 상한은 0 bytes다. HELLO 읽기와 WELCOME/거절 쓰기는 합쳐 5초 이내 끝내고 성공 뒤 일반 frame 한도로 전환하며 이미 읽은 다음 frame을 보존한다. |
| `SEC-014` | SDK 신규 socket은 slot·TLS 처리 전 GW-local rate budget을 통과한다. 초기 burst와 초당 refill을 제한하고 부족하면 새 socket만 종료한다. Clone은 같은 예산을 공유한다. |
| `SEC-015` | SDK PUBLISH/DIAL은 session별·GW 전체의 공유 제어 예산을 통과한 뒤 authorization과 state operation을 수행한다. 초과 요청은 `RESOURCE_EXHAUSTED`, DIAL은 `NOT_OBSERVED`다. |

## Operation authorization

```text
PUBLISH(Destination, AccessToken) --+
                                     +--> SPEC 009 verification --> state operation
DIAL(Destination, AccessToken) -----+                |
                                                      `-- raw token drop
```

[SPEC 009](009-operation-jwt-authorization-contract.md)가 custom JWT profile, protected header, claims JSON
Schema, static JWK trust, permission, verification ordering, response와 state 영향을 소유합니다. 이 문서는 그 결과를
관측하는 경계만 소유합니다.

Authorization은 `PUBLISH`와 `DIAL`의 admission 단계입니다. Raw token, decoded claim과 permission은
log·metric label 또는 error body에 기록하지 않습니다. 인증 성공 자체에는 별도 ACK가 없으며 이후 operation의
기존 `Published/PublishFailed` 또는 `Opened/DialFailed` 흐름을 관측합니다. 인증 실패는 해당 operation만 끝내고
existing session·Binding·Pipe를 유지합니다.

## SDK handshake 보호

```text
accept -> rate budget -> transport + handshake slot -> TLS(5s) -> HELLO/response(5s)
                                                        |-- success -> slot 반환 -> session
                                                        `-- failure -> state + slot 반환
```

| 설정 | 기본값·의미 |
| --- | --- |
| `RELAYGATE_MAX_PENDING_HANDSHAKES` | 256; 실제 한도는 `min(설정값, MAX_SESSIONS)`. 0은 시작 실패 |
| `RELAYGATE_MAX_SESSIONS` | 10,000; handshake와 admitted session을 포함한 전체 transport 수 |
| `RELAYGATE_SDK_CONNECTION_RATE_PER_SECOND` | 256; 초당 신규 admission budget refill. 0은 시작 실패 |
| `RELAYGATE_SDK_CONNECTION_BURST` | 256; 초기·유휴 후 budget 최대 보유량. 0은 시작 실패 |
| HELLO payload | 0 bytes |

256은 보호 상한이며 처리량 보장 수치가 아닙니다. 기존 Relay의 managed reconnect는 SDK backoff로 분산합니다.
초기 `Relay::connect`는 단일 시도이므로 socket admission 거절 뒤 재시도는 application이 결정합니다. Readiness
조회는 budget을 소비하지 않고 기존 session은 유지합니다. 소비한 rate budget은 실패·종료에도 반환하지 않고
시간으로만 보충합니다. 임의의 t초 구간에서 통과 수는 `burst + rate * t` 이하입니다. 이 제한은 GW-local이고
GW 재시작은 burst를 초기화하며 replica 증가는 cluster 총 예산을 늘립니다.

## SDK 제어 요청 보호

```text
PUBLISH / DIAL -> session budget -> Gateway budget -> authorization -> state operation
DATA / PING / OFFER 응답 / UNPUBLISH / CANCEL / FIN / CLOSE / RESET -> 기존 처리
```

| 설정 | 기본값 |
| --- | --- |
| `RELAYGATE_CONTROL_RATE_PER_SECOND` / `RELAYGATE_CONTROL_BURST` | GW 전체 4,096/s · burst 4,096 |
| `RELAYGATE_SESSION_CONTROL_RATE_PER_SECOND` / `RELAYGATE_SESSION_CONTROL_BURST` | session별 256/s · burst 256 |

두 operation은 같은 bucket을 공유합니다. Session budget이 없는 요청은 GW budget을 소비하지 않습니다. 소비한
budget은 결과·연결 종료와 무관하게 시간으로만 보충합니다. Session 종료는 해당 bucket을 제거하고 GW bucket은
유지합니다. 거절은 authorization·registry 변경·RT Resolve·peer OPEN 전에 결정합니다. DIAL ConnectionId fence는
거절 후에도 유지합니다. 기존 Binding·Pipe와 정리 메시지는 이 제한을 사용하지 않습니다.

## 로그와 metric

| category | lifecycle event |
| --- | --- |
| session | admitted, rejected, removed |
| Listener | active, suspended, blocked, closed |
| authorization | operation, terminal outcome, stable code |
| dial | result, code, observation |
| dependency | peer/RT connect, handshake, loss, recovery |
| shutdown | drain start, deadline, complete |
| protection | TLS, queue, capacity rejection |

Lifecycle field는 bounded identifier와 outcome을 사용합니다. AccessToken, decoded claim, credential, private key,
payload와 free-form error body는 redaction합니다. DATA RTT와 payload goodput은 명시적으로 실행한 SDK probe가
측정합니다.

| 운영 질문 | metric | 판정 |
| --- | --- | --- |
| process scrape | Prometheus `up` | process/endpoint reachability |
| SDK admission | `relaygate_gateway_sdk_admission_ready`, `relaygate_gateway_draining` | non-draining + transport·handshake capacity + rate budget 여유 |
| SDK handshake 포화 | `relaygate_gateway_resource_used{resource="sdk_handshakes"}`, `relaygate_gateway_resource_limit{resource="sdk_handshakes"}` | TLS/HELLO 진행 수·상한 |
| SDK transport 거절 | `relaygate_gateway_sdk_transport_rejections_total{reason}` | `rate_limit|session_limit|handshake_limit` |
| operation authorization | `relaygate_gateway_authorization_results_total{operation,outcome,code}` | `publish|dial`의 terminal verification result |
| authorization latency | `relaygate_gateway_authorization_duration_seconds{operation,outcome}` | bounded verification duration |
| SDK 제어 요청 거절 | `relaygate_gateway_control_rejections_total{operation,scope}` | `publish|dial` x `session|gateway` |
| RT dependency | `relaygate_gateway_route_dependency{state}` | `DISABLED|READY|DEGRADED|TERMINAL` one-hot |
| RT convergence | `relaygate_gateway_route_registrations_unsynced` | pending registration 수 |
| peer state | `relaygate_gateway_peer_transports_connecting`, `relaygate_gateway_peer_transports_ready` | connecting·reusable transport 수 |
| liveness failure | `relaygate_gateway_heartbeat_timeouts_total{transport}` | SDK/peer timeout 누계 |
| SDK process 자원 | `relaygate_sdk_resource_used/limit{resource}` | `live_pipes|buffered_bytes` 현재 점유·설정 상한 |
| SDK process 포화 | `relaygate_sdk_resource_rejections_total{resource}` | Listener/Relay/Pipe 단위 상한 거절 누계 |

### RED와 latency

| 구간 | metric | 측정 경계 |
| --- | --- | --- |
| GW DIAL | `relaygate_gateway_dial_requests_total`, `relaygate_gateway_dial_results_total`, `relaygate_gateway_dial_duration_seconds` | precheck 진입 -> OPENED/failure/cancel |
| SDK 접속·DIAL | `relaygate_sdk_operation_results_total`, `relaygate_sdk_operation_duration_seconds` | `session_connect`: transport·TLS·HELLO/WELCOME; `dial`: API 진입 -> Pipe/실패/cancel |
| publish | `relaygate_gateway_publish_results_total` | terminal result counter |
| authorization | `relaygate_gateway_authorization_results_total`, `relaygate_gateway_authorization_duration_seconds` | verifier start -> success/failure |
| heartbeat | timeout counter + `relaygate_gateway_heartbeat_duration_seconds{transport}` | committed PING -> matching PONG |
| Gateway->RT | `relaygate_gateway_route_table_requests_total`, `relaygate_gateway_route_table_request_duration_seconds` | client queue admission -> response/failure |
| RT actor | `relaygate_route_table_requests_total`, `relaygate_route_table_request_duration_seconds` | actor service start -> result |
| peer | `relaygate_gateway_peer_handshakes_total`, `relaygate_gateway_peer_transport_closed_total` | transport lifecycle outcome |
| SDK reconnect | `relaygate_sdk_reconnect_attempts_total`, `relaygate_sdk_reconnect_episode_duration_seconds{outcome}` | episode start -> `recovered|closed|aborted` |
| SDK 미복구 | `relaygate_sdk_reconnect_in_progress` | process 내 진행 중 episode 수 |

```text
SDK session_connect -> token source -> SDK dial -> established Pipe DATA RTT
                                      `-> GW precheck -> authorization -> local/RT resolve -> OFFER
```

GW->RT와 RT actor histogram은 표본 경계가 다릅니다. 두 p95 차이를 network p95로 해석하지 않습니다.
Heartbeat는 liveness RTT, DATA RTT는 payload 왕복입니다. SDK dial 시간은 token source와 session·queue 대기를
포함합니다.

| DIAL `class` | code |
| --- | --- |
| `success` | `ok` |
| `request` | `invalid_argument`, `unauthenticated`, `permission_denied`, `not_found`, `failed_precondition`, `protocol_error`, `already_exists` |
| `capacity` | `resource_exhausted` |
| `availability` | `unavailable`, `deadline_exceeded` |
| `internal` | `internal` |
| `cancelled` | `cancelled` |

### USE와 current state

| 영역 | 관측값 |
| --- | --- |
| Gateway | SDK sessions, bindings, pending offers, GW-local Pipe states, remote DIAL attempts |
| 고유 Pipe | `relaygate_gateway_originated_pipes`: 호출 SDK의 GW에서 한 번 집계 |
| Peer | connecting/ready transport endpoints, stream endpoints; one-hop 양단 포함 |
| Capacity | `relaygate_gateway_resource_used/limit{resource}`: 현재 점유 / 설정 상한 |
| RouteTable | registrations, BindingProjection, Destination index, expiry records |
| Saturation | writer·control·authorization rejection, `RESOURCE_EXHAUSTED` result |
| Recovery | reconnect 진행 개수·종료 시간, dependency transition, lease expiry, drain |

| `resource` | used | limit |
| --- | --- | --- |
| `sessions` | handshake 포함 SDK transport semaphore 점유 | max sessions |
| `sdk_handshakes` | TLS/HELLO 진행 점유 | max pending handshakes |
| `bindings` | local bindings | max bindings |
| `pending_opens` | pending offers + remote attempts | max pending offers |
| `remote_dials` | remote attempts | max remote dial attempts |
| `pipes` | GW-local open Pipe states | max live Pipes |

Authorization concurrency는 authorization result의 `resource_exhausted`와 duration으로 관측하며 raw token·Namespace·
Destination을 metric label로 사용하지 않습니다. 설정 상한은 지속 가능한 처리량이 아닙니다. Snapshot은 GW별
순간 관측이고 cluster 합계는 전역 원자적 값이 아닙니다.

Metric label set은 `operation`, `outcome`, `code`, `class`, `reason`, `scope`, `resource`, `state`, `direction`,
`transport` 같은 bounded enumeration입니다. Instance identity는 Prometheus target metadata, request identity는
lifecycle log가 담당합니다.

## 수집과 해석

```text
운영 개요: 준비율 · 세션/Pipe -> 요청 · 지연 · 결과 · 용량
  |-- GW·RT 진단: 연결/큐 -> 라우팅/수렴 -> Kubernetes
  `-- SDK 복구: 진행 중 재연결 <-> 종료 시간, 접속/DIAL
```

| 대상 | 수집 계약 |
| --- | --- |
| GW·RT | Prometheus target의 `cluster`, `namespace`, `instance` label 유지 |
| SDK | application이 recorder와 scrape endpoint, 배포 범위 target label 설치 |
| Kubernetes | cAdvisor CPU·throttling·working set·RSS·network와 kube-state-metrics limit·replica |
| 미수집 | No data로 표현; `up`은 발견 target, desired replica는 별도 비교 |
| 메모리 | logical baseline 복귀와 반복 부하 뒤 working set/RSS 추세 함께 관측 |
| 네트워크 | Pod RX/TX와 DATA probe echo payload goodput 구분 |

화면 구성은 [Design](../../DESIGN.md), 화면·쿼리 검증은
[TEST 006](../test/006-local-observability-test-plan.md)이 소유합니다.

## Probe

| probe | 판정 범위 | 상위 검증 |
| --- | --- | --- |
| startup/readiness `check` | TLS + credential-free `HELLO/WELCOME` | topology test가 authorization·RT·Destination·Pipe 검증 |
| liveness TCP | process socket reachability | health metric이 control/data plane 구분 |
| topology test | authorization/local/one-hop/dial/Pipe byte | application test가 업무 성공 검증 |
| Pipe latency probe | 고정 workload의 established Pipe RTT | application benchmark가 실제 payload 특성 검증 |

| ID | 계약 |
| --- | --- |
| `OBS-001` | snapshot gauge는 current value를 기록한다. |
| `OBS-002` | cleanup 뒤 gauge는 baseline으로 수렴한다. |
| `OBS-003` | access token·private key·payload marker의 log·metric·error 출현은 0이다. |
| `OBS-004` | Helm scrape surface는 metrics endpoint로 한정된다. |
| `OBS-005` | process, SDK transport admission, operation authorization, RT dependency와 peer health를 독립 지표로 관측한다. |
| `OBS-006` | authorization, DIAL, GW->RT, RT actor와 heartbeat는 각 측정 경계의 histogram을 가진다. |
| `OBS-007` | SDK/peer heartbeat timeout은 bounded `transport` label counter다. |
| `OBS-008` | DATA RTT는 explicit established-Pipe latency probe가 측정한다. |
| `OBS-009` | SDK admission ready는 non-draining, transport·handshake slot 여유와 rate budget 여유의 conjunction이다. |
| `OBS-010` | Dashboard runtime selector는 cluster·namespace 범위를 일관되게 적용한다. |
| `OBS-011` | 고유 Pipe와 GW-local Pipe state를 구분하며 resource used/limit 집계 기준을 일치시킨다. |
| `OBS-012` | SDK 계측은 error·polled future cancel·reconnect 미완료와 종료를 구분한다. |
| `OBS-013` | 결과 분류와 gauge/rate 단위를 유지하고 실제 PromQL 기대값으로 검증한다. |
| `OBS-014` | SDK live Pipe·buffered byte 점유는 cleanup 뒤 기준값으로 수렴하고 resource rejection은 bounded `resource` label로 구분한다. |
