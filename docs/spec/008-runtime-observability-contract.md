# SPEC 008: 런타임 관측성 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 역할 | process 로그, lifecycle event, current-state snapshot과 metrics의 최소 계약 |

관측성은 새 상태를 만들지 않는다. Gateway와 SDK가 이미 소유한 현재 상태와 전이를
구조화된 event로 관찰할 뿐이며, 로그는 복구 원본이나 전달 증명이 아니다.

```text
Gateway / SDK ── emit event ──► host subscriber ──► stdout collector
      │
      ├── current state ───────► GatewaySnapshot ──► optional periodic event
      └── bounded RED/USE ─────► optional Prometheus exporter
```

## 소유권

- library crate는 `tracing` event와 low-cardinality metric 값만 발행한다.
- 독립 process인 `relaygate-server`는 subscriber, 출력 형식과 exporter를 소유한다.
- SDK 또는 Gateway를 포함하는 application은 process당 subscriber 하나를 설치한다.
- library는 전역 subscriber, exporter, listening port를 만들지 않는다.

## 로그 형식

`relaygate-server`는 다음 환경변수를 해석한다.

| 환경변수 | 기본값 | 값 |
| --- | --- | --- |
| `RELAYGATE_LOG` | `info` | `tracing-subscriber` filter |
| `RELAYGATE_LOG_FORMAT` | `text` | `text` 또는 `json` |
| `RELAYGATE_STATS_INTERVAL_MS` | unset | 0보다 큰 millisecond; unset이면 snapshot event 비활성화 |
| `RELAYGATE_METRICS_BIND_ADDR` | unset | Prometheus HTTP listener socket; unset이면 exporter 비활성화 |
| `RELAYGATE_METRICS_INTERVAL_MS` | `5000` | Gateway gauge sampling interval; exporter가 활성화될 때만 유효 |

모든 구조화 event는 `component`와 안정적인 `event` 이름을 가진다. 객체를 식별할 수 있을
때는 해당 identity를 별도 field로 기록한다.

```text
component
event
session_id / role
request_id / client_id / binding_id
connector_session_id / listener_session_id / connection_id
entry_gateway_id / peer_gateway_id / peer_transport_id / stream_id
error_code / observation
```

`text`와 `json`은 같은 field 이름을 사용한다. JSON event field는 collector가 별도 message
파싱 없이 읽을 수 있어야 한다.

| Level | 용도 |
| --- | --- |
| `info` | process 시작·종료, 장애 episode 시작·복구와 명시적으로 활성화한 snapshot |
| `debug` | session, registration, open, Pipe와 반복 가능한 handshake/retry detail |
| `warn` | dependency 저하, active transport loss, resource limit, offer expiry와 writer queue failure |
| `error` | terminal dependency, 내부 invariant 또는 lock 복구 실패 |

반복 가능한 장애는 개별 재시도마다 `info`나 `warn`을 만들지 않고 하나의 episode로 관찰한다.
episode가 처음 시작되거나 상태가 실제로 바뀔 때 한 번 기록하고, 복구되면 시도 횟수와
경과 시간을 포함한 종료 event를 한 번 기록한다. 중간 재시도는 `debug` event와 counter로만
관찰한다.

```text
normal
  └── failure detected ──► episode started
                              ├── retry failure × N   debug + counter
                              └── recovered | terminal
```

## Lifecycle event

| 범주 | 관찰하는 전이 |
| --- | --- |
| process | server started / stopped, signal listener failure |
| session | SDK session ready / ended, reconnect attempt failure |
| registration | Listener registration active / rejected / suspended / blocked |
| open | OPEN succeeded / failed |
| Pipe | close / reset / session-loss terminal cleanup |
| transport liveness | SDK-Gateway heartbeat timeout, active PeerTransport heartbeat timeout |
| peer | handshake admission, PeerTransport loss, zero-stream idle retirement과 frame commit failure |

다음 운영 전이는 lifecycle event와 누적 metric을 함께 사용해 원인과 종료를 연결할 수 있어야 한다.

| 경계 | 시작·terminal | 복구 |
| --- | --- | --- |
| SDK–Gateway | `sdk.session.reconnect_started` | `sdk.session.reconnect_recovered` 또는 runtime close |
| Gateway–RouteTable | shard dependency `DEGRADED` 또는 `TERMINAL` | shard dependency `READY` |
| Gateway–Gateway | peer handshake failure, active PeerTransport loss | handshake success와 current ready transport |
| Gateway drain | drain 시작 또는 timeout | drain 정상 완료 |

같은 상태의 반복 관찰은 새 전이가 아니다. SDK Listener reconnect는 TCP handshake 성공이 아니라
현재 desired Listener 등록이 다시 수렴한 시점에 복구된 것으로 본다. PeerTransport의 정상
shutdown과 zero-stream idle retirement는 `debug`이고, active stream을 잃는 transport failure는
`warn`이다.

성공·실패 event는 기존 protocol/state 결과를 그대로 관찰한다. 여기서 관측성 event는
tracing/metrics event이며 Gateway 상태 전이를 전달하는 내부 `PeerEvent`가 아니다. 관측성 event
유실이나 collector 장애가 RelayGate 상태 전이와 Pipe 데이터 경로를 바꾸면 안 된다.
terminal failure event는 기존 결과에 있는 `error_code`와, 정의된 경우 `observation`을 기록한다.
정상 close와 source 결과에 `PeerObservation`이 없는 operation에는 존재하지 않는 오류 field를
합성하지 않는다.
예를 들어 `gateway.listener.registration_rejected`는 source protocol result에 `observation`이 없으므로
`error_code`만 기록하고, OPEN과 Pipe failure event는 기존 `observation`을 함께 기록한다.

다음 값은 기록하지 않는다.

```text
ClientKey value
InternalGatewayKey value
payload bytes
application data
DATA frame별 event
```

`ClientId`는 routing identity로 필요한 lifecycle event에만 기록할 수 있다. byte relay hot path는
로그하지 않는다.

## GatewaySnapshot

Gateway는 같은 local state index에서 다음 현재값을 계산한다.

```text
draining
sessions
listener_sessions
connector_sessions
listener_bindings
pending_offers
live_pipes
route_dependency_health
route_registrations_synced
route_registrations_unsynced
remote_open_attempts
peer_transports_connecting
peer_transports_ready
peer_streams
```

`route_registrations_*`는 Gateway의 routing worker가 마지막으로 관찰한 session-shard
registration 수렴 상태다. RT 전체 table이나 mapping 수가 아니다. publication 또는 RT 연결
상태가 바뀐 직후에는 worker가 desired state를 다시 읽을 때까지 짧게 이전 값을 보일 수 있고,
local state count와 하나의 원자적 시점으로 읽히지 않는다. worker가 단절을 관찰하면 local
binding은 유지된 채 해당 registration이 `unsynced`로 수렴한다. 이 값은 routing 결정에 쓰는
진실이 아니라 운영 관측값이다.

`remote_open_attempts`는 Entry Gateway의 local-miss control attempt 중 `RESOLVING`,
`STARTING_PEER`, `AWAITING_PEER` 상태의 수다. peer `OPENED`/`FAILED`, cancel,
ConnectorSession 또는 PeerTransport loss에서 제거한다. Owner inbound `OPEN`, established
Pipe/RelayStream과 terminal history는 포함하지 않는다. 따라서 external `OBSERVED`까지 이어지는
canonical Open attempt보다 좁은 관측 scope다.
`peer_transports_*`와 `peer_streams`는 peer manager가 마지막으로 관찰한 current pair transport와
RelayStream 수다. 이 값들도 RT count 및 local session index와 원자적으로 읽히지 않으며
cluster 전체 합계가 아니다. local-only mode에서는 모든 분산 count가 0이다.

snapshot은 한 Gateway process의 순간 관찰값이다. 누적 counter, application 처리 결과,
message delivery acknowledgement가 아니다.

`RELAYGATE_STATS_INTERVAL_MS`를 설정하면 `relaygate-server`는 `gateway.snapshot` event를
주기적으로 기록한다. 기본값은 비활성화이며 network port를 추가하지 않는다.

Heartbeat와 idle-retirement event는 transport lifecycle 관찰값이다. 해당 event를 Pipe
application health, payload delivery acknowledgement 또는 retry 명령으로 해석해서는 안 된다.

## Prometheus metrics

Exporter는 명시적으로 활성화한 `relaygate-server` process에만 생긴다. SDK를 포함한 application과
library crate는 port를 열지 않는다. metric은 process 재시작으로 초기화되는 현재 상태이며
`role=gateway|route_table` 외 동적 identity label을 사용하지 않는다. image version과 digest는
배포 metadata에서 관찰한다.

```text
Gateway gauges
  sessions / listener_sessions / connector_sessions
  listener_bindings / pending_offers / live_pipes
  route_registrations_synced / route_registrations_unsynced
  remote_open_attempts
  peer_transports_connecting / peer_transports_ready / peer_streams
  route_dependency{state=DISABLED|READY|DEGRADED|TERMINAL}  one-hot

Gateway RED/USE
  open_requests_total
  open_results_total{outcome=success|error|cancelled,code}
  open_duration_seconds{outcome}
  writer_queue_rejections_total{reason=full|closed}
  listener_registration_results_total{outcome,code}
  route_dependency_transitions_total{previous,current}
  route_connection_attempts_total{outcome,code}
  route_recovery_duration_seconds
  peer_handshakes_total{direction,outcome,code}
  peer_transport_closed_total{reason}

SDK reconnect
  reconnect_attempts_total{role,outcome}
  reconnect_duration_seconds{role}

RouteTable gauges
  registrations / mappings / routes / expiry_records

RouteTable RED
  handshakes_total{outcome,code}
  requests_total{operation,outcome=success|error,code}
  request_duration_seconds{operation,outcome}
  expired_registrations_total
```

### RED/USE 대시보드 해석

대시보드는 한 개의 종합 health 값을 만들지 않고 다음 질문을 순서대로 좁힌다.

| 질문 | 1차 신호 | 함께 볼 신호 |
| --- | --- | --- |
| process를 관찰할 수 있는가 | Gateway/RT `up` 비율 | 배포 환경의 liveness와 로그 |
| 새 Pipe를 열 수 있는가 | OPEN 요청률·결과·p95 | pending offer, remote attempt, terminal gap |
| owner route가 수렴했는가 | route dependency READY 비율 | unsynced registration, RT 요청 결과·p95 |
| one-hop relay가 포화됐는가 | writer queue rejection | connecting/ready PeerTransport, RelayStream |
| current state가 정리되는가 | session·binding·live Pipe | RT registration·mapping·route·expiry record |

`up`은 Prometheus scrape 성공이고 `SdkAdmissionReadiness`가 아니다. `cancelled`는 caller가
종료한 terminal 결과이므로 system `error`와 합산하지 않는다. OPEN terminal gap은 진행 중인
요청 때문에 순간적으로 생길 수 있으며 지속될 때만 누락·지연 후보로 해석한다. duration은
고정 bucket histogram으로 내보내며 p95는 선택한 instance별 bucket rate로 계산한다.

CPU·memory·network 같은 host/container USE는 RelayGate process metric이 아니라 배포 환경의
기본 exporter에서 가져온다. Gateway current-state gauge는 resource 사용량의 분자이며 그 자체가
capacity 비율은 아니다. hard saturation은 bounded writer queue rejection으로 관찰한다. alert와
SLO threshold는 실제 부하 측정 전에는 고정하지 않는다.

기본 dashboard는 Gateway와 RouteTable instance filter를 제공한다. `All`에서는 process의
current-state를 합산하고 특정 instance를 선택하면 같은 panel에서 drill-down한다. OPEN과 RT p95는
filter에 맞는 histogram bucket을 instance별로 집계한다.

`ClientId`, session/binding/connection/stream identity, credential, payload와 error message는 label이나
metric 값에 포함하지 않는다. `operation`, `outcome`, protocol `code`, queue `reason`은 구현이
닫힌 bounded enum만 사용한다.

`route_dependency_transitions_total`의 상태 집합은 `starting`, `ready`, `degraded`, `terminal`이다.
`peer_transport_closed_total`의 reason 집합은 `local_close`, `remote_closed`, `protocol_error`,
`writer_failed`, `heartbeat_timeout`, `idle_retired`이다. reconnect, dependency와 transport metric의
label도 닫힌 값만 사용한다. SDK metric은 library가
호출만 하며 embedding application이 recorder를 설치하지 않으면 no-op이다. SDK는 exporter나
listening port를 만들지 않는다. 하나의 process에 여러 SDK runtime이 있을 수 있으므로 SDK는
process-wide current reconnect gauge를 설정하지 않고 counter와 histogram만 사용한다.

Gateway OPEN duration은 유효한 새 `ConnectionId`의 `OPEN`을 수락한 시점부터 `OPENED`,
`OPEN_FAILED` 또는 local cancellation까지다. 중복·과거 `ConnectionId`처럼 protocol상 수락하지
않은 frame은 요청률에 포함하지 않는다. RouteTable duration은 인증된 요청을 shard actor가 꺼낸
뒤 domain 결과를 만들 때까지의 service time이며 network와 actor queue 대기시간은 포함하지 않는다.

## 운영 health 관찰

운영 health는 하나의 종합 신호로 노출하지 않는다.

| 신호 | 관찰 방법 | 확인하지 않는 것 |
| --- | --- | --- |
| `ProcessLiveness` | 배포 환경의 process supervision과 serve 종료 결과 | SDK admission, RT, Pipe와 application 성공 |
| `SdkAdmissionReadiness` | `relaygate-server check <SDK address>`의 TCP 연결과 `HELLO(Connector) -> WELCOME` | RT availability, binding, open, Pipe와 payload 성공 |
| `RouteDependencyHealth` | `GatewaySnapshot.route_dependency_health` | RT table 내용, cluster 전체 health와 payload 성공 |

`check`는 짧은 ConnectorSession을 만들 수 있지만 binding, open, Pipe 또는 application state를
만들지 않는다. SDK listener unavailable, shutdown 또는 session capacity 소진으로
`HELLO -> WELCOME`을 완료하지 못하면 실패한다. RT 단절만으로는 실패하지 않는다.

`RouteDependencyHealth`는 Gateway routing worker가 마지막으로 관찰한 값이다.

```text
DISABLED = local-only mode
READY    = 모든 configured shard client가 available이고 존재하는 desired registration이 모두 SYNCED
DEGRADED = terminal failure 없이 unavailable shard 또는 UNSYNCED desired registration이 존재
TERMINAL = shard 또는 desired registration에 non-retryable control failure가 존재
```

여러 shard의 summary는 `TERMINAL > DEGRADED > READY` 우선순위를 사용한다. 같은 process에서
local-only와 distributed mode를 섞지 않으므로 `DISABLED`는 local-only mode에서만 사용한다.
한 shard의 terminal failure가 summary를 `TERMINAL`로 만들더라도 unaffected shard의 operation까지
실패했다는 뜻은 아니다. health 값은 last-observed current observation이며 routing 결정의 입력,
RT 전체 truth 또는 restart 명령이 아니다.

## 요구사항

| ID | 요구사항 |
| --- | --- |
| `OBS-001` | 로그 형식은 `text`와 `json`만 허용하고 잘못된 값이면 serve 전에 실패해야 한다. |
| `OBS-002` | snapshot interval은 unset 또는 0보다 큰 millisecond만 허용해야 한다. |
| `OBS-003` | `GatewaySnapshot`은 drain 여부, session, binding, pending offer, live Pipe와 remote open attempt 수를 같은 local state index에서 계산해야 한다. distributed runtime이 있으면 routing worker의 session-shard registration `SYNCED/UNSYNCED` 및 peer manager의 connecting/ready transport와 current stream 수를 함께 제공하되 각 source 사이 원자적 시점을 보장하지 않는다. local-only mode의 분산 count는 0이어야 하며 RT 전체 mapping 수, cluster 합계 또는 routing 진실로 해석해서는 안 된다. |
| `OBS-004` | 구조화 event는 `component`, `event`와 현재 객체를 구분할 수 있는 identity field를 사용해야 한다. |
| `OBS-005` | event는 `ClientKey`, `InternalGatewayKey`, payload, application data를 기록하지 않고 DATA hot path에 per-frame 로그를 만들지 않아야 한다. |
| `OBS-006` | SDK와 Gateway terminal failure event는 source protocol/state 결과에 있는 `error_code`와, 정의된 경우 `observation`을 바꾸지 않고 관찰해야 한다. 정상 close와 source 결과에 `PeerObservation`이 없는 registration 같은 operation에는 오류 field를 합성하지 않는다. |
| `OBS-007` | library crate는 event만 발행하며 subscriber와 exporter는 embedding application 또는 server가 소유해야 한다. |
| `OBS-008` | snapshot event는 기본적으로 비활성화되고 활성화해도 새 listening port를 만들지 않아야 한다. |
| `OBS-009` | SDK-Gateway heartbeat timeout, active PeerTransport heartbeat timeout과 zero-stream PeerTransport idle retirement는 lifecycle event로 관찰 가능해야 하며, payload bytes나 application-level delivery result를 기록해서는 안 된다. |
| `OBS-010` | `relaygate-server check`는 새 ConnectorSession의 TCP 연결과 `HELLO -> WELCOME`만 검증하는 `SdkAdmissionReadiness` probe여야 한다. RT, binding, open, Pipe와 application 결과를 검증하거나 지속 state를 남겨서는 안 된다. |
| `OBS-011` | `GatewaySnapshot`은 `RouteDependencyHealth`를 제공해야 한다. local-only는 `DISABLED`, distributed mode는 terminal failure, unavailable shard와 `UNSYNCED` registration을 우선순위에 따라 `TERMINAL`, `DEGRADED`, `READY`로 집계해야 한다. 한 shard의 terminal failure는 summary를 `TERMINAL`로 만들지만 unaffected shard operation의 실패를 뜻하지 않아야 한다. |
| `OBS-012` | `ProcessLiveness`, `SdkAdmissionReadiness`와 `RouteDependencyHealth`는 별도 신호여야 한다. RT 저하는 process 또는 admission 실패를 직접 만들지 않고, admission 저하는 process failure를 뜻하지 않아야 한다. |
| `OBS-013` | Prometheus exporter는 `RELAYGATE_METRICS_BIND_ADDR`가 있을 때만 `relaygate-server`가 소유하며 library 또는 SDK가 listening port를 만들지 않아야 한다. interval만 단독 지정하거나 잘못된 address·0 interval이면 serve 전에 실패해야 한다. |
| `OBS-014` | Gateway metric은 `GatewaySnapshot`의 current count와 route dependency one-hot state, accepted OPEN의 request/result/duration, bounded writer queue rejection을 반영해야 한다. RT metric은 actor가 소유한 `RouteTableStats`와 operation request/outcome/service duration을 반영해야 한다. |
| `OBS-015` | metric label은 process role, route dependency state, bounded operation/outcome/code/reason만 사용하고 routing/session/Pipe identity, credential, payload와 application data를 포함하지 않아야 한다. image version과 digest는 metric에 복제하지 않고 배포 metadata에서 관찰해야 한다. |
| `OBS-016` | Gateway drain 시작, 정상 완료와 timeout 강제 종료는 bounded lifecycle event로 구분해야 한다. `GatewaySnapshot.draining`과 `relaygate_gateway_draining` gauge는 `RUNNING=0`, `DRAINING/STOPPING=1`을 나타내며 Pipe migration이나 drain 성공을 뜻하지 않아야 한다. |
| `OBS-017` | SDK reconnect는 session loss 뒤 episode 시작, bounded retry attempt와 desired state 복구를 구분해야 한다. 반복 attempt는 counter와 `debug`로 관찰하고 Connector는 새 session 수립, Listener는 desired Listener 재등록 수렴 시 episode를 한 번만 복구로 끝내야 한다. library는 exporter나 current reconnect gauge를 만들지 않아야 한다. |
| `OBS-018` | Gateway routing worker는 RT shard availability가 `STARTING`, `READY`, `DEGRADED` 또는 `TERMINAL` 사이에서 실제로 바뀔 때만 전이 counter와 lifecycle event를 만들고, 각 connection attempt를 bounded outcome/code로 집계해야 한다. retryable episode가 `READY`로 복구되면 경과 시간을 한 번 기록해야 한다. shard identity와 오류 message를 metric label에 넣지 않아야 한다. |
| `OBS-019` | peer handshake 결과와 PeerTransport 종료는 bounded direction, outcome, code와 reason으로 집계해야 한다. 경쟁하는 종료 원인이 있어도 하나의 PeerTransport는 terminal counter와 event를 정확히 한 번만 만들고, idle retirement와 정상 shutdown을 active failure와 구분해야 한다. |
| `OBS-020` | Listener registration 결과와 RT handshake·request 결과는 bounded outcome과 protocol code로 집계해야 한다. 인증 key와 자유 형식 오류 message를 label로 사용해서는 안 된다. |
| `OBS-021` | RT lease expiry는 제거된 registration 수를 누적 집계하되 0건 sweep이나 registration identity를 로그로 기록하지 않고, 이미 제거된 registration을 다음 sweep에서 다시 집계해서는 안 된다. |
| `OBS-022` | metrics publisher는 Gateway drain이 진행되는 동안 current-state sampling을 중단해서는 안 된다. 종료 중 Pod가 배포 환경의 scrape target에서 제거될 수 있으므로 중앙 수집에서 drain gauge 관측을 보장하지 않으며, 직접 endpoint scrape, Kubernetes terminating state와 drain lifecycle log를 함께 사용해야 한다. |
