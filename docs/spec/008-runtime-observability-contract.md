# SPEC 008: 런타임 관측성 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 역할 | process 로그, lifecycle event, current-state snapshot의 최소 계약 |

관측성은 새 상태를 만들지 않는다. Gateway와 SDK가 이미 소유한 현재 상태와 전이를
구조화된 event로 관찰할 뿐이며, 로그는 복구 원본이나 전달 증명이 아니다.

```text
Gateway / SDK ── emit event ──► host subscriber ──► stdout collector
      │
      └── current state ───────► GatewaySnapshot ──► optional periodic event
```

## 소유권

- `relaygate-gateway`와 `relaygate-sdk`는 `tracing` event만 발행한다.
- 독립 process인 `relaygate-server`는 subscriber와 출력 형식을 소유한다.
- SDK 또는 Gateway를 포함하는 application은 process당 subscriber 하나를 설치한다.
- library는 전역 subscriber, exporter, listening port를 만들지 않는다.

## 로그 형식

`relaygate-server`는 다음 환경변수를 해석한다.

| 환경변수 | 기본값 | 값 |
| --- | --- | --- |
| `RELAYGATE_LOG` | `info` | `tracing-subscriber` filter |
| `RELAYGATE_LOG_FORMAT` | `text` | `text` 또는 `json` |
| `RELAYGATE_STATS_INTERVAL_MS` | unset | 0보다 큰 millisecond; unset이면 snapshot event 비활성화 |

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
| `info` | process 시작·종료와 명시적으로 활성화한 snapshot |
| `debug` | session, registration, open과 Pipe lifecycle |
| `warn` | Gateway resource limit, offer expiry와 writer queue failure |
| `error` | 내부 invariant 또는 lock 복구 실패 |

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

## 현재 범위 밖

```text
Prometheus exporter / admin port
OpenTelemetry exporter
cross-process trace context propagation
payload 또는 application-level delivery trace
durable metric history
```

## 요구사항

| ID | 요구사항 |
| --- | --- |
| `OBS-001` | 로그 형식은 `text`와 `json`만 허용하고 잘못된 값이면 serve 전에 실패해야 한다. |
| `OBS-002` | snapshot interval은 unset 또는 0보다 큰 millisecond만 허용해야 한다. |
| `OBS-003` | `GatewaySnapshot`은 session, binding, pending offer, live Pipe와 remote open attempt 수를 같은 local state index에서 계산해야 한다. distributed runtime이 있으면 routing worker의 session-shard registration `SYNCED/UNSYNCED` 및 peer manager의 connecting/ready transport와 current stream 수를 함께 제공하되 각 source 사이 원자적 시점을 보장하지 않는다. local-only mode의 분산 count는 0이어야 하며 RT 전체 mapping 수, cluster 합계 또는 routing 진실로 해석해서는 안 된다. |
| `OBS-004` | 구조화 event는 `component`, `event`와 현재 객체를 구분할 수 있는 identity field를 사용해야 한다. |
| `OBS-005` | event는 `ClientKey`, `InternalGatewayKey`, payload, application data를 기록하지 않고 DATA hot path에 per-frame 로그를 만들지 않아야 한다. |
| `OBS-006` | SDK와 Gateway terminal failure event는 source protocol/state 결과에 있는 `error_code`와, 정의된 경우 `observation`을 바꾸지 않고 관찰해야 한다. 정상 close와 source 결과에 `PeerObservation`이 없는 registration 같은 operation에는 오류 field를 합성하지 않는다. |
| `OBS-007` | library crate는 event만 발행하며 subscriber와 exporter는 embedding application 또는 server가 소유해야 한다. |
| `OBS-008` | snapshot event는 기본적으로 비활성화되고 활성화해도 새 listening port를 만들지 않아야 한다. |
| `OBS-009` | SDK-Gateway heartbeat timeout, active PeerTransport heartbeat timeout과 zero-stream PeerTransport idle retirement는 lifecycle event로 관찰 가능해야 하며, payload bytes나 application-level delivery result를 기록해서는 안 된다. |
| `OBS-010` | `relaygate-server check`는 새 ConnectorSession의 TCP 연결과 `HELLO -> WELCOME`만 검증하는 `SdkAdmissionReadiness` probe여야 한다. RT, binding, open, Pipe와 application 결과를 검증하거나 지속 state를 남겨서는 안 된다. |
| `OBS-011` | `GatewaySnapshot`은 `RouteDependencyHealth`를 제공해야 한다. local-only는 `DISABLED`, distributed mode는 terminal failure, unavailable shard와 `UNSYNCED` registration을 우선순위에 따라 `TERMINAL`, `DEGRADED`, `READY`로 집계해야 한다. 한 shard의 terminal failure는 summary를 `TERMINAL`로 만들지만 unaffected shard operation의 실패를 뜻하지 않아야 한다. |
| `OBS-012` | `ProcessLiveness`, `SdkAdmissionReadiness`와 `RouteDependencyHealth`는 별도 신호여야 한다. RT 저하는 process 또는 admission 실패를 직접 만들지 않고, admission 저하는 process failure를 뜻하지 않아야 한다. |
