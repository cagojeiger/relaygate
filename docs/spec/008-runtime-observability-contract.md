# SPEC 008: 런타임 관측성 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
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
connector_session_id / connection_id
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

성공·실패 event는 기존 protocol/state 결과를 그대로 관찰한다. 관측성 event 유실이나 collector
장애가 RelayGate 상태 전이와 Pipe 데이터 경로를 바꾸면 안 된다.

다음 값은 기록하지 않는다.

```text
ClientKey value
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
```

snapshot은 한 Gateway process의 순간 관찰값이다. 누적 counter, RT mapping, application 처리 결과,
message delivery acknowledgement가 아니다.

`RELAYGATE_STATS_INTERVAL_MS`를 설정하면 `relaygate-server`는 `gateway.snapshot` event를
주기적으로 기록한다. 기본값은 비활성화이며 network port를 추가하지 않는다.

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
| `OBS-003` | `GatewaySnapshot`은 session, binding, pending offer와 live Pipe 수를 같은 local state index에서 계산해야 한다. |
| `OBS-004` | 구조화 event는 `component`, `event`와 현재 객체를 구분할 수 있는 identity field를 사용해야 한다. |
| `OBS-005` | event는 `ClientKey`, payload, application data를 기록하지 않고 DATA hot path에 per-frame 로그를 만들지 않아야 한다. |
| `OBS-006` | SDK와 Gateway lifecycle event는 기존 terminal code와 observation을 바꾸지 않고 관찰해야 한다. |
| `OBS-007` | library crate는 event만 발행하며 subscriber와 exporter는 embedding application 또는 server가 소유해야 한다. |
| `OBS-008` | snapshot event는 기본적으로 비활성화되고 활성화해도 새 listening port를 만들지 않아야 한다. |
