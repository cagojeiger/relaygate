# SPEC 007: 오류와 canonical 상태 모델

이 문서가 state/event 의미의 기준입니다.

## 오류

| code | 대표 조건 | retry 판단 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | UUID/config/frame 값 오류 | 입력 변경 전 금지 |
| `UNAUTHENTICATED` | TLS/ClusterToken/component credential 실패 | credential/config 변경 뒤 |
| `PERMISSION_DENIED` | permanent publication 정책 실패 | 정책 변경 뒤 |
| `NOT_FOUND` | current Binding 없음 | 상태 변경 뒤 새 dial |
| `FAILED_PRECONDITION` | self Binding만 존재, 닫힌 object | 전제 변경 뒤 |
| `UNAVAILABLE` | drain, dependency/transport 단절 | backoff 뒤 새 operation |
| `DEADLINE_EXCEEDED` | bounded deadline 만료 | observation 확인 뒤 |
| `RESOURCE_EXHAUSTED` | session/binding/pipe/queue/frame 상한 | 부하 감소 뒤 |
| `CANCELLED` | owner operation/session 종료 | caller 결정 |
| `PROTOCOL_ERROR` | version, frame 순서·소유권 위반 | 구현/config 수정 뒤 |
| `INTERNAL` | 내부 invariant/lock 실패 | 보수적으로 terminal |
| `ALREADY_EXISTS` | 같은 Relay의 동일 Destination Listener 중복 | 기존 Listener 종료 뒤 |

## RelaySession

```text
ABSENT -> CONNECTING -> ACTIVE -> RECONNECTING -> ACTIVE
                         │              │
                         ├────────────► BLOCKED
                         └────────────► CLOSED
```

| 이벤트 | 종료 범위 |
| --- | --- |
| TLS/HELLO 실패 | session 생성 없음 |
| transport/heartbeat/writer 실패 | current session, 그 session의 dial/Pipe; Listener는 SUSPENDED |
| terminal admission 실패 | Relay와 Listener BLOCKED |
| explicit Relay close | runtime, Listener, attempt와 Pipe CLOSED |

## Listener와 Binding

```text
Listener: REGISTERING -> ACTIVE -> SUSPENDED -> ACTIVE
                           ├─────► BLOCKED
                           └─────► CLOSED
Binding : ABSENT -> ACTIVE -> REMOVED   (terminal)
```

session reconnect는 Listener identity를 유지하지만 새 SessionId와 BindingId를 만듭니다. 늦은 old-session
PUBLISHED/OFFER는 새 Listener 또는 Binding을 만들지 못합니다.

## dial과 Pipe

```text
Dial: REQUESTED -> RESOLVING -> OFFERED -> OPENED
          └──────────── terminal failure ───────► FAILED

Pipe: ABSENT -> OFFERED -> OPEN -> HALF_CLOSED -> CLOSED
                  └──────── RESET/owner loss ───► CLOSED
```

성공과 실패는 terminal입니다. selected Binding의 실패가 sibling Binding fallback으로 이어지지 않으며,
새 연결은 application이 새 `dial`을 호출해야 합니다.

## RT registration

```text
ABSENT -> REGISTERING -> SYNCED -> UNSYNCED -> SYNCED
                           └────► DEREGISTERING -> REMOVED
terminal auth/config error ────────────────────► TERMINAL
```

RT restart와 connection loss는 `UNSYNCED`이며 local Binding은 유지합니다. current snapshot이 재등록되면
`SYNCED`로 돌아옵니다.

## 장애 전파 경계

| 장애 | 반드시 종료 | 반드시 보존 | 복구 |
| --- | --- | --- | --- |
| SDK–GW 단절 | session 소유 Pipe/dial/current Binding | 다른 session과 Binding | SDK reconnect + Listener republish |
| selected Listener OFFER 불확실 | selected RelaySession 전체 | sibling session/Binding | SDK reconnect; caller는 새 dial |
| GW–GW transport 단절 | 해당 transport의 stream/Pipe | local Binding, 다른 transport | 다음 dial이 새 transport 사용 |
| GW–RT 단절 | remote resolve와 sync 상태 | local Binding, established Pipe | worker reconnect + full snapshot |
| RT restart | shard의 모든 lease/mapping | GW local Binding, Pipe | Gateway 재등록 |
| GW drain/종료 | 신규 admission 중지 후 deadline에 owned state | 다른 GW/RT state | SDK/peer reconnect; 기존 Pipe resume 없음 |

모든 terminal cleanup은 idempotent해야 하며 unknown/late event는 no-op 또는 offending session의
`PROTOCOL_ERROR`로 닫힙니다. 종료된 state를 다시 활성화하지 않습니다.

## 상태 불변 조건

- **`STATE-001`**: terminal state는 같은 incarnation에서 다시 활성화되지 않는다.
- **`STATE-002`**: session 종료는 그 session이 소유한 Binding, attempt와 Pipe만 정리한다.
- **`STATE-003`**: 불확실한 publish/dial 결과는 current session 종료로 orphan 가능성을 닫는다.
- **`STATE-004`**: late, duplicate와 foreign event는 current sibling state를 변경하지 않는다.
- **`STATE-005`**: RT 장애와 restart는 local Binding과 established Pipe를 종료하지 않는다.
- **`STATE-006`**: 모든 cleanup은 반복 적용해도 같은 empty/current-state 결과로 수렴한다.
