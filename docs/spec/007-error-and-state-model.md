# SPEC 007: 오류와 canonical 상태 모델

State와 event 의미의 기준 문서입니다.

## 오류

process startup에서 unknown transport mode·legacy test flag 혼용·mTLS material 누락은 listener를
열기 전에 실패한다. 연결 후 TLS/mTLS 실패는 terminal connection failure이며 plaintext로 전환하지 않는다.

| code | 대표 조건 | 새 operation 조건 |
| --- | --- | --- |
| `INVALID_ARGUMENT` | UUID/config/frame 오류 | 입력 변경 |
| `UNAUTHENTICATED` | TLS 인증서/ClusterToken 검증 실패 | credential/config 변경 |
| `PERMISSION_DENIED` | authenticated component owner/operation 불일치 | identity/config 변경 |
| `NOT_FOUND` | current Binding 없음 | 상태 변경 |
| `FAILED_PRECONDITION` | self Binding만 존재, closed object | 전제 변경 |
| `UNAVAILABLE` | drain, dependency/transport loss | backoff |
| `DEADLINE_EXCEEDED` | bounded deadline 만료 | observation 확인 |
| `RESOURCE_EXHAUSTED` | session/binding/Pipe/dial/queue/frame 상한 | 부하 감소 |
| `CANCELLED` | owner operation/session 종료 | caller 결정 |
| `PROTOCOL_ERROR` | version, frame order·ownership 위반 | 구현/config 수정 |
| `INTERNAL` | internal invariant/lock failure | terminal |
| `ALREADY_EXISTS` | 같은 Relay·Destination Listener 중복 | 기존 Listener 종료 |

## 상태 전이

```mermaid
stateDiagram-v2
    state RelaySession {
        [*] --> CONNECTING
        CONNECTING --> ACTIVE
        ACTIVE --> RECONNECTING
        RECONNECTING --> ACTIVE
        CONNECTING --> BLOCKED
        ACTIVE --> BLOCKED
        RECONNECTING --> BLOCKED
        CONNECTING --> CLOSED
        ACTIVE --> CLOSED
        RECONNECTING --> CLOSED
        BLOCKED --> CLOSED
    }
    state Listener {
        [*] --> REGISTERING
        REGISTERING --> ACTIVE
        REGISTERING --> BLOCKED
        REGISTERING --> CLOSED
        ACTIVE --> SUSPENDED
        SUSPENDED --> ACTIVE
        ACTIVE --> BLOCKED
        SUSPENDED --> BLOCKED
        ACTIVE --> CLOSED
        SUSPENDED --> CLOSED
        BLOCKED --> CLOSED
    }
    state Binding {
        [*] --> ABSENT
        ABSENT --> ACTIVE: PUBLISHED
        ACTIVE --> REMOVED: Listener/session close
    }
```

Session reconnect는 Listener identity를 유지하고 새 SessionId와 BindingId를 만듭니다. 늦은 old-session
`PUBLISHED/OFFER`는 current state를 유지하며 `REMOVED` Binding은 terminal입니다.

```mermaid
stateDiagram-v2
    state Dial {
        [*] --> REQUESTED
        REQUESTED --> RESOLVING
        RESOLVING --> OFFERED
        OFFERED --> OPENED
        REQUESTED --> FAILED
        RESOLVING --> FAILED
        OFFERED --> FAILED
    }
    state Pipe {
        [*] --> OFFERED
        OFFERED --> OPEN
        OPEN --> HALF_CLOSED
        OPEN --> CLOSED
        HALF_CLOSED --> CLOSED
    }
```

```mermaid
stateDiagram-v2
    [*] --> REGISTERING: RT Register
    REGISTERING --> SYNCED
    SYNCED --> UNSYNCED: RT loss/restart
    UNSYNCED --> SYNCED: full snapshot
    SYNCED --> DEREGISTERING
    DEREGISTERING --> REMOVED
    REGISTERING --> TERMINAL: auth/config
    UNSYNCED --> TERMINAL: auth/config
```

## 장애 전파

SDK session 생성 전 handshake capacity 초과는 새 socket을 닫는다. 이 시점에는 wire 오류 응답을 보장하지 않는다.
TLS는 5초, HELLO 수신·응답은 합쳐 5초 이내 종료한다. WELCOME 쓰기 실패·만료는 이미 예약된 session을 정리하고
handshake/transport slot을 반환한다. 다른 admitted session은 유지한다.

| 장애 | 종료 범위 | 유지 범위 | 복구 |
| --- | --- | --- | --- |
| SDK–GW loss | session 소유 Pipe/dial/Binding | 다른 session·Binding | reconnect + Listener republish |
| OFFER uncertain | selected RelaySession | sibling session·Binding | reconnect; caller 새 dial |
| OFFER pre-commit full | 해당 dial | selected session·Binding·Pipe | 부하 감소 뒤 새 dial |
| GW–GW loss | 해당 transport의 stream/Pipe | local Binding·다른 transport | 다음 dial이 transport 생성 |
| GW–RT loss | remote resolve·sync | local Binding·established Pipe | worker reconnect + snapshot |
| RT restart | 해당 shard lease/mapping | Gateway local Binding·Pipe | Gateway 재등록 |
| GW drain | 신규 admission 후 deadline의 owned state | 다른 GW·RT state | SDK/peer reconnect |

## 불변 조건

| ID | 계약 |
| --- | --- |
| `STATE-001` | terminal incarnation은 terminal 상태를 유지한다. |
| `STATE-002` | session terminal cleanup은 그 session 소유 Binding, attempt와 Pipe에 한정된다. |
| `STATE-003` | uncertain publish/dial은 current session 종료로 orphan 가능성을 제거한다. |
| `STATE-004` | late·duplicate·foreign event는 current sibling state와 격리된다. |
| `STATE-005` | RT loss/restart 동안 local Binding과 established Pipe를 유지한다. |
| `STATE-006` | cleanup 반복 적용은 같은 empty/current-state 결과로 수렴한다. |
| `STATE-007` | remote DIAL rejection은 request scope이며 모든 terminal path가 admission을 반환한다. |
| `STATE-008` | OFFER pre-commit failure는 request scope, 그 밖의 writer uncertainty는 session scope다. |
