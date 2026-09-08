# SPEC 001: 용어와 객체 모델

```text
Relay SDK × N ──► Gateway × N ──► RouteTable shard × M       N >> M
                    └── Gateway 간 최대 one-hop PeerTransport
```

## 용어

| 용어 | 정의 |
| --- | --- |
| `Relay` | Gateway session 하나로 `listen`·`dial`을 수행하고 Listener lifecycle을 소유하는 SDK runtime |
| `RelaySession` | Relay와 Gateway 사이의 current transport incarnation |
| `SessionId` | Gateway가 session admission마다 발급하는 UUIDv4 |
| `DestinationId` | application-owned UUIDv4 routing address |
| `Listener` | Destination 하나의 지속적인 수신 의도를 소유하는 handle |
| `Binding` | Destination과 live RelaySession의 association |
| `BindingId` | Binding incarnation UUID |
| `BindingSet` | Destination의 current Binding 0..N |
| `Pipe` | 선택된 Relay 둘 사이의 1:1 opaque bidirectional byte stream |
| `PipeId` | `(origin SessionId, session-local ConnectionId)` |
| `ClusterToken` | RelayGate trust-domain session admission bearer secret |
| `GatewayLocator` | Owner Gateway의 peer transport address |
| `ShardDirectory` | `hash(DestinationId)`를 RT endpoint에 연결하는 immutable artifact |

## 관계와 불변 조건

```text
Relay 1 ── current RelaySession 0..1
Relay 1 ── Listener 0..N
Destination * ◄── Binding ──► * RelaySession
dial 1회 ──► eligible Binding 1개 ──► Pipe 1개 ──► Relay endpoint 2개
```

| ID | 불변 조건 |
| --- | --- |
| `TERM-001` | Relay 하나의 current RelaySession은 0..1개다. |
| `TERM-002` | Relay 하나의 Destination별 pending/active Listener는 0..1개다. |
| `TERM-003` | 서로 다른 Relay는 같은 Destination을 동시에 listen할 수 있다. |
| `TERM-004` | Destination identity는 process, RelaySession과 Gateway location에서 독립적이다. |
| `TERM-005` | Binding은 live RelaySession 하나와 Destination 하나에 속한다. |
| `TERM-006` | SessionId, BindingId와 PipeId는 incarnation마다 새 값이다. |
| `TERM-007` | dial 한 번은 Binding 하나를 선택하고 Pipe 하나를 만든다. |
| `TERM-008` | eligible BindingSet은 caller RelaySession 소유 Binding을 제외한다. |
| `TERM-009` | RT mapping은 Binding의 derived current state만 포함한다. |
| `TERM-010` | Connector와 Listener는 session 종류가 아니라 Pipe별 방향 역할이다. |

## 소유권

| 값 | 생성 | lifetime·보관 |
| --- | --- | --- |
| DestinationId | application | application lifetime |
| ClusterToken·certificate | operator | external Secret/config |
| SessionId·BindingId·PipeId | RelayGate runtime | incarnation lifetime |
| RT mapping·lease | Gateway/RT runtime | active registration lifetime |
