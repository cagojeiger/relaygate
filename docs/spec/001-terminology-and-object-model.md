# SPEC 001: 용어와 객체 모델

## 구성

```text
Relay SDK × N ──► Gateway × N ──► RouteTable shard × M       N >> M
                    │
                    └── Gateway 간 최대 one-hop PeerTransport
```

## 용어

| 용어 | 정의 |
| --- | --- |
| `Relay` | 하나의 Gateway 세션으로 `listen`과 `dial`을 수행하고 Listener 수명주기를 소유하는 SDK runtime |
| `RelaySession` | Relay와 Gateway 사이의 현재 transport incarnation |
| `SessionId` | Gateway가 성공한 session admission마다 새로 만드는 UUIDv4 |
| `DestinationId` | application이 생성·보관하는 UUIDv4 논리 라우팅 주소 |
| `Listener` | Relay가 Destination 하나를 계속 수신하겠다는 desired-state handle |
| `Binding` | Destination과 현재 RelaySession을 연결하는 live association |
| `BindingId` | Binding incarnation마다 새로 만드는 UUID |
| `BindingSet` | Destination 하나에 대해 현재 관측되는 0..N Binding |
| `Pipe` | 선택된 두 Relay 사이의 1:1 opaque bidirectional byte stream |
| `PipeId` | `(origin SessionId, session-local ConnectionId)` |
| `ClusterToken` | SDK가 RelayGate trust domain에 속함을 보이는 admission bearer secret |
| `GatewayLocator` | Owner Gateway의 peer transport 주소 |
| `ShardDirectory` | `hash(DestinationId)`를 RT shard endpoint에 연결하는 불변 배포 artifact |

## 관계

```text
Relay 1 ── current RelaySession 0..1
Relay 1 ── Listener 0..N

Destination * ◄──── Binding ────► * RelaySession
Destination 1 ── current Binding 0..N
RelaySession 1 ── current Binding 0..N

dial 1회 ──► eligible Binding 1개 ──► Pipe 1개
Pipe 1 ──► Relay endpoint 2개
```

## 불변 조건

- **`TERM-001`**: 한 Relay에는 current RelaySession이 최대 하나만 있다.
- **`TERM-002`**: 한 Relay 안에서 같은 Destination의 pending/active Listener는 합쳐서 최대 하나다.
- **`TERM-003`**: 서로 다른 Relay는 같은 Destination을 동시에 listen할 수 있다.
- **`TERM-004`**: Destination은 특정 process, RelaySession 또는 Gateway와 동일하지 않다.
- **`TERM-005`**: Binding은 live RelaySession 하나와 Destination 하나에만 속한다.
- **`TERM-006`**: SessionId, BindingId와 PipeId incarnation은 재사용하지 않는다.
- **`TERM-007`**: dial 한 번은 Binding 하나만 선택하며 broadcast하지 않는다.
- **`TERM-008`**: 같은 RelaySession이 소유한 Binding은 그 Relay의 dial 후보가 아니다.
- **`TERM-009`**: RT mapping은 Binding의 파생 current state이며 payload와 Pipe를 포함하지 않는다.
- **`TERM-010`**: Connector와 Listener는 session 종류가 아니라 한 Pipe에서만 정해지는 방향별 역할이다.

## 소유권

| 값 | 생성 | 영구 보관 |
| --- | --- | --- |
| DestinationId | application | application |
| ClusterToken/certificate | operator | external Secret/config |
| SessionId/BindingId/PipeId | RelayGate runtime | 보관하지 않음 |
| RT mapping/lease | Gateway/RT runtime | 보관하지 않음 |
