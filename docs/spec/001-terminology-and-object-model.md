# SPEC 001: 용어와 객체 모델

```text
Relay SDK x N --> Gateway x N --> RouteTable shard x M       N >> M
                    `-- Gateway 간 최대 one-hop PeerTransport
```

## 용어

| 용어 | 정의 |
| --- | --- |
| `Relay` | Gateway session 하나로 `listen`·`dial`을 수행하고 Listener lifecycle을 소유하는 SDK runtime |
| `RelaySession` | Relay와 Gateway 사이의 current transport incarnation |
| `SessionId` | Gateway가 session admission마다 발급하는 UUIDv4 |
| `Namespace` | Destination의 authority와 authorization trust 경계를 나타내는 단일 label |
| `DestinationName` | Namespace 안의 계층형 logical destination name |
| `Destination` | `Namespace/DestinationName` 형식의 exact routing key |
| `Listener` | Destination 하나의 지속적인 수신 의도를 소유하는 handle |
| `Binding` | Destination과 live RelaySession의 association |
| `BindingId` | Binding incarnation UUID |
| `BindingSet` | Destination의 current Binding 0..N |
| `AccessToken` | 새 `PUBLISH` 또는 `DIAL` 한 번의 권한을 증명하는 bounded JWT |
| `AccessTokenSource` | operation별 AccessToken을 공급하는 SDK의 static value 또는 async callback |
| `Pipe` | 선택된 Relay 둘 사이의 1:1 opaque bidirectional byte stream |
| `Dialer` | Pipe를 시작한 Relay의 operation별 역할 |
| `Acceptor` | incoming Pipe를 수락하거나 거절하는 Relay의 operation별 역할 |
| `PipeId` | `(Dialer SessionId, session-local ConnectionId)` |
| `GatewayLocator` | Owner Gateway의 peer transport address |
| `ShardDirectory` | `hash(Destination.canonical_key)`를 RT endpoint에 연결하는 immutable artifact |

## Destination 형식

```text
Destination = Namespace "/" DestinationName

Namespace     = label
DestinationName = label *("." label)
label           = lowercase letter/digit [lowercase letter/digit/hyphen] lowercase letter/digit
```

Namespace는 1..63 bytes의 label 하나입니다. DestinationName은 전체 1..253 bytes이고 각 label은
1..63 bytes입니다. 한 글자 label은 lowercase letter 또는 digit입니다.

## 관계와 불변 조건

```text
Relay 1 -- current RelaySession 0..1
Relay 1 -- Listener 0..N
Destination * <-- Binding --> * RelaySession
dial 1회 --> eligible Binding 1개 --> Pipe 1개 --> Relay endpoint 2개
```

| ID | 불변 조건 |
| --- | --- |
| `TERM-001` | Relay 하나의 current RelaySession은 0..1개다. |
| `TERM-002` | Relay 하나의 Destination별 pending/active Listener는 0..1개다. |
| `TERM-003` | 서로 다른 Relay는 같은 Destination을 동시에 listen할 수 있다. |
| `TERM-004` | Destination은 process, RelaySession과 Gateway location에서 독립적이며 application이 생성·보관한다. |
| `TERM-005` | Binding은 live RelaySession 하나와 exact Destination 하나에 속한다. |
| `TERM-006` | SessionId, BindingId와 PipeId는 incarnation마다 새 값이다. |
| `TERM-007` | dial 한 번은 Binding 하나를 선택하고 Pipe 하나를 만든다. |
| `TERM-008` | eligible BindingSet은 caller RelaySession 소유 Binding을 제외한다. |
| `TERM-009` | RT BindingProjection은 Binding의 derived current state만 포함한다. |
| `TERM-010` | Dialer와 Acceptor는 session 종류가 아니라 Pipe별 방향 역할이다. |
| `TERM-011` | RelaySession 수립은 application identity나 Destination 권한을 부여하지 않는다. |
| `TERM-012` | routing은 exact Destination만 사용하며 계층·wildcard는 authorization 범위에만 사용한다. |

## 소유권

| 값 | 생성 | lifetime·보관 |
| --- | --- | --- |
| Destination | application | application lifetime |
| JWT private key·token 발급·갱신 | application/backend | RelayGate runtime 외부 |
| operation token helper | `relaygate-token-issuer` | backend 선택 라이브러리 |
| issuer·audience·ES256 public JWK | operator | Gateway static config |
| certificate | operator/platform | external Secret/config |
| SessionId·BindingId·PipeId | RelayGate runtime | incarnation lifetime |
| RT BindingProjection·lease | Gateway/RT runtime | active registration lifetime |
