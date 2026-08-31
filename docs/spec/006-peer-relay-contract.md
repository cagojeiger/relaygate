# SPEC 006: Gateway peer relay 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 006](../adr/006-one-hop-peer-multiplexing.md), [ADR 007](../adr/007-transport-liveness-and-idle-retirement.md) |
| 관련 계약 | [SPEC 005](005-connection-establishment-contract.md), [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 Entry Gateway와 Owner Gateway가 다를 때 사용하는 one-hop peer data path를 정의한다.

```text
local  : Entry Gateway ───────────────────► Listener
remote : Entry Gateway ─► Owner Gateway ─► Listener
                         Gateway hop = 1

Gateway pair
  ├── A가 dial한 reusable PeerTransport 0..1
  │     ├── RelayStream A <──> Pipe A
  │     └── RelayStream B <──> Pipe B
  └── B가 dial한 reusable PeerTransport 0..1
        └── RelayStream C <──> Pipe C
```

## Stream identity와 frame

하나의 `PeerTransport`에서는 transport dialer를 initiator bit `0`, acceptor를 bit `1`로 고정한다. 양쪽 endpoint는 각자 63-bit local counter를 `0`부터 증가시키며 새 stream을 시작할 때 다음 식을 사용한다.

```text
StreamId = (local_counter << 1) | initiator_bit

dialer가 시작한 stream   = 0, 2, 4, ...
acceptor가 시작한 stream = 1, 3, 5, ...
```

두 endpoint가 동시에 같은 counter 값으로 stream을 열어도 `StreamId`는 다르다. counter는 OPEN 성공 여부와 관계없이 소비하며 같은 transport에서 wrap하거나 재사용하지 않는다.

각 PeerTransport endpoint의 단일 actor가 local counter 할당과 `OPEN`의 bounded writer-queue
commit을 함께 직렬화한다. 여기서 commit은 frame이 그 transport의 유일한 ordered writer
queue에 수락된 시점이다. 따라서 같은 endpoint에서는 counter 순서와 wire commit 순서가 같고,
뒤 counter의 `OPEN`이 먼저 전송되지 않는다. commit 전에 실패해도 할당한 counter는 재사용하지
않는다.

각 endpoint는 local counter 하나와 remote endpoint에서 본 가장 큰 counter 하나만 유지한다. 수신한 valid OPEN의 counter가 remote high-watermark보다 크지 않으면 늦거나 재사용된 ID로 거절한다. high-watermark는 OPEN 결과가 성공인지 실패인지와 관계없이 전진하므로 terminal StreamId별 tombstone을 보관하지 않아도 된다.

```text
OPEN(StreamId, OpenIdentity, BindingIdentity)
  └── OPENED(StreamId) | FAILED(StreamId, Error, PeerObservation)

DATA(StreamId, Bytes)
FIN(StreamId)          # sender -> receiver 방향 EOF
CLOSE(StreamId)        # 정상적인 전체 stream 종료
RESET(StreamId, Error) # 실패에 의한 전체 stream 종료
PING(Nonce)            # transport liveness probe
PONG(Nonce)            # transport liveness response
```

`OPENING` stream의 `RESET(CANCELLED)`은 remote OPEN 취소와 cleanup을 함께 표현한다. 별도
peer `CANCEL` frame은 없다. Owner가 이미 Listener queue admission을 마쳤더라도 같은 stream의
Pipe를 실패 종료하고, 늦거나 중복된 RESET은 새 state를 만들지 않는다.

`OpenIdentity = (EntryGatewayId, ConnectorSessionId, ConnectionId)`는 current `RelayStream`과
Entry 쪽 attempt를 상관시키는 값이다. SDK-origin `ConnectionId`의 strict high-watermark는
SDK→Entry `ConnectorSession`에서만 적용한다. peer leg의 중복·순서는 transport-local
`StreamId`로 검증한다. Owner Gateway는 current stream에 결합된 `OpenIdentity`만 보유하며 stream
종료 뒤 remote `ConnectorSession` high-watermark, `OpenIdentity` tombstone 또는 replay table을
보관하지 않는다.

최초 local/CI runtime profile은 Gateway별 `GatewayName -> InternalGatewayKey` allowlist를
startup configuration으로 고정한다. handshake의 name/key를 constant-time으로 검증한 뒤
connection이 제시한 fresh runtime `GatewayId`를 authenticated peer identity에 결합한다.
claimed `GatewayId`만으로 `READY`가 될 수 없고 key는 로그나 stream state에 기록하지 않는다.
이 trusted-local adapter는 plain TCP의 confidentiality나 production-grade integrity를 보장하지
않으며 실제 배포 identity adapter의 대체물이 아니다.

이 계약은 handshake를 통과한 conforming Gateway가 peer frame 규칙을 지킨다고 전제한다.
인증된 Gateway가 과거 `OpenIdentity`를 악의적으로 조합해 재생하는 Byzantine peer 방어는 범위
밖이다. transport-local `StreamId` 검증과 current-state cleanup을 durable replay protection으로
확대해서는 안 된다.

`PING`과 `PONG`은 transport-level control frame이다. 특정 `StreamId`나 Pipe에 속하지 않으며
application payload 처리, delivery acknowledgement 또는 peer health score로 해석하지 않는다.

frame의 encoding과 underlying transport는 범위가 아니지만 위 frame의 상태 의미는 구현에 관계없이 동일하다.

## PeerTransport liveness와 idle retirement

```text
PeerTransport READY
  ├── stream_count > 0
  │     └── activity-aware PING/PONG
  │           timeout -> transport close -> contained streams/Pipes fail
  └── stream_count == 0
        └── no keepalive
              idle-retire timeout -> normal transport close
```

`PeerTransport` heartbeat는 stream이 하나 이상 있는 동안만 필요하다. DATA, FIN, CLOSE, RESET,
OPEN, OPENED, FAILED, PING을 포함한 valid inbound frame은 `PING` 전송 전에는 heartbeat
timer를 연장할 수 있다. configured idle interval 동안 inbound activity가 없으면 endpoint는
`PING`을 commit하고, configured response deadline 전에 matching `PONG`을 확인하지 못하면 해당
`PeerTransport`를 닫는다. unrelated inbound frame과 outbound write는 commit된 probe를
만족시키지 않는다. pending probe가 없거나 nonce가 일치하지 않는 `PONG`도 liveness 근거로
사용하지 않는다. response deadline 이후의 늦은 matching `PONG`도 probe를 만족시키지 않는다.

stream 수가 0이 되면 endpoint는 heartbeat를 멈추고 idle-retirement timer를 시작한다. 새 stream이
그 transport를 재사용하면 timer를 취소한다. timer가 만료되면 transport를 정상 종료하고 slot은
이후 lazy connect 가능한 상태로 돌아간다. zero-stream transport에는 keepalive traffic을 보내지
않는다.

## 요구사항

| ID | 요구사항 |
| --- | --- |
| `PEER-001` | local binding에는 peer transport를 사용하지 않는다. remote binding의 Gateway 경유는 선택된 Owner Gateway 한 번으로 끝나야 한다. peer에서 OPEN을 받은 Owner Gateway는 RT를 Resolve하거나 다른 Gateway로 forwarding해서는 안 된다. |
| `PEER-002` | 하나의 unordered Gateway pair에는 `DialerGatewayId`로 구분되는 방향별 `PeerTransportSlot`이 두 개 있어야 한다. 각 slot은 `READY` PeerTransport를 최대 하나만 가지므로 pair 전체에는 최대 두 개가 존재할 수 있다. transport 자체는 양방향이다. |
| `PEER-003` | remote OPEN은 pair에 이미 있는 어느 `READY` PeerTransport든 재사용해야 한다. 하나도 없을 때만 요청을 받은 Gateway가 자기 방향 slot의 candidate를 lazy하게 연결한다. 모든 Gateway pair의 eager full mesh를 요구하지 않는다. |
| `PEER-004` | 한 Gateway는 같은 peer를 향한 자기 slot의 candidate 생성을 직렬화해야 한다. 그 slot이 `CONNECTING` 또는 `READY`인 동안 같은 방향 duplicate candidate는 RelayStream을 받기 전에 닫는다. 서로 반대 방향의 candidate는 duplicate가 아니며 둘 다 `READY`가 될 수 있다. |
| `PEER-005` | 각 candidate는 handshake에서 참여 Gateway identity와 `(DialerGatewayId, PeerTransportId)`를 교환하고, 그 identity가 배포 환경이 인증한 transport peer와 일치하며 양쪽이 같은 pair와 방향 slot임을 확인한 뒤에만 `READY`가 되어야 한다. claimed `GatewayId`만으로 peer를 신뢰해서는 안 된다. transport credential 또는 Gateway name을 인증하지 못하면 `UNAUTHENTICATED`, 인증된 peer가 다른 runtime owner·pair·direction을 주장하면 `PERMISSION_DENIED`다. 둘 다 candidate를 stream 없이 `CLOSED`로 만들고 해당 OPEN을 `NOT_OBSERVED`로 실패시켜야 한다. cross-Gateway winner 합의, candidate total order 또는 도착 순서 기반 arbitration을 해서는 안 되며 handshake 완료 전 RelayStream을 실어서는 안 된다. |
| `PEER-006` | 하나의 `READY` PeerTransport는 서로 구분되는 여러 bidirectional RelayStream을 multiplex한다. 각 RelayStream은 정확히 하나의 Pipe에 대응한다. |
| `PEER-007` | `StreamId`는 해당 PeerTransport 안에서 유일해야 한다. 중복 OPEN은 기존 stream에 영향을 주지 않고 `PROTOCOL_ERROR`로 실패한다. |
| `PEER-008` | per-stream buffer와 PeerTransport 전체 buffer·queue는 모두 bounded여야 한다. 수신 capacity가 없으면 unbounded buffering 대신 backpressure를 전달한다. |
| `PEER-009` | 한 stream의 limit과 transport 전체 limit을 별도로 적용한다. stream limit 도달은 다른 stream의 state를 닫지 않으며, transport limit은 새 write나 OPEN을 대기시키거나 `RESOURCE_EXHAUSTED`로 실패시킬 수 있다. |
| `PEER-010` | RelayStream close와 cleanup은 idempotent해야 하며, 닫힌 StreamId를 payload로 다시 활성화할 수 없다. |
| `PEER-011` | PeerTransport가 끊기면 그 transport에 실린 모든 RelayStream과 대응 Pipe를 `CLOSED`로 만든다. 기존 Pipe를 새 transport로 이동하지 않는다. |
| `PEER-012` | transport 단절 뒤 살아 있는 반대 방향 `READY` transport가 있으면 이후의 새 Pipe가 이를 사용할 수 있고, 하나도 없으면 새 candidate를 lazy하게 연결할 수 있다. 어느 경우에도 이전 Pipe나 payload를 새 transport로 이동, replay 또는 resume하지 않는다. |
| `PEER-013` | 같은 방향 duplicate, 연결 실패와 transport 단절에서 candidate, stream 및 buffer 상태는 configured bound 안에 정리되어야 한다. 한 slot의 cleanup이 반대 방향 slot과 그 stream을 닫아서는 안 된다. |
| `PEER-014` | remote OPEN으로 만든 RelayStream은 정확히 하나의 `OpenIdentity = (EntryGatewayId, ConnectorSessionId, ConnectionId)`에 결합되어야 한다. ConnectorSession 단절은 그 session의 current RelayStream마다 `RESET(CANCELLED)`을 보내고, commit에 성공하면 다른 session stream과 PeerTransport를 유지해야 한다. 하나라도 `RESET`을 bounded writer queue에 commit할 수 없으면 해당 PeerTransport를 닫아 그 transport의 모든 stream을 transport-loss cleanup으로 끝내야 한다. Owner는 current RelayStream의 `OpenIdentity`만 보유하고 종료 뒤 remote ConnectorSession high-watermark나 terminal identity history를 보관해서는 안 된다. |
| `PEER-015` | PeerTransport dialer는 initiator bit `0`, acceptor는 bit `1`을 사용해야 한다. 새 stream은 `StreamId = (local_counter << 1) \| initiator_bit`로 할당하고 counter는 `0`부터 단조 증가해야 한다. |
| `PEER-016` | OPEN 성공 여부와 관계없이 사용한 local counter와 `StreamId`를 같은 PeerTransport에서 재사용하거나 wrap해서는 안 된다. 63-bit counter를 더 할당할 수 없으면 기존 stream을 유지한 채 새 OPEN을 `RESOURCE_EXHAUSTED`로 실패시켜야 한다. receiver는 remote counter high-watermark를 bounded state로 유지하고 role bit가 remote endpoint와 다르거나 counter가 high-watermark 이하인 OPEN을 `PROTOCOL_ERROR`로 거절해야 하며, valid OPEN의 high-watermark는 OPEN 결과 전에 전진해야 한다. |
| `PEER-017` | peer stream protocol의 최소 frame은 `OPEN`, `OPENED`, `FAILED`, `DATA`, `FIN`, `CLOSE`, `RESET`이어야 한다. peer control frame과 같은 StreamId의 frame은 sender가 보낸 순서로 처리해야 한다. 하나의 OPEN은 `OPENED` 또는 `FAILED` 하나로만 끝나며 그 뒤 결과를 바꾸어서는 안 된다. local `OPENING` 중 받은 `RESET` 또는 stream-local protocol violation은 established stream용 `RESET` event가 아니라 `FAILED(code, MAYBE_OBSERVED)`로 Entry에 전달하여 current attempt를 끝내야 한다. |
| `PEER-018` | `FIN`은 sender 방향에서 먼저 받은 모든 `DATA` 뒤에 EOF를 전달해야 한다. 해당 방향은 더 이상 `DATA`를 허용하지 않지만 반대 방향은 유지하며, 양방향이 모두 `FIN`이면 RelayStream과 Pipe를 정상 종료해야 한다. |
| `PEER-019` | `CLOSE`는 정상적인 양방향 전체 종료이고 `RESET`은 error를 동반한 실패 종료여야 한다. `CLOSE`는 payload 처리나 delivery acknowledgement를 의미하지 않으며 graceful write drain은 `FIN`으로 표현해야 한다. |
| `PEER-020` | 한 방향의 `FIN` 뒤 그 sender에서 `DATA`가 오면 해당 RelayStream을 `PROTOCOL_ERROR`로 `RESET`해야 한다. 다른 RelayStream과 PeerTransport는 유지해야 한다. transport-level frame을 안전하게 demultiplex할 수 없는 위반만 transport 전체를 닫을 수 있다. |
| `PEER-021` | duplicate `FIN`, `CLOSE`, `RESET`과 cleanup은 idempotent해야 한다. 서로 다른 terminal 신호가 경쟁하면 각 endpoint에서 먼저 확정한 terminal 결과를 바꾸지 않는다. 이미 제거한 `StreamId`의 늦은 frame은 새 stream이나 Pipe를 만들지 않고 버려야 하며 remote StreamId counter high-watermark 외에 `OpenIdentity`를 포함한 unbounded terminal history를 보관해서는 안 된다. |
| `PEER-022` | 한 PeerTransport endpoint의 단일 actor는 local counter 할당과 `OPEN` writer-queue commit을 같은 순서로 직렬화해야 한다. receiver가 관찰하는 remote counter 순서는 strictly increasing이어야 하며, 할당 뒤 commit에 실패한 counter도 재사용해서는 안 된다. concurrent local OPEN이 낮은 counter를 뒤늦게 commit하여 receiver high-watermark에 거절되는 구조를 허용해서는 안 된다. |
| `PEER-023` | connect 또는 handshake deadline은 candidate를 `CLOSED`, 해당 slot을 `IDLE`로 만들고 peer `OPEN` commit 전 attempt를 `NOT_OBSERVED`로 실패시켜야 한다. commit 뒤 terminal result deadline 또는 transport loss는 `MAYBE_OBSERVED`다. peer `OPENED` 수신은 Listener queue admission을 확인하고 RelayStream을 열지만 external `OBSERVED`는 Connector SDK의 `OPENED` 확인 뒤에만 성립한다. `OPENING` stream cancel은 `RESET(CANCELLED)`로 표현하고 별도 `CANCEL` frame, 같은 attempt replay·reroute·resume를 두어서는 안 된다. |
| `PEER-024` | 최초 local/CI adapter는 Gateway별 `InternalGatewayKey`를 constant-time으로 검증하고 성공한 connection의 fresh runtime `GatewayId`에 peer identity를 결합해야 한다. unknown name·잘못된 key는 `UNAUTHENTICATED`, name/key 검증 뒤 다른 runtime owner·pair·direction claim은 `PERMISSION_DENIED`다. 둘 다 candidate를 stream 없이 `CLOSED`, 해당 OPEN을 `NOT_OBSERVED`로 끝내야 한다. key를 로그·RelayStream state에 기록하거나 plain TCP adapter를 production confidentiality/integrity 보장으로 표현해서는 안 된다. 인증을 통과한 conforming Gateway를 신뢰하며 Byzantine peer의 semantic replay 방어는 이 계약의 범위가 아니다. |
| `PEER-025` | Gateway 상태 전이를 전달하는 bounded `PeerEvent` queue가 실행 중 Full이면 event를 버리거나 peer manager를 block하지 않고 peer runtime을 `RESOURCE_EXHAUSTED` terminal로 끝내야 한다. receiver가 실행 중 닫히면 `UNAVAILABLE` terminal이다. 두 경우 distributed Gateway는 fail-closed shutdown으로 current PeerTransport와 SDK session을 정리한다. 이미 시작된 정상 shutdown 중 Full 또는 Closed는 새 장애가 아니라 idempotent cleanup으로 처리한다. |
| `PEER-026` | `stream_count > 0`인 READY PeerTransport는 activity-aware heartbeat를 수행해야 한다. idle interval 동안 valid inbound activity가 없으면 `PING`을 commit하고 configured response deadline 전에 matching `PONG`을 확인하지 못하면 transport loss와 같이 해당 PeerTransport를 닫아 그 안의 RelayStream과 Pipe를 terminal cleanup해야 한다. unrelated inbound frame, outbound write, nonce가 다른 `PONG`, deadline 이후의 늦은 `PONG`은 commit된 probe를 만족시키지 않는다. |
| `PEER-027` | `stream_count == 0`인 READY PeerTransport는 keepalive를 보내지 않고 idle-retirement timer만 유지해야 한다. 새 stream이 재사용하면 timer를 취소하고, timeout까지 재사용되지 않으면 transport를 정상 종료해야 한다. 이 종료는 기존 Pipe replay, reroute 또는 payload resume을 만들지 않는다. |

## 연결 수 증가

```text
SDK Pipe 수 증가       -> RelayStream 수 증가
통신하는 unordered Gateway pair E -> READY PeerTransport 0..2E
SDK Pipe마다 PeerTransport 생성 -> 금지

Gateway 수 G의 full mesh 최악값
  한 Gateway가 보유하는 transport endpoint <= 2(G - 1)
  전체 READY transport                 <= G(G - 1)
```

두 READY transport 사이의 stream 선택, transport protocol, flow-control 수치와
scheduling algorithm은 이 계약의 범위가 아니다. 선택 정책은 어느 transport를 사용해도 Pipe의
정확성이나 failure isolation 의미를 바꾸어서는 안 된다.

transport peer identity와 integrity는 필수 precondition이지만 이를 제공하는 TLS, mTLS, certificate 또는 service-mesh 구현은 이 계약의 범위가 아니다.
