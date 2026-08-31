# ADR 006: Gateway data plane은 one-hop multiplexed relay다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 001](001-relayed-pipe-responsibility-boundary.md), [ADR 002](002-application-protocol-boundary.md), [ADR 004](004-current-state-routing-topology.md) |

## 맥락

Entry Gateway와 Listener owner Gateway가 다를 수 있다. SDK 연결마다 Gateway 간 transport를 새로 만들거나 여러 Gateway를 경유하면 connection 수와 failure surface가 증가한다.

## 결정

```text
local  : Entry Gateway ───────────────► Listener
remote : Entry Gateway ─► Owner Gateway ─► Listener
                         최대 one hop

reusable peer transport
  ├── StreamId 0/2/4... -> dialer가 시작한 Pipe
  ├── StreamId 1/3/5... -> acceptor가 시작한 Pipe
  └── FIN | CLOSE | RESET

unordered Gateway pair
  ├── A가 dial한 PeerTransport 0..1
  └── B가 dial한 PeerTransport 0..1
```

Gateway 간 payload path는 최대 one hop이다. 각 Gateway가 dial한 방향별 transport는 최대 하나이며, unordered Gateway pair에는 동시에 최대 두 개의 reusable bidirectional peer transport가 존재할 수 있다. 각 transport는 여러 Pipe를 독립적인 logical stream으로 multiplex한다.

양쪽 Gateway가 동시에 dial하면 서로 반대 방향의 두 transport를 모두 유지한다. cross-Gateway winner 합의나 total-order arbitration은 하지 않는다. 같은 Gateway가 같은 peer로 만드는 중복 candidate만 자기 방향 안에서 직렬화하고 제거한다.

peer handshake의 Gateway identity는 배포 환경이 인증한 transport context와 일치해야 한다. handshake frame이 주장하는 `GatewayId`만으로 peer를 신뢰하지 않는다. TLS, mTLS 또는 service mesh의 구체적인 선택은 이 ADR이 정하지 않는다.

하나의 PeerTransport 안에서는 transport dialer와 acceptor가 서로 다른 initiator bit를 사용해 `StreamId` 공간을 나눈다. Pipe의 한 방향 graceful shutdown은 `FIN`, 정상적인 전체 종료는 `CLOSE`, 실패에 의한 전체 종료는 `RESET`으로 구분한다.

## 결과

- SDK connection 수만큼 Gateway 간 transport를 만들지 않는다.
- RouteTable은 Pipe가 수립된 뒤 payload lifecycle에 관여하지 않는다.
- peer transport failure는 영향을 받는 Pipe failure로 관찰되며 기존 Pipe를 이동·replay·resume하지 않는다.
- 한 방향 transport가 끊겨도 반대 방향 transport와 그 stream은 유지된다.
- 통신 중인 unordered Gateway pair 수가 `E`이면 READY transport 수는 최대 `2E`다.
- 양쪽이 동시에 stream을 열어도 별도 stream-ID 합의 없이 충돌하지 않는다.
- half-close와 정상 종료, 실패 종료가 하나의 최소 상태 모델로 구분된다.

## 이 ADR에서 정하지 않는 것

- 두 READY transport 사이의 stream scheduling
- transport protocol과 wire format
- transport identity와 integrity를 제공하는 TLS, mTLS 또는 service-mesh 구현
- flow-control window, scheduling과 resource limit
- transport liveness와 zero-stream idle retirement. 이는 [ADR 007](007-transport-liveness-and-idle-retirement.md)이 정한다.

## 참고

- [RFC 4254](../rfc/rfc-4254-ssh-channel.md)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [RFC 9000](../rfc/rfc-9000-quic-streams.md)
- [RFC 3439](../rfc/rfc-3439-simplicity-principle.md)
