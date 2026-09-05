# ADR 010: 하나의 Relay 세션이 송신과 수신을 함께 수행한다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택, 구현됨 |
| 변경 대상 | ADR 001·003의 고정 Connector/Listener 세션 역할 |

## 결정

```text
Relay runtime 1 ── current RelaySession 0..1
Relay::listen(DestinationId) ──► Listener
Relay::dial(DestinationId)   ──► Pipe
Listener::accept()           ──► Pipe

Destination * ◄── Binding ──► * RelaySession
dial 1회 ──► Binding 1개 ──► 양방향 Pipe 1개
```

SDK, wire와 Gateway 상태 모델에서 고정 `Connector/Listener` 역할과 `SessionRole`을 제거한다.
송신자와 수신자는 Pipe마다 정해지는 역할이며 세션 종류가 아니다. `Listener`는 하나의
Destination을 계속 수신하겠다는 SDK handle이고 별도 transport session이 아니다.

한 Relay 안에는 같은 Destination의 닫히지 않은 Listener를 최대 하나만 둔다. 다른 Relay는 같은
Destination을 동시에 listen할 수 있다. 자기 RelaySession이 소유한 Binding은 dial 후보에서 제외하며,
다른 후보가 없으면 `FAILED_PRECONDITION`으로 끝낸다.

세션이 끊기면 기존 Pipe와 commit된 dial은 복구하거나 replay하지 않는다. SDK는 RelaySession을
재연결하고 이미 반환된 live Listener만 새 SessionId와 BindingId로 다시 publish한다.

## 결과

- NAT 뒤의 한 application이 outbound 세션 하나로 수신과 송신을 모두 수행한다.
- public SDK는 `Relay`, `Listener`, `Pipe`의 socket-like API를 제공한다.
- Listener의 bounded queue admission이 수신 Pipe 생성 시점이고 `accept()`는 Pipe를 한 번만 꺼낸다.
- Pipe는 항상 1:1이며 fan-out, 기존 Pipe 이동과 payload replay를 제공하지 않는다.
- 0.1 wire와 0.2 wire의 혼용은 지원하지 않는다.

## 참고

- [RFC 4254](../rfc/rfc-4254-ssh-channel.md)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [SPEC 001](../spec/001-terminology-and-object-model.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
