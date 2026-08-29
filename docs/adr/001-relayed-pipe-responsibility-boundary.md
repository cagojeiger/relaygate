# ADR 001: RelayGate는 logical destination으로 Pipe를 수립한다

| 항목 | 값 |
| --- | --- |
| 상태 | Proposed |

## 맥락

NAT 뒤의 endpoint는 외부의 inbound 연결을 직접 받을 수 없다. RelayGate는 endpoint가 먼저 만든 outbound session을 통해 logical destination으로 연결할 최소 중간 계층이 필요하다.

## 결정

```text
Connector ── logical destination ──► Listener
Connector Pipe ◄════ opaque bidirectional bytes ════► Listener Pipe
```

RelayGate의 책임은 logical destination을 현재 Listener에 연결하여 하나의 양방향 `Pipe`를 수립하고, 그 Pipe의 bytes, backpressure와 close를 중계하는 것까지다.

하나의 성공한 연결은 정확히 하나의 Connector와 하나의 Listener 사이에 하나의 Pipe를 만든다.

## 결과

- RelayGate는 port forwarding, message broker 또는 application server가 아니다.
- application message의 처리·저장·전달 확인은 Pipe 수립과 별개의 의미다.
- 닫힌 Pipe의 payload를 replay하거나 Pipe를 다른 Listener로 resume하지 않는다.

## 이 ADR에서 정하지 않는 것

- SDK API 형태와 연결 성공 시점
- 상태, 오류, timeout, retry
- buffer와 flow-control 크기
- transport와 wire format

## 참고

- [RFC 1928](../rfc/rfc-1928-socks5-relay.md)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
