# ADR 006: Gateway data plane은 one-hop multiplexed relay다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted |
| path | local 또는 Entry Gateway → Owner Gateway 한 hop |
| transport | Gateway 방향별 reusable PeerTransport 최대 하나 |

## 결정

```text
local  : Entry Gateway ───────────────► Listener
remote : Entry Gateway ─► Owner Gateway ─► Listener

unordered pair {A, B}
  ├── PeerTransport A -> B   0..1
  └── PeerTransport B -> A   0..1

PeerTransport
  ├── StreamId 0/2/4...  dialer initiated
  ├── StreamId 1/3/5...  acceptor initiated
  └── FIN | CLOSE | RESET
```

| 규칙 | 선택 |
| --- | --- |
| pair arbitration | 방향별 slot을 독립적으로 유지 |
| duplicate | 같은 방향 candidate를 local에서 직렬화·정리 |
| multiplexing | 여러 Pipe를 독립 RelayStream으로 전달 |
| StreamId | initiator bit + 방향별 monotonic counter |
| identity | mTLS context와 logical Gateway handshake를 함께 검증 |
| terminal scope | stream 종료는 sibling stream 유지, transport loss는 소속 stream 종료 |

통신 중인 unordered Gateway pair가 `E`개이면 READY transport는 최대 `2E`개입니다.

## 참고

- [RFC 4254](../rfc/rfc-4254-ssh-channel.md)
- [RFC 9000](../rfc/rfc-9000-quic-streams.md)
- [RFC 9293](../rfc/rfc-9293-tcp-connection-roles.md)
- [RFC 7426](../rfc/rfc-7426-sdn-architecture.md)
