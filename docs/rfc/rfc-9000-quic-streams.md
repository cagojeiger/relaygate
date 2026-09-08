# RFC 9000: QUIC streams

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc9000.html) |
| 성격 | Standards Track, 2021-05 |
| 목적 | secure multiplexed transport의 stream·connection lifecycle |

| 개념 | 의미 |
| --- | --- |
| stream | 독립 ordered byte sequence, uni/bidirectional |
| stream ID | initiator·direction bit + monotonic stream number |
| stream flow control | 한 stream의 receive-buffer 사용 제한 |
| connection flow control | 모든 stream의 총 receive-buffer 사용 제한 |
| reset | 한 송신 방향의 terminal state |
| idle timeout | negotiated connection state expiry |
| liveness | PING 또는 ack-eliciting frame으로 path 확인 |

```text
Connection credit
  ├── Stream A credit
  ├── Stream B credit
  └── Stream C credit
```

Security, loss recovery, congestion control과 migration은 QUIC 전체 protocol의 구성입니다. Stream priority,
scheduling과 application liveness policy는 사용하는 application protocol이 결정합니다.

원문 절: [§2](https://www.rfc-editor.org/rfc/rfc9000.html#section-2), [§3](https://www.rfc-editor.org/rfc/rfc9000.html#section-3), [§4](https://www.rfc-editor.org/rfc/rfc9000.html#section-4), [§10.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-10.1)
