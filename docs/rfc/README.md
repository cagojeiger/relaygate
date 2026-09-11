# RFC 참고 노트

비규범적 한글 색인입니다. protocol의 권위와 세부 규칙은 연결된 RFC Editor 원문입니다.

```text
RFC 원문 ──► 일반 개념 색인 ──► ADR 결정 ──► SPEC 계약 ──► TEST 증거
```

| 층 | RFC | 참고 개념 |
| --- | --- | --- |
| 설계 원칙 | [1958](rfc-1958-internet-architecture.md), [3439](rfc-3439-simplicity-principle.md) | end-to-end, self-healing state, simplicity |
| plane | [7426](rfc-7426-sdn-architecture.md) | application, control, forwarding, operational, management |
| relay request | [1928](rfc-1928-socks5-relay.md) | destination, success/failure, byte relay |
| soft state | [2205](rfc-2205-rsvp-soft-state.md), [8656](rfc-8656-turn-lifetime.md) | refresh, expiry, teardown |
| liveness | [5880](rfc-5880-bfd-liveness.md), [9113](rfc-9113-http2-connection-lifecycle.md) | path detection, PING, graceful close |
| multiplexing | [4254](rfc-4254-ssh-channel.md), [9000](rfc-9000-quic-streams.md) | channel/stream ID, flow control, terminal state |
| connection | [9293](rfc-9293-tcp-connection-roles.md) | active/passive open, byte stream, FIN/RST |
| mapping | [9299](rfc-9299-lisp-architecture.md), [9301](rfc-9301-lisp-control-plane.md) | identifier-to-locator register/resolve |
| operation grant | [7515](rfc-7515-json-web-signature.md), [7517](rfc-7517-json-web-key.md), [7518](rfc-7518-json-web-algorithms.md), [7519](rfc-7519-json-web-token.md), [8725](rfc-8725-jwt-best-current-practices.md) | JWS header, static public JWK, JWT claim, ES256, explicit `typ` 검증 |

각 문서는 RFC의 목적, 핵심 개념, 표준 고유 영역과 필요한 원문 절만 기록합니다.
