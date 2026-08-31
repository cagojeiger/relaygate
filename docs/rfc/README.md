# RFC 참고 노트

이 디렉터리는 외부 RFC를 빠르게 읽기 위한 비규범적(non-normative) 한글 요약이다.

```text
원문 RFC = 사실과 protocol의 권위
이 디렉터리 = 일반 개념을 찾기 위한 색인
ADR        = RelayGate의 결정
SPEC       = RelayGate의 관찰 가능한 계약
```

## 원칙

- 외부 RFC 하나를 파일 하나로 요약한다.
- 일반적인 목적, 용어, 상태와 보장만 기록한다.
- RelayGate의 선택, 기본값과 구현 세부사항은 기록하지 않는다.
- 요약과 원문이 다르면 RFC Editor 원문이 우선한다.
- 세부 wire format과 예외 규칙은 각 파일에 연결된 원문 절을 읽는다.

## 목록

| 층 | 문서 | 핵심 주제 |
| --- | --- | --- |
| 설계 원칙 | [RFC 1958](rfc-1958-internet-architecture.md) | end-to-end, self-healing state, 단순성과 모듈성 |
| 설계 원칙 | [RFC 3439](rfc-3439-simplicity-principle.md) | 대규모 network의 Simplicity Principle |
| Relay API | [RFC 1928](rfc-1928-socks5-relay.md) | destination 기반 relay request |
| Soft state | [RFC 2205](rfc-2205-rsvp-soft-state.md) | refresh-or-expire soft state |
| Multiplexing | [RFC 4254](rfc-4254-ssh-channel.md) | 한 connection 위의 channel multiplexing |
| Relay lifecycle | [RFC 8656](rfc-8656-turn-lifetime.md) | relay resource의 lifetime과 refresh |
| Flow control / liveness | [RFC 9000](rfc-9000-quic-streams.md) | stream과 connection flow control, idle timeout |
| Connection | [RFC 9293](rfc-9293-tcp-connection-roles.md) | active/passive open, byte stream, optional keepalive |
| Multiplexed connection lifecycle | [RFC 9113](rfc-9113-http2-connection-lifecycle.md) | PING, GOAWAY, persistent connection과 idle close |
| Mapping | [RFC 9299](rfc-9299-lisp-architecture.md) | identifier-to-locator mapping architecture |
| Mapping control | [RFC 9301](rfc-9301-lisp-control-plane.md) | mapping register와 resolve control plane |
