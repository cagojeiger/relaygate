# RFC 9293: Transmission Control Protocol

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc9293.html) |
| 성격 | Standards Track, 2022-08 |
| 목적 | reliable, in-order, full-duplex byte stream |

| 개념 | 의미 |
| --- | --- |
| active OPEN | remote endpoint로 연결 수립 시작 |
| passive OPEN | inbound 연결 요청 대기 |
| established | 양쪽 full-duplex byte 전송 |
| FIN | 한 방향 data 종료와 half-close |
| RST | 비정상 connection 종료 |
| receive window | receiver가 허용한 byte 범위 |
| keepalive | optional low-level path probe |

Active/passive는 연결 수립 역할입니다. Application message boundary, SDK API, bounded application liveness와
delivery acknowledgement는 TCP 위 protocol이 정의합니다.

원문 절: [§2.2](https://www.rfc-editor.org/rfc/rfc9293.html#section-2.2), [§3.5](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.5), [§3.6](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.6), [§3.8.4](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.8.4)
