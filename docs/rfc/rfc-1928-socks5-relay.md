# RFC 1928: SOCKS Protocol Version 5

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc1928.html) |
| 성격 | Standards Track, 1996-03 |
| 목적 | firewall을 경유한 TCP/UDP relay 수립 |

| 핵심 개념 | 의미 |
| --- | --- |
| negotiation | client와 server가 인증 방법 협상 |
| request | command + destination address/port |
| result | success 또는 구체적인 failure |
| CONNECT | 성공 뒤 양방향 data relay |
| BIND·UDP ASSOCIATE | CONNECT와 구별되는 command lifecycle |

| 영역 | 내용 |
| --- | --- |
| SOCKS 고유 | 인증 협상, IP/port 주소, BIND, UDP relay |
| relay 일반화 | destination 요청 → 결과 → opaque data relay |
| application 영역 | CONNECT 이후 payload 처리와 acknowledgement |

원문 절: [§3](https://www.rfc-editor.org/rfc/rfc1928.html#section-3), [§4](https://www.rfc-editor.org/rfc/rfc1928.html#section-4), [§6](https://www.rfc-editor.org/rfc/rfc1928.html#section-6)
