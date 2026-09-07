# RFC 9113: HTTP/2 connection lifecycle

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc9113.html) |
| 성격 | Standards Track, 2022-06 |
| 목적 | 한 connection의 multiplexed stream과 control frame |

| 개념 | 의미 |
| --- | --- |
| stream | identifier와 독립 lifecycle |
| PING | connection RTT와 connectivity 확인 |
| GOAWAY | 신규 stream 경계와 graceful shutdown 전달 |
| retry | application semantics가 영향받은 request의 새 시도 결정 |
| failure scope | connection control과 stream state 분리 |

HTTP frame, header compression, priority와 request semantics는 HTTP/2 고유 영역입니다. Application payload
처리 결과와 stream health는 각 application protocol이 별도로 판정합니다.

원문 절: [§5](https://www.rfc-editor.org/rfc/rfc9113.html#section-5), [§6.7](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.7), [§6.8](https://www.rfc-editor.org/rfc/rfc9113.html#section-6.8)
