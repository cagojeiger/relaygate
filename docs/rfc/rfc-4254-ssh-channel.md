# RFC 4254: SSH Connection Protocol

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc4254.html) |
| 성격 | Standards Track, 2006-01 |
| 목적 | 하나의 SSH transport에 여러 logical channel multiplexing |

| channel 개념 | 의미 |
| --- | --- |
| identifier | 양 endpoint가 각자 local number 할당 |
| open | request → confirmation 또는 failure |
| data | channel identifier로 demultiplex |
| flow control | channel별 receive window와 maximum packet size |
| EOF·CLOSE | data 방향 종료와 channel state 제거 구분 |

SSH 인증·암호화와 channel type은 표준 고유 영역입니다. Physical connection pooling과 shared transport
congestion control은 별도 transport 설계 영역입니다.

원문 절: [§5](https://www.rfc-editor.org/rfc/rfc4254.html#section-5), [§5.1](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.1), [§5.2](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.2), [§5.3](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.3)
