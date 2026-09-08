# RFC 5880: Bidirectional Forwarding Detection

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc5880.html) |
| 성격 | Standards Track |
| 목적 | 인접 forwarding system 사이의 양방향 경로 장애 감지 |

```text
identity   = My Discriminator / Your Discriminator
state      = AdminDown | Down | Init | Up
detection  = negotiated interval × Detect Mult
```

| 개념 | 의미 |
| --- | --- |
| asynchronous mode | 양쪽 control packet과 detection deadline |
| demand mode | 별도 연결성 검증을 전제로 periodic traffic 축소 |
| discriminator | 같은 system pair의 session 구분 |
| jitter | 동기화된 probe burst 분산 |
| result | 경로의 Up/Down 판정 |

Routing, failover와 application recovery는 BFD 결과를 소비하는 별도 영역입니다. Application 처리 성공,
payload acknowledgement와 장애 원인도 별도 관측이 담당합니다.

원문의 timer 협상, 상태 전이, 인증과 packet format이 정확한 protocol 계약입니다.
