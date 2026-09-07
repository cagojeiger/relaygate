# RFC 7426: SDN layers and architecture terminology

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc7426.html) |
| 성격 | Informational, IRTF |
| 목적 | network 기능의 plane·abstraction·interface 용어 정리 |

| Plane | 책임 |
| --- | --- |
| Application | network service 사용과 동작 정의 |
| Control | forwarding 결정과 반영 |
| Forwarding | 실제 data path 처리 |
| Operational | port, interface, queue, memory의 current state |
| Management | 설정, 감시와 유지보수 |

```text
Plane = responsibility boundary
Process topology = implementation choice
```

Control과 Management plane은 변경 주기, persistence와 locality가 다를 수 있습니다. 하나의 physical 또는
virtual element가 여러 plane을 함께 구현할 수 있습니다. Abstraction layer는 각 plane의 resource와
service를 interface 뒤에 둡니다.

Controller, southbound protocol, topology와 orchestration은 구체 system의 결정입니다.
