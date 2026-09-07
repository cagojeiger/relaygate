# RFC 1958: Internet architecture 원칙

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc1958.html) |
| 성격 | Informational |
| 목적 | Internet architecture의 반복 가능한 설계 원칙 |

```text
endpoint  ── end-to-end meaning · integrity
network   ── minimal · derived · self-healing state
```

| 원칙 | 방향 |
| --- | --- |
| end-to-end | application 의미와 무결성을 endpoint에 배치 |
| state recovery | topology와 activity 변화에 맞춰 network state 재구성 |
| failure | connectivity가 남으면 state loss를 일시적 service denial로 수렴 |
| simplicity | 내부 state와 수동 설정 최소화 |
| reuse | 이미 검증된 일반 방법 우선 |
| modularity | 기능 경계와 scale-out을 함께 설계 |

특정 protocol, storage와 recovery mechanism은 이 원칙을 적용하는 system이 결정합니다.
