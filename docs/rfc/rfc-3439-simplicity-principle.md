# RFC 3439: Simplicity Principle

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc3439.html) |
| 성격 | Informational |
| 목적 | 대규모 network의 complexity와 scale 비용 설명 |

```text
simple core + explicit boundary + endpoint-owned complexity
            = lower scaling and operational cost
```

| 원칙 | 효과 |
| --- | --- |
| 작은 core | 변경·실패 surface 축소 |
| end-to-end placement | core와 endpoint state 결합 감소 |
| evidence-driven optimization | 검증되지 않은 feature 비용 억제 |
| explicit responsibility | 계층 간 장애 전파 제한 |
| bounded protection | 단순성과 correctness를 함께 유지 |

구체적인 topology, protocol과 optimization은 system requirement가 결정합니다.
