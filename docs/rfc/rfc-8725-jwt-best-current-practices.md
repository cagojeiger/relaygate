# RFC 8725: JWT Best Current Practices

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc8725.html) |
| 성격 | Best Current Practice |
| 목적 | JWT의 일반적인 공격과 검증 요구 정리 |

| 검증 원칙 | 적용 |
| --- | --- |
| 허용 algorithm 고정 | ES256만 수락 |
| cryptographic input 검증 | signature와 `kid`에 대응하는 configured public key 검증 |
| audience 검증 | RelayGate용 configured audience와 일치 |
| issuer 검증 | Destination Namespace의 configured issuer와 일치 |
| validation rule 분리 | Destination action·scope 계약을 명시적으로 적용 |

RFC는 RelayGate의 Namespace, `PUBLISH`/`DIAL`, Exact/Subtree/All permission 형식을 정의하지 않는다.
구현은 원문의 [Algorithm Verification](https://www.rfc-editor.org/rfc/rfc8725.html#section-3.1),
[Audience](https://www.rfc-editor.org/rfc/rfc8725.html#section-3.9),
[Issuer and Subject](https://www.rfc-editor.org/rfc/rfc8725.html#section-3.8)을 참고한다.
