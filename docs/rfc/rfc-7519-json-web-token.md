# RFC 7519: JSON Web Token

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc7519.html) |
| 성격 | Standards Track |
| 목적 | 당사자 사이의 claim을 compact JSON object로 전달 |

```text
JWT Claims Set ── JWS 서명 또는 JWE 암호화 ──► JWT
RelayGate profile = protected JOSE header + claims + ES256 signature
```

| claim | 일반 의미 |
| --- | --- |
| `iss` | token issuer |
| `aud` | token을 사용할 대상 |
| `exp` | 이후 수락하지 않는 시각 |
| `nbf` | 이전에는 수락하지 않는 시각 |

JWT는 RelayGate의 permission schema, token 발급 정책과 operation lifecycle을 정의하지 않는다.
구현은 원문의 [Registered Claim Names](https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1)와
[JWT Validation](https://www.rfc-editor.org/rfc/rfc7519.html#section-7.2)을 참고한다.
