# RFC 7515: JSON Web Signature

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc7515.html) |
| 성격 | Standards Track, 2015-05 |
| 목적 | payload의 무결성과 서명을 JOSE header와 함께 표현하고 검증 |

```text
JWS Compact Serialization
  = BASE64URL(Protected Header) "."
  + BASE64URL(Payload)          "."
  + BASE64URL(Signature)
```

| header | RFC 7515 의미 | RelayGate profile |
| --- | --- | --- |
| `alg` | 서명 algorithm, 필수 | `ES256`만 허용 |
| `kid` | key 선택 hint, 선택 | Namespace issuer 안의 key 선택을 위해 필수 |
| `typ` | 전체 JWS의 media type, 선택 | canonical `relaygate-operation+jwt`, 검증 필수 |
| `crit` | 수신자가 모두 이해·처리해야 하는 non-empty extension 이름 목록, 선택 | 지원 extension이 없으므로 member 존재 자체를 거절 |

`typ`의 media type과 subtype은 대소문자를 구분하지 않습니다. `/`가 없는 값은 `application/`을 앞에 붙인
값과 동일하게 처리하므로 verifier는 `relaygate-operation+jwt`와
`application/relaygate-operation+jwt` 및 그 ASCII case variant를 동등하게 수락합니다. Producer는 compact한
canonical 표기인 `relaygate-operation+jwt`를 사용합니다.

`kid`는 신뢰의 근거가 아니라 lookup hint입니다. RelayGate는 token이 제시한 `jku`, `jwk`, `x5u`를 key source로
사용하거나 URL을 조회하지 않고, 요청 Namespace에 미리 구성된 issuer와 key 집합만 사용합니다.

RFC 7515는 JWT claim과 RelayGate permission 의미를 정의하지 않습니다.

원문 절: [`alg` §4.1.1](https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.1),
[`kid` §4.1.4](https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.4),
[`typ` §4.1.9](https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.9),
[`crit` §4.1.11](https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.11),
[검증 §5.2](https://www.rfc-editor.org/rfc/rfc7515.html#section-5.2)
