# RFC 7517: JSON Web Key

| 항목 | 값 |
| --- | --- |
| 원문 | [RFC Editor](https://www.rfc-editor.org/rfc/rfc7517.html) |
| 성격 | Standards Track, 2015-05 |
| 목적 | cryptographic key와 key set을 JSON object로 표현 |

| JWK member | 표준 요구 | RelayGate authorization config |
| --- | --- | --- |
| `kty` | 모든 JWK에 필수 | `EC` 필수 |
| `use` | 선택; `sig` 또는 `enc` | 서명 검증 key이므로 `sig` 필수 |
| `alg` | 해당 key의 의도된 algorithm, 선택 | `ES256` 필수 |
| `kid` | key 선택 identifier, 선택 | 1..128 bytes, issuer 안에서 unique, 필수 |
| `crv` | RFC 7518 EC key curve, 필수 | `P-256` 필수 |
| `x`, `y` | RFC 7518 EC public coordinate, 필수 | 각 32-octet P-256 coordinate의 base64url 표현 |

RFC 7517에서 `kid`, `alg`, `use`는 선택 사항이지만 RelayGate static trust config는 잘못된 key 용도를 시작 시점에
거절하기 위해 더 엄격하게 요구합니다. EC-specific `crv`, `x`, `y` 형식은
[RFC 7518 §6.2.1](https://www.rfc-editor.org/rfc/rfc7518.html#section-6.2.1)이 정의하며 ES256 key는
`crv=P-256`과 각각 32-octet coordinate의 base64url 표현을 사용합니다.

JWS header의 `kid`는 configured JWK의 `kid`와 일치하는 key를 고릅니다. 서로 다른 key는 rotation 중에도 같은
issuer 안에서 서로 다른 `kid`를 사용합니다. RelayGate는 remote JWK Set fetch, private key와 issuer discovery를
소유하지 않습니다.

원문 절: [`kty` §4.1](https://www.rfc-editor.org/rfc/rfc7517.html#section-4.1),
[`use` §4.2](https://www.rfc-editor.org/rfc/rfc7517.html#section-4.2),
[`alg` §4.4](https://www.rfc-editor.org/rfc/rfc7517.html#section-4.4),
[`kid` §4.5](https://www.rfc-editor.org/rfc/rfc7517.html#section-4.5),
[JWK Set §5](https://www.rfc-editor.org/rfc/rfc7517.html#section-5)
