# ADR 016: PUBLISH와 DIAL은 operation별 JWT grant로 허가한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted |
| session handshake | `HELLO/WELCOME`, credential 없음 |
| protected operation | `PUBLISH`, `DIAL` |
| token profile | RFC 7515·7517·7518·7519·8725 기반 RelayGate custom JWT |
| verification | Namespace별 static ES256 issuer·public key |
| 비채택 | OAuth 2.0 access-token profile, RFC 9068 |

## 결정

```text
Application backend                    RelayGate Gateway
private signing key                    public verification key only
        │                                        │
        └── signed JWT ──────► SDK operation ───►│
                                 │
              typ · alg · kid · iss · aud · nbf · exp
                    permissions[action, namespace, scope]
```

| 규칙 | 결과 |
| --- | --- |
| trust mapping | Namespace 하나 → issuer 하나 → current/next `kid` 1..2개 |
| profile | producer는 canonical `typ=relaygate-operation+jwt`, `alg=ES256`, `kid`를 protected header에 넣고 verifier는 RFC 7515의 `typ` case·`application/` equivalence를 적용 |
| admission | Gateway가 Binding 생성·RT Resolve·peer `OPEN`·Pipe 생성 전에 header·signature·claims·action·exact RouteAddress 검증 |
| scope | `Exact`, whole-label `Subtree`, Namespace `All` |
| token lifecycle | operation 검증 뒤 폐기; Binding·Pipe에 보관하지 않음 |
| renewal | Listener republish와 새 dial이 application token source를 다시 호출 |
| existing state | token 만료가 이미 승인된 Binding·Pipe를 종료하지 않음 |
| transport | raw token을 RT·peer Gateway로 전달하지 않음 |

`permissions` private claim은 RelayGate의 action·RouteAddress 권한만 표현합니다. OAuth `scope`는 계층
Destination selector를 위한 문자열 문법을 추가로 필요로 합니다. RFC 9396 `authorization_details`를 표준대로
채택하려면 OAuth request·grant context와 type별 검증 의미가 필요하지만 RelayGate runtime에는 그 흐름이 없어
사용하지 않습니다.

Gateway는 private key, token issuer, refresh token, revocation database와 JWKS network fetch를 소유하지
않습니다. SDK는 token source만 연결하고 Pipe 상대 identity·payload authorization은 application protocol이
소유합니다. 세부 profile과 실행 계약은 [SPEC 009](../spec/009-operation-jwt-authorization-contract.md)가
소유합니다.

## 참고

- [RFC 7515](../rfc/rfc-7515-json-web-signature.md)
- [RFC 7517](../rfc/rfc-7517-json-web-key.md)
- [RFC 7518](../rfc/rfc-7518-json-web-algorithms.md)
- [RFC 7519](../rfc/rfc-7519-json-web-token.md)
- [RFC 8725](../rfc/rfc-8725-jwt-best-current-practices.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
- [SPEC 009](../spec/009-operation-jwt-authorization-contract.md)
