# ADR 017: operation token 발급 helper는 server-side library로 제공한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted |
| 제공 | `relaygate-token-issuer` Rust library |
| 입력 | application이 승인한 permission, lifetime, ES256 private key |
| 출력 | RelayGate operation JWT |

## 결정

```text
Application backend
  authenticate -> authorize -> relaygate-token-issuer -> AccessTokenSource
                                      │
                                      └─ canonical ES256 operation JWT
```

| helper가 소유 | application이 소유 |
| --- | --- |
| header·claim serialization, ES256 signing, profile bounds | caller 인증, permission 정책, HTTP API, private-key 보관·회전, token 전달·cache |

Gateway는 public key 검증만 수행하고 SDK runtime은 `AccessTokenSource`만 수행합니다. Helper는 token DB,
OAuth server, refresh, revocation, JWKS discovery와 background task를 만들지 않습니다.

## 참고

- [ADR 016](016-per-operation-jwt-authorization.md)
- [SPEC 002](../spec/002-sdk-pipe-contract.md)
- [SPEC 009](../spec/009-operation-jwt-authorization-contract.md)
