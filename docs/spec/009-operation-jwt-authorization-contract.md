# SPEC 009: operation JWT authorization 계약

이 문서는 `PUBLISH`와 `DIAL`의 token profile, static trust, permission 판정과 Gateway 실패 경계를
소유합니다. SDK token 공급·재공급은 [SPEC 002](002-sdk-pipe-contract.md), canonical 오류와 상태는
[SPEC 007](007-error-and-state-model.md), metric·log는
[SPEC 008](008-runtime-observability-contract.md)이 소유합니다.

## Profile과 경계

```text
Application backend                 SDK                         Gateway
private ES256 key                   AccessTokenSource           static public JWK
       │                                   │                           │
       └─ JWS Compact JWT ────────────────► PUBLISH / DIAL ───────────►│
                                           action + exact Destination │
                                                                       ├─ verify
                                                                       └─ state operation
```

| 항목 | RelayGate 계약 |
| --- | --- |
| serialization | signed JWS Compact Serialization의 payload로 JWT Claims Set을 전달 |
| 기반 표준 | RFC 7515(JWS), RFC 7517(JWK), RFC 7518(ES256), RFC 7519(JWT), RFC 8725(JWT BCP) |
| profile 식별 | `relaygate-operation+jwt` |
| registered claim | `iss`, `aud`, `nbf`, `exp` |
| private claim | `permissions` |
| protected operation | 새 `PUBLISH`, 새 `DIAL` |
| session handshake | `HELLO/WELCOME`은 credential-free이며 session identity를 grant하지 않음 |
| producer helper | `relaygate-token-issuer`는 backend가 이 profile의 JWS를 만들 때 쓰는 선택 라이브러리 |
| 비채택 | OAuth 2.0 access token, RFC 9068 JWT access-token profile, RFC 9396 Rich Authorization Requests |

RelayGate token은 OAuth access token이라고 주장하지 않습니다. RFC 9068의 `at+jwt` type과 Authorization
Server/Resource Server 계약을 부분적으로 흉내 내지 않고, RFC 8725의 explicit typing과 mutually exclusive
validation rule에 따라 RelayGate operation token만 별도 profile로 검증합니다.

| 권한 표현 | 채택 여부 | 이유 |
| --- | --- | --- |
| OAuth `scope` 문자열 | 비채택 | application-defined 공백 구분 문자열만으로 action, Namespace와 계층 selector를 표현하려면 RelayGate 전용 문자열 문법이 다시 필요함 |
| RFC 9396 `authorization_details` | 비채택 | 표준 준수에는 OAuth request·grant context와 type별 검증 의미가 필요하지만 RelayGate runtime에는 그 흐름이 없음 |
| private `permissions` claim | 채택 | 기존 exact `Destination`와 `Exact/Subtree/All` 판정을 최소 JSON 구조로 직접 표현함 |

비채택 표준의 근거는 [OAuth scope](https://www.rfc-editor.org/rfc/rfc6749.html#section-3.3),
[RFC 9068 profile](https://www.rfc-editor.org/rfc/rfc9068.html#section-2),
[RFC 9396](https://www.rfc-editor.org/rfc/rfc9396.html)을 참조합니다.

## Protected header

Canonical producer header는 다음과 같습니다. JWS Compact Serialization이므로 이 JOSE header 전체가
protected header입니다.

```json
{
  "alg": "ES256",
  "kid": "issuer-key-v2",
  "typ": "relaygate-operation+jwt"
}
```

| member | 발급 규칙 | Gateway 검증 |
| --- | --- | --- |
| `alg` | `ES256` 필수 | 정확히 `ES256`만 허용하며 token 값으로 algorithm을 확장하지 않음 |
| `kid` | configured issuer의 key ID 필수 | 요청 Namespace의 issuer 안에서 exact `kid` 하나를 선택; 누락·불일치는 `UNAUTHENTICATED` |
| `typ` | canonical `relaygate-operation+jwt` 필수 | RFC 7515 §4.1.9에 따라 ASCII case-insensitive short form과 `application/relaygate-operation+jwt`를 동등 허용 |
| `crit` | 사용 금지 | 이해하는 extension이 없으므로 member가 존재하면 빈 배열도 포함해 `UNAUTHENTICATED` |
| `jku`, `jwk`, `x5u`, `x5c` | canonical producer가 넣지 않음 | key source로 사용하거나 URL을 조회하지 않음 |

허용하는 `typ` equivalence는 아래 두 형태의 ASCII case variant뿐입니다. Producer는 항상 첫 번째 표기를
출력합니다.

```text
relaygate-operation+jwt
application/relaygate-operation+jwt
```

RFC에서 `kid`와 `typ`가 선택 사항이어도 이 application profile에서는 필수입니다. Header·signature 규칙의
원문 절은 [RFC 7515 노트](../rfc/rfc-7515-json-web-signature.md), key member는
[RFC 7517 노트](../rfc/rfc-7517-json-web-key.md)를 따릅니다.

## Claims JSON Schema

다음 JSON Schema는 발급자가 만들어야 하는 유효 RelayGate Claims Set의 닫힌 형태입니다. Gateway는 같은
구조를 decode하고 `permissions`가 없으면 빈 배열로 취급합니다. Unknown top-level claim·unknown
permission/scope member는 `UNAUTHENTICATED`, `maxItems` 초과 grant는 `PERMISSION_DENIED`로 fail closed합니다.

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "RelayGate operation grant claims",
  "type": "object",
  "additionalProperties": false,
  "required": ["iss", "aud", "nbf", "exp"],
  "properties": {
    "iss": { "type": "string", "minLength": 1 },
    "aud": {
      "oneOf": [
        { "type": "string", "minLength": 1 },
        {
          "type": "array",
          "minItems": 1,
          "items": { "type": "string", "minLength": 1 }
        }
      ]
    },
    "nbf": { "type": "integer", "minimum": 0 },
    "exp": { "type": "integer", "minimum": 0 },
    "permissions": {
      "type": "array",
      "maxItems": 128,
      "default": [],
      "items": { "$ref": "#/definitions/permission" }
    }
  },
  "definitions": {
    "permission": {
      "type": "object",
      "additionalProperties": false,
      "required": ["action", "namespace", "scope"],
      "properties": {
        "action": { "enum": ["publish", "dial"] },
        "namespace": {
          "type": "string",
          "maxLength": 63,
          "pattern": "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
        },
        "scope": { "$ref": "#/definitions/scope" }
      }
    },
    "scope": {
      "oneOf": [
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["kind", "name"],
          "properties": {
            "kind": { "const": "exact" },
            "name": { "$ref": "#/definitions/destination_name" }
          }
        },
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["kind", "name"],
          "properties": {
            "kind": { "const": "subtree" },
            "name": { "$ref": "#/definitions/destination_name" }
          }
        },
        {
          "type": "object",
          "additionalProperties": false,
          "required": ["kind"],
          "properties": { "kind": { "const": "all" } }
        }
      ]
    },
    "destination_name": {
      "type": "string",
      "maxLength": 253,
      "pattern": "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$"
    }
  }
}
```

Schema를 통과하는 것만으로 허가되지는 않습니다. Gateway는 다음 cross-field·trust 조건도 검증합니다.

| claim | 종류 | 추가 검증 |
| --- | --- | --- |
| `iss` | RFC 7519 registered | 요청 Namespace에 구성된 issuer와 exact match |
| `aud` | RFC 7519 registered | string 또는 string array가 configured audience를 포함 |
| `nbf` | RFC 7519 registered | clock skew를 적용해 아직 이르지 않음 |
| `exp` | RFC 7519 registered | clock skew를 적용해 만료되지 않았고 `nbf < exp` |
| `permissions` | RelayGate private | 없거나 빈 배열이면 structurally valid하지만 어떤 operation도 허가하지 않아 `PERMISSION_DENIED` |
| `sub`, `jti`, `iat` | 이 profile에 없음 | session identity·replay cache·issuance lifecycle을 소유하지 않으므로 closed schema가 거절 |
| `scope`, `authorization_details` | 이 profile에 없음 | OAuth profile을 혼합하지 않으므로 closed schema가 거절 |

`nbf`와 `exp`는 RFC 7519 NumericDate를 음이 아닌 정수 초로 표현합니다. Token 전체의 SDK–Gateway wire
상한은 4,096 bytes입니다. Public SDK는 빈 token 생성을 거절하고, lower-level wire에서 들어온 빈 값도 JWS
profile을 충족하지 못해 `UNAUTHENTICATED`입니다.

### Permission 판정

```json
{
  "iss": "https://issuer.example",
  "aud": "relaygate",
  "nbf": 1789050000,
  "exp": 1789050300,
  "permissions": [
    {
      "action": "publish",
      "namespace": "inference",
      "scope": { "kind": "exact", "name": "stt.seoul" }
    },
    {
      "action": "dial",
      "namespace": "inference",
      "scope": { "kind": "subtree", "name": "stt" }
    }
  ]
}
```

```text
allow(operation) = permissions 중 하나가
  action == requested action
  AND namespace == requested Namespace
  AND scope.contains(requested DestinationName)
```

| scope | 허용 DestinationName | 허용하지 않는 예 |
| --- | --- | --- |
| `exact(stt.seoul)` | `stt.seoul` | `stt`, `api.stt.seoul` |
| `subtree(stt)` | `stt`, `stt.seoul`, `stt.seoul.worker` | `sttx`, `api.stt` |
| `all` | 같은 Namespace의 모든 DestinationName | 다른 Namespace의 모든 Destination |

`subtree`는 root 자체를 포함하는 whole-label descendant 판정입니다. 이 판정은 authorization에만 사용하며
RouteTable과 local registry의 routing은 계속 exact `Destination` lookup입니다.

## Static trust config

```text
requested Destination
        │
        └─► Namespace ──► exactly one TrustedIssuer
                                  │
                                  └─► 1..2 public ES256 keys ── exact kid ──► verify
```

| 항목 | 범위·규칙 |
| --- | --- |
| config document | JSON version `1`, regular file, 최대 1 MiB, unknown field 거절 |
| audience | Gateway당 하나, 1..256 bytes |
| issuer mapping | 1..256 Namespace entries; Namespace당 issuer 정확히 하나 |
| issuer | non-empty, 최대 2,048 bytes |
| keys | issuer당 1..2, issuer 안에서 unique `kid` |
| clock skew | 기본 30초, 0..300초 |
| verification concurrency | 기본 32, 1..1,024 |
| verification timeout | 기본 1,000 ms, 1..5,000 ms |

| JWK member | config 값 | 의미 |
| --- | --- | --- |
| `kty` | `EC` | EC public key |
| `crv` | `P-256` | ES256 curve |
| `alg` | `ES256` | 이 key에 허용한 algorithm |
| `use` | `sig` | signature verification 전용 |
| `kid` | 1..128 bytes | issuer 안의 current/next key 선택 |
| `x`, `y` | valid base64url P-256 coordinate | public coordinate만 저장 |

Gateway는 unverified `iss`로 trust root를 선택하지 않습니다. 먼저 요청 Destination의 Namespace로 issuer
entry를 선택하고, protected header의 `kid`로 그 entry 안의 key 하나를 고른 뒤 서명과 configured
`iss`·`aud`를 검증합니다. Remote JWKS, issuer discovery와 token-supplied key material은 조회하지 않습니다.

## Gateway 검증과 commit

```text
PUBLISH / DIAL frame
  -> current session / DIAL ConnectionId fence / drain / control budget
  -> bounded verification slot
  -> JWS header profile
  -> Namespace-configured issuer + exact kid
  -> ES256 signature + closed claims + iss/aud/nbf/exp
  -> action + exact Destination permission
  -> current session + same action·Destination + monotonic expiry 재확인
  -> existing PUBLISH 또는 DIAL state operation
```

| 단계 | 실패 code | state·외부 효과 |
| --- | --- | --- |
| JWS/header/key/signature/registered claim/unknown·malformed claim 구조 | `UNAUTHENTICATED` | registry·RT Resolve·peer `OPEN` 없음 |
| valid token이 permission을 허가하지 않음, permission 128개 초과 | `PERMISSION_DENIED` | registry·RT Resolve·peer `OPEN` 없음 |
| verification slot 포화 | `RESOURCE_EXHAUSTED` | crypto 시작 없음 |
| verification deadline | `DEADLINE_EXCEEDED` | state commit 없음; 결과를 폐기하며 이미 시작한 blocking work는 bounded slot 안에서 끝날 수 있음 |
| verification task failure | `INTERNAL` | state commit 없음 |
| verify 뒤 session 소실 | 전달할 session이 없으므로 응답 없이 operation 종료 | state commit 없음 |
| verify 뒤 token 만료·operation 불일치 | `UNAUTHENTICATED` | state commit 없음 |
| verify와 commit 성공 | auth 전용 ACK 없음 | `PUBLISH`는 기존 `Published/PublishFailed`, `DIAL`은 기존 `Opened/DialFailed` 흐름으로 진행 |

Signature와 claims를 decode한 뒤 wall-clock `exp + skew`의 남은 시간을 monotonic deadline으로 변환합니다.
따라서 crypto/claims 처리에 이미 든 시간이 grant lifetime을 늘리지 않으며, commit 직전에 같은 action·Destination과
monotonic expiry를 다시 확인합니다.

Authorization 전에 수행하는 precheck도 operation 상태의 일부입니다. DIAL `ConnectionId` fence와 이미 차감한
session/Gateway control budget은 뒤의 authorization 실패 시 되돌리지 않습니다. 그 외 실패는 해당 operation에
한정되며 RelaySession과 기존 sibling Binding·Pipe는 유지됩니다.

### Wire response

| request | authorization 실패 response |
| --- | --- |
| `PUBLISH(request_id, ...)` | 같은 `request_id`의 `PublishFailed { code, "operation authorization failed" }` |
| `DIAL(connection_id, ...)` | 같은 `connection_id`의 `DialFailed { code, observation=NOT_OBSERVED, "operation authorization failed" }` |

Failure message는 token·claim·key 정보를 포함하지 않는 고정 문자열입니다. Session이 사라져 response를 전달할
수 없는 경우를 제외하면 operation-specific failure frame이 correlation ID와 안정된 code를 보존합니다.

## 수명과 소유권

| 주체 | 소유 |
| --- | --- |
| application/backend | private key, token 발급·갱신·revocation 정책, permission 정책 |
| `relaygate-token-issuer` | application/backend가 이미 결정한 grant를 canonical RelayGate operation JWT로 serialize/sign |
| SDK | static/dynamic `AccessTokenSource`, operation별 공급, Listener republish 시 재공급 |
| Gateway | static public trust config, bounded verification, operation admission |
| RelayGate runtime이 소유하지 않음 | OAuth authorization server, refresh token, token cache, revocation DB, JWKS fetch, subject identity, `jti` replay cache, private key |

Raw token, decoded claims와 permission은 operation 검증을 넘겨 RT·peer Gateway로 전달하거나
Binding·Pipe·log·metric·error에 보관하지 않습니다. Authorization은 admission-only이므로 commit된 Binding·Pipe는
token 만료만으로 종료하지 않습니다. 새 PUBLISH, Listener republish와 새 DIAL은 새 token을 다시 검증합니다.

## Requirements

| ID | 계약 |
| --- | --- |
| `AUTH-001` | 새 PUBLISH와 DIAL은 signed JWS Compact JWT인 비어 있지 않은 최대 4,096-byte AccessToken을 각각 포함한다. 다른 SDK frame과 HELLO에는 token이 없다. |
| `AUTH-002` | Protected header는 `alg=ES256`, non-empty `kid`, 지원 `typ`를 요구한다. Producer의 canonical `typ`는 `relaygate-operation+jwt`이고 verifier는 ASCII case variant와 `application/` prefix 표기를 동등 허용한다. `crit` member가 존재하면 거절한다. |
| `AUTH-003` | Gateway는 요청 Namespace에 구성된 exactly one TrustedIssuer와 header의 exact `kid`로 key 하나를 선택한 뒤 ES256 signature와 required `iss`, `aud`, `nbf`, `exp`를 검증한다. Token-supplied key source는 사용하지 않는다. |
| `AUTH-004` | `nbf < exp`이고 current time은 configured clock skew를 적용한 validity interval 안에 있어야 한다. Decode 뒤 expiry를 monotonic deadline으로 변환하고 commit 직전에 다시 검사한다. |
| `AUTH-005` | Claims Set과 permission/scope object는 unknown field를 허용하지 않는다. Permission은 최대 128개이며 action은 `publish|dial`, Namespace와 Destination은 canonical grammar, scope는 `exact|subtree|all` 중 하나다. |
| `AUTH-006` | Permission 하나가 요청 action, exact Namespace와 requested DestinationName의 Exact/whole-label Subtree/All 조건을 모두 만족해야 한다. Authorization scope가 routing wildcard·subtree lookup을 만들지 않는다. |
| `AUTH-007` | JWS·header·key·signature·registered claim·unknown/malformed claim 구조 실패는 `UNAUTHENTICATED`, valid token의 권한 불일치·permission 상한 초과는 `PERMISSION_DENIED`, verifier 포화·timeout·task failure는 각각 `RESOURCE_EXHAUSTED`·`DEADLINE_EXCEEDED`·`INTERNAL`이다. |
| `AUTH-008` | Namespace마다 TrustedIssuer 하나를 구성하고 issuer마다 회전용 unique-kid ES256 public JWK를 1..2개 구성한다. Private key와 remote key discovery는 config에 없다. |
| `AUTH-009` | Verification concurrency는 기본 32, 1..1,024이고 timeout은 기본 1,000 ms, 1..5,000 ms다. Crypto는 state lock 밖의 bounded blocking work로 실행한다. |
| `AUTH-010` | Gateway는 current session, DIAL ConnectionId fence, drain과 control budget을 먼저 확인하고 verify 뒤 current session·same action·Destination·monotonic expiry를 재확인해 commit한다. 이미 소비한 fence와 rate budget은 authorization 실패 시 환불하지 않는다. |
| `AUTH-011` | Authorization 성공은 별도 ACK를 만들지 않는다. 실패 response는 operation correlation ID와 stable code를 보존하고 DIAL은 `NOT_OBSERVED`이며, registry·RT Resolve·peer OPEN 전 해당 operation만 끝내 기존 session·Binding·Pipe를 유지한다. |
| `AUTH-012` | Raw token, decoded claim과 permission은 Gateway operation을 넘지 않으며 RT·peer·Binding·Pipe·log·metric·error에 전달하거나 보관하지 않는다. |
| `AUTH-013` | Authorization은 admission-only다. Token expiry는 established Binding·Pipe를 종료하지 않고 새 PUBLISH, Listener republish와 새 DIAL만 다시 검증한다. |
| `AUTH-014` | RelayGate runtime은 OAuth authorization server, token cache, refresh, revocation DB, JWKS fetch, subject identity, replay cache, private key와 token 발급 정책을 소유하지 않는다. |
| `AUTH-015` | Authorization config 누락·unknown field·unsupported version/algorithm·범위 위반은 Gateway listener를 열기 전 startup failure다. |
| `AUTH-016` | `relaygate-token-issuer`는 canonical producer header와 closed permission claim을 생성하되, application 사용자 인증·정책 판단·키 보관을 대신하지 않는다. |

## 표준 참고

- [RFC 7515: JSON Web Signature](https://www.rfc-editor.org/rfc/rfc7515.html)
- [RFC 7517: JSON Web Key](https://www.rfc-editor.org/rfc/rfc7517.html)
- [RFC 7518: JSON Web Algorithms](https://www.rfc-editor.org/rfc/rfc7518.html)
- [RFC 7519: JSON Web Token](https://www.rfc-editor.org/rfc/rfc7519.html)
- [RFC 8725: JWT Best Current Practices](https://www.rfc-editor.org/rfc/rfc8725.html)
