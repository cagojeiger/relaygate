# ADR 015: RouteAddress는 Namespace와 계층 이름으로 구성한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted |
| routing key | `NamespaceId/DestinationName` |
| 생성·보관 | application |
| lookup | exact match |

## 결정

```text
RouteAddress = NamespaceId "/" DestinationName
example      = inference/stt.seoul.worker-1

NamespaceId     = lowercase DNS-like label 1개
DestinationName = lowercase DNS-like label 1..N개
```

| 의미 | 규칙 |
| --- | --- |
| Namespace | issuer와 권한의 고정 경계 |
| DestinationName | application이 관리하는 계층형 논리 이름 |
| routing | 전체 RouteAddress exact match |
| authorization | Exact·Subtree·All scope가 DestinationName label 경계를 사용 |
| ownership | RelayGate는 주소 발급·영속 registry를 제공하지 않음 |

RouteTable authority는 canonical RouteAddress bytes를 hash한다. wildcard와 subtree lookup은 routing에
들어가지 않으며 같은 RouteAddress의 여러 RelaySession은 live BindingSet을 구성한다.

## 참고

- [RFC 9299](../rfc/rfc-9299-lisp-architecture.md)
- [ADR 005](005-current-state-routing-topology.md)
- [SPEC 001](../spec/001-terminology-and-object-model.md)
- [SPEC 003](../spec/003-destination-binding-contract.md)
