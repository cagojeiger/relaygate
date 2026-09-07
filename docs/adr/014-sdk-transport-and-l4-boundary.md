# ADR 014: protocol transport와 외부 L4 진입점을 분리한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted, implemented |
| SDK transport | RelayGate framing over TLS/TCP |
| internal transport | mTLS/TCP |
| public entry | platform-owned L4 passthrough |

## 결정

```text
SDK <-> GW : TLS/TCP + server authentication + ClusterToken
GW  <-> GW : mTLS/TCP + logical Gateway handshake
GW  <-> RT : mTLS/TCP + logical Gateway/shard handshake

public L4
  ├── dedicated Service type LoadBalancer
  └── shared TCPRoute/TLSRoute passthrough -> Gateway ClusterIP
```

| 경계 | 현재 계약 |
| --- | --- |
| public SDK API | `Relay.listen/dial`, `Listener.accept`, `Pipe` |
| transport config | `GatewayTransportConfig::tls_tcp` |
| TLS verification | certificate chain, server name, `relaygate/2` ALPN |
| edge termination | RelayGate Gateway process |
| internal identity | mTLS certificate와 logical handshake의 일치 |
| certificate load | process startup, reload token 또는 platform reloader가 rollout |
| Helm ownership | Gateway/RT Service와 Secret wiring |
| platform ownership | public GatewayClass, L4 route, load balancer, Issuer·CA |

TLS validation failure는 terminal connection failure입니다. 공개 API는 transport adapter와 분리되어 이후
새 transport 결정이 SDK의 Relay·Listener·Pipe 사용법을 유지할 수 있습니다.

## 효과

- SDK-facing TLS와 internal mTLS trust domain을 독립 운영합니다.
- L4는 byte stream을 passthrough하고 Gateway가 protocol 보안을 종단합니다.
- application E2E 보호와 Pipe peer 인증은 Pipe 위 protocol이 담당합니다.
- RT shard topology와 service mesh는 transport 선택과 독립된 결정입니다.

## 참고

- [RFC 8446 §1](https://www.rfc-editor.org/rfc/rfc8446.html#section-1)
- [RFC 7301 §3](https://www.rfc-editor.org/rfc/rfc7301.html#section-3)
- [Kubernetes Service](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Envoy Gateway TLS passthrough](https://gateway.envoyproxy.io/docs/tasks/security/tls-passthrough/)
