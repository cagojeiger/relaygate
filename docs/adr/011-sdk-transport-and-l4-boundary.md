# ADR 011: protocol transport와 외부 L4 진입점을 분리한다

| 항목 | 결정 |
| --- | --- |
| 상태 | Accepted, implemented |
| SDK transport | TLS/TCP 기본값, 제공자가 명시한 TCP endpoint 지원 |
| internal transport | mTLS/TCP 기본값, [ADR 014](014-explicit-internal-transport-mode.md)의 명시적 plaintext |
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
| transport config | `Config::new(endpoint)`, 특수 환경은 `Config::with_transport` |
| TLS verification | certificate chain, server name, `relaygate/2` ALPN |
| edge termination | RelayGate Gateway process |
| internal identity | mTLS certificate와 logical handshake의 일치 |
| certificate load | process startup, platform이 갱신 적용을 위한 rollout 관리 |
| Helm ownership | Gateway/RT Service와 Secret wiring |
| platform ownership | public GatewayClass, L4 route, load balancer, Issuer·CA |

TLS validation failure는 terminal connection failure입니다. 공개 API는 transport adapter와 분리되어 이후
새 transport 결정이 SDK의 Relay·Listener·Pipe 사용법을 유지할 수 있습니다.

## 효과

제공자는 `host:port`/`tls://host:port` 또는 `tcp://host:port`를 안내합니다. SDK는 TLS endpoint의
도메인과 기본 공인 CA로 자동 검증합니다. Gateway의 `RELAYGATE_SDK_TRANSPORT`는 `tls` 기본값이며
`plaintext`는 인증서 없는 TCP listener입니다. 평문에서는 token과 payload가 노출되므로 제공자가
격리·전송 보호를 책임집니다. TLS 실패 후 자동 평문 fallback은 없습니다. 내부 transport는 독립 설정입니다.

- SDK-facing TLS와 internal mTLS trust domain을 독립 운영합니다.
- L4는 byte stream을 passthrough하고 Gateway가 protocol 보안을 종단합니다.
- application E2E 보호와 Pipe peer 인증은 Pipe 위 protocol이 담당합니다.
- RT shard topology와 service mesh는 transport 선택과 독립된 결정입니다.

## 참고

- [RFC 8446 §1](https://www.rfc-editor.org/rfc/rfc8446.html#section-1)
- [RFC 7301 §3](https://www.rfc-editor.org/rfc/rfc7301.html#section-3)
- [Kubernetes Service](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Envoy Gateway TLS passthrough](https://gateway.envoyproxy.io/docs/tasks/security/tls-passthrough/)
