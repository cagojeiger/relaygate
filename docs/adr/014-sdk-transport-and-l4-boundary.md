# ADR 014: SDK transport와 외부 L4 진입점을 분리한다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택, 구현됨 |
| 관계 | ADR 012의 SDK-facing transport와 배포 경계를 구체화 |

## 결정

```text
Relay API              : transport 독립적인 Relay.listen/dial + Listener.accept + Pipe
0.2 SDK transport      : RelayGate framing over TLS/TCP
GW <-> GW, GW <-> RT   : mTLS/TCP 유지
외부 L4 진입점         : platform 소유
Helm 기본 Service      : ClusterIP
선택적 직접 노출       : Service type LoadBalancer
공유 Gateway 경유      : TCPRoute 또는 TLSRoute passthrough
```

SDK 공개 설정은 `GatewayTransportConfig`를 받는다. 0.2는 `tls_tcp`만 제공하며 HTTP/2와 HTTP/3를
구현하지 않는다. 이후 transport가 추가돼도 `Relay`, `Listener`, `Pipe` API와 application 호출
흐름은 바뀌지 않는다.

TLS/TCP는 application fallback 목록의 한 후보가 아니다. certificate chain, server name, ALPN 또는
ClusterToken 검증 실패는 해당 연결의 terminal failure이며 평문이나 다른 transport로 자동 전환하지
않는다.

SDK-facing TLS와 cluster-internal mTLS는 별도 Secret과 trust domain으로 운영할 수 있다. Helm은
certificate를 발급하지 않고 두 Secret을 read-only로 mount한다. certificate hot reload는 제공하지
않으며 각 reload token 변경으로 대상 workload를 rollout한다.

RelayGate chart는 공용 Envoy Gateway, `GatewayClass`, `TCPRoute`, `TLSRoute`를 소유하지 않는다.
공유 L4 entry를 쓰는 환경은 platform/GitOps가 TLS passthrough 경로를 만들고, Gateway process가 TLS를
종단한다.

## 결과

- 현재 구현과 검증 범위는 TLS/TCP 하나로 유지된다.
- 향후 edge transport 추가가 public Relay API 변경을 강제하지 않는다.
- edge certificate rotation은 내부 mTLS rotation과 분리된다.
- `ClusterIP` 환경, 직접 `LoadBalancer`, 공유 L4 Gateway를 같은 chart로 지원한다.
- native HTTP/2·HTTP/3 stream mapping과 transport fallback 정책은 구현 전 별도 결정이 필요하다.

## 참고

- [RFC 8446 §1](https://www.rfc-editor.org/rfc/rfc8446.html#section-1)
- [RFC 7301 §3](https://www.rfc-editor.org/rfc/rfc7301.html#section-3)
- [Kubernetes Service](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Envoy Gateway TLS passthrough](https://gateway.envoyproxy.io/docs/tasks/security/tls-passthrough/)
