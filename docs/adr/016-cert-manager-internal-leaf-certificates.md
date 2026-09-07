# ADR 016: cert-manager는 내부 leaf certificate만 자동화한다

| 항목 | 값 |
| --- | --- |
| 상태 | 채택 |
| 관계 | ADR 012의 Helm certificate 공급 방식을 확장 |

## 결정

```text
platform                RelayGate chart                 runtime
Issuer/ClusterIssuer ──► Gateway Certificate ──Secret──► Gateway
CA trust Secret ───────► RT Certificate      ──Secret──► RouteTable
```

Helm의 기본값은 기존 단일 Secret을 mount하는 `existingSecret`이다. 선택적 `certManager` source는
Gateway role과 RouteTable role의 leaf `Certificate`를 각각 만들고 cert-manager가 발급·갱신하게 한다.
Issuer/ClusterIssuer와 CA public trust bundle Secret은 platform이 소유하며 chart는 CA private key,
Issuer와 trust bundle을 만들지 않는다.

leaf certificate Secret과 CA trust Secret은 분리한다. Gateway workload에는 Gateway leaf private key만,
RouteTable workload에는 RouteTable leaf private key만 전달한다. Gateway certificate는 peer server와
peer/RT client 용도의 server/client auth를, RouteTable certificate는 server auth만 허용한다. 인증서는
기존 logical Gateway/shard handshake를 대체하지 않는다.

runtime은 certificate file을 process 시작 시 읽고 hot reload하지 않는다. Secret 갱신은
`tls.internal.reloadToken`을 바꾼 rollout 또는 platform reloader로 적용한다. CA 교체는 old/new trust
overlap과 leaf 재발급 순서를 포함한 platform 작업이며 chart가 자동화하지 않는다.

## 결과

- leaf 발급과 만료 전 갱신은 cert-manager가 담당한다.
- trust anchor와 CA rotation 책임은 RelayGate chart 밖에 남는다.
- cert-manager가 없는 cluster와 기존 Vault/ExternalSecret 흐름은 기본 모드에서 그대로 동작한다.
- 갱신된 Secret을 runtime에 적용하려면 rollout/reloader가 필요하다.

## 참고

- [cert-manager Certificate](https://cert-manager.io/docs/usage/certificate/)
- [cert-manager trust](https://cert-manager.io/docs/trust/)
- [cert-manager CA Issuer](https://cert-manager.io/docs/configuration/ca/)
