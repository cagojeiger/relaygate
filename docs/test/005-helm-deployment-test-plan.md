# TEST 005: Helm 계약

```text
StatefulSet relaygate-gateway      replicaCount = N
StatefulSet relaygate-rt           shardCount   = M
headless peer/RT Services + SDK ClusterIP/LoadBalancer Service
ShardDirectory ConfigMap
external credential Secret + edge TLS Secret
internal mTLS = existing Secret | cert-manager leaf Certificates + CA trust Secret
```

- chart는 Secret/Issuer/CA/SDK workload/PVC를 만들지 않는다. `certManager` mode에서만 Gateway와
  RouteTable용 leaf `Certificate`를 각각 하나 만든다.
- Gateway와 RT image는 독립 값이며 한 component image 변경은 다른 component의 Pod template을
  변경하지 않는다.
- chart version만 변경해도 Gateway와 RT의 Pod template은 변경되지 않는다.
- Gateway pod name이 GatewayName이며 peer headless DNS가 locator다.
- RT ordinal이 shard ID이며 ShardDirectory와 일치한다.
- SDK port는 기본 ClusterIP이고 선택적으로 LoadBalancer를 사용하며 peer/RT는 cluster 내부다.
- LoadBalancer class는 Kubernetes qualified name만 허용하며 기존 Service에서 class 변경은 지원하지 않는다.
- edge TLS와 internal mTLS material은 분리해 read-only mount하고 production env에 insecure switch를 노출하지 않는다.
- 기본 `existingSecret` mode는 기존 단일 internal Secret을 유지한다.
- `certManager` mode는 platform 소유 Issuer와 CA trust Secret을 요구하며, 각 workload에는 자기 role의
  leaf private key만 mount한다.
- `certManager` source, Issuer, trust Secret이 없거나 잘못되면 render를 거부한다.
- edge `customCa`는 CA key를 mount하고 `webPkiRoots`는 certificate/key만 mount한다.
- managed identity/address/credential/TLS env를 `extraEnv`로 덮어쓰지 못한다.
- credential/TLS reload token 변경은 문서화된 대상의 Pod template만 바꿔 rollout한다.
- private key는 non-root process가 읽을 수 있고 일반 사용자에게 writable하지 않다.

`helm lint`, default/cert-manager/custom topology render, component별 Pod template 격리와 invalid values
rejection을 CI에서 수행합니다. cert-manager render는 leaf 수·SAN·usage·Issuer·trust/leaf Secret 배선을
검증하며, 실제 발급과 rotation은 설치된 cert-manager/Issuer가 있는 cluster acceptance의 책임입니다.
