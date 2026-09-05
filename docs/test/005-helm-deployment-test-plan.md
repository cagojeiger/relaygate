# TEST 005: Helm 계약

```text
StatefulSet relaygate-gateway      replicaCount = N
StatefulSet relaygate-rt           shardCount   = M
headless peer/RT Services + SDK ClusterIP/LoadBalancer Service
ShardDirectory ConfigMap
external credential Secret + edge TLS Secret + internal mTLS Secret
```

- chart는 Secret/certificate/SDK workload/PVC를 만들지 않는다.
- Gateway와 RT image는 독립 값이다.
- Gateway pod name이 GatewayName이며 peer headless DNS가 locator다.
- RT ordinal이 shard ID이며 ShardDirectory와 일치한다.
- SDK port는 기본 ClusterIP이고 선택적으로 LoadBalancer를 사용하며 peer/RT는 cluster 내부다.
- LoadBalancer class는 Kubernetes qualified name만 허용하며 기존 Service에서 class 변경은 지원하지 않는다.
- edge TLS와 internal mTLS Secret은 분리해 read-only mount하고 production env에 insecure switch를 노출하지 않는다.
- managed identity/address/credential/TLS env를 `extraEnv`로 덮어쓰지 못한다.
- credential/TLS reload token 변경은 pod template hash를 바꿔 rollout한다.
- private key는 non-root process가 읽을 수 있고 일반 사용자에게 writable하지 않다.

`helm lint`, default render, custom topology render와 invalid values rejection을 CI에서 수행합니다.
