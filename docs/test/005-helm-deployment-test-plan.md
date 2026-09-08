# TEST 005: Helm 계약

```text
Gateway StatefulSet × N
RouteTable StatefulSet × M shards
headless peer/RT Services + SDK Service
ShardDirectory ConfigMap
external credential/edge TLS Secret
internal mTLS = existing Secret | cert-manager leaf + CA trust Secret
```

| 범주 | 검증 계약 |
| --- | --- |
| resource scope | RT/GW runtime, Service, directory와 Secret wiring |
| component release | Gateway와 RT image가 서로의 Pod template을 유지 |
| chart release | package version만 바뀌면 runtime Pod template 유지 |
| identity | Gateway pod name과 RT ordinal을 logical identity로 사용 |
| network | SDK Service는 ClusterIP/LoadBalancer, peer/RT는 cluster-internal |
| TLS isolation | edge/internal trust와 role별 private key 분리 |
| internal source | `existingSecret` 또는 platform Issuer 기반 `certManager` |
| reload | credential/TLS token이 해당 workload만 rollout |
| security context | non-root read, 일반 사용자 write 차단 |
| state | memory-only RT와 persistent volume 0개 |

| negative render | 예상 결과 |
| --- | --- |
| invalid LoadBalancer class | schema rejection |
| missing cert-manager issuer/trust | template rejection |
| managed env override | template rejection |
| wrong edge trust material | template rejection |

CI는 `helm lint`, default/custom/cert-manager render, component Pod-template isolation, leaf SAN·usage와
Secret mount를 검증합니다. Cluster acceptance는 installed cert-manager/Issuer의 실제 issuance와 rollout을
검증합니다.
