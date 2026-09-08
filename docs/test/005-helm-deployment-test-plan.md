# TEST 005: Helm 계약

```text
Gateway StatefulSet × N
RouteTable StatefulSet × M shards
headless peer/RT Services + SDK Service
ShardDirectory ConfigMap
external credential/edge TLS Secret
internal = plaintext | mTLS(existing Secret | cert-manager leaf + CA trust Secret)
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
| internal plaintext | 내부 certificate·mount 없이 SDK TLS와 key admission 유지 |
| reload | credential/TLS token이 해당 workload만 rollout |
| automatic reload | StatefulSet metadata에서 role별 leaf와 공개 trust Secret만 watch |
| security context | non-root read, 일반 사용자 write 차단 |
| state | memory-only RT와 persistent volume 0개 |

| negative render | 예상 결과 |
| --- | --- |
| invalid LoadBalancer class | schema rejection |
| missing cert-manager issuer/trust | template rejection |
| managed env override | template rejection |
| wrong edge trust material | template rejection |
| plaintext + certManager/autoReload | template rejection |
| unknown mode / test flag extraEnv | schema/template rejection |

CI는 `helm lint`, default/custom/cert-manager render, component Pod-template isolation, leaf SAN·usage와
Secret mount를 검증합니다. Cluster acceptance는 installed cert-manager/Issuer의 실제 issuance와 rollout을
검증합니다.

`.github/scripts/helm_transport_test.py`는 두 모드와 reload watch 위치를 검증한다. Kind CI는
동일한 RT2/GW3 acceptance를 `mtls`, `plaintext`, `cert-manager`로 실행한다.
plaintext에서는 internal Secret을 만들지 않는다. cert-manager에서는 CA Issuer와 Reloader를 설치하고
key spec 변경으로 leaf 재발급을 유도한다. 새 certificate serial·Pod 교체·SDK 재수렴을 함께 확인한다.
이 검증은 재발급 적용 경로를 다루며 실제 만료 시각까지 기다리는 테스트는 아니다.
