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
| internal plaintext | 내부 certificate·mount 없이 SDK TLS와 ClusterToken 유지, 내부 무인증 |
| platform policy | 기본 controller annotation·reload token 없음, 범용 annotations는 StatefulSet metadata로 전달 |
| reload isolation | platform watch 설정만으로 Pod template 변경 없음, edge 갱신 시 RT 유지 |
| security context | non-root read, 일반 사용자 write 차단 |
| state | memory-only RT와 persistent volume 0개 |

| negative render | 예상 결과 |
| --- | --- |
| invalid LoadBalancer class | schema rejection |
| missing cert-manager issuer/trust | template rejection |
| managed env override | template rejection |
| wrong edge trust material | template rejection |
| plaintext + internal certManager | template rejection |
| 제거된 autoReload/reloadToken 또는 non-string annotation | schema rejection |
| unknown mode / test flag extraEnv | schema/template rejection |

CI는 `helm lint`, default/custom/cert-manager render, component Pod-template isolation, leaf SAN·usage와
Secret mount를 검증합니다. Cluster acceptance는 installed cert-manager/Issuer의 실제 issuance와 rollout을
검증합니다.

`.github/scripts/helm_transport_test.py`는 두 모드와 범용 annotation 위치·role 격리·제거된 설정 거절을 검증한다. Kind CI는
동일한 RT2/GW3 acceptance를 `mtls`, `plaintext`, `cert-manager`로 실행한다.
plaintext에서는 internal Secret을 만들지 않는다. cert-manager에서는 CA Issuer와 Reloader를 설치하고
`tests/kind/cert-manager-values.yaml`의 platform override로 실제 감시 Secret을 지정한다.
key spec 변경으로 edge·internal leaf 재발급을 유도한다. 새 certificate serial·해당 role Pod 교체·다른 role 유지·SDK 재수렴을 함께 확인한다.
edge는 각 Gateway와 Envoy passthrough에서 실제 제공하는 serial도 비교한다. 테스트 edge는 로컬 CA를 사용하며 ACME 발급을 검증하지 않는다.
이 검증은 재발급 적용 경로를 다루며 실제 만료 시각까지 기다리는 테스트는 아니다.
