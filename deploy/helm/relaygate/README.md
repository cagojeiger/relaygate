# RelayGate Helm chart

기본 구성은 Gateway 1개와 RouteTable shard 1개다. Kubernetes 1.32 이상에서 실행한다.

```text
배포자: credential · 인증서 Secret · 운영 정책
                         ↓ values
차트:  SDK ─ TLS ─ Gateway ─ mTLS ─ RouteTable
```

## 책임

| Chart | 배포자 / GitOps |
| --- | --- |
| GW·RT StatefulSet, Service, ShardDirectory, probe | replica·자원·배치·외부 노출 |
| TLS 설정과 기존 Secret mount | 인증서 발급·CA·Secret 공급·갱신 |
| 범용 `annotations`, `podAnnotations`, `extraEnv` | rollout controller·ArgoCD 보존 규칙 |
| metrics endpoint | 수집·대시보드·경보 |

차트는 인증서나 credential을 발급하지 않는다. Secret 공급 도구와 controller는 배포자가 선택한다.

## 사전 준비

release namespace에 다음 Secret을 공급한다. key는 표준 이름을 사용한다.

| values | 기본 Secret | key |
| --- | --- | --- |
| `credentials.existingSecret` | `relaygate-credentials` | `cluster-token`, 선택적 `next-cluster-token` |
| `tls.edge.existingSecret` | `relaygate-edge-tls` | `tls.crt`, `tls.key`; customCa는 `ca.crt` 추가 |
| `tls.internal.trustSecret` | `relaygate-internal-trust` | `ca.crt` |
| `tls.internal.gatewaySecret` | `relaygate-gw-internal-tls` | `tls.crt`, `tls.key` |
| `tls.internal.routeTableSecret` | `relaygate-rt-internal-tls` | `tls.crt`, `tls.key` |

Gateway leaf는 `gatewayServerName` SAN과 clientAuth/serverAuth를, RT leaf는
`routeTableServerName` SAN과 serverAuth를 가진다. CA private key는 workload에 제공하지 않는다.

## 설치

Secret을 준비한 뒤 실행한다.

```bash
helm upgrade --install relaygate deploy/helm/relaygate \
  --namespace relaygate --create-namespace --wait
```

| 설정 | 기본값 / 선택 |
| --- | --- |
| SDK TLS | 필수, `customCa` 기본값 또는 `webPkiRoots` |
| internal transport | `mtls` 기본값; 격리 테스트는 `plaintext` 명시 |
| SDK Service | `ClusterIP`; 필요 시 `LoadBalancer` |
| 외부 L4 | platform passthrough, Gateway에서 TLS 종료 |
| 데이터 | memory-only, persistent volume 없음 |

internal plaintext는 무인증·무암호화이며 내부 Secret을 mount하지 않는다. SDK TLS와 ClusterToken은 유지한다.
SDK는 `tls.edge.serverName`과 `relaygate/2` ALPN을 검증한다.

## 운영 override

```yaml
tls:
  edge:
    existingSecret: public-edge-tls
    trustMode: webPkiRoots
    serverName: relaygate.example.com
  internal:
    trustSecret: internal-trust
    gatewaySecret: gateway-leaf
    routeTableSecret: route-table-leaf

gateway:
  replicaCount: 3
  annotations: {}
  podAnnotations: {}
  resources: {}

routeTable:
  shardCount: 2
  annotations: {}
  podAnnotations: {}
  resources: {}
```

`annotations`는 StatefulSet metadata, `podAnnotations`는 Pod template에 전달한다.
GitOps는 실제 Secret 이름에 맞춰 갱신 watch를 설정하고 controller가 변경한 필드를 ArgoCD sync에서 보존한다.
실행 가능한 platform override 예시는 [Kind fixture](../../../tests/kind/cert-manager-values.yaml)에 있다.

## 변경 계약

| 변경 | 적용 |
| --- | --- |
| Gateway/RT image | 해당 StatefulSet만 교체 |
| chart version만 변경 | Pod template 유지 |
| credential·인증서 갱신 | platform rollout 정책 또는 해당 StatefulSet의 `rollout restart` |
| CA rotation | old/new trust overlap → leaf 교체 → old trust 제거 |
| 내부 mode·RT shard directory | maintenance window에서 coordinated restart |

RT shard 수는 immutable ShardDirectory를 바꾼다. 기존 workload 종료를 확인한 뒤 새 directory로 재설치한다.

인증서는 startup 시 읽는다. GW 교체는 신규 admission 중단 → active Pipe drain → deadline cleanup 순서다.
SDK는 reconnect·republish하고, 기존 Pipe 연속성은 보장하지 않는다.
`terminationGracePeriodSeconds`는 `drainTimeoutMs`보다 길게 설정한다.

## 릴리스

기능 PR과 chart version을 올리는 release PR을 분리한다. main CI가 새 immutable chart package를 발행한다.
