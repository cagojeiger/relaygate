# RelayGate Helm chart

RouteTable shard와 Gateway runtime을 StatefulSet으로 배포합니다.

## 설정 경계

| Helm chart 기본 책임 | GitOps / 배포자 책임 |
| --- | --- |
| GW·RT, Service, ShardDirectory, probe | replica 수, 자원 상한, Pod 배치 정책 |
| TLS 기본값과 Secret 참조·mount | 도메인, Secret 공급, Issuer·CA·인증서 갱신 정책 |
| `annotations`, `podAnnotations`, `extraEnv` 전달 | Reloader watch, ArgoCD diff 보존, 운영 확장 설정 |
| metrics endpoint | ServiceMonitor, 대시보드, 경보 |

기본 실행은 credential·TLS Secret 공급을 전제로 한다. 기본 chart는 Reloader 설치·annotation이나
수동 reload token을 생성하지 않는다. 보안 기본값은 SDK TLS와 internal mTLS다.

```mermaid
flowchart LR
    SDK[외부 SDK] -->|TLS/TCP| L4[Platform L4 passthrough]
    L4 --> GWA[Gateway A]
    L4 --> GWB[Gateway B]
    GWA <-->|mTLS peer| GWB
    GWA -->|hash DestinationId · mTLS| RT[RouteTable StatefulSet × M]
    GWB -->|hash DestinationId · mTLS| RT
```

## 사전 준비

Kubernetes 1.32 이상과 release namespace의 다음 Secret이 필요합니다.

| Secret | key | 용도 |
| --- | --- | --- |
| credential | `cluster-token`, 선택적 `next-cluster-token` | SDK trust-domain admission과 rotation |
| edge TLS | `tls.crt`, `tls.key` | SDK-facing Gateway identity |
| edge TLS | `ca.crt` | custom CA mode의 trust anchor |
| internal mTLS | `ca.crt` | Gateway와 RT trust anchor |
| internal mTLS | `gateway.crt`, `gateway.key` | peer/RT Gateway identity |
| internal mTLS | `route-table.crt`, `route-table.key` | RT server identity |

```bash
kubectl create namespace relaygate
kubectl -n relaygate create secret generic relaygate-credentials \
  --from-literal=cluster-token='replace-cluster-token'
```

## TLS source

SDK edge TLS와 내부 전송은 독립 설정이다. 기본 `tls.internal.mode=mtls`는 내부 인증서를 요구한다.
격리된 테스트 환경에서는 다음 설정으로 내부 인증서 없이 설치한다. SDK edge Secret과 credential Secret은 유지한다.

```yaml
tls:
  internal:
    mode: plaintext
```

plain TCP는 내부 인증과 암호화를 제공하지 않는다. mTLS는 신뢰 CA와 Gateway 역할 SAN을 검증하며 별도 내부 키를 사용하지 않는다. 모드 전환과 내부 wire 변경은 GW/RT를 함께 변경하는 maintenance 작업이다.
GW–GW와 GW–RT는 내부 wire v2를 사용하며 다른 wire 버전의 연결을 거절한다.

| 구간 | mode | 공급 방식 |
| --- | --- | --- |
| SDK edge | `customCa` | edge Secret의 CA/certificate/key |
| SDK edge | `webPkiRoots` | bundled public roots + edge certificate/key |
| internal | `existingSecret` | operator-managed 단일 internal Secret |
| internal | `certManager` | platform Issuer가 Gateway/RT leaf를 각각 발급 |

cert-manager mode는 platform-owned Issuer와 CA trust Secret을 사용합니다.

```yaml
tls:
  internal:
    source: certManager
    gatewayServerName: relaygate-gateway.internal
    routeTableServerName: relaygate-route-table.internal
    certManager:
      issuerRef:
        name: relaygate-internal
        kind: ClusterIssuer
        group: cert-manager.io
      trustSecret:
        name: relaygate-internal-ca
        key: ca.crt
```

cert-manager는 leaf를 갱신하고 platform이 workload 교체를 관리한다.
CA rotation은 old/new trust overlap과 leaf 재발급 순서로 진행한다.

## GitOps override 예시

아래 Secret 이름은 배포자가 실제 mount한 Secret에 맞춘다. chart는 annotation을 해석하지 않고
StatefulSet `metadata.annotations`에 전달한다. `podAnnotations`는 Pod template용으로 별도다.

```yaml
gateway:
  annotations:
    secret.reloader.stakater.com/reload: edge-tls,internal-trust,gw-leaf
routeTable:
  annotations:
    secret.reloader.stakater.com/reload: internal-trust,rt-leaf
```

Reloader는 platform에 설치한다. 이 예시에서 edge·GW leaf 변경은 GW만, RT leaf 변경은 RT만,
공통 trust 변경은 두 역할을 교체한다. 인증서 발급과 재시작 정책은 별도 책임이다.
Gateway/RT는 시작 시 인증서를 읽는다. 자동 적용을 사용하지 않으면 해당 StatefulSet을 `kubectl rollout restart`한다.

GitOps는 Reloader 전략에 맞는 ArgoCD `ignoreDifferences`와 `RespectIgnoreDifferences=true`를 설정한다.
`annotations` 전략은 `/spec/template/metadata/annotations/reloader.stakater.com~1last-reloaded-from`을,
기본 `env-vars` 전략은 감시 Secret별 `STAKATER_<SECRET_NAME>_SECRET` env를 해당 container에서만 보존한다.
Secret 이름은 대문자로 변환하고 구분 문자는 `_`로 정규화한다. edge Secret도 Gateway의 보존 대상에 포함한다.
갱신 rollout은 graceful drain을 사용하며 deadline 뒤 기존 Pipe가 끊길 수 있다.
CA 키는 Issuer에만 제공하고 GW/RT에는 공개 trust bundle과 해당 role leaf만 mount한다.

## 설치

```bash
helm upgrade --install relaygate deploy/helm/relaygate \
  --namespace relaygate \
  --wait
```

| 진입 방식 | values·platform 설정 |
| --- | --- |
| cluster 내부 | 기본 `gateway.service.type=ClusterIP` |
| 전용 외부 주소 | `gateway.service.type=LoadBalancer` |
| 공유 L4 | ClusterIP + platform `TCPRoute`/`TLSRoute` passthrough |

SDK endpoint 기본값은 `relaygate.relaygate.svc.cluster.local:27420`입니다. Gateway process가 SDK TLS를
종단하고 peer/RT Service는 cluster 내부에서 사용합니다.

## 배포 계약

| 대상 | 계약 |
| --- | --- |
| Gateway ordinal | stable GatewayName과 peer locator |
| RT ordinal | `rt-0..rt-(M-1)` hash partition |
| ShardDirectory | 모든 process가 동일한 read-only ConfigMap 사용 |
| RT restart | 빈 상태에서 Gateway current Binding snapshot으로 재구축 |
| Gateway shutdown | 신규 admission 중단 → active Pipe drain → deadline cleanup |
| readiness | TLS + ClusterToken `HELLO/WELCOME` 확인 |
| filesystem | memory-only RT, persistent volume 0개 |

## 변경 절차

| 변경 | 절차 |
| --- | --- |
| Gateway image | Gateway StatefulSet rolling replacement |
| RT image | RT StatefulSet rolling replacement |
| chart package version | runtime Pod template 유지 |
| ClusterToken | current+next → SDK 이동 → next를 current로 승격 |
| edge certificate | Secret 갱신 → platform 정책으로 GW만 rollout |
| internal certificate | Secret/leaf 갱신 → platform 정책으로 해당 role rollout |
| Gateway 증가 | replica 증가 → mTLS Gateway 역할 인증 → 현재 상태 등록 |
| RT shard directory | maintenance window의 coordinated restart |

## 주요 values

```yaml
credentials:
  existingSecret: relaygate-credentials

tls:
  edge:
    existingSecret: relaygate-edge-tls
    trustMode: customCa
    serverName: relaygate-gateway.internal
  internal:
    source: existingSecret
    existingSecret: relaygate-internal-tls
    gatewayServerName: relaygate-gateway.internal
    routeTableServerName: relaygate-route-table.internal

gateway:
  replicaCount: 3
  service:
    type: ClusterIP
  drainTimeoutMs: 120000
  terminationGracePeriodSeconds: 135

routeTable:
  shardCount: 2

metrics:
  enabled: true
```

`gateway.terminationGracePeriodSeconds`는 `gateway.drainTimeoutMs`보다 길게 설정합니다. resource request와
limit은 측정값으로 정하고 `extraEnv`는 chart-managed identity, address, credential과 TLS 값 바깥에서
사용합니다.

## 릴리스

기능 PR은 현재 chart version으로 검증합니다. 별도 release PR이 version을 증가시키고 main CI가 새
immutable chart package를 발행합니다.
