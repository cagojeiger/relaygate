# RelayGate Helm chart

차트는 RelayGate의 Gateway와 RouteTable만 배포합니다.

```text
host/외부 SDK ── TLS/TCP ──► platform L4 ── passthrough ──► Gateway ClusterIP
                           └── Gateway StatefulSet × N
                                  │ mTLS peer
                                  │
                                  └── hash(DestinationId)
                                         └── RouteTable StatefulSet × M shards
```

SDK application, certificate 발급, Secret, platform Gateway, PVC와 application 저장소는 포함하지 않습니다.
RouteTable은 memory-only이며 `emptyDir`와 `volumeClaimTemplates`를 사용하지 않습니다.

## 사전 준비

Kubernetes 1.32 이상이 필요합니다. release namespace에 세 Secret을 먼저 만듭니다.

credential Secret:

| key | 값 |
| --- | --- |
| `internal-gateway-keys` | `GatewayName=InternalGatewayKey,...` |
| `cluster-token` | SDK trust-domain admission token |
| `next-cluster-token` | 선택적 rotation overlap token |

edge TLS Secret:

| key | 용도 |
| --- | --- |
| `ca.crt` | SDK가 신뢰할 CA |
| `tls.crt`, `tls.key` | SDK-facing Gateway TLS server identity |

internal mTLS Secret:

| key | 용도 |
| --- | --- |
| `ca.crt` | Gateway와 RouteTable이 신뢰할 내부 CA |
| `gateway.crt`, `gateway.key` | peer/RT mTLS Gateway identity |
| `route-table.crt`, `route-table.key` | GW–RT mTLS RouteTable identity |

기본 release 이름 `relaygate`, Gateway 3개이면 GatewayName은
`relaygate-gateway-0..2`입니다. certificate SAN은 기본 server name인
`relaygate-gateway.internal`, `relaygate-route-table.internal`을 포함해야 합니다. 필요하면 values에서
server name을 바꿉니다.

```bash
kubectl create namespace relaygate
kubectl -n relaygate create secret generic relaygate-credentials \
  --from-literal=internal-gateway-keys='relaygate-gateway-0=replace-a,relaygate-gateway-1=replace-b,relaygate-gateway-2=replace-c' \
  --from-literal=cluster-token='replace-cluster-token'

kubectl -n relaygate create secret generic relaygate-edge-tls \
  --from-file=ca.crt=edge-ca.crt \
  --from-file=tls.crt=edge-gateway.crt \
  --from-file=tls.key=edge-gateway.key

kubectl -n relaygate create secret generic relaygate-internal-tls \
  --from-file=ca.crt \
  --from-file=gateway.crt \
  --from-file=gateway.key \
  --from-file=route-table.crt \
  --from-file=route-table.key
```

## 설치

```bash
helm upgrade --install relaygate deploy/helm/relaygate \
  --namespace relaygate \
  --wait
```

기본 SDK endpoint는
`relaygate.relaygate.svc.cluster.local:27420`입니다. SDK는 CA와
`tls.edge.serverName`, ClusterToken을 사용합니다. peer와 RT Service는 cluster 내부 전용입니다.

외부 노출은 두 방식 중 하나를 사용합니다.

```text
전용 주소 : gateway.service.type=LoadBalancer
공유 L4   : ClusterIP 유지 + platform의 TCPRoute/TLSRoute passthrough
```

공유 Envoy Gateway를 사용하는 경우 chart가 `GatewayClass`나 route를 만들지 않습니다. GitOps에서
전용 listener와 `TLSRoute` passthrough를 Gateway SDK Service로 연결하며 TLS는 RelayGate Gateway가
종단합니다.

## 배포 계약

- Gateway pod ordinal은 stable GatewayName과 peer locator를 만듭니다.
- RT pod ordinal은 `rt-0..rt-(M-1)` shard이며 `M`은 replica 수가 아닌 hash partition 수입니다.
- 모든 process가 exact ShardDirectory ConfigMap을 read-only로 공유합니다.
- Gateway는 SDK TLS server이고 내부 mTLS client/server입니다.
- RT는 내부 mTLS server입니다.
- chart는 평문 fallback이나 test-only insecure env를 렌더하지 않습니다.
- RT restart는 빈 상태로 시작하고 Gateway current Binding snapshot으로 재구축됩니다.
- Gateway 교체는 해당 session과 Pipe를 끝냅니다. SDK는 reconnect하고 Listener를 republish하지만
  기존 Pipe와 payload를 복구하지 않습니다.
- graceful drain은 신규 admission을 중단하고 configured deadline까지 기존 Pipe를 기다린 뒤 종료합니다.
- pod readiness는 TLS + ClusterToken HELLO/WELCOME만 확인하며 E2E Pipe 성공은 보장하지 않습니다.

## 변경

| 변경 | 절차 |
| --- | --- |
| Gateway/RT image | 각각 독립 tag로 rolling replacement |
| ClusterToken rotation | current + next 배포 → SDK 이동 → new current만 배포 |
| edge certificate | Secret 갱신 뒤 `tls.edge.reloadToken` 변경으로 Gateway rollout |
| internal certificate | Secret 갱신 뒤 `tls.internal.reloadToken` 변경으로 Gateway/RT rollout |
| Gateway 증가 | 새 GatewayName/key를 먼저 허용하고 rollout한 뒤 replica 증가 |
| RT shard 수/domain/port | maintenance window에서 기존 release/pod 완전 종료 후 새 directory로 설치 |

RT online resharding, replication, quorum, Gateway Pipe migration과 certificate hot reload는 제공하지
않습니다.

## 주요 values

```yaml
credentials:
  existingSecret: relaygate-credentials
  reloadToken: ""

tls:
  edge:
    existingSecret: relaygate-edge-tls
    serverName: relaygate-gateway.internal
    reloadToken: ""
  internal:
    existingSecret: relaygate-internal-tls
    gatewayServerName: relaygate-gateway.internal
    routeTableServerName: relaygate-route-table.internal
    reloadToken: ""

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

`gateway.terminationGracePeriodSeconds`는 `gateway.drainTimeoutMs`보다 길어야 합니다. resource request와
limit은 측정 없이 임의 기본값을 두지 않습니다. `extraEnv`는 chart-managed identity, 주소,
credential과 TLS 변수를 덮어쓸 수 없습니다.

## 릴리스

기능 변경 PR은 `Chart.yaml` version을 유지할 수 있으며 검증만 수행합니다. 별도 릴리스 PR이 version을
증가시키면 main CI 뒤 새 immutable chart를 발행합니다. version 감소와 이미 발행된 version의 다른
package 덮어쓰기는 거절합니다.
