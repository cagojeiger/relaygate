# RelayGate Helm chart

이 차트는 RelayGate의 runtime 두 종류만 배포한다.

```text
Rust SDK ── TCP ──► Gateway Service
                      │
                      ├── Gateway StatefulSet × N
                      │      └── pod별 stable peer DNS
                      │
                      └── hash(ClientId)
                             └── RouteTable headless Service
                                    └── StatefulSet × M logical shards
```

SDK application, TLS ingress, service mesh, Secret 발급기, 저장소는 포함하지 않는다. Gateway와
RouteTable image version은 서로 독립적으로 지정한다. RouteTable은 memory-only이므로 PVC와
`emptyDir`를 사용하지 않는다.

## 사전 조건

Kubernetes 1.32 이상이 필요하다. RouteTable StatefulSet은 stable
`apps.kubernetes.io/pod-index` label을 logical shard ordinal로 사용한다.

현재 runtime의 내부 TCP adapter는 Gateway 이름별 key allowlist를 요구한다. 차트는 Secret을
생성하지 않으며 다음 두 key가 있는 기존 Secret만 참조한다.

| Secret data key | 형식 |
| --- | --- |
| `internal-gateway-keys` | `GatewayName=InternalGatewayKey,...` |
| `client-keys` | `ClientId=ClientKey,...` |

release 이름이 `relaygate`이고 Gateway가 3개인 기본값의 Gateway 이름은
`relaygate-gateway-0`, `relaygate-gateway-1`, `relaygate-gateway-2`다.

```bash
kubectl create namespace relaygate
kubectl -n relaygate create secret generic relaygate-credentials \
  --from-literal=internal-gateway-keys='relaygate-gateway-0=replace-a,relaygate-gateway-1=replace-b,relaygate-gateway-2=replace-c' \
  --from-literal=client-keys='example.listener=replace-client-key'
```

release 이름 또는 `gateway.replicaCount`가 다르면 Secret의 Gateway 이름도 실제 StatefulSet
pod 이름과 맞춰야 한다. credential 문자열은 쉼표로 항목을 구분하므로 개별 이름과 key에
쉼표를 사용하지 않는다.

## 설치

```bash
helm upgrade --install relaygate deploy/helm/relaygate \
  --namespace relaygate \
  --create-namespace \
  --set internalTransport.trustedLocalAdapter=true \
  --wait

helm test relaygate --namespace relaygate
```

기본 SDK Service는 cluster 내부에서
`relaygate.relaygate.svc.cluster.local:27420`으로 접근한다. SDK, Gateway peer와 RouteTable
Service는 모두 cluster 내부 전용이다. 외부 진입이 필요하면 이 차트 밖의 TLS-enabled L4 proxy나
service mesh를 통해 SDK Service로 연결한다.

## 배포 계약

- Gateway는 StatefulSet이다. pod ordinal이 `GatewayName`과 pod별 peer locator를 안정적으로
  만든다. SDK Service는 ready Gateway 중 하나로 새 session을 전달한다.
- peer headless Service는 특정 Owner Gateway pod를 one hop으로 찾는 DNS만 제공한다. payload가
  RouteTable을 통과하지 않는다.
- `routeTable.shardCount=M`은 하나의 RouteTable StatefulSet에 pod `0..M-1`을 만든다. 각
  ordinal은 정확히 하나의 logical shard `rt-0..rt-(M-1)`와 pod별 headless DNS endpoint를
  소유한다. `M`은 replica 수가 아니라 shard 수다.
- 모든 process는 같은 immutable ConfigMap의 exact JSON bytes를 read-only로 mount한다.
  Helm upgrade로 directory content를 바꾸면 immutable update가 거절된다.
- RT restart는 빈 `READY` 상태로 시작한다. Gateway가 current Listener snapshot을 새 lease로
  등록하면서 복구한다. RT 중단만으로 established Pipe를 닫지 않는다.
- Gateway 교체로 해당 pod의 session과 Pipe는 끝난다. SDK는 transport session을 재연결하지만
  commit된 `OPEN`, 기존 Pipe, application request를 replay하지 않는다.
- Gateway가 비정상 종료되어 `Deregister`하지 못하면 old mapping은 최대
  `routeTable.leaseTtlMs` 동안 남을 수 있다. 이 구간의 새 remote `OPEN`은 실패할 수 있으며
  application이 새 `OPEN` 시도를 결정한다.

`helm --wait`와 pod readiness는 process/socket 및 SDK admission 준비를 뜻한다. 모든 Listener
snapshot의 RT 재등록 완료나 end-to-end Pipe 성공을 뜻하지 않는다.

## 변경 규칙

| 변경 | 절차와 영향 |
| --- | --- |
| image tag | Gateway와 RT는 StatefulSet ordinal별 rolling replacement |
| Secret startup config reload | Secret 갱신 뒤 `credentials.reloadToken`을 새 값으로 바꾸어 Gateway와 RT restart. 무중단 key rotation 보장은 아님 |
| Gateway 수 증가 | 새 ordinal의 GatewayName/key 추가 → reloadToken 변경 rollout 완료 → replica 증가 |
| Gateway 수 감소 | replica 감소 → 제거된 GatewayName/key 삭제 → reloadToken 변경 rollout 완료 |
| RT shard 수·port·cluster domain | immutable directory를 바꾸므로 maintenance window에서 기존 runtime 완전 종료를 확인한 뒤 새 구성으로 다시 설치 |

RouteTable replication, online shard resize와 기존 Pipe 무중단 migration은 이 차트의 보장이
아니다. directory ConfigMap만 수동 삭제한 뒤 같은 release를 upgrade하는 방식도 지원하지 않는다.
내부 key adapter는 confidentiality를 제공하지 않는다. 실제 운영에서는 trusted network 또는
service mesh/mTLS 같은 배포 계층으로 내부 channel identity와 integrity를 제공해야 한다.

StatefulSet identity는 partition된 node의 process fencing, force-delete 뒤의 단일 writer, RT
replication과 consensus를 제공하지 않는다. RouteTable StatefulSet에는 PVC와
`volumeClaimTemplates`가 없으므로 pod 재생성은 같은 shard identity의 빈 memory state로 시작한다.

ShardDirectory를 바꿀 때는 terminating old RT와 새 RT가 겹치지 않도록 다음 순서를 지킨다.

```bash
helm uninstall relaygate --namespace relaygate --wait
kubectl wait --namespace relaygate \
  --for=delete pod \
  --selector app.kubernetes.io/instance=relaygate \
  --timeout=120s

helm install relaygate deploy/helm/relaygate \
  --namespace relaygate \
  --set internalTransport.trustedLocalAdapter=true \
  --wait
```

삭제 대기가 timeout이면 재설치하지 않고 남은 workload와 pod가 없는지 먼저 확인한다. 이 절차도
partition된 node에서 계속 실행되는 process를 fence하지는 못한다.

Gateway 수 증가를 한 번의 Secret 변경과 replica update로 합치면 기존 RT/Gateway가 새 이름을
아직 신뢰하지 않을 수 있다. 위의 두 단계 순서를 지킨다. 이미 존재하는 GatewayName의 key를
in-place rotation하는 절차는 차트가 제공하지 않으며 release 제거 후 새 설치가 필요하다.

## 주요 values

```yaml
internalTransport:
  trustedLocalAdapter: true

credentials:
  existingSecret: relaygate-credentials

gateway:
  replicaCount: 3
  image:
    repository: ghcr.io/cagojeiger/relaygate-gateway
    tag: "0.1.0"
  service:
    port: 27420

routeTable:
  shardCount: 2
  image:
    repository: ghcr.io/cagojeiger/relaygate-route-table
    tag: "0.1.0"
```

resource request/limit은 부하 측정 없이 임의 기본값을 두지 않는다. `resources`, scheduling
필드와 `extraEnv`는 환경별 values file에서 지정한다. `extraEnv`는 chart-managed identity,
주소, directory, credential, log 변수를 중복 정의할 수 없다.
