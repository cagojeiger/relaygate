# TEST 005: Helm 배포 검증

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 대상 | [`deploy/helm/relaygate`](../../deploy/helm/relaygate/) |
| 기준 | [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 005](../adr/005-soft-state-registration-lifecycle.md), [ADR 008](../adr/008-operational-health-boundaries.md), [SPEC 008](../spec/008-runtime-observability-contract.md) |

이 문서는 protocol 규칙을 만들지 않는다. Gateway와 RouteTable의 기존 runtime 계약이
Kubernetes resource로 보존되는지 검증한다.

## 배포 불변식

```text
SDK workload       = chart 밖
Gateway            = StatefulSet × N
SDK endpoint       = ready Gateway를 고르는 Service 1개
peer endpoint      = Gateway pod별 stable DNS, one hop
RouteTable         = StatefulSet × M logical shards
RT endpoint        = headless Service의 shard pod별 stable DNS
ShardDirectory     = 모든 Gateway/RT가 읽는 exact bytes 1개
persistent volume  = 없음
credential value   = chart 밖의 existing Secret
```

| ID | 통과 조건 |
| --- | --- |
| `AC-HELM-01` | 기본값은 Gateway 3개와 RT shard 2개를 렌더하고, 최소 profile은 Gateway 1개와 RT shard 1개를 렌더한다. |
| `AC-HELM-02` | Gateway는 StatefulSet ordinal을 `GatewayName`으로, headless Service의 pod FQDN을 `GatewayLocator`로 사용한다. SDK Service와 peer locator는 서로 다른 endpoint다. |
| `AC-HELM-03` | RT는 headless Service 하나와 StatefulSet 하나를 사용한다. `replicas=M`의 ordinal `0..M-1`은 각각 logical shard `rt-0..rt-(M-1)`이며, `apps.kubernetes.io/pod-index`를 `ShardId`에 연결한다. |
| `AC-HELM-04` | immutable ConfigMap은 ordered `rt-0..rt-(M-1)`와 각 RT pod FQDN을 한 exact JSON artifact로 만들고 모든 process가 같은 directory를 read-only mount한다. directory checksum은 Gateway와 RT pod template에 같고, ordinary Helm upgrade로 artifact 변경을 허용하지 않는다. |
| `AC-HELM-05` | chart는 Secret data, SDK workload, PVC, `emptyDir`, Ingress, HPA와 logical RT shard의 복제본을 만들지 않는다. credential은 existing Secret key reference로만 전달한다. |
| `AC-HELM-06` | workload는 non-root UID/GID 10001, read-only root filesystem, dropped capabilities, RuntimeDefault seccomp와 service-account token 비활성화를 기본으로 한다. |
| `AC-HELM-07` | Gateway readiness는 SDK `HELLO -> WELCOME` check를 사용하고 liveness는 RT dependency를 검사하지 않는다. RT socket readiness는 READY-empty를 정상으로 취급한다. |
| `AC-HELM-08` | schema는 replica/shard 수와 shard 상한, port, image, log format, Secret reference의 잘못된 값을 거절한다. render는 숫자로 시작하는 release 이름, chart-managed extraEnv 충돌과 생성된 pod FQDN의 DNS 길이 초과를 거절한다. |
| `AC-HELM-09` | chart test가 SDK Service를 통해 새 Gateway admission session을 만들 수 있다. 이는 RT, Listener binding, Pipe 또는 application payload 성공을 뜻하지 않는다. |
| `AC-HELM-10` | `credentials.reloadToken` 변경은 Gateway와 모든 RT pod template을 바꾸어 startup credential을 다시 읽게 한다. Gateway scale-out은 새 name/key의 additive reload가 끝난 뒤 수행한다. |
| `AC-HELM-11` | SDK, peer와 RT Service는 ClusterIP 내부 endpoint만 만들고, current plain-TCP adapter는 `internalTransport.trustedLocalAdapter=true`를 명시하지 않으면 render를 거절한다. |
| `AC-HELM-12` | chart는 stable `apps.kubernetes.io/pod-index`를 사용할 수 있는 Kubernetes 1.32 이상만 허용한다. |

## CI 정적 검증

```text
helm lint --strict
  -> values.schema.json과 chart 구조

helm template default
  -> Gateway 3 / RT 2

helm template minimal
  -> Gateway 1 / RT 1

helm template --kube-version
  -> 1.32 허용 / 1.31 거절

kubeconform -strict
  -> 표준 Kubernetes resource schema

helm package
  -> 독립 chart package 생성 가능성
```

CI는 render 결과의 resource 수, StatefulSet identity/locator env, ShardDirectory bytes,
Secret reference와 volume 종류를 정적으로 검사한다. `helm test` Pod는 chart에 포함되지만 실제
cluster 실행 결과는 정적 render만으로 증명되지 않는다.

## cluster acceptance

실제 cluster 검증은 다음 순서로 수행한다.

```text
1. 외부 Secret 생성
2. helm upgrade --install
3. Gateway N개와 RT M개 Ready 확인
4. RT ordinal, ShardId, pod FQDN과 directory generation 일치 확인
5. helm test로 SDK admission 확인
6. public Rust SDK Listener/Connector로 local 및 one-hop Pipe 확인
7. Gateway pod 하나 삭제
   -> 해당 Pipe 종료, SDK session 재연결, Listener current binding 재등록
8. RT shard pod 하나 삭제
   -> READY-empty, 해당 shard current snapshot 재등록, established Pipe 유지
9. StatefulSet rolling restart와 Helm upgrade 뒤 새 Pipe matrix 재수렴 확인
10. PVC/emptyDir와 chart-managed Secret이 없음을 확인
```

## 증거 경계

정적 CI는 Kubernetes API server, CNI/DNS, load balancer, scheduler, image pull, Secret 내용과 실제
process readiness를 증명하지 않는다. `helm test`도 SDK admission만 확인하며 RT availability,
Listener binding, remote one-hop Pipe와 payload 결과는 별도의 cluster acceptance가 필요하다.

Gateway 교체는 기존 Pipe를 migration하지 않는다. RT shard 수·port·cluster domain 변경은 exact
directory generation과 authority를 바꾸므로 ordinary rolling scale이 아니다. maintenance
window에서 `helm uninstall --wait`와 기존 pod 삭제 완료를 확인한 뒤 새 설치한다.
비정상 Gateway 종료 후 old mapping은 lease TTL까지 남을 수 있으며 그동안 새 remote `OPEN`이
실패할 수 있다. 이는 Pipe replay나 same-attempt fallback을 뜻하지 않는다.
이 차트는 RT replication, consensus, online shard resize와 zero-downtime Pipe migration을 보장하지
않는다. StatefulSet identity는 partition된 node의 process fencing을 증명하지 않는다.
