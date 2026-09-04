# TEST 006: 로컬 Prometheus와 Grafana 검증

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 목적 | RT2/GW3 metric 수집과 RelayGate RED/USE dashboard provisioning을 실제 Compose network에서 검증한다. |
| 기준 | [SPEC 008](../spec/008-runtime-observability-contract.md), [TEST 004](004-rt2-gw3-closed-loop-test-plan.md) |

이 profile은 관측성 adapter 검증용이다. RelayGate의 routing·Pipe 상태를 바꾸거나 Prometheus를
복구 원본으로 사용하지 않는다.

```text
Gateway A/B/C :27422 ─┐
                      ├──► Prometheus :9090 ──► Grafana :3000
RouteTable 0/1 :27431 ┘              │
                                     └──► observability-probe
```

## 실행

```bash
docker compose --profile observability up --build -d --wait \
  prometheus grafana listener-a listener-b listener-c
docker compose run --rm --no-deps topology-probe relaygate-echo-probe matrix
docker compose --profile observability run --rm observability-probe
```

## 통과 조건

1. Prometheus에서 `relaygate-gateway` 3개와 `relaygate-route-table` 2개 target이 모두 `up=1`이다.
2. Grafana datasource `prometheus`가 자동 provisioning되고 `relaygate-overview` dashboard가 검색된다.
3. matrix 뒤 Gateway OPEN과 RouteTable operation의 request/result/duration series가 존재한다.
4. Gateway와 RouteTable filter가 실제 target 목록을 제공한다. Gateway filter는 OPEN·p95·current
   state·peer panel에, RouteTable filter는 request·p95·current state panel에 적용된다. `All`은
   합계를 표시하고 p95는 선택된 instance별 histogram bucket으로 계산된다.
5. metric과 dashboard query에 `ClientId`, credential, payload 또는 unbounded error message label을 추가하지 않는다.
6. Prometheus data는 container tmpfs에만 있고 cleanup 뒤 host에 시계열 파일을 남기지 않는다.
7. RT shard 하나를 중단하면 RT `up` 비율과 Gateway route dependency READY 비율이 감소하고
   `route_registrations_unsynced`가 증가해야 한다.
8. 중단한 RT shard를 복구하면 두 비율은 1로, unsynced 합계는 0으로 수렴하고 전체 matrix가
   다시 통과해야 한다.

`observability-probe`는 target과 provisioning을 자동 검증한다. panel의 시각적 배치와 query 결과는
Grafana에서 확인한다. alert와 SLO threshold는 실제 운영 부하를 측정한 뒤 별도 결정한다.

## 정리

```bash
docker compose --profile observability down --volumes --remove-orphans
```
