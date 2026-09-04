# ADR 009: Gateway 종료는 bounded drain하고 재연결은 jitter로 분산한다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 007](007-transport-liveness-and-idle-retirement.md), [ADR 008](008-operational-health-boundaries.md) |

## 맥락

Gateway가 정상 배포 종료와 transport 장애를 같은 즉시 취소로 처리하면 완료 가능한 기존 Pipe도
중단된다. 여러 SDK가 같은 Gateway 종료를 동시에 관찰하고 고정된 backoff로 재연결하면 시도가
같은 시점에 집중된다. RouteTable 재시작 때 여러 Gateway가 동일한 backoff로 연결해도 같은 문제가
생긴다. RouteTable은 application Pipe를 소유하지 않으므로 같은 drain 문제는 없다.

## 결정

```text
Gateway normal shutdown
  -> 신규 session, REGISTER, OPEN, peer OPEN 중단
  -> current Listener publication을 RT에서 철회
  -> 이미 시작된 attempt와 Pipe만 drain
  -> active work == 0 또는 drain timeout
  -> 남은 session과 distributed runtime 종료

SDK reconnect
  -> bounded exponential backoff
  -> 각 runtime이 매 시도 delay를 jitter

Gateway -> RouteTable reconnect
  -> shard별 bounded exponential backoff
  -> Gateway와 shard identity에서 분리된 매 시도 delay를 jitter
```

Drain 중 거절은 `UNAVAILABLE`이다. Gateway가 새 작업을 관찰했으나 실행하지 않았음을 확인할 수
있는 경우 `NOT_OBSERVED`를 사용한다. Drain deadline 뒤 남은 Pipe는 기존 transport-loss cleanup으로
종료하며 replay, migration 또는 resume하지 않는다. RT publication 철회는 Gateway-local binding과
기존 Pipe를 제거하지 않으며 RT 장애 시에는 lease expiry가 stale mapping을 최종 정리한다.

재연결 jitter는 현재 exponential backoff 단계의 `2/3..1` 범위에서 선택한다. SDK는 runtime별
entropy를, Gateway의 RT worker는 Gateway와 shard별 entropy를 사용한다. 성공한 Connector session이
안정 구간을 넘거나 Listener recovery registration이 성공하면 SDK backoff 단계를 초기화하고,
RT connection 성공은 해당 shard worker의 backoff 단계를 초기화한다.

RouteTable 정상 종료는 connection과 queued request를 종료한다. 완료 응답을 관찰하지 못한 요청은
기존 operation uncertainty와 current-state 재등록 규칙으로 수렴한다. Gateway Pipe drain, RT
persistence, replication 또는 blue/green orchestration을 소유하지 않는다.

## 결과

- 정상 Gateway 교체는 완료 가능한 기존 Pipe에 drain 기회를 준다.
- 무기한 Pipe가 rollout을 막지 않도록 timeout 뒤 강제 종료한다.
- 동시 SDK 재연결 시도를 시간 범위에 분산한다.
- RT 재시작 뒤 Gateway의 shard connection 재시도를 시간 범위에 분산한다.
- 기존 Pipe 무중단 migration과 배포 순서 제어는 제공하지 않는다.
