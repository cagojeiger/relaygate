# ADR 002: Raft와 Gateway topology

## Context

RelayGate는 작은 cluster에서 한 authority만 새 route와 Pipe를 승인해야 한다. 연결의 실제 생존 여부는
Raft가 아니라 owning Gateway가 안다.

## Decision

- 하나의 Raft group은 leader/quorum과 `ClusterEpoch`만 소유한다.
- Current leader는 memory에서 control session과 현재 Listener directory를 소유한다.
- Gateway는 자기 Listener, Pipe segment, buffer와 payload를 소유한다.
- Authority나 control session이 끝나면 해당 memory state를 삭제하고 Gateway가 다시 선언한다.
- Raft voter는 최대 7개다. Relay 용량은 voter가 아니라 Gateway replica를 늘려 확장한다.

```text
Raft quorum -> current Authority -> current session directory -> owning Gateway
     safety          decision             location                live truth
```

## Consequences

- Raft는 key-value route store가 아니다.
- Failover 직후 directory는 비어 있고 reconnect한 Gateway의 현재 Listener부터 다시 사용할 수 있다.
- 저장량은 과거 churn이 아니라 현재 session/Listener 수에 비례한다.
