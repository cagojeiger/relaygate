# ADR 004: Raft safety만 영속화

## Context

Pipe와 route는 재시작 뒤 복구하지 않지만 Raft identity와 합의 안전은 재사용 시 보존되어야 한다.

## Decision

영속화하는 것은 Raft의 term/vote, log, membership, snapshot과 고정 크기 `ClusterEpoch` marker뿐이다.
Gateway, control session, Listener, route, attempt, Pipe, payload, credential과 tombstone은 Raft command나
snapshot에 넣지 않는다.

Confirmed leader/quorum이 없으면 새 session, bind, resolve와 OpenContext 발급을 멈춘다. 이미 열린 Pipe의
local relay와 teardown은 Raft와 독립적으로 계속한다.

`ClusterEpoch` 변경은 모든 old authority path를 외부에서 fence한 offline bootstrap에서만 허용한다.
Store를 잃은 voter는 기존 NodeId를 재사용하지 않고 surviving quorum에 새 identity로 합류해야 한다.

## Consequences

- 정상 재시작은 Raft safety/epoch만 복구하고 route는 reconnect/redeclare한다.
- Local voter store loss를 자동 복구하는 dynamic membership은 별도 운영 기능이다.
- Old path 전체 fence를 증명하지 못하면 새 epoch를 열지 않고 fail closed한다.
