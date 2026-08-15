# ADR 002: Raft control state와 Gateway의 상태 경계

> 영속성은 [ADR 004](004-raft-safety-state-durability.md), client 격리는
> [ADR 006](006-client-isolation-and-external-credentials.md)을 따른다.

## Context

Cluster는 Endpoint 위치에 합의해야 하지만 실제 연결의 생존 여부는 owning Gateway만 안다.

## Decision

하나의 Raft group은 **현재 cluster epoch의 작은 routing control record**만 합의한다. Raft library,
storage와 node transport는 Go runtime 내부 구현이다.

Raft는 Gateway registration과 `(ClientId, EndpointPattern, TargetId) → ListenerBindingRef` control
record를 소유한다. Route는 committed record, 현재 leader가 재검증한 session, owning Gateway의 live
binding 확인이 모두 있을 때만 유효하다.

Gateway는 자신의 Listener와 Pipe segment를 소유한다. Pipe, inflight, buffer와 payload는 Raft에
넣지 않으며 Gateway 간 relay는 public data-plane gRPC를 사용한다.

Cluster는 하나의 Raft group과 최대 7개의 voter로 제한한다. 연결 용량은 quorum을 키우지 않고
Gateway-only node를 추가해 확장한다.

## Consequences

- Raft는 범용 key-value database가 아니다.
- 재시작한 Gateway는 새 instance로 등록하며 연결과 Pipe를 복구하지 않는다.
- Gateway 용량과 Raft 합의 규모는 독립적으로 확장된다.
