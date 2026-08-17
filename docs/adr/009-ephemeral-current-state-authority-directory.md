# ADR 009: 현재 상태 전용 authority directory

## Context

RelayGate가 필요한 것은 지금 연결 가능한 Listener다. 과거 route와 삭제 이력을 저장하면 transient relay에
불필요한 generation, tombstone, GC와 reconciliation이 생긴다.

## Decision

```text
ControlSessionRef = (ClusterEpoch, AuthorityId, ControlSessionId,
                     GatewayId, GatewayInstanceId)
DirectoryEntry    = (BindingKey, ListenerBindingRef,
                     ControlSessionRef, OwnerRelayAddress)
```

- 새 control session은 현재 살아 있는 Listener 전체를 snapshot으로 선언한다.
- Bind는 exact entry를 추가하고 unbind는 true delete한다.
- Session 종료는 소유 entry 전체를 삭제하고 authority 종료는 directory 전체를 삭제한다.
- 같은 session/ref 재선언만 idempotent다. 같은 key의 다른 owner/ref는 conflict다.
- Stale mutation은 exact authority/session/instance/binding identity로 막는다.
- Replica 수나 전체 명단은 admission 조건이 아니다. Presence는 현재 authority가 관찰한 수치일 뿐이다.

New-Pipe admission은 `A ∧ L ∧ Q ∧ D ∧ V ∧ O`다. `D`는 exact current directory entry, `V`는 그
entry의 current revalidated owner session, `O`는 owning Gateway의 atomic local reservation이다.

## Consequences

- Memory는 현재 live registration에 비례하며 historical key churn으로 늘지 않는다.
- Failover 뒤 route는 0에서 시작하고 각 Gateway의 fresh declaration으로 점진 복구한다.
- ACK를 잃은 mutation outcome은 복구/replay하지 않는다. Session 종료가 possible effect를 삭제한다.
