# ADR 009: 현재 상태 전용 authority directory

> **Status:** Accepted

## Context

RelayGate가 필요한 것은 지금 연결 가능한 Listener의 위치다. 과거 route, 삭제 이력과 재시작 전 연결을
복구하지 않는데도 이를 Raft record와 tombstone으로 유지하면 저장량, reconcile과 failure state가 실제
문제보다 커진다.

## Decision

Raft에는 term/vote, log, membership, snapshot 같은 합의 안전 상태와 old-epoch message를 막는 고정 크기의
`ClusterEpoch` marker만 둔다. Gateway, control session, Listener binding, route, tombstone과 presence는
RelayGate application command나 FSM snapshot에 넣지 않는다. Raft 자체 membership에는 Raft node identity와
transport address가 계속 포함된다.

Current authority는 memory에서 다음 현재 상태만 소유한다.

```text
ControlSessionRef = (ClusterEpoch, AuthorityId, ControlSessionId,
                     GatewayId, GatewayInstanceId)

DirectoryEntry = (BindingKey, ListenerBindingRef,
                  ControlSessionRef, OwnerRelayAddress)
```

- Gateway는 새 control session에서 현재 살아 있는 Listener 전체를 full snapshot으로 선언한다.
- Bind는 exact entry를 추가하고 unbind는 그 entry를 실제로 삭제한다. Tombstone과 generation은 없다.
- Control session이 끝나면 그 session의 entry를 한 번에 삭제한다.
- Authority가 바뀌면 모든 session과 directory entry를 비운다. 살아 있는 Gateway가 reconnect한 뒤 현재
  Listener를 다시 선언한다.
- 한 Gateway의 선언이 검증되면 다른 Gateway를 기다리지 않고 그 exact entry부터 route할 수 있다.
- 같은 current session의 exact declaration replay는 idempotent다. 같은 `BindingKey`의 다른 owner/ref는
  conflict로 fail closed한다.
- Replica 전체 명단이나 예상 replica 수는 admission 조건이 아니다. 필요하면 배포 계층이 운영 관찰값으로
  제공할 수 있지만 RelayGate는 이를 complete cluster view로 주장하지 않는다.

New-Pipe admission은 `A ∧ L ∧ Q ∧ D ∧ V ∧ O`다. `D`는 current authority directory의 exact entry,
`V`는 그 entry가 가리키는 exact current revalidated owner session이다. `O`는 context의 `AuthorityId`와
`OwnerControlSessionId`가 아직 exact current인지 다시 확인하는 owning Gateway의 local
binding/auth/capacity reservation이다.

OpenContext는 발급만으로 authority/session fence를 통과하지 않는다. Same `ClusterEpoch` 안에서도 authority나
owner control session이 바뀌면 O 이전 context는 즉시 무효다. O reservation과 `AttemptId` fence insert가
원자적으로 끝난 attempt만 이후 control change로 소급 취소하지 않고 Listener/Pipe의 volatile local lifecycle을
따른다. Participant/hop 종료는 그대로 terminal이다.

[ADR 008](008-cross-gateway-hop-and-replay.md)의 bounded `AttemptId` replay fence, absolute expiry와 trusted-hop
제약은 유지한다. 이는 살아 있는 attempt의 중복 실행을 막는 유한한 runtime state이지 route history가 아니다.
Queue, retry, response replay, Pipe resume/attach와 payload replay는 계속 지원하지 않는다.

## Supersedes

이 결정은 다음 Accepted 결정의 일부를 명시적으로 대체한다.

| 기존 결정 | 대체되는 부분 | 유지되는 부분 |
| --- | --- | --- |
| [ADR 002](002-control-plane-and-gateway-topology.md) | Raft의 Gateway registration과 routing control record 소유 | Gateway가 Listener/Pipe live truth를 소유하고 Raft transport가 runtime 내부라는 경계 |
| [ADR 004](004-raft-safety-state-durability.md) | Routing record 복구, Binding tombstone/누적, O 이전 issued context의 authority/session fence 이후 진행 | Raft safety 영속화, epoch fencing과 runtime/payload 비영속성 |
| [ADR 008](008-cross-gateway-hop-and-replay.md) | `BindingGeneration` provenance와 same-epoch authority 변경 뒤 O 이전 context 생존 | Session-memory owner address, bounded `AttemptId` fence/expiry, trusted-hop, O 이후 no retry/resume/replay |

## Consequences

- Control state 저장량은 과거 churn이 아니라 현재 session과 Listener 수에 비례한다.
- Failover 직후 route는 0개이며 reconnect/redeclare된 entry만 점진적으로 다시 사용할 수 있다.
- 재시작 전 route나 정확한 Open/Pipe outcome은 복구하지 않는다. Participant가 새 identity로 다시 연결한다.
- Stale operation은 generation/tombstone이 아니라 exact authority/session/instance/binding identity로 막는다.
- Presence는 현재 authority가 지금 관찰한 수치일 뿐 cluster completeness 또는 durable history가 아니다.

## 관련 문서

- [SPEC 001: RelayGate System Model](../spec/001-system-model.md)
- [SPEC 003: Failure and Recovery Model](../spec/003-failure-and-recovery-model.md)
- [SPEC 004: State Transition Model](../spec/004-state-transition-model.md)
- [TEST 001: Core Correctness Test Plan](../test/001-core-correctness-test-plan.md)
