# ADR 004: Raft safety state 최소 영속화

## Context

RelayGate의 연결은 일시적이지만 Raft safety는 process 재시작 뒤에도 유지되어야 한다.

## Decision

Raft가 요구하는 term/vote, log, membership과 snapshot을 영속화한다. 합의된 routing control
record는 Raft log/snapshot에만 두며 별도 database를 만들지 않는다.

Pipe, Listener connection, inflight, buffer, payload, control session과 client credential은
영속화하지 않는다.

복구된 control record는 현재 leader의 session 재검증과 owning Gateway 확인 전에는 route가 아니다.

Confirmed leader나 quorum을 사용할 수 없으면 새 binding commit, resolve와 one-attempt admission context
발급을 멈춘다. Quorum confirmation 뒤 이미 발급된 single-use attempt는 같은 `ClusterEpoch` 안에서만 local
reservation/accept ordering을 따를 수 있다. Local teardown과 이미 성립한 Pipe relay는 계속하며, 이 상태에서
새 cluster epoch를 만들지 않는다.

`ClusterEpoch`는 모든 old authority path를 외부적으로 fence한 명시적 reset/bootstrap에서만 바꾼다. 새
epoch는 늦은 old-epoch message를 거부하지만 epoch 값 자체가 unreachable old cluster를 중지시키지는 않는다.

Local storage를 잃은 voter는 기존 identity를 재사용하지 않고 살아 있는 quorum에 새 identity로
합류한다.

## Consequences

- Raft safety만 재시작을 넘어 유지된다.
- 연결, payload와 credential은 복구 대상이 아니다.
- Quorum 상실은 자동 epoch 전환이 아니라 새 control operation의 정지다.
- Current-epoch binding tombstone은 ABA 방지를 위해 보존한다. v0은 distinct-key 상한에서 새 key를
  fail closed하며 routine tombstone GC를 하지 않는다.
