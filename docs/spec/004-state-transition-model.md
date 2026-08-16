# SPEC 004: State Transition Model

> **Status:** Draft
>
> RelayGate의 모든 runtime state, event와 전이를 하나의 닫힌 상태 모델로 정의한다.

이 문서는 **v0의 관찰 가능한 상태 의미와 전이**에 대한 canonical source다. 구현은 동일한 identity,
linearization, outcome과 failure oracle을 보존하고 [TEST 001](../test/001-core-correctness-test-plan.md)을
통과하는 한 machine을 합치거나 내부 type·goroutine을 다르게 구성할 수 있다. 이 문서의 곱은 의미 모델이지
필수 process/type topology가 아니다. [SPEC 001](001-system-model.md)은 system identity와
Pipe 계약, [SPEC 002](002-client-configuration-and-presence.md)는 config·presence 의미,
[SPEC 003](003-failure-and-recovery-model.md)은 failure·race·복구 의미를 소유한다.

## 상태 모델의 닫힘

Global `Healthy` state는 없다. 전체 상태는 독립 machine의 곱이고 route·복구 상태는 파생값이다.

```text
SystemState = Authority × Quorum × GatewayRegistration × ControlSession × Presence × AuthSnapshot
            × ClientSession × LocalBinding × FlowControl
            × OwnerPipe × IngressPipe × ListenerPipe × CallerPipe
            × OpenAllAccumulator
```

각 machine은 다음 total function을 만족해야 한다.

```text
δM : StateM × EventM → StateM × Effect*
∀s ∈ StateM, ∀e ∈ EventM, δM(s, e) is defined
```

표에 직접 적히지 않은 조합은 다음 우선순위로 정확히 하나의 결과를 갖는다.

| 순서 | 조건 | 결과 |
| --- | --- | --- |
| 1 | Event의 epoch가 old이거나 그 전이를 소유하는 machine 기준 identity/generation이 stale함 | State 불변 + stable rejection. Cleanup/terminal replay라면 idempotent no-op. |
| 2 | 아래 표의 `From + Event + Guard`와 일치함 | 표의 `To + Effect`를 적용한다. |
| 3 | 이미 적용한 duplicate 또는 같은 ended identity의 terminal/fenced/retired state에 도착한 event | State 불변 + no-op. State-advancing success로 응답하지 않는다. |
| 4 | Current identity지만 현재 state에서 허용되지 않는 command 또는 guard 실패 | State 불변 + stable rejection. 성공으로 숨기지 않는다. |

각 입력은 `Applied(next, effects)`, `NoOp(reason)` 또는 `Rejected(reason)` 중 정확히 하나로 관찰 가능해야 한다.
둘 이상의 explicit row guard가 동시에 참이면 spec/implementation error다. 따라서 어떤 event도 암묵적인 전이,
partial update 또는 부활을 만들지 않는다.

Default 규칙은 수학적 domain을 닫을 뿐 lifecycle 누락을 정당화하지 않는다. Non-terminal state가 자기
identity/session/hop/epoch의 종료를 관찰했는데 stable rejection으로 남아 고립될 수 있다면 spec error다.
그 종료 경로는 아래 표에 explicit transition으로 있어야 한다.

`OpenContext.AuthorityId`는 발급 provenance이지 `OwnerPipe` identity가 아니다. 따라서 quorum-confirmed exact
context가 current `ClusterEpoch`에 있고 아직 consume/cancel/retire되지 않았다면, same-epoch authority 교체만으로
priority 1의 stale input이 되지 않는다. Owner는 current AuthorityId equality 대신 context authenticity,
single-use, exact binding과 current local `O`를 검사한다.

## 상태 소유권과 수명

| Machine | Identity / owner | Initial | Absorbing | 영속성 |
| --- | --- | --- | --- | --- |
| `Authority` | `(ClusterEpoch, AuthorityId)` | `Absent` | `Fenced` | Memory. Raft safety state에서 다시 판단한다. |
| `Quorum` | Current epoch의 control plane | `Unavailable` | 없음 | Derived memory observation |
| `GatewayRegistration` | Stable `GatewayId` | `AbsentG` | 없음 | Latest generation/ref 또는 tombstone만 Raft snapshot에 유지 |
| `ControlSession` | `ControlSessionRef` | `AbsentC` | `EndedC` | Memory only |
| `Presence` | `CurrentAuthority` | `NoAuthority` | 없음 | Memory only; failover 때 재구축 |
| `AuthSnapshot` | Gateway process | `StartupBlocked` | 없음 | External config가 source of truth |
| `ClientSession` | `ClientSessionId` | `Authenticating` | `TerminalS` | Memory only |
| `LocalBinding` | `ListenerBindingRef` | `AbsentB` | `RetiredB` | Live state는 memory, `BindingSlot`만 Raft |
| `FlowControl` | `(PipeId, HopId)` | `Flowing` | `TerminalF` | Memory only |
| `OwnerPipe` | `(AttemptId, PipeId?)` | `OpeningO` | `TerminalO` | Memory only |
| `IngressPipe` | `(AttemptId, PipeId?)` | `OpeningI` | `TerminalI` | Memory only |
| `ListenerPipe` | `(AttemptId, PipeId?)` | `OfferedL` | `TerminalL` | SDK memory only |
| `CallerPipe` | `(AttemptId, PipeId?)` | `OpeningC` | `TerminalC` | SDK memory only |
| `OpenAllAccumulator` | SDK-local call + target | `Pending` | Target outcome별 terminal | SDK memory only |

`PipeId?`는 Owner가 Open을 선형화하기 전에는 아직 없다는 뜻이다. Process restart, reconnect, rebind와 retry는
이전 machine을 되살리지 않고 새 identity의 machine을 만든다. `Absorbing`은 state가 부활하지 않는다는
뜻이며, 명시된 conditional cleanup처럼 state를 바꾸지 않는 idempotent effect는 수행할 수 있다.

## State와 event universe

아래 집합이 `StateM × EventM`을 생성하는 canonical alphabet이다. Slash로 묶은 이름도 각각 독립 event다.
Protocol은 wire message를 이 event에 매핑할 수 있지만 집합 밖의 의미를 암묵적으로 추가할 수 없다.

| Machine | `StateM` | `EventM` |
| --- | --- | --- |
| Authority | `Absent`, `Current`, `Fenced` | `AuthorityConfirmed`, `CallerVerificationAborted`, `StepDown`, `QuorumLost`, `EpochEnded` |
| Quorum | `Unavailable`, `Available` | `QuorumConfirmed`, `QuorumLost`, `EpochEnded` |
| GatewayRegistration | `AbsentG`, `LiveG`, `TombstonedG` | `Register`, `ReplaceInstance`, `Remove`, `EpochEnded` |
| ControlSession | `AbsentC`, `Syncing`, `Revalidated`, `EndedC` | `SyncStarted`, `SnapshotValidated`, `Close`, `Timeout`, `AuthorityEnded`, `GatewayEnded` |
| Presence | `NoAuthority`, `Rebuilding`, `Complete` | `AuthorityConfirmed`, `AllGatewaysClassified`, `CommittedSetChanged`, `ControlGenerationChanged`, `AuthorityEnded` |
| AuthSnapshot | `StartupBlocked`, `ActiveAuth`, `Validating` | `StartupValid`, `StartupInvalid`, `ReloadStarted`, `ReloadValid`, `ReloadInvalid` |
| ClientSession | `Authenticating`, `Active`, `RetiringS`, `TerminalS` | `AuthSucceeded`, `AuthFailed`, `AuthenticationTimedOut`, `Close`, `CredentialRevoked`, `TransportEnded`, `GatewayEnded`, `RetirementDone`, `EpochEnded` |
| LocalBinding | `AbsentB`, `RegisteringB`, `LiveB`, `RetiringB`, `RetiredB` | `BindStarted`, `InstallApplied`, `InstallRejected`, `Cancel`, `Unbind`, `SessionEnded`, `GatewayEnded`, `RetirementDone`, `EpochEnded` |
| FlowControl | `Flowing`, `Backpressured`, `Exhausted`, `TerminalRequested`, `TerminalF` | `QueueHigh`, `DownstreamDrained`, `BoundExceeded`, `RequestTerminal`, `PipeTerminal`, `LocalTerminalConfirmed` |
| OwnerPipe | `OpeningO`, `AdmittedO`, `AcceptedO`, `TerminalO` | `ReservationSucceeded`, `ReservationRejected`, `ListenerAccepted`, `ListenerRejected`, `AttemptDeadline`, `Cancel`, `SessionOrHopEnded`, `TerminalReceived`, `EpochEnded` |
| IngressPipe | `OpeningI`, `OpenI`, `TerminalI` | `OwnerAccepted`, `OwnerRejected`, `Cancel`, `Deadline`, `CallerSessionEnded`, `OwnerHopEnded`, `TerminalReceived`, `EpochEnded` |
| ListenerPipe | `OfferedL`, `ProvisionalL`, `OpenL`, `TerminalL` | `AcceptProposed`, `Reject`, `OwnerConfirmed`, `AttemptDeadline`, `Cancel`, `SessionOrHopEnded`, `TerminalReceived`, `EpochEnded` |
| CallerPipe | `OpeningC`, `OpenC`, `TerminalC` | `AckObserved`, `Rejected`, `Cancel`, `Deadline`, `TransportEnded`, `TerminalReceived`, `EpochEnded` |
| OpenAllAccumulator | Target별 `Pending`, `Opened`, `Failed`, `Cancelled`, `Unknown` | `TargetOpened`, `TargetFailed`, `TargetUnknown`, `TargetCancel`, `AggregateCancel`, `CallerEnded` |

`ListenerPipe`는 exact offer가 검증된 뒤 `OfferedL`로 생성된다. Stale/invalid offer는 machine을 만들지 않고
stable rejection이다. 나머지 per-operation machine도 표의 initial state로 생성되기 전에는 state entry가 없다.

## Control state transitions

### Authority와 Quorum

| Machine | From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- | --- |
| Authority | `Absent` | `AuthorityConfirmed` | Same epoch, quorum-confirmed acquisition | `Current` | 새 `AuthorityId`; presence rebuild 시작 |
| Authority | `Absent/Current/Fenced` | `CallerVerificationAborted` | Caller-owned HTTP/RPC context cancel 또는 deadline; definitive role/epoch loss 아님 | Same state | 해당 호출만 unavailable; authority와 다른 control session은 불변 |
| Authority | `Current` | `StepDown` | Current identity | `Fenced` | 새 control operation과 context 발급 중단 |
| Authority | `Current` | `QuorumLost` | Current identity | `Fenced` | 새 context 발급 중단; same-epoch issued attempt만 유지 |
| Authority | `Current` | `EpochEnded` | Current epoch | `Fenced` | Old context와 runtime을 terminal/fenced 처리 |
| Quorum | `Unavailable` | `QuorumConfirmed` | Same epoch | `Available` | 새 합의와 admission 판단 가능 |
| Quorum | `Available` | `QuorumLost` | Current observation | `Unavailable` | 새 binding/resolve/context 중단 |
| Quorum | `Available` | `EpochEnded` | Current epoch | `Unavailable` | Old-epoch operation 중단 |
| Quorum | `Unavailable` | `EpochEnded` | Current epoch | `Unavailable` | 이미 unavailable; epoch observation 종료 |

### GatewayRegistration

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `AbsentG` | `Register` | New `GatewayId`, capacity 있음, expected `(generation=0, tombstone)` | `LiveG` | Generation 1과 exact `GatewayInstanceId` commit |
| `LiveG` | `ReplaceInstance` | Exact current generation/ref CAS | `LiveG` | Generation 증가; 새 instance commit; old session/snapshot fence |
| `LiveG` | `Remove` | Exact current generation/ref CAS | `TombstonedG` | Generation 증가; current committed set에서 제외 |
| `TombstonedG` | `Register` | Exact tombstone generation CAS | `LiveG` | Generation 증가; 새 instance commit |
| `AbsentG/LiveG/TombstonedG` | `EpochEnded` | Current epoch | 새 epoch의 `AbsentG` | Old slot과 message는 old epoch에 남아 current state를 변경하지 못함 |

같은 target generation/ref의 `Register`/`ReplaceInstance`/`Remove` replay는 `AlreadyApplied` no-op이다. 다른
generation/ref mismatch와 capacity를 넘는 새 `GatewayId`는 stable rejection이며 current slot을 바꾸지 않는다.

### ControlSession과 Presence

| Machine | From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- | --- |
| ControlSession | `AbsentC` | `SyncStarted` | Current authority + exact live `GatewaySlot` generation/ref | `Syncing` | 새 `ControlSessionId`; server-side generation 고정, full snapshot 요구 |
| ControlSession | `Syncing` | `SnapshotValidated` | Exact current ref + complete valid snapshot | `Revalidated` | `V` 후보와 presence classification 설치 |
| ControlSession | `Syncing/Revalidated` | `Close/Timeout` | Exact current ref | `EndedC` | 해당 snapshot ineligible |
| ControlSession | `Syncing/Revalidated` | `AuthorityEnded/GatewayEnded` | 해당 identity 종료 | `EndedC` | Session과 snapshot 폐기 |
| Presence | `NoAuthority` | `AuthorityConfirmed` | New current authority | `Rebuilding` | 빈 view에서 시작; `complete=false` |
| Presence | `Rebuilding` | `AllGatewaysClassified` | 모든 live committed `GatewaySlot`이 revalidated/timeout 분류됨 | `Complete` | Current snapshot publish 가능 |
| Presence | `Complete` | `CommittedSetChanged/ControlGenerationChanged` | Current authority | `Rebuilding` | 이전 complete/config-converged publication 무효 |
| Presence | `Rebuilding` | `CommittedSetChanged/ControlGenerationChanged` | Current authority | `Rebuilding` | 영향받은 classification 폐기 |
| Presence | `Rebuilding/Complete` | `AuthorityEnded` | Current authority 종료 | `NoAuthority` | Unavailable 또는 explicitly incomplete |
| Presence | `NoAuthority` | `AuthorityEnded` | 종료 replay | `NoAuthority` | Idempotent no-op |

## Config와 client runtime transitions

### AuthSnapshot

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `StartupBlocked` | `StartupValid` | 전체 config parse·validate 성공 | `ActiveAuth` | Immutable snapshot 활성화; service open 가능 |
| `StartupBlocked` | `StartupInvalid` | Validation 실패 | `StartupBlocked` | Fail closed; partial snapshot 없음 |
| `ActiveAuth` | `ReloadStarted` | Process-local SIGHUP | `Validating` | 기존 snapshot이 계속 auth source |
| `Validating` | `ReloadValid` | 전체 candidate 유효 | `ActiveAuth` | Atomic swap; 제거 대상 local retirement 시작 |
| `Validating` | `ReloadInvalid` | Candidate 무효 | `ActiveAuth` | 기존 snapshot과 runtime 유지 |

### ClientSession

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `Authenticating` | `AuthSucceeded` | `Relay.Connect` 첫 메시지의 exact `(ClientId, ApiKeyId, presented key)`가 current snapshot에서 유효 | `Active` | 새 `ClientSessionId`와 implicit `ClientId`를 stream lifetime에 고정 |
| `Authenticating` | `AuthFailed/AuthenticationTimedOut/Close/CredentialRevoked/TransportEnded/GatewayEnded/EpochEnded` | 인증 실패, first-message deadline 또는 해당 attempt/identity 종료 | `TerminalS` | Session 생성 안 함 |
| `Active` | `Close/CredentialRevoked` | Exact session | `RetiringS` | 새 attempt/bind 금지; local child retirement 시작 |
| `Active` | `TransportEnded/GatewayEnded/EpochEnded` | 해당 identity 종료 | `TerminalS` | Local child terminal 전파 |
| `RetiringS` | `RetirementDone/TransportEnded/GatewayEnded/EpochEnded` | Local child 정리 완료 또는 identity 종료 | `TerminalS` | Identity 재사용 금지 |

`AuthSucceeded`는 local session admission ordering 안에서 exact credential을 current immutable snapshot으로
마지막 재검증하는 순간 선형화된다. 이 revalidation 뒤 removal swap이 오면 session 등록보다 retirement가 먼저
지나갈 수 없으므로 같은 reload가 그 session을 retire한다. Swap이 먼저면 `AuthSucceeded` guard는 false다.

### LocalBinding

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `AbsentB` | `BindStarted` | Active listener session + capacity | `RegisteringB` | 새 `ListenerBindingId`; conditional install 제출 |
| `RegisteringB` | `InstallApplied` | Exact generation/value CAS 성공 | `LiveB` | New-Pipe `O` 후보 가능 |
| `RegisteringB` | `InstallRejected/Cancel/Unbind/SessionEnded/GatewayEnded/EpochEnded` | Exact binding attempt | `RetiredB` | 즉시 `O=false`; 뒤늦은 commit은 아래 cleanup 전이로 처리 |
| `LiveB` | `Unbind/SessionEnded/GatewayEnded` | Exact live ref | `RetiringB` | 즉시 `O=false`; conditional tombstone 제출 |
| `LiveB` | `EpochEnded` | Current epoch end | `RetiredB` | Old binding 즉시 ineligible |
| `RetiringB` | `RetirementDone/SessionEnded/GatewayEnded/EpochEnded` | Local cleanup 완료 또는 identity 종료 | `RetiredB` | Late cleanup은 generation/ref mismatch면 no-op |
| `RetiredB` | `InstallApplied` | Retire 전에 제출한 exact install이 뒤늦게 commit됨 | `RetiredB` | Live로 부활하지 않고 exact ref의 conditional tombstone 제출 |

## Flow and Pipe transitions

### FlowControl

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `Flowing` | `QueueHigh` | Bounded queue high watermark | `Backpressured` | Upstream payload read 중지/감속 |
| `Flowing` | `PipeTerminal` | Local terminal 관찰 | `TerminalF` | Payload 전달 중단 |
| `Backpressured` | `DownstreamDrained` | Low watermark 이하 | `Flowing` | Payload flow 재개 |
| `Backpressured` | `BoundExceeded` | Bound 안에서 진행 불가 | `Exhausted` | Silent drop 없이 terminal 요청 |
| `Backpressured/Exhausted` | `PipeTerminal` | Local terminal 관찰 | `TerminalF` | Payload 전달 중단 |
| `Exhausted` | `RequestTerminal` | 아직 local Pipe가 terminal 아님 | `TerminalRequested` | Terminal/control signal이 payload queue 우회 |
| `TerminalRequested` | `PipeTerminal/LocalTerminalConfirmed` | Local Pipe terminal | `TerminalF` | Buffer 폐기; replay 없음 |

### OwnerPipe

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `OpeningO` | `ReservationSucceeded` | `O=true`, current epoch의 exact single-use context; `AuthorityId`는 verified issuance provenance | `AdmittedO` | Attempt를 binding retirement와 원자적으로 순서화; Listener offer |
| `OpeningO` | `ReservationRejected/AttemptDeadline/Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalO` | Listener offer 없음 |
| `AdmittedO` | `ListenerAccepted` | Cancel/terminal보다 먼저 관찰 | `AcceptedO` | **Open linearization point**; `PipeId` 생성 |
| `AdmittedO` | `ListenerRejected/AttemptDeadline/Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalO` | Late accept는 no-op |
| `AcceptedO` | `AttemptDeadline` | Open LP 뒤의 late timer | `AcceptedO` | No-op; attempt timer 폐기 |
| `AcceptedO` | `Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 첫 local terminal | `TerminalO` | Best-effort·idempotent terminal 전파 |

### IngressPipe

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `OpeningI` | `OwnerAccepted` | Exact attempt + `PipeId` | `OpenI` | Ingress local relay segment 설치; caller ACK 가능 |
| `OpeningI` | `OwnerRejected/Cancel/Deadline/CallerSessionEnded/OwnerHopEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalI` | Caller에 stable failure/unknown outcome |
| `OpenI` | `Deadline` | Open apply 뒤의 late timer | `OpenI` | No-op; attempt timer 폐기 |
| `OpenI` | `Cancel/CallerSessionEnded/OwnerHopEnded/TerminalReceived/EpochEnded` | 첫 local terminal | `TerminalI` | Best-effort terminal 전파 |

### ListenerPipe

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `OfferedL` | `AcceptProposed` | Listener application accept | `ProvisionalL` | 아직 Pipe handle 노출 안 함 |
| `OfferedL` | `Reject/AttemptDeadline/Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalL` | Owner에 reject/terminal 전파 |
| `ProvisionalL` | `OwnerConfirmed` | Exact `PipeId` | `OpenL` | 이 시점부터 SDK가 Pipe handle 노출 |
| `ProvisionalL` | `AttemptDeadline/Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalL` | Handle 노출 없음 |
| `OpenL` | `AttemptDeadline` | Owner confirm 뒤의 late timer | `OpenL` | No-op; attempt timer 폐기 |
| `OpenL` | `Cancel/SessionOrHopEnded/TerminalReceived/EpochEnded` | 첫 local terminal | `TerminalL` | Best-effort terminal 전파 |

### CallerPipe

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `OpeningC` | `AckObserved` | Exact `PipeId` | `OpenC` | Caller-visible `Opened` |
| `OpeningC` | `Rejected/Cancel/Deadline/TransportEnded/TerminalReceived/EpochEnded` | 해당 attempt | `TerminalC` | `Failed`, `Cancelled` 또는 `Unknown` |
| `OpenC` | `Deadline` | ACK 뒤의 late timer | `OpenC` | No-op; attempt timer 폐기 |
| `OpenC` | `Cancel/TransportEnded/TerminalReceived/EpochEnded` | 첫 local terminal | `TerminalC` | Best-effort terminal 전파 |

### OpenAllAccumulator

| From | Event | To | Effect |
| --- | --- | --- | --- |
| Target `Pending` | `TargetOpened` | `Opened` | Handle을 caller가 개별 소유 |
| Target `Pending` | `TargetFailed` | `Failed` | 다른 target을 rollback하지 않음 |
| Target `Pending` | `TargetCancel/AggregateCancel/CallerEnded` | `Cancelled` | Caller-local terminal + best-effort remote cancel |
| Target `Pending` | `TargetUnknown` | `Unknown` | Same-Pipe attach/resume 없음 |
| `Opened/Failed/Cancelled/Unknown` | 어떤 후속 outcome event | Same state | Outcome은 monotonic; no-op |

`Cancelled`는 remote never-accepted의 증명이 아니다. Caller session end는 모든 child를 caller-local terminal로
만들지만 permanent partition 아래의 remote 즉시 종료를 보장하지 않는다.

## Derived state table

| Derived state | Exact predicate | False일 때 |
| --- | --- | --- |
| `AdmitOpen` | `A ∧ L ∧ Q ∧ C ∧ V ∧ O` | Listener offer와 새 Pipe 없음 |
| `AcceptedUnconfirmed` | Live `OwnerPipe=AcceptedO ∧ CallerPipe≠OpenC` | Owner crash 뒤에는 exact outcome 자체가 R3 |
| `presence.complete` | Current authority가 모든 committed Gateway를 revalidated/timeout 분류 | Unavailable 또는 explicitly incomplete |
| `config_converged` | 모든 current committed Gateway가 live/revalidated + 같은 revision + local retirement 완료 | `false`; revocation proof로 사용 불가 |
| `RevocationSafe` | SPEC 003의 convergence + operator revision + 모든 candidate retirement/termination/fence | `false`; candidate completeness 불명도 false |
| `ServiceRecoverable` | SPEC 003의 config/runtime/connectivity + same/fresh epoch predicate | `false`; fail closed |

`AdmitOpen`의 64개 Boolean vector 중 `111111`만 Owner reservation으로 진행한다. 다른 63개는 모두
`Not admitted`이며 기존 Pipe에는 영향을 주지 않는다.

## 구현과 protocol의 의무

- Wire message/status는 이 문서의 semantic event에 매핑해야 하며 새 전이 의미를 만들지 않는다.
- Control stream은 `Hello → SessionOpened → FullSnapshot → BindingMutation*` 순서를 지키며 mutation은 stream별로 직렬화한다.
- State mutation은 identity, generation과 expected current value를 함께 검증한다.
- 모든 table과 buffer는 bounded하며 capacity 부족은 새 state 생성을 fail closed한다.
- 구현 최적화가 lease를 사용하면 필요한 clock bound를 별도 protocol 문서에 명시한다.
- 필수 검증 목록은 [TEST 001](../test/001-core-correctness-test-plan.md)을 따른다.

## 관련 문서

- [SPEC 001: RelayGate System Model](001-system-model.md)
- [SPEC 002: Client Configuration and Presence](002-client-configuration-and-presence.md)
- [SPEC 003: Failure and Recovery Model](003-failure-and-recovery-model.md)
- [TEST 001: Core Correctness Test Plan](../test/001-core-correctness-test-plan.md)
