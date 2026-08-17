# SPEC 004: State Transition Model

> **Status:** Draft
>
> RelayGate v0의 current-state-only runtime state, event와 total transition을 정의한다.

이 문서는 관찰 가능한 state/event 의미의 canonical source다. 구현은 같은 identity, linearization, outcome과
failure oracle을 보존하면 internal type/goroutine을 다르게 구성할 수 있다.

## 닫힌 상태 모델

```text
SystemState = Authority × Quorum × ControlSession × DirectoryEntry × Presence
            × AuthSnapshot × ClientSession × LocalBinding × FlowControl
            × OwnerPipe × IngressPipe × ListenerPipe × CallerPipe
            × ForwardedAttemptFence × RemoteHop
```

```text
δM : StateM × EventM → StateM × Effect*
∀s ∈ StateM, ∀e ∈ EventM, δM(s,e) is defined
```

표에 없는 조합은 다음 우선순위로 정확히 하나의 결과를 낸다.

| 순서 | 조건 | 결과 |
| --- | --- | --- |
| 1 | Old epoch 또는 exact authority/session/instance/binding identity가 stale | State 불변 + stable rejection. Cleanup/terminal replay만 no-op |
| 2 | 아래 explicit `From + Event + Guard`와 일치 | `To + Effect` 적용 |
| 3 | Exact duplicate 또는 ended/terminal identity의 terminal replay | State 불변 + idempotent no-op |
| 4 | Current identity지만 state/guard가 허용하지 않음 | State 불변 + stable rejection |

각 input은 `Applied`, `NoOp` 또는 `Rejected` 중 하나다. Partial update, implicit success와 ended state 부활은
없다. Identity/session/hop 종료를 관찰한 non-terminal machine은 아래 explicit transition으로 끝나야 한다.

## 상태 소유권

| Machine | Identity / owner | Initial | Absorbing | Persistence |
| --- | --- | --- | --- | --- |
| `Authority` | `(ClusterEpoch, AuthorityId)` | `Absent` | `Fenced` | Memory; Raft safety에서 새 authority 판단 |
| `Quorum` | Current epoch control plane | `Unavailable` | 없음 | Derived observation |
| `ControlSession` | Exact `ControlSessionRef` | `AbsentC` | `EndedC` | Authority memory |
| `DirectoryEntry` | `(BindingKey, ControlSessionRef, ListenerBindingRef)` | `AbsentD` | Session-scoped | Authority memory; true delete |
| `Presence` | Current authority | `NoAuthority` | 없음 | Current observed counts only |
| `AuthSnapshot` | Gateway process | `StartupBlocked` | 없음 | External config source |
| `ClientSession` | `ClientSessionId` | `Authenticating` | `TerminalS` | Gateway memory |
| `LocalBinding` | `ListenerBindingRef` | `AbsentB` | `RetiredB` | Gateway memory |
| `FlowControl` | `(PipeId, HopId)` | `Flowing` | `TerminalF` | Memory |
| `OwnerPipe` | `(AttemptId, OwnerEvaluationId, PipeId?)` | `OpeningO` | `TerminalO` | Memory |
| `IngressPipe` | `(AttemptId, IngressEvaluationId, PipeId?)` | `OpeningI` | `TerminalI` | Memory |
| `ListenerPipe` | `(AttemptId, PipeId?)` | `OfferedL` | `TerminalL` | SDK memory |
| `CallerPipe` | `(AttemptId, PipeId?)` | `OpeningC` | `TerminalC` | SDK memory |
| `ForwardedAttemptFence` | `(OwnerGatewayInstanceId, AttemptId)` | `AbsentR` | 없음 | Memory; expiry까지 bounded retention |
| `RemoteHop` | `(InternalStreamId, AttemptId, ingress instance, owner instance, PipeId?)` | `DialingH` | `TerminalH` | Dedicated bidi stream memory |

Raft는 위 domain machine을 저장하지 않는다. Raft store에는 term/vote/log/membership/snapshot과 constant-size
`ClusterEpoch` marker만 있다. `GatewayId`, binding, route, tombstone, presence와 payload가 RelayGate
application command/FSM snapshot에 나타나면 contract violation이다. Raft membership의 NodeId/address는
합의 safety state다.

## State/event universe

| Machine | States | Events |
| --- | --- | --- |
| Authority | `Absent`, `Current`, `Fenced` | `AuthorityConfirmed`, `CallerVerificationAborted`, `StepDown`, `QuorumLost`, `EpochEnded` |
| Quorum | `Unavailable`, `Available` | `QuorumConfirmed`, `QuorumLost`, `EpochEnded` |
| ControlSession | `AbsentC`, `Syncing`, `Revalidated`, `EndedC` | `SyncStarted`, `SnapshotValidated`, `Close`, `Timeout`, `AuthorityEnded`, `GatewayEnded` |
| DirectoryEntry | `AbsentD`, `DeclaredD` | `SnapshotDeclared`, `Declare`, `Withdraw`, `SessionEnded`, `AuthorityEnded` |
| Presence | `NoAuthority`, `Current` | `AuthorityConfirmed`, `ObservationChanged`, `AuthorityEnded` |
| AuthSnapshot | `StartupBlocked`, `ActiveAuth`, `Validating` | `StartupValid`, `StartupInvalid`, `ReloadStarted`, `ReloadValid`, `ReloadInvalid` |
| ClientSession | `Authenticating`, `Active`, `RetiringS`, `TerminalS` | `AuthSucceeded`, `AuthFailed`, `AuthenticationTimedOut`, `Close`, `CredentialRevoked`, `TransportEnded`, `GatewayEnded`, `RetirementDone`, `EpochEnded` |
| LocalBinding | `AbsentB`, `RegisteringB`, `LiveB`, `RetiringB`, `RetiredB` | `BindStarted`, `DeclarationApplied`, `DeclarationRejected`, `ControlSessionEnded`, `Unbind`, `SessionEnded`, `CredentialRevoked`, `GatewayEnded`, `RetirementDone`, `EpochEnded` |
| FlowControl | `Flowing`, `Backpressured`, `Exhausted`, `TerminalRequested`, `TerminalF` | `PayloadIngress`, `PayloadWriteCompleted`, `PayloadWriteFailed`, `QueueHigh`, `DownstreamDrained`, `BoundExceeded`, `RequestTerminal`, `PipeTerminal`, `LocalTerminalConfirmed` |
| OwnerPipe | `OpeningO`, `AdmittedO`, `AcceptedO`, `TerminalO` | `ReservationSucceeded`, `ReservationRejected`, `ListenerAccepted`, `ListenerRejected`, `AttemptDeadline`, `Cancel`, `SessionOrHopEnded`, `TerminalReceived`, `EpochEnded` |
| IngressPipe | `OpeningI`, `OpenI`, `TerminalI` | `OwnerAccepted`, `OwnerRejected`, `Cancel`, `Deadline`, `CallerSessionEnded`, `OwnerHopEnded`, `TerminalReceived`, `EpochEnded` |
| ListenerPipe | `OfferedL`, `ProvisionalL`, `OpenL`, `TerminalL` | `AcceptProposed`, `Reject`, `OwnerEstablished`, `ConfirmationAcknowledged`, `AttemptDeadline`, `Cancel`, `SessionOrHopEnded`, `TerminalReceived`, `EpochEnded` |
| CallerPipe | `OpeningC`, `OpenC`, `TerminalC` | `AckObserved`, `Rejected`, `Cancel`, `Deadline`, `TransportEnded`, `TerminalReceived`, `EpochEnded` |
| ForwardedAttemptFence | `AbsentR`, `ReservedR` | `ReservationSucceeded`, `ReservationRejected`, `DuplicateReceived`, `CacheFull`, `Expired`, `GatewayEnded`, `EpochEnded` |
| RemoteHop | `DialingH`, `OpeningH`, `AdmittedH`, `AcceptedH`, `OpenH`, `TerminalH` | `StreamOpened`, `OwnerAdmitted`, `OwnerAccepted`, `OwnerRejected`, `IngressActivated`, `PayloadIngress`, `Deadline`, `HopEnded`, `TerminalReceived`, `EpochEnded` |

## Control transitions

### Authority and quorum

| Machine | From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- | --- |
| Authority | `Absent` | `AuthorityConfirmed` | Same epoch, quorum-confirmed acquisition | `Current` | New `AuthorityId`; empty sessions/directory; presence current counts start at zero |
| Authority | any | `CallerVerificationAborted` | Caller-owned cancel/deadline, no definitive role/epoch loss | Same | This call only unavailable; no global fence |
| Authority | `Current` | `StepDown/QuorumLost` | Exact current identity | `Fenced` | Stop new control/context; invalidate pre-O contexts; end all sessions; clear directory |
| Authority | `Current` | `EpochEnded` | Current epoch | `Fenced` | Clear directory and terminal/fence old runtime/context |
| Quorum | `Unavailable` | `QuorumConfirmed` | Same epoch | `Available` | New control/admission decision allowed |
| Quorum | `Available` | `QuorumLost/EpochEnded` | Current observation | `Unavailable` | Stop new bind/resolve/context |
| Quorum | `Unavailable` | `EpochEnded` | Current epoch | `Unavailable` | No-op after epoch observation ends |

Authority acquisition and all-session/directory initialization are one manager-owned effect. Old directory data is never
copied into the new authority.

### ControlSession

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `AbsentC` | `SyncStarted` | Current authority + exact valid GatewayId/InstanceId + valid owner address | `Syncing` | New `ControlSessionId`; empty session route set; require full snapshot |
| `Syncing` | `SnapshotValidated` | Exact current session + complete valid snapshot + no conflict + capacity | `Revalidated` | Atomically install all `SnapshotDeclared` entries and enable V |
| `Syncing/Revalidated` | `Close/Timeout` | Exact current session | `EndedC` | Bulk delete session entries and owner address |
| `Syncing/Revalidated` | `AuthorityEnded/GatewayEnded` | Exact identity ends | `EndedC` | Bulk delete session entries/address; late messages stale |

Invalid/oversized/conflicting snapshot changes neither session revalidation nor directory. Protocol termination then maps to
`Close`, so a partial snapshot cannot remain installed.

### DirectoryEntry

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `AbsentD` | `SnapshotDeclared` | Exact Syncing session; whole snapshot validation succeeds | `DeclaredD` | Install as part of atomic snapshot+revalidation effect |
| `AbsentD` | `Declare` | Exact current Revalidated session; valid binding; BindingKey absent; capacity | `DeclaredD` | Insert exact entry and add it to session-owned set |
| `DeclaredD` | `Declare` | Exact same current session + exact same `LiveBinding` | `DeclaredD` | `AlreadyApplied`; no duplicate/cardinality change |
| `AbsentD/DeclaredD` | `Declare` | Same `BindingKey` has different ref/session | Same | Stable conflict; existing entry unchanged |
| `DeclaredD` | `Withdraw` | Exact owner session + exact `LiveBinding` | `AbsentD` | True delete from directory and session set |
| `DeclaredD` | `SessionEnded` | Exact owner session | `AbsentD` | Bulk true delete; no tombstone/history |
| `DeclaredD` | `AuthorityEnded` | Current authority ends | `AbsentD` | Clear all entries |
| `AbsentD` | `Withdraw/SessionEnded/AuthorityEnded` | Exact cleanup replay | `AbsentD` | Idempotent no-op |

Old-session Withdraw/Declare cannot affect a new session entry because priority 1 rejects stale `AuthorityId` or
`ControlSessionId`. No generation, expected value CAS, tombstone or mutation replay across sessions exists.

### Presence

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `NoAuthority` | `AuthorityConfirmed` | Current confirmed authority | `Current` | Publish current `sessions=0, revalidated=0, bindings=0` |
| `Current` | `ObservationChanged` | Session/snapshot/declare/withdraw/close on current authority | `Current` | Recount current memory only |
| `Current` | `AuthorityEnded` | Exact authority ends | `NoAuthority` | Old observation unavailable; do not cache as current |
| `NoAuthority` | `AuthorityEnded` | Replay | `NoAuthority` | No-op |

There is no `Rebuilding/Complete`, committed/classified roster or expected replica gate. `Current(0)` is a current
count, not completeness proof.

## Config and local runtime

### AuthSnapshot and ClientSession

| Machine | From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- | --- |
| AuthSnapshot | `StartupBlocked` | `StartupValid` | Whole config valid | `ActiveAuth` | Activate immutable snapshot; service may open |
| AuthSnapshot | `StartupBlocked` | `StartupInvalid` | Validation fails | `StartupBlocked` | Fail closed; no partial snapshot |
| AuthSnapshot | `ActiveAuth` | `ReloadStarted` | Process-local SIGHUP | `Validating` | Old snapshot remains auth source |
| AuthSnapshot | `Validating` | `ReloadValid` | Whole candidate valid | `ActiveAuth` | Atomic swap; retire removed local credentials |
| AuthSnapshot | `Validating` | `ReloadInvalid` | Candidate invalid | `ActiveAuth` | Keep old snapshot/runtime |
| ClientSession | `Authenticating` | `AuthSucceeded` | Exact credential passes final current-snapshot revalidation | `Active` | New session and implicit ClientId |
| ClientSession | `Authenticating` | auth failure/timeout/identity end | Exact attempt | `TerminalS` | No session |
| ClientSession | `Active` | `Close/CredentialRevoked` | Exact session | `RetiringS` | Stop new attempt/bind; retire children |
| ClientSession | `Active` | `TransportEnded/GatewayEnded/EpochEnded` | Exact identity | `TerminalS` | Local child terminal effects |
| ClientSession | `RetiringS` | `RetirementDone` or identity end | Exact session | `TerminalS` | Identity cannot be reused |

### LocalBinding

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `AbsentB` | `BindStarted` | Active listener session + control Syncing/Revalidated + current-session queue capacity | `RegisteringB` | New `ListenerBindingId`; snapshot 뒤 same-session exact Declare |
| `RegisteringB` | `DeclarationApplied` | Exact current-session Applied/AlreadyApplied | `LiveB` | Return ListenerBound; local O candidate |
| `RegisteringB` | `DeclarationRejected/ControlSessionEnded/Unbind/SessionEnded/CredentialRevoked/GatewayEnded/EpochEnded` | Exact bind attempt | `RetiredB` | Bind fails; immediate O=false; no cross-session replay |
| `LiveB` | `ControlSessionEnded` | Same Gateway process/listener still live | `LiveB` | D/V false until new session FullSnapshot redeclares this binding |
| `LiveB` | `Unbind/SessionEnded/CredentialRevoked/GatewayEnded/EpochEnded` | Exact binding | `RetiringB` | Immediate O=false; submit best-effort exact Withdraw |
| `RetiringB` | `RetirementDone` or identity end | Withdraw returned or local identity ended | `RetiredB` | Release live capacity; retain only bounded local terminal history |
| `RetiredB` | any late declaration/cleanup result | Same or stale identity | `RetiredB` | No-op/rejection; no resurrection; bounded history may evict entry |

Authority failover does not force a live Listener to application-level rebind. Gateway reconnect/full snapshot restores its
current route. Gateway process/session/credential end does retire the local binding.

## Admission and Pipe transitions

### Derived admission

| Derived state | Exact predicate | False |
| --- | --- | --- |
| `DirectoryCurrent` | Exact `BindingKey → LiveBinding + owner ControlSessionRef/address` exists in current authority memory | No route/fallback |
| `OwnerSessionCurrent` | Entry owner session is exact current `Revalidated` and contains the same binding | V=false |
| `IssueOpenContext` | `A ∧ L ∧ Q ∧ D ∧ V` | No context/reservation/offer/Pipe |
| `OwnerContextCurrent` | Context AuthorityId + OwnerControlSessionId equal exact current revalidated owner session | O=false; same-epoch stale context rejected |
| `AdmitOpen` | `IssueOpenContext ∧ O` | No AdmittedO/offer; failed O does not consume context |
| `AcceptLogicalPipe` | `AdmitOpen ∧ OwnerPipe=AcceptedO` | No PipeId/logical Pipe |
| `RemotePipeActive` | `OwnerPipe=AcceptedO ∧ IngressPipe=OpenI ∧ RemoteHop=OpenH` | No Listener→Caller release |
| `AcceptedUnconfirmed` | Live `OwnerPipe=AcceptedO ∧ CallerPipe≠OpenC` | Owner crash makes exact outcome R3 |

The 64 `(A,L,Q,D,V,O)` vectors have one success: `111111`. That success creates O reservation/fence and Listener
offer, not Pipe success. Listener accept/AcceptedO is the separate Open LP.

### OwnerPipe and replay fence

| Machine | From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- | --- |
| OwnerPipe | `OpeningO` | `ReservationSucceeded` | Exact current AuthorityId/OwnerControlSessionId, local auth/binding, strict expiry, capacity, fence absent | `AdmittedO` | Atomic O reservation + fence insert + hop admission; then Listener offer |
| OwnerPipe | `OpeningO` | rejection/deadline/cancel/identity end | Exact attempt | `TerminalO` | No offer/PipeId; failed guard does not consume context |
| OwnerPipe | `AdmittedO` | `ListenerAccepted` | Before cancel/terminal | `AcceptedO` | Open LP: record accept + mint PipeId |
| OwnerPipe | `AdmittedO` | reject/deadline/cancel/identity end | Exact attempt | `TerminalO` | Late accept no-op; fence stays to expiry |
| OwnerPipe | `AcceptedO` | late deadline | Open already LP | `AcceptedO` | No-op |
| OwnerPipe | `AcceptedO` | cancel/session/hop/terminal/epoch end | First local terminal | `TerminalO` | Best-effort terminal propagation |
| Fence | `AbsentR` | `ReservationSucceeded` | Same atomic O guard | `ReservedR` | Insert AttemptId with ExpiresAt |
| Fence | `AbsentR` | `ReservationRejected/CacheFull` | Guard false/capacity full | `AbsentR` | Fail closed; no consume/replay |
| Fence | `ReservedR` | `DuplicateReceived` | Same AttemptId, expiry mutation irrelevant | `ReservedR` | Fail closed; no prior response/PipeId replay |
| Fence | `ReservedR` | `Expired` | `now >= ExpiresAt` | `AbsentR` | GC allowed; context also expired so no ABA |
| Fence | `ReservedR` | `GatewayEnded/EpochEnded` | Owner identity ends | `AbsentR` | Volatile cache lost; old outcome not recovered |

```text
RemoteOCommit = atomic(OwnerPipe OpeningO→AdmittedO,
                       ForwardedAttemptFence AbsentR→ReservedR,
                       RemoteHop OpeningH→AdmittedH)
```

### Ingress, listener and caller

| Machine | From | Event | To | Effect |
| --- | --- | --- | --- | --- |
| IngressPipe | `OpeningI` | exact `OwnerAccepted(PipeId)` | `OpenI` | Install ingress segment; caller ACK possible |
| IngressPipe | `OpeningI` | reject/cancel/deadline/session/hop/terminal/epoch end | `TerminalI` | Stable failure only if LP-not-passed proven; otherwise Unknown |
| IngressPipe | `OpenI` | cancel/session/hop/terminal/epoch end | `TerminalI` | First local terminal; best-effort propagation |
| ListenerPipe | `OfferedL` | `AcceptProposed` | `ProvisionalL` | No application Pipe handle yet |
| ListenerPipe | `OfferedL` | reject/deadline/cancel/identity end | `TerminalL` | No handle |
| ListenerPipe | `ProvisionalL` | exact `OwnerEstablished(PipeId)` | `ProvisionalL` | Send exact ListenerConfirmed |
| ListenerPipe | `ProvisionalL` | exact `ConfirmationAcknowledged` | `OpenL` | Expose Listener Pipe handle |
| ListenerPipe | `ProvisionalL/OpenL` | cancel/identity/terminal/epoch end | `TerminalL` | First local terminal |
| CallerPipe | `OpeningC` | exact `AckObserved(PipeId)` | `OpenC` | Expose caller Opened |
| CallerPipe | `OpeningC` | reject/cancel/deadline/transport/terminal/epoch end | `TerminalC` | Failed/Cancelled/Unknown |
| CallerPipe | `OpenC` | cancel/transport/terminal/epoch end | `TerminalC` | First local terminal |

Late attempt deadline after `AcceptedO/OpenI/OpenL/OpenC` is a no-op. It does not close an opened Pipe.

### RemoteHop

| From | Event | Guard | To | Effect |
| --- | --- | --- | --- | --- |
| `DialingH` | `StreamOpened` | Dedicated one-attempt internal stream | `OpeningH` | Send one ForwardOpen |
| `DialingH` | deadline/hop/terminal/epoch end | Exact attempt | `TerminalH` | No redial/resume |
| `OpeningH` | `OwnerAdmitted` | Same atomic O effect | `AdmittedH` | Wait Listener decision |
| `OpeningH` | exact `OwnerRejected` | Non-consuming O failure | `TerminalH` | Stable failure; unexpired new evaluation may use new stream |
| `OpeningH/AdmittedH` | hop/terminal/epoch end | LP uncertainty preserved | `TerminalH` | Unknown unless LP-not-passed proven; no retry/resume |
| `AdmittedH` | exact `OwnerAccepted(PipeId)` | Exact attempt/PipeId | `AcceptedH` | Install ingress segment; payload still gated |
| `AdmittedH` | reject/deadline | Pre-Open outcome | `TerminalH` | Fence stays to expiry |
| `AcceptedH` | `IngressActivated` | Public PipeOpened write succeeded | `OpenH` | Release Listener→Caller payload |
| `AcceptedH` | pre-activation payload | Exact PipeId, bounded hold | `AcceptedH` | Hold; terminal/overflow wins |
| `OpenH` | valid exact payload | Exact PipeId | `OpenH` | Directional FIFO FlowControl |
| `AcceptedH/OpenH` | hop/terminal/epoch end | First local terminal | `TerminalH` | Both segments local terminal; no payload replay |

## FlowControl

| From | Event | To | Effect |
| --- | --- | --- | --- |
| `Flowing` | valid `PayloadIngress` with capacity | `Flowing` | Enqueue exact directional frame |
| `Flowing` | `QueueHigh` | `Backpressured` | Stop/slow upstream |
| `Flowing/Backpressured` | `PayloadWriteFailed` | `TerminalRequested` | No frame replay; request Pipe terminal |
| `Backpressured` | `DownstreamDrained/PayloadWriteCompleted` | `Flowing` | Resume upstream; no peer-app ACK meaning |
| `Backpressured` | `BoundExceeded` | `Exhausted` | No silent drop |
| `Exhausted` | `RequestTerminal` | `TerminalRequested` | Terminal/control bypass payload queue |
| non-terminal | `PipeTerminal` | `TerminalF` | Cancel buffer/write and stop delivery |
| `TerminalRequested` | `LocalTerminalConfirmed` | `TerminalF` | Buffer discard; no replay |
| `TerminalF` | payload/write event | `TerminalF` | Stable rejection/no-op |

## Public/internal wire mapping

| Input | Canonical effect |
| --- | --- |
| New exact `Open(request_id, endpoint, target_id)` | Create bounded Caller/Ingress/Owner attempt; request_id is in-flight correlation only |
| Duplicate live request_id | Reject new input; original Open alone emits outcome |
| `CancelOpen` | Signal live worker or idempotent no-op; ACK is not remote never-accept proof |
| Exact ListenerConfirmed | Apply confirmation then echo exact ACK; mismatch exposes no handle |
| `ClosePipe` exact participant | First local terminal. Bounded terminal history 안의 exact duplicate는 idempotent owned result; eviction 뒤 unknown/foreign과 동일 |
| Valid 1..60 KiB PipePayload | Exact participant and activated Pipe directional ingress |
| Invalid/foreign/terminal payload | Stable rejection; no route ownership leak/revival |
| Queue bound exceeded | Cancel queued frame, fail/join in-flight destination write, request terminal |
| Public stream ends | Cancel/join Open workers, retire exact session/Pipes; no resume |
| First internal `ForwardOpen` | One attempt on dedicated bidi stream; evaluate exact O |
| Second attempt/Pipe or mismatched PipeId on same hop | Stable rejection + local terminal |
| Internal EOF/loss | Both Gateway segments observe local terminal; Unknown if Open LP uncertainty |

## Implementation obligations

- Control order is `Hello → SessionOpened → FullSnapshot → BindingMutation*`.
- FullSnapshot comes from current local `LiveB`, not client mutation history.
- Session/authority end bulk-deletes directory entries. Unbind true-deletes the exact entry.
- No `GatewaySlot`, `BindingSlot`, generation, tombstone, durable route, replica completeness state or cross-session mutation replay.
- All state mutation validates exact authority/session/instance/binding identity.
- Client/session/Pipe caches, mutation queues, payload buffers and each control-session route set are bounded. Authority
  memory otherwise stays proportional to currently connected Gateway sessions; historical churn does not accumulate.
  Capacity rejects only new state and does not evict current state.
- SDK/runtime pending/live/terminal history is process-local and bounded. It supports duplicate terminal handling only;
  it is not a Raft route tombstone, response replay log or recovery source.
- Owner address exists only in exact current session memory.
- An issued OpenContext is not a lease. AuthorityId or owner ControlSessionId change before O makes it stale even in the
  same epoch. O-complete attempts alone continue their volatile local lifecycle.
- `ExpiresAt`, bounded successful AttemptId retention, no duplicate response replay and no Pipe/hop resume follow ADR 008.
- Internal hop remains trusted local/dev only until peer auth/mTLS and peer-to-context binding are implemented.
- [TEST 001](../test/001-core-correctness-test-plan.md) is the required validation map.

## 관련 문서

- [SPEC 001: RelayGate System Model](001-system-model.md)
- [SPEC 002: Client Configuration and Presence](002-client-configuration-and-presence.md)
- [SPEC 003: Failure and Recovery Model](003-failure-and-recovery-model.md)
- [TEST 001: Core Correctness Test Plan](../test/001-core-correctness-test-plan.md)
- [ADR 008: Cross-Gateway hop과 bounded replay](../adr/008-cross-gateway-hop-and-replay.md)
- [ADR 009: Current-state-only authority directory](../adr/009-ephemeral-current-state-authority-directory.md)
