# SPEC 004: Canonical State Transitions

## Closed Interpretation Rule

Every state/event input has exactly one result.

1. Old epoch or stale exact authority/session/instance/binding/participant identity is `Rejected`.
2. A row whose `From + Event + Guard` matches is `Applied`.
3. Exact duplicate cleanup/terminal or absorbing terminal re-entry is `NoOp`.
4. Any other current identity combination is `Rejected`.

This is semantic closure, not a claim that one test enumerates the full Cartesian product.

## Ownership

| Machine | Owner | Persistence | Clear/terminal owner |
| --- | --- | --- | --- |
| Raft membership | Controller Raft | durable store/log/snapshot | Raft membership change or explicit disaster reset |
| Current FSM `C` | Controller Raft FSM | durable log/snapshot | exact withdraw/remove/replacement |
| Authority/session mirror `V` | Current controller leader | leader-local memory | step-down/quorum loss/session end |
| Auth/ClientSession | Gateway access runtime | external config + process memory | credential/session/Gateway end |
| LocalBinding | Owning Gateway | process memory | unbind/session/control/Gateway end |
| Attempt/OwnerPipe | Owning Gateway | process memory | cancel/deadline/participant/hop/Gateway end |
| Ingress/Caller/ListenerPipe | Exact participant Gateway/SDK | process memory | first local terminal |
| RemoteHop | Ingress + owner segment | process memory | stream/hop/participant end |
| FlowControl | Each stream/segment | process memory | bound/write/terminal |
| SDK Supervisor | Go/Rust `ManagedClient` | process memory | close/permanent connect failure |

`C` contains only `ClusterEpoch`, capacity limits, current `GatewaySession`, and exact current route rows. `V` contains only current leader observations: control sessions, revalidation, owner relay address, and current binding mirror.

## Raft Membership And Controller Storage

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `EmptyStoreR` | external one-shot bootstrap with valid initial voters | `MemberR` | Persist `NodeId`, Raft state, membership, log/snapshots |
| `MemberR` | process restart with same store and same `NodeId` | `MemberR` | Reopen durable Raft state; no bootstrap |
| `MemberR` | same-epoch leader lost, quorum survives | `MemberR` | New leader may be elected |
| `MemberR` | quorum unavailable | `UnavailableR` | No confirmed authority/admission |
| `UnavailableR` | quorum restored with valid existing members | `MemberR` | Same Raft machine continues |
| `MemberR` | replacement has fresh `NodeId` and leader `AddVoter` succeeds | `MemberR` | New member catches up through Raft |
| `MemberR` | exact existing voter Add retry | `MemberR` | NoOp; return current membership |
| `MemberR` | leader `RemoveServer(lost NodeId)` succeeds | `MemberR` | Lost member removed from committed membership |
| `MemberR` | exact absent member Remove retry | `MemberR` | NoOp; return current membership |
| any old cohort | operator proves full old-path fence and chooses new epoch | `EmptyStoreR'` | Separate disaster-reset machine; old state not recovered |

An erased controller store with the old `NodeId` has no recovery transition. `bootstrap=true` is valid only for initial empty-cluster formation. Membership commands are accepted only by the verified current leader through its controller-local Unix socket; follower or quorum-loss calls are rejected before mutation.

## Current FSM `C`

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `UninitializedC` | `InitializeCluster` with valid epoch/capacity | `ReadyC` | Set immutable epoch/capacity |
| `ReadyC` | exact duplicate `InitializeCluster` | `ReadyC` | NoOp/AlreadyApplied |
| `ReadyC` | different epoch/capacity initialize | `ReadyC` | Reject |
| `AbsentGatewayC` | `RegisterGateway(gateway_id, instance_id)` within capacity | `CurrentGatewayC` | Insert current GatewaySession |
| `CurrentGatewayC(old instance)` | `RegisterGateway(same gateway_id, new instance_id)` | `CurrentGatewayC(new instance)` | Delete old owned routes, replace session |
| `CurrentGatewayC` | duplicate `RegisterGateway` | `CurrentGatewayC` | NoOp/AlreadyApplied |
| `CurrentGatewayC` | `ReplaceSnapshot` valid/non-conflicting/in-capacity | `CurrentGatewayC` | Atomic delete old owned routes, install exact new set |
| `CurrentGatewayC` | invalid/conflicting/over-cap snapshot | `CurrentGatewayC` | Reject; install nothing |
| `AbsentRouteC` | current revalidated `DeclareRoute` | `DeclaredRouteC` | Insert exact route |
| `DeclaredRouteC` | same route declaration | `DeclaredRouteC` | NoOp/AlreadyApplied |
| `DeclaredRouteC` | different owner/ref for same key | `DeclaredRouteC` | Reject conflict; preserve route |
| `DeclaredRouteC` | exact `WithdrawRoute` | `AbsentRouteC` | True delete |
| `CurrentGatewayC` | exact `RemoveGateway` | `AbsentGatewayC` | Delete GatewaySession and cascade owned routes |
| any `C` | Raft snapshot compact/restore | same logical `C` | Persist/restore current rows only |

No transition creates route tombstones, history, payload, Pipe state, control sessions, relay addresses, or credentials in `C`.

## Authority And Session Mirror `V`

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `NoAuthorityV` | controller confirms leader + quorum + initialized `C` | `AuthorityV` | New `AuthorityId`; `V` starts empty |
| `AuthorityV` | caller-owned verification cancel/deadline while leadership still current | `AuthorityV` | Call fails only |
| `AuthorityV` | step-down, term change, definitive verify failure, quorum loss | `NoAuthorityV` | Fence old authority; clear all sessions/addresses/revalidation |
| `AbsentSessionV` | exact current Hello after `RegisterGateway` commit | `SyncingSessionV` | New `ControlSessionId`; owner relay address recorded |
| `SyncingSessionV` | full snapshot accepted and `ReplaceSnapshot` committed | `RevalidatedSessionV` | Install leader-local binding mirror |
| `SyncingSessionV` | invalid snapshot, timeout, close, authority end | `AbsentSessionV` | Clear session/address |
| `RevalidatedSessionV` | close/timeout/replacement/authority end | `AbsentSessionV` | Clear session/address/mirror; `C` cleanup may follow by exact remove |

Presence is `NoAuthority` without `AuthorityV`, otherwise `Current` with observed memory counters. Observation does not change admission state.

## Authentication, Session, Binding

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `StartupBlocked` | whole config valid | `ActiveAuth` | Activate immutable snapshot |
| `ActiveAuth` | reload start | `Validating` | Old snapshot remains source |
| `Validating` | whole candidate valid | `ActiveAuth` | Atomic swap + removed-state retirement |
| `Validating` | invalid candidate | `ActiveAuth` | Keep old snapshot/runtime |
| `AuthenticatingS` | exact credential + final current-snapshot check | `ActiveS` | New session, implicit `ClientId` |
| `AuthenticatingS` | failure/deadline | `TerminalS` | No session |
| `ActiveS` | close/revocation | `RetiringS` | Stop new work, retire children |
| `RetiringS` | local retirement complete | `TerminalS` | Identity cannot revive |
| `AbsentB` | Bind start + capacity | `RegisteringB` | Allocate exact `ListenerBindingId` |
| `RegisteringB` | current directory ACK | `LiveB` | Eligible for O |
| `RegisteringB/LiveB` | unbind/revocation/session/control end | `RetiringB` | Immediately O=false, conditional withdraw |
| `RetiringB` | cleanup complete | `RetiredB` | Release capacity; late ACK cannot revive |

## Admission, Open, Replay Fence

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `OpeningO` | all `A·L·Q·D·V·O` guards | `AdmittedO` | Atomic reservation + `AttemptId` fence; Listener offer |
| `OpeningO` | any guard/deadline/cancel failure | `TerminalO` | No offer/PipeId; failed guard does not consume context |
| `AdmittedO` | Listener accept wins | `AcceptedO` | Open LP; mint `PipeId` |
| `AdmittedO` | reject/deadline/cancel/end wins | `TerminalO` | Late accept NoOp; fence remains to expiry |
| `AcceptedO` | late attempt deadline | `AcceptedO` | NoOp |
| `AcceptedO` | participant/hop/terminal end | `TerminalO` | Best-effort peer terminal |
| `AbsentAttemptR` | successful O | `ReservedAttemptR` | Insert `AttemptId` until expiry |
| `ReservedAttemptR` | duplicate | `ReservedAttemptR` | Reject; do not replay outcome/PipeId |
| `ReservedAttemptR` | expired | `AbsentAttemptR` | GC allowed; old context expired |

| Participant machine | Open transition | Terminal transition |
| --- | --- | --- |
| Ingress | exact owner accepted installs segment | reject/cancel/deadline/session/hop end; `Unknown` if LP uncertain |
| Listener | offer -> provisional -> confirm -> exact confirm ACK exposes handle | reject/cancel/session/hop end |
| Caller | exact `PipeOpened` ACK exposes handle | failure/cancel/transport/terminal |
| RemoteHop | dial -> forward -> admitted -> accepted -> activated/open | deadline, mismatch, EOF/hop/participant end |

One remote Pipe uses one hop stream. Mismatched identity, second attempt, redial, retry, or resume is rejected/terminal.

## Flow Control And Terminal

| From | Event | To/effect |
| --- | --- | --- |
| `Flowing` | valid payload | bounded enqueue/write, per-direction FIFO |
| `Flowing` | queue high | `Backpressured`; stop accepting payload |
| `Backpressured` | drain before timeout | `Flowing` |
| `Flowing/Backpressured` | bound, timeout, or write failure | request Pipe terminal; no silent drop |
| any non-terminal | exact participant close or session/hop/Gateway end | first local terminal |
| terminal | duplicate/late success/payload | terminal NoOp or ownership rejection |

Control/terminal messages use priority handling. Shutdown cancels and joins owned workers; it does not replay queued or inflight payload on a new Pipe.

## SDK Session Supervisor

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `ConnectingM` | fresh session authenticated | `RebindingM` | Install new raw Client; old handles stay terminal |
| `ConnectingM` | transient transport failure | `BackoffM` | Bounded exponential backoff + jitter |
| `ConnectingM` | permanent config/auth/protocol failure | `FailedM` | Terminal; no retry storm |
| `RebindingM` | every current logical Listener fresh-bound | `ReadyM` | Publish new underlying Listener generation |
| `RebindingM` | session loss | `BackoffM` | Clear current raw Listener handles; retain declarations only |
| `ReadyM` | session loss | `BackoffM` | Old Listener/Offer/Pipe/Open terminal; no replay |
| `ReadyM` | Open | `ReadyM` | Submit exactly once to current raw Client |
| `ConnectingM/RebindingM/BackoffM` | Open | same | Reject `NotReady`; no queue |
| any non-terminal | Close | `ClosedM` | Cancel connect/backoff and join one supervisor task |

Logical Listener drop/unbind removes its declaration before current-session cleanup, so later reconnect cannot redeclare it.
