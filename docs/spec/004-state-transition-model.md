# SPEC 004: Canonical State Transitions

## Closed Interpretation Rule

Every state/event input has exactly one result.

1. Old epoch or stale exact authority/session/instance/binding/participant identity is `Rejected`.
2. A row whose `From + Event + Guard` matches is `Applied`.
3. Exact duplicate cleanup/terminal or absorbing terminal re-entry is `NoOp`.
4. Any other current identity combination is `Rejected`.

This is semantic closure, not a claim that one test enumerates the full Cartesian product.

`Rejected` is a stable request/frame refusal, `Failed` is a stable operation failure (and is pre-LP for Open), `Unknown`
means the Open LP may have happened, `Acknowledged` is an exact correlated apply barrier, and `Terminated` is an absorbing
resource end. Public operation-local failures use response messages; authentication/session/protocol/transport failures
end the gRPC stream.
An unrecognized, malformed, foreign, or conflicting public response is a protocol failure, not an operation-local `Rejected` result.

## Ownership

| Machine | Owner | Persistence | Clear/terminal owner |
| --- | --- | --- | --- |
| Raft membership | Controller Raft | durable store/log/snapshot | Raft membership change or explicit disaster reset |
| Current FSM `C` | Controller Raft FSM | durable log/snapshot | exact withdraw/remove/replacement |
| Authority/session mirror `V` | Current controller leader | leader-local memory | step-down/quorum loss/session end |
| Auth/ClientSession | Gateway access runtime | external config + process memory | credential/session/Gateway end |
| LocalBinding | Owning Gateway | process memory | failed registration, unbind, credential/client session, or Gateway end; control end only removes global publication through `V` |
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

Presence is `NoAuthority` without `AuthorityV`. `Current` separates committed Gateway/route counters from `C`, revalidated
Gateway counters from `V`, and eligible route counters where exact `C` and `V` agree. These observed counters do not prove
completeness or change admission state.

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
| `RegisteringB` | exact Raft-backed declare or full-snapshot ACK | `LiveB` | Local declaration is O-capable; end-to-end admission still requires current `C` and `V` |
| `RegisteringB` | declare failure, caller cancel, unbind, revocation, client session/Gateway end, or control end before ACK | `RetiredB` | O=false; do not replay into a later control session; late success is conditionally withdrawn before capacity is released |
| `LiveB` | control session end while Gateway process and client session remain live | `LiveB` | Keep the local declaration for the next FullSnapshot; global `V` is false, so no new O until revalidation |
| `LiveB` | unbind/revocation/client session/Gateway end | `RetiringB` | Immediately O=false, conditional withdraw |
| `RetiringB` | cleanup complete | `RetiredB` | Release capacity; late ACK cannot revive |

## Admission, Open, Replay Fence

`AdmitOpen` owns one confirmed read boundary: it verifies leader/quorum and a Raft read barrier once, then requires the
same exact `AuthorityId` while looking up `C` and `V`. An authority change before those lookups rejects the request; no
second verification, state mutation, or full-FSM copy belongs to the steady-state admission path.

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `OpeningO` | all `A·L·Q·C·V·O` guards | `AdmittedO` | Atomic reservation + `AttemptId` fence; Listener offer |
| `OpeningO` | any guard/deadline/cancel failure | `TerminalO` | No offer/PipeId; failed guard does not consume context |
| `AdmittedO` | Listener accept wins | `AcceptedO` | Open LP; mint `PipeId` |
| `AdmittedO` | reject/deadline/cancel/end wins | `TerminalO` | Late accept NoOp; fence remains to expiry |
| `AcceptedO` | late attempt deadline | `AcceptedO` | NoOp |
| `AcceptedO` | participant/hop/terminal end | `TerminalO` | Best-effort peer terminal |
| `AbsentAttemptF` | successful O | `ReservedAttemptF` | Insert `AttemptId` until expiry |
| `ReservedAttemptF` | duplicate | `ReservedAttemptF` | Reject; do not replay outcome/PipeId |
| `ReservedAttemptF` | expired | `AbsentAttemptF` | GC allowed; old context expired |

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
| `Flowing/Backpressured` | any payload rejection | terminalize the SDK's exact Pipe view; server mutates only an exact owned Pipe |
| any non-terminal | exact participant close or session/hop/Gateway end | first local terminal |
| terminal | duplicate/late success/payload | terminal NoOp or ownership rejection |

The multiplexed public Relay stream uses separate bounded lanes so ready control/terminal work bypasses queued payload.
A one-stream-per-Pipe peer hop instead serializes sends; blocked send timeout or cancellation fails that Pipe and cancels the
stream. Shutdown cancels and joins owned workers; neither path replays queued or inflight payload on a new Pipe.

## SDK Session Supervisor

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `ConnectingM` | fresh session authenticated | `RebindingM` | Install new raw Client; old handles stay terminal |
| `ConnectingM` | transient transport failure | `BackoffM` | Bounded exponential backoff + jitter |
| any non-terminal | permanent config/auth/protocol failure | `FailedM` | Terminal in connect, rebind, or ready phase; no retry storm |
| `RebindingM` | every current logical Listener fresh-bound | `ReadyM` | Publish new underlying Listener generation |
| `RebindingM` | transient session/transport loss | `BackoffM` | Clear current raw Listener handles; retain declarations only |
| `ReadyM` | transient session/transport loss | `BackoffM` | Old Listener/Offer/Pipe/Open terminal; no replay |
| `ReadyM` | Open | `ReadyM` | Submit exactly once to current raw Client |
| `ConnectingM/RebindingM/BackoffM` | Open | same | Reject `NotReady`; no queue |
| any non-terminal | Close | `ClosedM` | Cancel connect/backoff and join one supervisor task |

Logical Listener drop/unbind removes its declaration before current-session cleanup, so later reconnect cannot redeclare it.

## Public Error Scope

| Request/result family | Operation-local | Session-fatal |
| --- | --- | --- |
| Bind/Unbind | invalid request, capacity, conflict, control unavailable | session ended/revoked, context/stream end, internal protocol failure |
| Open/cancel | `PipeOpenFailed`, `PipeOpenUnknown`, exact duplicate-in-flight `OpenRequestRejected`, exact cancel ACK | malformed/unknown failure code, stream state, or transport failure |
| Listener decision | `ListenerDecisionRejected`, exact confirmation ACK | malformed/conflicting correlated response |
| Payload/close | payload rejection and exact close ACK/terminal; `owned=false` is an explicit NotOwned terminal result | malformed/conflicting correlated response or transport failure |

Go and Rust managed supervisors retry only transient transport/availability failures. Invalid configuration, authentication,
permission, failed precondition, and protocol errors enter `FailedM`. No supervisor retry replays Open/Pipe/payload state.
Every enum-valued response rejects `UNSPECIFIED` and unknown numeric values as protocol-fatal. The valid
duplicate-in-flight Open rejection and close NotOwned result remain distinct from InvalidRequest and generic transport failure.
