# SPEC 004: Canonical State Transitions

## Closed interpretation rule

모든 state/event 입력은 다음 우선순위로 정확히 하나의 결과를 갖는다.

1. Old epoch 또는 exact authority/session/instance/binding/participant가 stale이면 `Rejected`.
2. 아래 transition의 `From + Event + Guard`와 일치하면 `Applied`.
3. Exact duplicate cleanup/terminal 또는 absorbing terminal 재입력이면 `NoOp`.
4. 그 밖의 current identity지만 허용되지 않은 조합은 `Rejected`.

이 규칙은 누락된 transition을 성공으로 해석하지 않기 위한 semantic default다. 모든 machine의 거대한
Cartesian product를 자동 생성했다는 뜻은 아니며, 구현은 아래 owner별 transition과 경계 test로 증명한다.

## Ownership

| Machine | Owner | Persistence | Terminal/clear owner |
| --- | --- | --- | --- |
| Authority/Quorum | Raft + authority manager | Raft safety/epoch only | Step-down/quorum/epoch |
| ControlSession/Directory | Current authority memory | None | Session/authority end |
| Auth/ClientSession | Gateway access runtime | External config only | Credential/session/Gateway end |
| LocalBinding | Owning Gateway | None | Unbind/session/control/Gateway end |
| Attempt/OwnerPipe | Owning Gateway | None | Cancel/deadline/participant/hop/Gateway end |
| Ingress/Caller/ListenerPipe | Exact participant Gateway/SDK | None | First local terminal |
| RemoteHop | Ingress + Owner segment | None | Stream/hop/participant end |
| FlowControl | Each stream/segment | None | Bound/write/terminal |

## Authority, session and directory

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `Absent` | Same-epoch leader + quorum confirmed | `Current` | New AuthorityId, empty session/directory |
| `Current` | Caller-owned verification cancel/deadline, leadership still current | `Current` | Call fails only; no global fence |
| `Current` | Step-down, definitive verification failure, quorum/epoch loss | `Fenced` | Stop new work, clear all current memory |
| `AbsentC` | Exact current Hello | `SyncingC` | New ControlSessionId, replace older Gateway session |
| `SyncingC` | Whole snapshot valid/non-conflicting/in-capacity | `RevalidatedC` | Atomic session entry-set install |
| `SyncingC` | Invalid/conflicting snapshot | `EndedC` | Install nothing |
| `SyncingC/RevalidatedC` | Close/timeout/replacement/authority end | `EndedC` | Delete session address and all owned entries |
| `AbsentD` | Current revalidated Declare | `DeclaredD` | Insert exact entry |
| `DeclaredD` | Same session/ref Declare | `DeclaredD` | NoOp/AlreadyApplied |
| `DeclaredD` | Different current owner/ref Declare | `DeclaredD` | Reject conflict; preserve current |
| `DeclaredD` | Exact Withdraw/session end | `AbsentD` | True delete, no tombstone |

Presence is `NoAuthority` without current confirmed authority, otherwise `Current` with observed memory counters.
Observation does not change admission state.

## Authentication, session and binding

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `StartupBlocked` | Whole config valid | `ActiveAuth` | Activate immutable snapshot |
| `ActiveAuth` | Reload start | `Validating` | Old snapshot remains source |
| `Validating` | Whole candidate valid | `ActiveAuth` | Atomic swap + removed-state retirement |
| `Validating` | Invalid candidate | `ActiveAuth` | Keep old snapshot/runtime |
| `AuthenticatingS` | Exact credential + final current-snapshot check | `ActiveS` | New session, implicit ClientId |
| `AuthenticatingS` | Failure/deadline | `TerminalS` | No session |
| `ActiveS` | Close/revocation | `RetiringS` | Stop new work, retire children |
| `RetiringS` | Local retirement complete | `TerminalS` | Identity cannot revive |
| `AbsentB` | Bind start + capacity | `RegisteringB` | Allocate exact ListenerBindingId |
| `RegisteringB` | Current directory ACK | `LiveB` | Eligible for O |
| `RegisteringB/LiveB` | Unbind/revocation/session/control end | `RetiringB` | Immediately O=false, conditional withdraw |
| `RetiringB` | Cleanup complete | `RetiredB` | Release capacity; late ACK cannot revive |

## Admission, Open and replay fence

| From | Event + guard | To | Effect |
| --- | --- | --- | --- |
| `OpeningO` | All `A·L·Q·D·V·O` guards | `AdmittedO` | Atomic reservation + AttemptId fence; Listener offer |
| `OpeningO` | Any guard/deadline/cancel failure | `TerminalO` | No offer/PipeId; failed guard does not consume context |
| `AdmittedO` | Listener accept wins | `AcceptedO` | Open LP; mint PipeId |
| `AdmittedO` | Reject/deadline/cancel/end wins | `TerminalO` | Late accept NoOp; fence remains to expiry |
| `AcceptedO` | Late attempt deadline | `AcceptedO` | NoOp |
| `AcceptedO` | Participant/hop/terminal end | `TerminalO` | Best-effort peer terminal |
| `AbsentR` | Successful O | `ReservedR` | Insert AttemptId through ExpiresAt |
| `ReservedR` | Duplicate | `ReservedR` | Reject; do not replay outcome/PipeId |
| `ReservedR` | Expired | `AbsentR` | GC allowed; old context is also expired |

| Participant machine | Open transition | Terminal transition |
| --- | --- | --- |
| Ingress | Exact owner accepted installs segment | Reject/cancel/deadline/session/hop end; `Unknown` if LP uncertain |
| Listener | Offer → provisional; established → confirm; exact confirm ACK → handle open | Reject/cancel/session/hop end |
| Caller | Exact `PipeOpened` ACK → handle open/activation | Failure/cancel/transport/terminal |
| RemoteHop | Dial → forward → admitted → accepted → activated/open | Deadline, mismatch, EOF/hop/participant end |

One remote Pipe uses one hop stream. A second attempt/Pipe or mismatched identity on that stream is rejected and the
local segment terminates.

## Flow control and terminal

| From | Event | To/effect |
| --- | --- | --- |
| `Flowing` | Valid payload | Bounded enqueue/write, per-direction FIFO |
| `Flowing` | Queue high | `Backpressured`; stop accepting payload |
| `Backpressured` | Drain before timeout | `Flowing` |
| `Flowing/Backpressured` | Bound, timeout or write failure | Request Pipe terminal; no silent drop |
| Any non-terminal | Exact participant Close or session/hop/Gateway end | First local terminal |
| Terminal | Duplicate/late success/payload | NoOp for terminal; payload/ownership request rejected |

Control/terminal messages use a separate priority lane. Shutdown cancels and joins owned workers; it does not replay
queued/inflight payload on a new Pipe.
