# SPEC 003: Failure And Recovery Model

## Failure Model

| Domain | Failure | Safe result |
| --- | --- | --- |
| Controller process | crash/restart with existing store | Same `NodeId` and Raft/FSM state reopen |
| Controller leader | same-epoch leader loss with quorum | New leader, new `AuthorityId`, volatile `V` reset, gateways reconnect/full-snapshot |
| Controller store | disk/PVC loss for one member | Replacement uses new `NodeId`; add/catch-up/remove through surviving quorum |
| Controller quorum | majority unavailable | New authority/control/admission fail closed |
| Gateway control | disconnect/reconnect while Gateway process lives | `V` false during outage; existing local `LiveBinding` declarations survive and fresh FullSnapshot revalidates them |
| Gateway process | crash/restart | Local sessions, bindings, attempts, Pipes, and payload disappear; fresh Gateway instance and SDK reconnect/rebind only |
| SDK session | disconnect/reconnect | Old child handles terminal; opt-in supervisor may fresh-auth and rebind current logical Listener declarations only |
| Network | delay, loss, duplicate, reorder, partition | Exact current identity required; stale state rejected |
| Config | invalid/delayed/process-local reload | Validated local snapshot only |
| Clock | authority-owner skew | Remote expiry ready only with operational `ClockSkewBound < open_timeout` |
| Operator | initial bootstrap or disaster reset | Initial bootstrap is one-shot; disaster reset requires old-path fence and new epoch/cohort |

Timeout is failure suspicion, not death proof. False positives may reduce availability but must not make an admission gate true.

## Linearization Points

| Operation | Linearization point | Loss meaning |
| --- | --- | --- |
| Config reload | Valid snapshot atomic swap | Removed local runtime retires |
| Register Gateway | Raft `RegisterGateway` commit | `C` has current session; `V` still false until full snapshot |
| Full snapshot | Raft `ReplaceSnapshot` commit | Atomic replace of that gateway's exact routes |
| Declare | Raft `DeclareRoute` commit | Exact duplicate idempotent; conflict preserves current route |
| Withdraw/remove | Raft `WithdrawRoute`/`RemoveGateway` commit | True delete/cascade, no tombstone |
| Authority change | Leader confirmation in a new term | New `AuthorityId`; `V` empty |
| Authority admission | One authority-owned confirmed read fence binds exact `A·L·Q·D·V` to the issued context | Not owner reservation or Pipe success |
| Owner admission O | Local reservation + `AttemptId` fence | O-after success continues locally |
| Open | Listener accept + `PipeId` creation | Later response loss can be `Unknown` |
| Pipe terminal | First participant-local terminal | Local absorbing; peer propagation best effort |

## Required Race Outcomes

| Race | Winner | Result |
| --- | --- | --- |
| Caller verification cancel vs authority | Caller cancel/deadline only | Call unavailable; authority/session/routes stay current |
| Definitive step-down/quorum loss vs authority | Loss | Authority `V` cleared; new admissions fail closed |
| Authority/session change vs O | O first | That attempt may continue |
| Authority/session change vs O | Fence first | Context stale; no offer/PipeId |
| Gateway replacement vs old route | New `GatewayInstanceId` | Old owned routes deleted; stale messages cannot repopulate |
| Old withdraw vs new owner | New owner current | Old exact identity cannot delete new route |
| Session end vs declare | Declare commits first | End clears `V` immediately; exact `C` remains only through revalidation grace |
| Session end vs in-flight declare | End first | Late commit may affect `C` but cannot restore `V`; caller is stale and exact grace cleanup or reconnect snapshot converges |
| Unbind vs O | O first | That attempt may continue; future attempts blocked |
| Unbind vs O | Retirement first | O=false, no offer |
| Listener accept vs cancel | Accept first | Open LP reached; later result may be terminal/`Unknown` |
| Listener accept vs cancel | Cancel first | Late accept no-op |
| Expiry vs O | O before strict expiry | Attempt continues; expiry does not close opened Pipe |
| Expiry vs O | `now >= ExpiresAt` | No reservation/offer/Pipe |
| Duplicate ForwardOpen vs original | First successful O | One reservation max; no outcome/PipeId replay |
| Public ACK vs payload | ACK write | Listener-to-caller payload released only after ACK |
| Backpressure vs close/crash | First terminal | Bounded stop; no silent drop |

## Crash Cuts

| Flow | Cut | Oracle |
| --- | --- | --- |
| Controller restart | before/after snapshot and log compaction | Same durable store restores current FSM |
| Lost controller store | replacement starts empty | Must use fresh `NodeId`, add/catch-up/remove |
| Membership response | commit before CLI response loss | Exact retry converges to current membership; same identity at another address rejects |
| Snapshot | validate before commit / commit before ACK / stream end | Partial install 0; committed current state exact |
| Declare/withdraw | local effect and ACK before/after session end | No response replay; current cardinality converges |
| Gateway control | disconnect before/after FullSnapshot ACK | Existing `LiveB` remains local but `V` is false until fresh exact revalidation; unacknowledged `RegisteringB` fails and is not replayed |
| Failover | before O / after O / after `V` clear / partial redeclare | Before O stale; after O may continue; fresh exact route only after revalidation |
| Open | O / offer / accept+PipeId / response / public ACK | Pre-LP stable failure; post-LP can be `Unknown`; no replay |
| Payload | activation / enqueue / write / pressure / hop loss | FIFO per direction and no silent drop/replay; public queued control/terminal has priority, while a blocked peer send times out and cancels its dedicated Pipe stream |
| Disaster reset | before/after external fence | No reset without fence; new epoch is a separate machine |

## Recovery Levels

| Level | Meaning | Examples |
| --- | --- | --- |
| `R0` | Automatic within existing state | Same-store restart, surviving-quorum election, Gateway reconnect/revalidate |
| `R1` | Participant action | Re-auth, rebind, new Open/Pipe |
| `R2` | Operator action | Repair quorum, replace lost store with new `NodeId`, explicit disaster reset |
| `R3` | Not recoverable | Old Pipe, payload position, uncertain Open outcome, erased member identity |

```text
CurrentCohortServiceRecoverable = surviving and/or same-store-restored
                                  compatible current members can form
                                  the committed Raft quorum

MemberReplacementAllowed = current quorum exists
                        AND a fresh NodeId catches up before old member removal

RouteEligible = CurrentCohortServiceRecoverable
             AND current route exists in C
             AND owner reconnects/revalidates V
```

한 개의 durable member store만 복구한 것은 3-voter cohort의 service recovery가 아니다. Membership replacement도
이미 current quorum이 있을 때만 진행할 수 있다.

Disaster reset after full old-path fencing creates a new cohort and empty current FSM. It does not recover old outcomes, Pipes, payload positions, or route history.

## Production Blockers

| Contract | Why local code cannot close it |
| --- | --- |
| Disaster reset safety | Needs operator evidence that old controller/control/gateway paths are fenced |
| Controller storage HA | Local Unix-socket add/remove exists; production PVC/storage class and replacement runbook evidence remain external |
| Remote expiry readiness | Needs real node clock-skew bound evidence |
| Internal transport trust | Control/peer/Raft authentication or mTLS is not implemented here |
