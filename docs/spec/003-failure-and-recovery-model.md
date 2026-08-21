# SPEC 003: 장애와 복구 모델

## 장애 모델

| 영역 | 장애 | 안전한 결과 |
| --- | --- | --- |
| Controller process | 기존 store를 가진 crash/restart | 같은 `NodeId`와 Raft/FSM state를 다시 연다 |
| Controller leader | quorum이 있는 same-epoch leader loss | 새 leader/`AuthorityId`, volatile `V` reset, Gateway reconnect/full snapshot |
| Controller store | member 하나의 disk/PVC loss | 새 `NodeId` replacement를 surviving quorum으로 add/catch-up/remove |
| Controller quorum | majority unavailable | 새 authority/control/admission fail closed |
| Gateway control | Gateway process가 살아 있는 disconnect/reconnect | Outage 동안 `V=false`; local `LiveBinding`은 유지하고 fresh FullSnapshot으로 revalidate |
| Gateway process | crash/restart | Local session/binding/attempt/Pipe/payload 소실; fresh Gateway instance와 SDK reconnect/rebind만 수행 |
| SDK session | disconnect/reconnect | Old child handle terminal; managed supervisor는 fresh auth와 current Listener rebind만 수행 |
| Network | delay/loss/duplicate/reorder/partition | Exact current identity만 허용하고 stale state 거부 |
| Config | invalid/delayed/process-local reload | Validated local snapshot만 사용 |
| Clock | authority-owner skew | `ClockSkewBound < open_timeout` 운영 근거가 있을 때만 remote expiry ready |
| Operator | initial bootstrap/disaster reset | Bootstrap은 one-shot, reset은 old-path fence와 새 epoch/cohort 요구 |

Timeout은 failure suspicion이지 death proof가 아니다. False positive는 availability를 낮출 수 있지만 admission gate를 true로 만들 수 없다.

## Linearization point

| 동작 | Linearization point | 손실 의미 |
| --- | --- | --- |
| Config reload | Valid snapshot atomic swap | 제거된 local runtime retire |
| Register Gateway | Raft `RegisterGateway` commit | `C`에 current session 존재, full snapshot 전 `V=false` |
| Full snapshot | Raft `ReplaceSnapshot` commit | 해당 Gateway exact route atomic replace |
| Declare | Raft `DeclareRoute` commit | Exact duplicate idempotent, conflict는 current route 보존 |
| Withdraw/remove | Raft `WithdrawRoute`/`RemoveGateway` commit | Tombstone 없는 true delete/cascade |
| Authority change | 새 term의 leader confirmation | 새 `AuthorityId`, 빈 `V` |
| Authority admission | Authority-owned confirmed read fence 하나가 exact `A·L·Q·C·V`를 context에 묶음 | Owner reservation/Pipe success가 아님 |
| Owner admission O | Local reservation + `AttemptId` fence | 성공한 O 이후 attempt는 local에서 계속 가능 |
| Open | Listener accept + `PipeId` creation | 이후 response loss는 `Unknown` 가능 |
| Pipe terminal | 첫 participant-local terminal | Local absorbing, peer propagation best effort |
| Payload delivery | Peer SDK bounded receive queue admission | Exact receipt가 없으면 sender는 `Unknown` 가능 |

## 필수 race 결과

| Race | 승자 | 결과 |
| --- | --- | --- |
| Caller verification cancel vs authority | caller cancel/deadline | 해당 call만 unavailable, authority/session/route 유지 |
| Definitive step-down/quorum loss vs authority | loss | `V` clear, new admission fail closed |
| Authority/session change vs O | O first | 해당 attempt 계속 가능 |
| Authority/session change vs O | fence first | Stale context, offer/PipeId 없음 |
| Gateway replacement vs old route | new `GatewayInstanceId` | Old owned route 삭제, stale message 재생성 불가 |
| Old withdraw vs new owner | new owner current | Old exact identity가 new route 삭제 불가 |
| Session end vs declare | declare commit first | `V` 즉시 clear, exact `C`는 revalidation grace 동안만 유지 |
| Session end vs in-flight declare | end first | Late commit이 `C`에 반영될 수 있으나 `V` 복구 불가; exact cleanup/snapshot이 수렴 |
| Unbind vs O | O first | 해당 attempt 계속 가능, future attempt 차단 |
| Unbind vs O | retirement first | O=false, offer 없음 |
| Listener accept vs cancel | accept first | Open LP 도달, 이후 terminal/`Unknown` 가능 |
| Listener accept vs cancel | cancel first | Late accept NoOp |
| Expiry vs O | strict expiry 전 O | Attempt 계속, expiry가 opened Pipe를 닫지 않음 |
| Expiry vs O | `now >= ExpiresAt` | Reservation/offer/Pipe 없음 |
| Duplicate ForwardOpen vs original | first successful O | Reservation 최대 하나, outcome/PipeId replay 없음 |
| Public Open ACK vs payload | Open ACK write | ACK 뒤에만 Listener→caller payload release |
| Payload queue admission vs receipt | queue admission first | Exact receipt, sender `Received` 가능 |
| Payload queue admission vs receipt | receipt observation 불가 | `InFlight` 뒤 deadline/terminal에서 `Unknown` |
| Peer stream failure vs sibling stream | failed stream | 해당 Pipe만 terminal; shared ClientConn과 sibling Pipe 유지 |
| Peer connection failure vs streams | connection failure | 해당 connection의 Pipe stream 모두 terminal, 다음 Open은 connection recovery 여부에 따라 새 stream 시도 |
| Backpressure vs close/crash | first terminal | Bounded stop, silent drop 없음 |

## Error 경계

| 결과 | 의미 | Retry/session 영향 |
| --- | --- | --- |
| `Rejected` | Current request/frame을 accept/apply할 수 없음 | Auth/session/protocol integrity 문제 외에는 named request/resource local |
| `Failed` | Named operation이 stable outcome으로 종료; Open `Failed`는 Listener-accept LP 이전 | 새 logical operation 가능, old attempt replay 금지 |
| `Unknown` | Operation LP를 지났을 수 있으나 exact result/receipt 손실 | Stable failure로 보고하지 않고 자동 retry/resume/replay 금지 |
| `Acknowledged` | Exact correlated operation이 apply/observe됨 | Exact duplicate ACK는 bounded NoOp, conflicting ACK는 protocol-fatal |
| `Terminated` | Exact participant-local resource의 absorbing terminal | Revival/resume/payload replay 금지 |

Bind/Unbind validation, capacity, conflict, control-unavailable은 operation-local이다. Authentication failure, session end, malformed protocol, stream transport failure는 session-fatal이다. Payload receipt/rejection은 exact `PipeId + PayloadId`로 correlate한다. `PipePayloadRejected`는 SDK exact Pipe view를 terminalize하며 server는 exact owned Pipe만 바꾼다.

## Crash cut

| Flow | Cut | Oracle |
| --- | --- | --- |
| Controller restart | snapshot/log compaction 전후 | 같은 durable store에서 current FSM 복구 |
| Lost Controller store | replacement empty start | Fresh `NodeId`, add/catch-up/remove 필수 |
| Membership response | commit 뒤 CLI response loss | Exact retry가 current membership으로 수렴, same identity/different address 거부 |
| Snapshot | validate 전/commit 뒤 ACK 전/stream end | Partial install 0, committed current state exact |
| Declare/withdraw | local effect/ACK와 session end 전후 | Response replay 없이 current cardinality 수렴 |
| Gateway control | FullSnapshot ACK 전후 disconnect | Local `LiveB` 유지, fresh exact revalidation 전 `V=false`; unacked `RegisteringB` 실패/무 replay |
| Failover | O 전후/`V` clear/partial redeclare | O 전 stale, O 뒤 계속 가능, revalidation 뒤 fresh exact route만 eligible |
| Open | O/offer/accept+PipeId/response/public ACK | Pre-LP stable failure, post-LP `Unknown` 가능, replay 없음 |
| Shared peer connection | sibling stream close/owner identity-address 교체/last old stream close | Sibling 유지, new Open은 new connection, old connection은 ref drain 뒤 close |
| Idle peer cache | owner churn으로 idle 상한 초과 | Active stream은 유지하고 least-recently-used idle connection만 close |
| Payload | prepare/handoff/queue admission/receipt/pressure/hop loss | `NotSent`/`Rejected`/`Received`/`Unknown`, FIFO, silent drop/replay 없음 |
| Disaster reset | external fence 전후 | Fence 없이는 reset 금지, new epoch는 별도 machine |

## 복구 등급

| 등급 | 의미 | 예시 |
| --- | --- | --- |
| `R0` | Existing state 안의 자동 복구 | Same-store restart, surviving-quorum election, Gateway reconnect/revalidate |
| `R1` | Participant action | Re-auth, rebind, new Open/Pipe |
| `R2` | Operator action | Quorum repair, fresh `NodeId` replacement, explicit disaster reset |
| `R3` | 복구 불가 | Old Pipe, payload position, uncertain Open outcome, erased member identity |

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

Three-voter cohort에서 durable member store 하나만 복구해서는 service를 복구할 수 없다. Membership replacement도 current quorum이 있을 때만 가능하다. Full old-path fence 뒤 disaster reset은 새 cohort와 empty current FSM을 만들 뿐 old outcome/Pipe/payload position/route history를 복구하지 않는다.

## Production blocker

| 계약 | Local code만으로 닫히지 않는 이유 |
| --- | --- |
| Disaster reset safety | Old controller/control/gateway path가 fence되었다는 operator evidence 필요 |
| Controller storage HA | Local add/remove는 있으나 production PVC/storage class/replacement runbook evidence는 외부 |
| Remote expiry readiness | Real node clock-skew bound evidence 필요 |
| Internal transport trust | Control/peer/Raft authentication 또는 mTLS 미구현 |
