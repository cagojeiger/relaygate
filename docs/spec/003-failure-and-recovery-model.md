# SPEC 003: Failure and Recovery Model

## Failure model

| Domain | 허용하는 failure | 안전한 결과 |
| --- | --- | --- |
| Process | SDK/Gateway/authority/voter crash와 restart | Old runtime identity를 복구하지 않고 새 identity/session/Pipe 사용 |
| Network | delay, loss, duplicate, reorder, partition | Exact current identity가 아니면 state advancement 거부 |
| Control | leader/quorum/session loss | 새 bind/resolve/context fail closed; directory clear |
| Storage | intact restart, voter store loss/corruption | Safety/epoch만 복구; route 0 |
| Config | invalid/delayed/process별 reload | Validated local snapshot만 사용 |
| Clock | authority-owner skew | `ClockSkewBound < open_timeout`일 때만 remote expiry ready |
| Operator | reset/bootstrap | 모든 old path를 외부 fence한 뒤 fresh epoch |

Timeout은 failure suspicion이지 death proof가 아니다. False positive는 session/route availability만 줄이며
admission gate를 새로 true로 만들지 않는다.

## Linearization points

| Operation | 선형화점 | ACK/loss 뒤 의미 |
| --- | --- | --- |
| Config reload | Valid snapshot atomic swap | Removed local state retirement 진행 |
| Full redeclare | 전체 validate 뒤 session+entry set atomic install | Stream end면 bulk delete; fresh session이 current set 재선언 |
| Declare | Exact current directory insert | Same-session exact duplicate만 idempotent |
| Unbind | Local binding ineligible | Withdraw는 true delete; stale cleanup은 new entry 보존 |
| Authority loss/change | Old authority fence + all session/directory clear | New authority는 empty view |
| Authority admission | `A·L·Q·D·V` context 발급 | O/Pipe success가 아님 |
| Owner admission O | Local reservation + `AttemptId` fence insert | O 이후 attempt만 local lifecycle 계속 |
| Open | Listener accept 기록 + `PipeId` 생성 | Response loss는 `Unknown` 가능 |
| Pipe terminal | First participant-local terminal | Local absorbing, peer propagation best-effort |

## Required race outcomes

| Race | Winner | Result |
| --- | --- | --- |
| Caller verification cancel ↔ authority | Caller cancel/deadline only | 해당 call만 unavailable; global authority/session 유지 |
| Definitive step-down/quorum loss ↔ authority | Loss | Authority/session/directory clear |
| Authority/session change ↔ O | O | 그 attempt만 Listener lifecycle 계속 |
| Authority/session change ↔ O | Fence/end | Context stale, no offer/PipeId |
| Session end ↔ declare | Declare then end | End bulk delete |
| Session end ↔ declare | End first | Late declare stale rejection |
| Old withdraw ↔ new owner | New owner current | Old exact identity가 new entry를 지우지 못함 |
| Unbind ↔ O | O | 그 attempt만 계속; future attempt 차단 |
| Unbind ↔ O | Retirement | O=false, no offer |
| Listener accept ↔ cancel | Accept | Open LP 뒤 local terminal/`Unknown` 가능 |
| Listener accept ↔ cancel | Cancel | Late accept no-op |
| Expiry ↔ O | O before strict expiry | Attempt 계속; expiry가 opened Pipe를 닫지 않음 |
| Expiry ↔ O | `now >= ExpiresAt` | No reservation/offer/Pipe |
| Duplicate ForwardOpen ↔ original | First successful O | 최대 한 reservation/offer/PipeId; response replay 없음 |
| Public ACK ↔ payload | ACK write | 그 뒤에만 Listener→Caller payload release |
| Backpressure ↔ close/crash | First local terminal | Silent drop 없이 bounded 종료 |

## Crash cuts

| Flow | 반드시 검사할 cut | Oracle |
| --- | --- | --- |
| Snapshot | validate 전 / install 뒤 ACK 전 / stream end | Partial install 0; ended session entry 0 |
| Declare/withdraw | local effect와 ACK 전후 / session end | Outcome 추측·history 없음; current cardinality로 수렴 |
| Failover | O 전 / O 뒤 / directory clear / partial redeclare | O 전 context stale; clear 후 fresh exact route만 사용 |
| Open | O / offer / accept+PipeId / response / public ACK 전후 | Pre-LP stable failure, uncertain post-LP `Unknown`, no replay |
| Payload | activation / enqueue / write / pressure / hop loss | Direction FIFO, no silent drop/replay, terminal priority |
| Voter restart | safety write/log/snapshot restore | Safety/epoch만 복구, route domain data 0 |
| Epoch reset | external fence proof 전후 | Proof 전 bootstrap 금지; 두 current epoch 금지 |

## Recovery levels

| Level | 의미 | 예 |
| --- | --- | --- |
| `R0` | 자동 복구 | Surviving quorum election, Gateway auto reconnect/redeclare |
| `R1` | Participant action | Re-auth, rebind, 새 Open/Pipe |
| `R2` | Operator/infrastructure action | Voter replacement, network repair, safe fresh epoch |
| `R3` | Exact old target 복구 불가 | Old Pipe, payload position, uncertain Open outcome, fenced identity |

```text
ServiceRecoverable = surviving quorum
                  OR (all old paths externally fenced AND safe fresh epoch)

RouteRecoverable = ServiceRecoverable AND owner reconnects/redeclares
```

정확한 old Pipe/outcome/payload position은 저장하지 않으므로 복구할 수 없다. Application은 새 operation과
업무 수준 deduplication을 사용한다. Same epoch가 불가능한데 old authority path 하나라도 fence할 수 없으면
service도 fail closed 상태가 최종 결과다.

## Production blockers

| Contract | Local code로 닫히지 않는 이유 |
| --- | --- |
| Lost voter store replacement | Dynamic membership/new NodeId operator flow가 없음 |
| Fresh epoch safety | Partition된 old process/network path 전체 fence는 배포 계층 책임 |
| Remote expiry readiness | 실제 node 간 clock-skew bound는 배포 evidence 필요 |
| Internal transport trust | Control/peer/Raft authentication 또는 mTLS가 아직 없음 |
