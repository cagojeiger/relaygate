# TEST 001: Core Correctness Test Plan

> **Status:** Draft — v0 release gate는 `Planned`
>
> Current-state-only directory, 닫힌 state model, failure semantics와 복구 경계를 검증한다.

Test의 semantic source는 [SPEC 004](../spec/004-state-transition-model.md), failure/recovery oracle은
[SPEC 003](../spec/003-failure-and-recovery-model.md)이다. Test ID가 있다는 사실이나 과거 CAS/tombstone test의
pass는 이 변경의 runtime evidence가 아니다.

## 합격 증거

| Evidence | 확인할 것 |
| --- | --- |
| Input | Initial identity/state, exact event order, crash/loss/partition cut |
| Raft safety | term/vote/log/membership/snapshot와 constant-size `ClusterEpoch`; domain route data가 없음 |
| Authority | `ClusterEpoch`, `AuthorityId`, quorum, exact control session와 current directory |
| Runtime | Local auth/session/binding/attempt/Pipe/hop/terminal state |
| SDK | Go/Rust가 관찰한 `Opened/Failed/Cancelled/Unknown`, handle lifetime와 payload order |
| Recovery | Target별 R0–R3, new identity/redeclare/new Pipe 여부 |

Sleep과 timeout만 oracle로 쓰지 않는다. 가능한 test는 barrier, fake clock, event hook와 exact crash cut을 쓴다.
Failure detector timeout은 무한 대기를 막을 뿐 death proof나 admission success가 아니다.

## M — Model and invariant

| ID | Test | Pass oracle |
| --- | --- | --- |
| `M01` | State×event totality | SPEC 004의 모든 Cartesian product가 exactly one Applied/NoOp/Rejected다. |
| `M02` | Absorbing terminal/fence | Late success/duplicate terminal이 ended/retired Pipe/session/binding을 부활시키지 않는다. |
| `M03` | Exact identity fence | Old epoch/AuthorityId/ControlSessionId/GatewayInstanceId/ListenerBindingId message가 current state를 바꾸지 않는다. |
| `M04` | Strict ClientId namespace | Same endpoint/target의 두 client 사이 lookup/fallback 결과가 없다. |
| `M05` | Six-gate truth table | 아래 64 vector 중 `111111`만 O reservation/fence와 Listener offer를 만든다. |
| `M06` | Capacity fail closed | Client session/binding/mutation/attempt/Pipe/fence/buffer cap 초과가 새 state만 거부하고 current state를 evict하지 않는다. |
| `M07` | Lifecycle completion | 모든 non-terminal state에 owner identity/session/hop/epoch end를 주입하면 explicit terminal/clear/no-op으로 수렴한다. |
| `M08` | No queue/replay/resume | Control reconnect, owner hop loss와 Pipe loss 뒤 mutation/response/payload/Pipe를 replay/resume하지 않는다. |

### 64 admission vectors

각 row는 `(A,L,Q,D,V)` prefix 두 개의 O 값, 즉 2 vectors를 검증한다.

| `A L Q D V` | `O=0` | `O=1` |
| --- | --- | --- |
| `0 0 0 0 0` | Not admitted | Not admitted |
| `0 0 0 0 1` | Not admitted | Not admitted |
| `0 0 0 1 0` | Not admitted | Not admitted |
| `0 0 0 1 1` | Not admitted | Not admitted |
| `0 0 1 0 0` | Not admitted | Not admitted |
| `0 0 1 0 1` | Not admitted | Not admitted |
| `0 0 1 1 0` | Not admitted | Not admitted |
| `0 0 1 1 1` | Not admitted | Not admitted |
| `0 1 0 0 0` | Not admitted | Not admitted |
| `0 1 0 0 1` | Not admitted | Not admitted |
| `0 1 0 1 0` | Not admitted | Not admitted |
| `0 1 0 1 1` | Not admitted | Not admitted |
| `0 1 1 0 0` | Not admitted | Not admitted |
| `0 1 1 0 1` | Not admitted | Not admitted |
| `0 1 1 1 0` | Not admitted | Not admitted |
| `0 1 1 1 1` | Not admitted | Not admitted |
| `1 0 0 0 0` | Not admitted | Not admitted |
| `1 0 0 0 1` | Not admitted | Not admitted |
| `1 0 0 1 0` | Not admitted | Not admitted |
| `1 0 0 1 1` | Not admitted | Not admitted |
| `1 0 1 0 0` | Not admitted | Not admitted |
| `1 0 1 0 1` | Not admitted | Not admitted |
| `1 0 1 1 0` | Not admitted | Not admitted |
| `1 0 1 1 1` | Not admitted | Not admitted |
| `1 1 0 0 0` | Not admitted | Not admitted |
| `1 1 0 0 1` | Not admitted | Not admitted |
| `1 1 0 1 0` | Not admitted | Not admitted |
| `1 1 0 1 1` | Not admitted | Not admitted |
| `1 1 1 0 0` | Not admitted | Not admitted |
| `1 1 1 0 1` | Not admitted | Not admitted |
| `1 1 1 1 0` | Not admitted | Not admitted |
| `1 1 1 1 1` | Not admitted | **AdmittedO + fence + offer** |

Every non-success vector has no Listener offer, Pipe or PipeId. Protocol invariant로 unreachable한 vector는 skip하지
않고 proof artifact를 남긴다. `111111`도 아직 Pipe success가 아니며 Listener accept/AcceptedO가 Open LP다.

## D — Current directory and Raft boundary

| ID | Test | Injection/input | Pass oracle |
| --- | --- | --- | --- |
| `D01` | Atomic full snapshot | Valid N entries / invalid one / conflict one / oversized snapshot | Valid set 전체만 session revalidation과 함께 설치된다. Invalid/conflict에는 0 partial entry다. |
| `D02` | Exact declaration replay | Same current session의 same LiveBinding 반복 | `AlreadyApplied`; directory/session cardinality와 ref 불변 |
| `D03` | BindingKey conflict | Same key에 other ref/session declare | Stable conflict; existing exact entry unchanged |
| `D04` | True withdraw | Exact owner session/ref withdraw와 duplicate | First call deletes entry, duplicate no-op; tombstone/history 없음 |
| `D05` | Session bulk delete | Revalidated session에 N bindings 뒤 close/timeout/Gateway end | 그 session의 N entries/address 모두 삭제; other session entries 불변 |
| `D06` | Authority change clear | Multiple sessions/routes 뒤 step-down/quorum loss/new authority | Old sessions/routes/address 0; new authority starts current observed zero |
| `D07` | Partial redeclare availability | Failover 뒤 Gateway A만 snapshot, B/C는 disconnected | A exact routes immediately satisfy D/V; B/C routes fail. No total replica wait |
| `D08` | Stale session fencing | Old session declare/withdraw/snapshot을 new session entry와 reorder | Old message cannot add/delete/overwrite current entry |
| `D09` | Live-cardinality churn | K keys를 bind/unbind/session-close cycle로 many rounds | Directory size equals current live declared set, not historical unique keys/operations |
| `D10` | Reconnect current snapshot only | Disconnect while local LiveB/RetiredB/RegisteringB mix exists | New session declares exact LiveB only; old mutations/tombstones are not replayed |
| `D11` | No domain state in Raft | Declare/withdraw/churn/failover 뒤 application log command/FSM snapshot inspect | Gateway/session/binding/route/tombstone/presence absent; Raft safety/membership + ClusterEpoch만 남음 |
| `D12` | Raft restart | Intact voter store restart after route activity | Safety/epoch recover; authority directory starts empty and requires redeclare |
| `D13` | Snapshot envelope/cap | Maximum field sizes and 512 live bindings, then 513th new binding | Legal snapshot fits envelope; excess declaration rejects without evicting current routes |
| `D14` | Declare ACK loss | Entry insert 전/후 stream loss, RegisteringB retirement | Bind success is not guessed. Session end deletes possible entry; new session does not replay mutation |
| `D15` | Withdraw ACK loss | `LiveB→RetiringB`, server delete와 ACK 전/후 stream loss | Local O=false immediately; RetiredB cleanup completes; exact/session delete leaves no route history |
| `D16` | Owner address lifetime | Session close/failover/reconnect with new address | Address exists only on exact current session; absent from Raft/REST/directory history |

Directory property oracle:

```text
routes = disjoint union of bindings owned by current revalidated sessions
∀ route: route.owner is current ∧ route.owner.bindings[key] = route.binding
after EndSession(s): count(routes owned by s) = 0
after EndAuthority: sessions = 0 ∧ routes = 0
```

## O — Open and observation

| ID | Test | Pass oracle |
| --- | --- | --- |
| `O01` | Exact successful Open | A/L/Q/D/V then atomic O, offer, Listener accept/AcceptedO/PipeId, confirmation ACK, caller ACK/activation 순서를 지킨다. |
| `O02` | Missing D vs stale V | Absent key와 existing entry whose owner session ended를 각각 inject하면 no offer/Pipe다. |
| `O03` | Listener reject/deadline | No PipeId/handle; successful O fence remains to expiry. |
| `O04` | Listener accept vs cancel | Both orders match SPEC 003; cancel-first late accept no-op, accept-first local terminal/Unknown 가능 |
| `O05` | Unbind vs O | O-first attempt only continues; retirement-first no offer and non-consuming context |
| `O06` | Credential removal vs O | O-first admitted ordering or retirement-first O=false; removed local credential not revived |
| `O07` | Listener confirmation loss | Exact confirmation ACK before Listener handle; mismatch/loss exposes no handle |
| `O08` | Ingress/caller ACK loss | Caller Unknown where LP passed may be possible; same Pipe resume 없음 |
| `O09` | Owner crash cuts | O/fence and AcceptedO/PipeId atomic boundaries; old instance/context fenced, exact outcome R3 where uncertain |
| `O10` | Late expiry | Strict `now < ExpiresAt`; opened Pipe is not closed by attempt expiry |
| `O11` | Duplicate in-flight request ID | Original worker alone emits outcome; duplicate request rejected |
| `O12` | CancelOpen semantics | ACK means local signal only; outcome ordering preserved |
| `O13` | Exact ClosePipe ownership | Exact caller/listener only. Bounded terminal history의 participant duplicate는 owned no-op; eviction 뒤 unknown/foreign과 동일하며 route/Pipe를 복구하지 않는다. |
| `O14` | Process Open bound | Concurrent streams cannot exceed global attempt/Pipe capacity |
| `O15` | Stream close joins Open workers | In-flight Open workers cancel/join; no same-Pipe resume |
| `O16` | Go/Rust interoperability | Go→Go, Go→Rust, Rust→Go, Rust→Rust share exact wire/outcome semantics |

## C — Control, process and storage failure

| ID | Test | Pass oracle |
| --- | --- | --- |
| `C01` | Same-epoch failover | New AuthorityId, empty directory/current counts; partial redeclare exact routes only |
| `C02` | Quorum loss before context | No new context/bind/resolve; existing Pipe/local teardown continue |
| `C03` | Quorum/authority loss after context | O-before-fence attempt continues local lifecycle; fence-before-O context is stale even in same epoch and makes no offer |
| `C04` | Stale authority/control replay | Old snapshot/declare/withdraw cannot mutate new authority directory |
| `C05` | Control partition timeout | Suspected session routes bulk-delete; timeout cannot make a gate true |
| `C06` | Control reconnect order | `Hello→SessionOpened→FullSnapshot→Mutation*`; Syncing has V=false and racing mutation runs only after snapshot on that session |
| `C07` | Gateway restart | New instance/session and current Listener rebind/redeclare; old route/Pipe/fence outcome not recovered |
| `C08` | Voter restart | Safety/epoch preserved; no domain route recovered |
| `C09` | Voter store loss | Old voter identity not reused; live quorum joins new identity after operator/runtime policy |
| `C10` | Same-epoch quorum unavailable | No forced bootstrap; fail closed |
| `C11` | Unsafe fresh epoch | Any old authority path unfenced means no fresh epoch |
| `C12` | Safe fresh epoch | All old paths externally fenced; new epoch only, old continuity R3 |
| `C13` | Follower endpoint | Follower/no-quorum rejects Hello; confirmed authority alone opens session |
| `C14` | Caller verification cancellation | Status call cancellation does not fence current authority/other sessions |
| `C15` | Keepalive blackhole | Healthy idle stream stays; actual blackhole ends session in bounded time and deletes routes |

## K — Config and current presence

| ID | Test | Pass oracle |
| --- | --- | --- |
| `K01` | Invalid startup | StartupBlocked; no partial auth/service |
| `K02` | Invalid reload | Old revision/session/binding/Pipe unchanged |
| `K03` | Key rotation/removal | Final auth revalidation vs swap both orders; removed credential local runtime retires |
| `K04` | Verifier mutation | Reload entirely rejected |
| `K05` | Local removal completion | Local attempt/session/binding/Pipe all retired before LocalRetirementDone |
| `K06` | Presence authority loss | Old Current counts never returned after loss; `503 + NoAuthority` |
| `K07` | Current counts | Session/snapshot/declare/withdraw/close changes exact sessions/revalidated/bindings counts |
| `K08` | Zero is not complete | New authority before reconnect returns Current zero without `complete`/replica-total claim |
| `K09` | Partial observation | One of multiple deployment replicas connects; counts include only it and admission uses only exact D/V |
| `K10` | No revocation proof inference | Missing/partitioned Gateway cannot be declared retired from observed counts alone |
| `K11` | Auth boundary | Exact credential only; no cross-client fallback |
| `K12` | Trusted-local REST/redaction | Unauthenticated read-only local/dev status; no secret/payload/buffer/mutation surface; shared/untrusted exposure prohibited |
| `K13` | Auth resource bounds | First-message deadline and global session cap preserve existing sessions |

## P — Pipe terminal and flow control

| ID | Test | Pass oracle |
| --- | --- | --- |
| `P01` | Each participant closes | First local terminal absorbing; peer propagation best-effort/idempotent |
| `P02` | Permanent partition | No global cause/order/convergence; hop loss is local terminal trigger |
| `P03` | Duplicate terminal | First effect only, later terminal no-op |
| `P04` | EOF/half-close | v0 whole local Pipe terminal |
| `P05` | Bounded backpressure | Upstream stops; no silent drop; terminal on bound exhaustion |
| `P06` | Terminal bypass | Control/terminal bypasses full payload queue |
| `P07` | Payload non-replay | New Pipe never receives old payload/delivery position |
| `P08` | Bidirectional FIFO | Exact bytes and per-direction order; no cross-direction global order |
| `P09` | Activation gate | Listener→Caller payload only after public PipeOpened write; early terminal/overflow wins |
| `P10` | Frame boundary | Empty and >60 KiB reject; 60 KiB exact delivery |
| `P11` | Worker shutdown cycle | Transport cancellation can release in-flight write; no goroutine/Pipe leak |

## H — Cross-Gateway hop and replay

[ADR 008](../adr/008-cross-gateway-hop-and-replay.md)의 bounded replay/trust 계약은 유지하고
`BindingGeneration`만 [ADR 009](../adr/009-ephemeral-current-state-authority-directory.md)에 따라 exact current
session/binding identity로 대체한다.

| ID | Test | Pass oracle |
| --- | --- | --- |
| `H01` | Session owner-address lifetime | Exact current session memory only; close/failover 뒤 absent; fresh snapshot 전 remote route 없음 |
| `H02` | Trust boundary | Separate internal listener; no peer auth/mTLS means trusted local/dev only |
| `H03` | Ingress own-session fence | Response ingress tuple mismatch/malformed is rejected before forward |
| `H04` | Owner local O | Old/same-epoch stale AuthorityId, owner session ID, binding ref or local auth/binding mutation causes no O/offer/entry |
| `H05` | Absolute expiry/skew | Strict `<`; deployment skew bound missing means remote readiness proof fails |
| `H06` | Forwarded duplicate | One atomic O/fence/hop admission; duplicate/mutated expiry no response/PipeId replay |
| `H07` | Fence capacity/retention | Live entry remains through Listener reject until expiry; full cache fail closed; crash loses cache but fences old instance |
| `H08` | Remote Open cuts | LP-not-passed proof gives stable failure; uncertainty/after LP gives Unknown; no resume/replay |
| `H09` | Dedicated stream cardinality | N remote Pipes = N internal bidi streams; no multiplex/redial |
| `H10` | Activation gate | Payload release after public ACK write only |
| `H11` | Remote FIFO/backpressure/terminal | FIFO, no silent drop/replay, terminal priority, both segments local terminal |
| `H12` | 3-node Compose remote owner | Caller G1, Listener G2, third voter; exact current redeclare→Open→payload→close |

## X — Cross-failure coverage

All `F1–F9` axis class pairs in SPEC 003 must be observed or proved unreachable. LP races run in both orders.

| ID | Scenario | Pass oracle |
| --- | --- | --- |
| `X01` | Authority change × stale session × partial redeclare | Only fresh exact entries route; no completeness wait |
| `X02` | Session end × declare ACK loss × reconnect | Old entry 0; new FullSnapshot contains current LiveB only |
| `X03` | Credential removal × partition × observed presence | Observed counts do not prove cluster revocation |
| `X04` | Listener accept × ACK loss × owner crash | Caller Unknown; exact Pipe/outcome R3 |
| `X05` | Old epoch partition × state loss × reset | Missing external fence means fail closed |
| `X06` | Duplicate × expiry × response loss | At most one O/offer; no response replay; Unknown on LP uncertainty |
| `X07` | Backpressure × cancel × crash | Local absorbing terminal, no silent drop/global order |

## R — Recovery and irrecoverability

| ID | Incident | Classification |
| --- | --- | --- |
| `R01` | Surviving quorum election + empty directory + auto Gateway redeclare | Service `R0`; each redeclared route resumes independently |
| `R02` | Gateway/SDK reconnect, re-auth, rebind/new Pipe | Route `R1`; old Pipe `R3` |
| `R03` | Voter/network/config operator repair | Plan `R2` |
| `R04` | Safe offline fresh epoch | Service `R2`; old continuity `R3` |
| `R05` | Unfenceable old authority + same-epoch unrecoverable | Service `R3`, fail closed |
| `R06` | ACK loss Unknown | Exact Open outcome/Pipe `R3`; retry is new operation |
| `R07` | Inflight/buffer/delivery position loss | Payload position `R3`; no replay |
| `R08` | External config/backup loss | Old namespace `R3`; new identity enrollment is separate R2 |

## Traceability

| Contract | Mandatory tests |
| --- | --- |
| Total state/identity/no replay | `M01–M08` |
| Current directory and no Raft domain data | `D01–D16`, `C01`, `C08`, `X01–X02` |
| `A∧L∧Q∧D∧V∧O` | `M05`, `O01–O06`, `C01–C06` |
| Auth and observed-only presence | `K01–K13`, `X03` |
| Open/SDK outcome | `O01–O16`, `X04`, `R06` |
| Bounded flow/volatile payload | `P01–P11`, `X07`, `R07` |
| Cross-Gateway trust/replay/hop | `H01–H12`, `X04`, `X06` |
| Epoch/storage/recovery | `C08–C12`, `R01–R08` |

## Release gate

v0 correctness를 주장하려면 다음을 모두 만족해야 한다.

- `M/D/O/C/K/P/H/X/R` mandatory tests와 harness/operator artifact가 자동화되어 pass한다.
- 64 admission vectors 모두가 executed 또는 invariant-proved다.
- SPEC 003의 crash-cut 직전/직후와 F1–F9 pairwise manifest가 complete다.
- RelayGate application command/FSM snapshot에 ClusterEpoch 외 Gateway/binding/route domain data가 없음을
  검사한다. Raft core membership/safety record는 별도다.
- Long churn 뒤 memory/directory cardinality가 current live declarations에만 비례함을 검사한다.
- Go/Rust 네 조합과 isolated 3-node failover/partial redeclare smoke를 통과한다.
- Timeout 증가, hidden retry/replay 또는 replica completeness 가정으로 failure를 숨기지 않는다.
- 실행하지 않은 test를 passed로 기록하지 않는다.

이 current-state refactor 전의 generation CAS, tombstone, committed Gateway completeness evidence는 새 `D/V`
contract를 통과한 증거가 아니다. Refactor 뒤 test run과 artifact로 evidence inventory를 다시 작성한다.

현재 working-tree slice에서는 다음이 통과했다.

- `go test -shuffle=on ./...`, `go vet ./...`, `go test -race ./...`
- `cargo fmt --all --check`, workspace check/test와 warnings-as-errors clippy
- Isolated `./scripts/compose-smoke.sh`, including leader failover and live-binding full redeclare
- `C15` real TCP blackhole: healthy idle 유지, bounded session/route delete, fresh reconnect/full redeclare

이는 focused implementation/Compose evidence다. 위 planned release gate, 모든 crash-cut/F1–F9 pairwise와
production trust를 완료했다는 뜻은 아니다.

현재 자동화 증거에서 명시적으로 남은 항목은 다음과 같다.

| Gap | 현재 상태 | 완료 조건 |
| --- | --- | --- |
| `C09–C12` operator/epoch fencing | 문서화된 fail-closed 절차 | 재현 가능한 operator harness와 old-authority 차단 증거 |
| `H05` clock skew readiness | strict expiry unit contract | 배포 clock-skew bound를 검사하는 runtime/운영 증거 |
| SPEC 003 crash cuts, `F1–F9` pairwise | 대표 unit/race/Compose 사례만 존재 | mandatory matrix manifest의 각 행이 test 또는 invariant proof에 연결됨 |

따라서 상태표는 total하지만 v0 release evidence는 위 항목이 닫힐 때까지 partial이다.

## 관련 문서

- [SPEC 001: RelayGate System Model](../spec/001-system-model.md)
- [SPEC 002: Client Configuration and Presence](../spec/002-client-configuration-and-presence.md)
- [SPEC 003: Failure and Recovery Model](../spec/003-failure-and-recovery-model.md)
- [SPEC 004: State Transition Model](../spec/004-state-transition-model.md)
- [ADR 008: Cross-Gateway hop과 bounded replay](../adr/008-cross-gateway-hop-and-replay.md)
- [ADR 009: Current-state-only authority directory](../adr/009-ephemeral-current-state-authority-directory.md)
