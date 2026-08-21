# TEST 002: 현재 장애 근거

이 문서는 현재 automation이 실제 실행한 것과 external/operator evidence가 남은 것을 구분한다. 없는 production evidence를 passed로 표시하지 않는다.

## Status 의미

| Status | 의미 |
| --- | --- |
| `executed` | Test/harness가 fault order와 oracle을 직접 관찰 |
| `representative` | Deterministic automation이 local boundary를 관찰하지만 모든 OS/network/deployment realization은 아님 |
| `invariant` | Ownership/API boundary가 interaction을 불가능 또는 독립으로 만들고 boundary test 존재 |
| `missing-evidence` | 필수지만 current automation으로 증명되지 않음 |
| `external-blocked` | Repository 밖 deployment/operator evidence 필요 |

## 장애 축

| 축 | 경우 | Status | 근거 |
| --- | --- | --- | --- |
| Controller restart | Same store/`NodeId`, no bootstrap | `executed` | Same-store restart test |
| Snapshot recovery | Compacted log에서 current FSM restore | `executed` | Node/FSM snapshot test |
| Controller replacement primitive | Fresh `NodeId` add/catch-up/remove, old identity reuse 거부 | `executed` | Add/remove와 identity test |
| Local membership operator | Unix socket list/add/remove, leader guard, idempotent retry | `executed` | Membership/Compose operator test |
| Production replacement | Deployed runbook의 safe start/add/readiness/remove | `external-blocked` | Production operator evidence 필요 |
| Initial bootstrap | One-shot voter manifest, steady Compose에서 bootstrap 제거 | `executed` | Config/Compose bootstrap stage |
| Production PVC/runbook | Real storage class/backup/replacement | `external-blocked` | Operator evidence 필요 |
| Disaster reset fence | New epoch 전 old path fence | `external-blocked` | Compose stop으로 증명 불가 |
| Authority | Current/cancel/term/ref/follower/quorum loss/point lookup | `executed` | Authority manager test |
| Control session | Syncing/revalidated/timeout/replacement/stale message | `executed` | Client/server blackhole integration |
| Directory `C` | Exact/absent/conflict/churn/max/cascade | `executed` | FSM/authority directory test |
| Open | Six gates/reject/cancel/deadline/ACK loss/Unknown | `executed` | 64-vector/opening race test |
| Remote hop | Provenance/replay/expiry/stream loss | `representative` | Decoder/peer/forwarded test, arbitrary network campaign 없음 |
| Peer connection sharing | Same owner reuse, sibling isolation, owner replacement drain, bounded idle eviction | `executed` | `TestGatewayRelaySharesOneConnectionAcrossOwnerPipes`, `TestGatewayRelayReplacesChangedOwnerIdentityAfterOldPipesDrain`, `TestGatewayRelayIdleConnectionCacheIsBounded` |
| Payload receipt | 양방향/queue LP/outcome/duplicate/pressure/terminal | `representative` | Opening/public/peer/Go/Rust, OS process-kill cut 없음 |
| Public error scope | Operation-local stable failure, strict correlation/enum | `executed` | Public + Go/Rust strict/error test |
| SDK retry state | Protocol은 Failed, transient transport만 Backoff | `executed` | Go/Rust managed classification |
| Auth config | Current/invalid/removal/process skew | `executed` | Auth/runtime/admin test |
| Runtime role | Controller Raft/control, Gateway Relay/no store | `representative` | Composition/config/admin/Compose port test |
| Presence | Committed `C`, leader-local `V`, completeness claim 없음 | `representative` | Authority/admin exact value test |
| Data-plane process crash | Active Pipe/payload write 중 OS/container kill | `missing-evidence` | In-process cancellation은 process crash가 아님 |
| Arbitrary network campaign | 모든 cut의 loss/duplicate/reorder/delay/partition | `missing-evidence` | Representative loss/timeout만 존재 |
| Remote clock bound | Real node clock skew | `external-blocked` | 운영 근거 필요 |
| Internal identity | Untrusted/shared network | `external-blocked` | Authentication/mTLS 필요 |
| Go SDK module | Server/workspace 없는 build/test | `executed` | `sdk/go` GOWORK=off test/vet |

## 복합 근거

| 상호작용 | 직접 근거 | 결과 |
| --- | --- | --- |
| Authority change × stale session × partial redeclare | Authority failover/stale cleanup test | 먼저 빈 `V`, fresh exact route만 |
| Same-store restart × persisted FSM | Raft restart/snapshot test | Durable `C` 생존 |
| Lost store × replacement | Add/remove test | Fresh `NodeId`, old reuse 거부 |
| Session end × ACK loss × reconnect | End/reconnect/blackhole test | `V` 즉시 clear, snapshot/cleanup으로 `C` 수렴 |
| Credential removal × skew | X03 | Process-local retirement만 |
| Listener accept × ACK loss × shutdown | X04 | Caller `Unknown`, active 0 |
| Replay × expiry × response loss | Forwarded owner test | O 하나, result replay 없음 |
| Peer sharing × stream close × owner replacement | Connection pool test | Sibling 유지, identity change connection 교체/drain |
| Backpressure × cancel × crash | X07 | Bounded terminal/slot drain |
| Payload handoff × receipt loss × terminal | Go/Rust receipt-wait test | Pre=`NotSent`, post=`Unknown`, late result NoOp |
| Duplicate payload × pressure | Public/peer/SDK fingerprint test | Exact bytes once/re-ACK, conflict fail closed, full queue reject |

## Runtime 근거

`./scripts/compose-smoke.sh`는 command-scoped bootstrap retirement, Controller named volume, storeless Gateway, leader-only membership socket, same/cross-Gateway Relay, SDK 조합, leader failover, insufficient quorum fail-closed를 검증한다.

Local Compose는 production PVC durability, backup/restore, mTLS, clock skew, disaster reset fencing의 근거가 아니다.
