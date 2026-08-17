# TEST 001: Core Correctness Plan

## Evidence rule

각 test는 initial identity/state, exact event order 또는 crash cut, 관찰 가능한 결과와 잔여 state cardinality를
검사한다. Timeout 증가, hidden retry/replay 또는 log 문자열만으로 통과시키지 않는다.

`SPEC 004`의 default rejection rule은 semantic closure다. 자동화가 전 system Cartesian product를
생성했다고 주장하지 않는다. 대신 state owner별 transition/race test와 admission 64-vector 전수 검사를
release evidence로 사용한다.

## Mandatory matrix

| ID | 계약 | 필수 oracle | 현재 대표 자동화 |
| --- | --- | --- | --- |
| `A01` | Six-gate admission | 64개 `A,L,Q,D,V,O` 조합 중 `111111`만 reservation/offer | `TestSixGateAdmissionComposition` |
| `A02` | Authority call cancellation | Caller cancel/deadline은 해당 call만 실패하고 authority/session/routes 유지 | `TestCallerVerificationCancellationDoesNotFenceCurrentAuthority` |
| `A03` | Definitive leadership loss | Follower/quorum verification failure는 authority와 directory를 clear | `TestDefinitiveLeadershipLossFencesAuthorityAndDirectory` |
| `D01` | Atomic full snapshot | Invalid/conflict/over-cap snapshot은 partial install 0 | `TestDirectorySnapshotConflictIsAtomicAndExactDuplicateIsIdempotent`, `TestSnapshotCapacityDoesNotPartiallyPublish` |
| `D02` | Current-only cardinality | Bind/unbind churn 뒤 directory size = current live set | `TestDirectoryCardinalityFollowsCurrentLiveChurn` |
| `D03` | Exact stale fence | Old session declare/withdraw/snapshot이 new owner를 add/delete하지 못함 | `TestEndAndStaleWithdrawCannotDeleteNewRoute`, `TestSessionReplacementBulkDeletesAndStaleSnapshotCannotRepopulate` |
| `D04` | ACK-loss convergence | Session end가 possible effect를 삭제하고 reconnect는 current snapshot만 선언 | `TestX02SessionEndAfterUnknownDeclareRedeclaresCurrentSnapshotOnly`, client reconnect tests |
| `D05` | Maximum snapshot wire | Maximum fields × 512는 envelope 안, 513은 pre-state reject | `TestSnapshotEnvelopeAcceptsMaximumLegalSetAndRejectsExcess` |
| `D06` | No domain state in Raft | Restart/snapshot에 route/session/binding/tombstone 없음 | Raft FSM/node state tests, `TestSingleNodeSnapshotAndDurableEpochRestart` |
| `O01` | Open LP ordering | O → offer → accept/PipeId → confirmation ACK → caller ACK/activation | Opening/public relay integration tests |
| `O02` | Accept/cancel/unbind races | First LP wins; late success no revival; pre-O failure no offer | `TestOpenAcceptVersusCancelBothOrders` and retirement tests |
| `O03` | Unknown boundary | Post-LP response/hop loss는 no retry/resume + `Unknown` | `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown`, peer loss tests |
| `O04` | Exact participant close | Caller/Listener만 close; duplicate bounded no-op; foreign unknown | Opening/public relay/SDK lifecycle tests |
| `O05` | Existing Pipe independence | Future authority admission failure가 accepted Pipe payload를 끊지 않음 | `TestAcceptedPipeContinuesWhenFutureAdmissionIsUnavailable` |
| `H01` | Forward provenance | Any mismatched ingress/auth/binding/owner field는 forwarding 전 reject | `TestOpenContextFromProtoRejectsEveryMismatchedProvenanceField` |
| `H02` | Replay/expiry fence | 한 AttemptId에 최대 한 O; strict expiry; prior outcome/PipeId no replay | `TestForwardedOwnerSingleUseExpiryAndFailedGuard` |
| `H03` | One remote hop | Remote Pipe당 stream 하나, no redial/multiplex, exact bidirectional lifecycle | Peer tests and cross-Gateway Compose smoke |
| `P01` | Payload boundary/FIFO | 1..60 KiB, exact bytes, per-direction FIFO, no cross-direction order claim | Opening/public/peer/SDK payload tests |
| `P02` | Bounded pressure | Full queue/timeout/write failure는 no silent drop, exact terminal, slots drain | `TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots` |
| `P03` | Terminal priority | Control/terminal이 payload pressure를 우회하고 workers cancel/join | Public relay actor/coordinator and SDK close/send race tests |
| `K01` | Auth/reload | Invalid keeps old; valid removal swaps then retires local children | Auth/runtime reload tests |
| `K02` | Strict namespace | Same endpoint/target라도 다른 ClientId lookup/fallback 없음 | Auth/authority/routing tests |
| `K03` | Observed-only presence | Follower/loss는 NoAuthority; counts에 completeness/revocation 추론 없음 | Admin/authority tests |
| `R01` | Failover/redeclare | New AuthorityId + route 0, stale reject, fresh partial route만 사용 | `TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration`, Compose failover |
| `R02` | Control blackhole | Healthy idle 유지; real blackhole는 bounded session delete/reconnect/redeclare | `TestControlKeepaliveBlackholeDeletesAndRedeclaresCurrentRoutes` |
| `R03` | Epoch/store safety | Same-epoch quorum loss는 bootstrap 금지; fenced fresh stores만 new epoch | `TestC10C12QuorumLossDoesNotChangeEpochAndFullyFencedResetStartsFreshEpoch` |
| `S01` | SDK parity | Go↔Go, Go↔Rust, Rust↔Go, Rust↔Rust exact Open/payload/close | SDK conformance Compose stage |

## Cross-failure cuts

다음 조합은 독립 unit test 두 개로 대체하지 않고 한 harness에서 함께 주입한다.

| Interaction | Oracle |
| --- | --- |
| Authority change × stale session × partial redeclare | Clear 뒤 route 0; fresh exact entry만 route |
| Session end × mutation ACK loss × reconnect | Old possible effect 0; no mutation response replay |
| Credential removal × config skew × presence | Reloaded process만 retire; observed counts로 cluster revocation 주장 금지 |
| Listener accept × confirmation loss × owner crash | Caller `Unknown`; exact outcome/Pipe 복구 없음 |
| Duplicate attempt × expiry × response loss | 최대 한 O/offer; prior result replay 없음 |
| Backpressure × cancel × participant crash | 모든 waiter bounded 종료; slots 0; late write 없음 |

## Release commands

```bash
go test -shuffle=on ./...
go vet ./...
go test -race ./...
cargo fmt --all --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./scripts/compose-smoke.sh
```

## External blockers

다음은 local test pass로 완료 처리하지 않는다.

- Lost voter store를 새 NodeId로 교체하는 dynamic membership/operator flow
- Fresh epoch 전 old process/network path 전체를 차단했다는 deployment evidence
- `ClockSkewBound < relay.open_timeout` 운영 evidence
- Internal control/peer/Raft authentication 또는 mTLS
