# TEST 002: Failure Evidence Matrix

> **Status:** Implemented local/runtime evidence complete; unsupported/external prerequisites explicit
>
> [SPEC 003](../spec/003-failure-and-recovery-model.md)의 failure axis, crash cut와 복합 장애를 실제 자동화
> 증거에 연결한다. Test 이름만 존재하거나 서로 독립인 두 test가 각각 pass했다는 사실만으로 동시 장애를
> 관찰했다고 기록하지 않는다.

## Coverage rule

`F1–F9`의 모든 equivalence class는 최소 한 번 실행하거나 invariant로 증명한다. Cross-axis pair는 두 축이
같은 admission gate, identity fence, linearization point, cleanup owner 또는 recovery decision을 공유할 때
필수다. 서로 상태를 읽거나 쓰지 않는 독립 component의 raw Cartesian product는 test case를 복제하지 않고
그 독립 경계를 증명한다.

이 규칙은 9개 축의 모든 class를 무조건 곱한 323개 pair를 “실행됨”으로 부풀리는 것을 막는다. 생략한
interaction은 단순 미작성으로 처리하지 않고 독립 경계 또는 외부 prerequisite를 아래 표에 남긴다.

Evidence 상태는 다음 셋뿐이다.

| 상태 | 의미 |
| --- | --- |
| `executed` | 한 자동화 test/harness가 해당 event order와 oracle을 직접 관찰한다. |
| `invariant-proved` | Type/API/ownership 경계와 그 경계 test가 interaction을 unreachable 또는 독립으로 증명한다. |
| `external-blocked` | Application이 안전하게 만들 수 없는 배포/operator prerequisite가 남았다. Passed가 아니다. |

## Axis class coverage

| Axis | Class | 상태 | 자동화 증거 |
| --- | --- | --- | --- |
| `F1 Authority` | current | `executed` | `TestDirectorySnapshotConflictIsAtomicAndExactDuplicateIsIdempotent` |
|  | changing-absent | `executed` | `TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration`, Compose failover |
|  | stale message | `executed` | `TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration`, `TestX02SessionEndAfterUnknownDeclareRedeclaresCurrentSnapshotOnly` |
| `F2 Quorum` | available | `executed` | `TestThreeNodeStaticBootstrapPreservesElectionMembershipAndEpoch`, Compose bootstrap |
|  | unavailable-recovering | `executed` | `TestC10C12QuorumLossDoesNotChangeEpochAndFullyFencedResetStartsFreshEpoch`, Compose failover |
| `F3 Control session` | syncing | `executed` | `TestSyncingMutationIsSerializedAfterCurrentSnapshot` |
|  | revalidated | `executed` | `TestControlKeepaliveBlackholeDeletesAndRedeclaresCurrentRoutes` |
|  | ended-superseded | `executed` | `TestSessionReplacementBulkDeletesAndStaleSnapshotCannotRepopulate`, `TestX02SessionEndAfterUnknownDeclareRedeclaresCurrentSnapshotOnly` |
| `F4 Directory` | exact current | `executed` | `TestExactSameGatewayOpenAcrossRealBindingOpeningAndRelayLayers` |
|  | absent | `executed` | `TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration`의 clear→redeclare cut |
|  | conflicting-stale | `executed` | `TestDirectorySnapshotConflictIsAtomicAndExactDuplicateIsIdempotent`, `TestEndAndStaleWithdrawCannotDeleteNewRoute` |
| `F5 Gateway runtime` | live | `executed` | same/cross-Gateway Compose smoke |
|  | crashed | `executed` | `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown`, control blackhole timeout |
|  | restarted new instance | `executed` | `TestSessionReplacementBulkDeletesAndStaleSnapshotCannotRepopulate` |
| `F6 Auth config` | current | `executed` | `TestAuthenticateRequiresExactClientAndKey` |
|  | skewed | `executed` | `TestX03CredentialRemovalDuringGatewayConfigSkewRemainsProcessLocal` |
|  | invalid candidate | `executed` | `TestReloadRejectsVerifierMutationAndKeepsCurrentSnapshot`, config validation tests |
|  | removed credential | `executed` | `TestApplySwapsAuthBeforeRetiringRemovedSessions`, `TestApplyRetiresBindingsBeforeReturning`, `TestApplyRetiresPipesBeforeReturning` |
| `F7 Runtime capacity` | available | `executed` | `TestOpenExactSuccessAndAcceptedCapacityLifetime` |
|  | bounded-full | `executed` | `TestOpenCapacityAndTerminalHistoryAreBounded`, Gateway relay client/server capacity tests |
|  | backpressured-terminal | `executed` | `TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots`, payload pressure tests |
| `F8 Remote hop` | exact-unexpired | `executed` | `TestGatewayRelayRoundTripActivationPayloadAndClose`, cross-Gateway Compose smoke |
|  | duplicate-expired-full | `executed` | `TestForwardedOwnerSingleUseExpiryAndFailedGuard` |
|  | interrupted before-after LP | `executed` | `TestGatewayRelayTransportLossAfterForwardOpenIsUnknownAndNotRetried`, `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown` |
| `F9 Voter storage` | intact | `executed` | `TestSingleNodeSnapshotAndDurableEpochRestart` |
|  | one local lost | `external-blocked` | `C09`: dynamic membership/new NodeId replacement policy가 아직 없다. |
|  | same-epoch quorum unavailable | `executed` | `TestC10C12QuorumLossDoesNotChangeEpochAndFullyFencedResetStartsFreshEpoch` |

## Shared-boundary matrix

| ID | Fault interaction | Model/setup | Command or harness | Expected signal | 상태 |
| --- | --- | --- | --- | --- | --- |
| `X01` | Authority change × stale session × partial redeclare | Current directory를 채운 뒤 term을 바꾸고 old session message와 fresh snapshot을 순서대로 적용 | `go test ./internal/gateway/control/authority -run TestX01 -count=20`; Compose leader stop | clear 직후 route 0, stale message no-op/reject, fresh exact route만 성공 | `executed` |
| `X02` | Session end × declare ACK loss × reconnect | Declare apply 뒤 ACK를 관찰하지 못했다고 가정하고 exact session end, new instance snapshot, late old Declare | `go test ./internal/gateway/control/authority -run TestX02 -count=20`; client session-loss test | old route 0, mutation replay 0, current snapshot route 1 | `executed` |
| `X03` | Credential removal × partition/skew × observed presence | 동일 snapshot의 두 runtime 중 하나만 key 제거; REST surface completeness field 금지 | `go test ./internal/gateway/access/runtime -run TestX03 -count=20`; `go test ./internal/app/admin -run TestTrustedLocal` | reloaded process rejects, partitioned process stays on old valid snapshot, cluster revocation proof 없음 | `executed` |
| `X04` | Listener accept × ACK loss × owner crash | Confirm을 barrier에서 멈춘 뒤 Owner manager shutdown | `go test ./internal/gateway/routing/opening -run TestX04 -count=20` | caller `Unknown`, exact termination, active 0, Pipe/outcome 복구 없음 | `executed` |
| `X05` | Old epoch partition × state loss × reset | Intact old store와 quorum-loss cluster를 멈추고 별도 identity/address/store로 fresh epoch bootstrap | `go test ./internal/raft/node -run TestC10C12 -count=5` | local old epoch mismatch와 one-voter bootstrap은 fail closed; external all-old-path fence 없이는 전체 pass 아님 | `external-blocked` |
| `X06` | Forward duplicate × expiry × response loss | Accepted owner confirmation을 잃은 동일 AttemptId를 expiry 전/후 재전송 | `go test ./internal/gateway/routing/opening -run TestForwardedOwnerSingleUseExpiryAndFailedGuard -count=20` | offer/confirm 각 1회, duplicate/expired replay 0, prior response/PipeId 미재생 | `executed` |
| `X07` | Backpressure × cancel × participant crash | in-flight + full queue + enqueue-gate waiters 상태에서 stream actor close | `go test ./internal/gateway/relay/public -run TestX07 -count=20` | 모든 waiter bounded 종료, payload slot 0, silent replay 없음 | `executed` |

## Crash-cut mapping

| Flow | 직전/직후 증거 | Oracle |
| --- | --- | --- |
| Full redeclare | snapshot atomic conflict/capacity tests, real control blackhole reconnect | partial install 0; ended session 0; fresh LiveBinding만 복구 |
| Declare | `X02`, client pending-mutation session-loss test | commit 여부 추측/replay 없음; session end가 possible effect 삭제 |
| Withdraw | local binding retirement/capacity tests, stale withdraw authority test | local O=false; exact current entry true delete; new owner 보존 |
| Authority failover | `X01`, 3-node Compose failover | empty directory first; old session fenced; partial exact route 즉시 사용 |
| Open/hop | six-gate 64 vectors, accept/cancel both orders, `X04`, remote transport-loss test | pre-LP stable failure, post-LP uncertainty `Unknown`, retry/resume 없음 |
| Replay/expiry | forwarded single-use/expiry/cache test와 `X06` | 최대 한 O/offer; prior response/PipeId replay 없음 |
| Payload | activation, queue pressure, in-flight timeout, `X07` | FIFO, no silent drop/replay, terminal priority, slot drain |
| Config removal | concurrent auth/remove barriers와 `X03` | invalid candidate keeps old; removal은 process-local retirement까지만 증명 |
| Voter restart | intact restart/snapshot test, `C10/C12` | safety/epoch만 복구; route domain data 0 |
| Offline reset | `C10/C12` local half | old-path external fence proof는 `external-blocked`; 자동 epoch 전환 없음 |

## Local verification command

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

Compose harness는 자기 project/container/volume/network/test image만 만들고 종료 시 제거한다. Success 문자열이
아니라 각 command의 exit code와 exact oracle을 사용한다.

## Residual external evidence

| Contract | 막힌 이유 | 필요한 증거 |
| --- | --- | --- |
| `C09`, `F9 one local lost` | v0에 dynamic voter replacement가 없음 | Surviving quorum이 lost store의 old NodeId를 재사용하지 않고 new NodeId를 합류시키는 policy+harness |
| `C11`, `X05` external half | Application은 partitioned old process/network path 전체를 관찰·차단할 수 없음 | 배포 계층의 old Pod/process/network fence와 그 뒤 fresh epoch bootstrap 증거 |
| `H05` deployment skew | Local fake-clock은 strict expiry만 증명하고 node 간 실제 clock bound는 증명하지 못함 | `ClockSkewBound < relay.open_timeout` 운영/배포 readiness evidence |

이 세 행은 local test pass로 대체할 수 없으며 v0 production readiness를 계속 막는다.
