# TEST 001: Core Correctness Plan

## Evidence Rule

Each test must define initial identity/state, exact event order or crash cut, observable result, and residual state cardinality. Passing by longer timeout, hidden retry/replay, or log text is not evidence.

`SPEC 004` is the canonical state table. This plan maps each required row family to representative automation or explicit missing evidence.

## Mandatory Matrix

| ID | Contract | Required oracle | Current representative automation |
| --- | --- | --- | --- |
| `R01` | Controller same-store restart | Existing durable store reopens without bootstrap and preserves initialized/current FSM | `TestSameStoreRestartRestoresStateWithoutBootstrap`, `TestSnapshotRecoveryRestoresCurrentState` |
| `R02` | Lost store replacement | Replacement uses fresh `NodeId`, catches up by `AddVoter`, then lost server can be removed | `TestAddCatchUpAndRemoveVoter`, `TestExistingStoreRejectsDifferentNodeID` |
| `R03` | Quorum fail closed | No confirmed authority/admission without quorum | node/authority quorum tests and Compose fail-closed stage |
| `R04` | Bootstrap is initial only | Empty store requires valid bootstrap voter manifest; same-store restart does not bootstrap | `TestValidateRequiresCohortManifestForInitialBootstrap`, bootstrap config tests |
| `R05` | Runtime role boundary | `gateway` opens no Raft/store/control listener; `controller` owns Raft/control and no relay | runtime composition and config/admin tests; Compose verifies the Gateway-side closed controller ports |
| `R06` | Local membership operator | Leader-only Unix-socket list/add/remove; duplicate Add and absent Remove converge without changing current membership; conflicts and limit reject | membership service/client tests and Compose operator stage |
| `D01` | Atomic full snapshot | Invalid/conflict/over-cap snapshot has partial install 0 | `TestFSMReplaceSnapshotIsAtomicOnConflict`, `TestSnapshotConflictIsAtomicAndExactCAndVAreRequired` |
| `D02` | Current-only cardinality | Bind/unbind churn leaves directory size equal to current live set | `TestFSMChurnDoesNotConsumeCapacityAndTrueDeleteReclaimsIt` |
| `D03` | True delete/cascade | Withdraw/remove/replacement leaves no tombstone/history and deletes exact owned routes | FSM and authority stale/replacement tests |
| `D04` | Exact stale fence | Old session declare/withdraw/snapshot cannot add/delete replacement owner state | `TestFSMRegisterReplacementCascadesRoutesAndFencesStaleABA`, `TestStaleGraceCleanupCannotDeleteReplacementInstance` |
| `D05` | ACK-loss convergence | Session end clears `V`; reconnect full-snapshot replaces current state, or exact grace cleanup deletes unrevalidated `C`; no response replay | `TestEndSessionRetainsCAndReconnectCancelsGraceCleanup`, `TestControlKeepaliveBlackholeDeletesAndRedeclaresCurrentRoutes` |
| `D06` | Maximum snapshot wire | 512 legal bindings fit; 513 rejects before state change | `TestSnapshotEnvelopeAcceptsMaximumLegalSetAndRejectsExcess` |
| `A01` | Six-gate admission | 64 `A,L,Q,D,V,O` combinations admit only `111111` | `TestSixGateAdmissionComposition` |
| `A02` | Authority call cancellation | Caller cancel/deadline affects only that call | authority manager call-scoped verification tests |
| `A03` | Definitive leadership loss | Authority-local `V` clears and admissions fail closed | `TestAuthorityFailoverRetainsCommittedDirectoryButDropsV` |
| `A04` | One confirmed admission boundary | One VerifyLeader+Barrier binds exact authority `D/V`; a changed authority ref rejects; steady Open does no full FSM copy | `TestAdmissionRejectsChangedAuthorityRef`, `TestSteadyStateConfirmAndAdmitOpenDoNotCopyFullState` |
| `O01` | Open LP ordering | O -> offer -> accept/PipeId -> confirmation ACK -> caller activation | Opening/public relay integration tests |
| `O02` | Accept/cancel/unbind races | First LP wins; late success does not revive; pre-O failure has no offer | `TestOpenAcceptVersusCancelBothOrders` and retirement tests |
| `O03` | Unknown boundary | Post-LP response/hop loss has no retry/resume and may be `Unknown` | `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown`, peer loss tests |
| `O04` | Existing Pipe independence | Future authority/admission failure does not terminate accepted Pipe payload | `TestAcceptedPipeContinuesWhenFutureAdmissionIsUnavailable` |
| `H01` | Forward provenance | Mismatched ingress/auth/binding/owner field rejects before forwarding | `TestOpenContextFromProtoRejectsEveryMismatchedProvenanceField` |
| `H02` | Replay/expiry fence | One `AttemptId` has at most one O; expiry strict; no outcome/PipeId replay | `TestForwardedOwnerSingleUseExpiryAndFailedGuard` |
| `H03` | One remote hop | One stream per remote Pipe; no redial/multiplex/resume | Peer tests and cross-Gateway Compose smoke |
| `P01` | Payload boundary/FIFO | 1..60 KiB exact bytes, per-direction FIFO, no cross-direction order claim | Opening/public/peer/SDK payload tests |
| `P02` | Bounded pressure | Full queue/timeout/write failure has no silent drop and releases slots | `TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots` |
| `P03` | Bounded terminal under payload pressure | Public multiplexed Relay control/terminal bypasses queued payload; a one-stream-per-Pipe peer hop instead times out and cancels the whole stream; all owned workers join | `TestOutboundActorPayloadPressureAndControlBypass`, peer lifecycle cancellation tests, and SDK close/send race tests |
| `K01` | Auth/reload | Invalid keeps old; valid removal swaps then retires local children | Auth/runtime reload tests |
| `K02` | Strict namespace | Same endpoint/target under another `ClientId` never falls back | Auth/authority/routing tests |
| `K03` | Observed-only C/V presence | Presence separates committed Gateway/route counts from revalidated/eligible leader-local counts and never claims completeness or cluster-wide revocation | Admin/authority tests |
| `S01` | SDK parity | Go-Go, Go-Rust, Rust-Go, Rust-Rust exact Open/payload/close | SDK conformance Compose stage |
| `S02` | Go SDK module isolation | `GOWORK=off` build/test imports no server/internal API | Go SDK module test/vet and import scan |
| `S03` | SDK supervision | Fresh auth/current Listener rebind; outage Open=`NotReady`; old Pipe terminal/no replay | `TestManagedClientReconnectsAndRedeclaresCurrentListenerOnly`, `TestManagedClientUnbindDuringBackoffDoesNotRedeclare`, Rust managed tests |

## Cross-Failure Cuts

| Interaction | Oracle |
| --- | --- |
| Same-epoch leader failover x stale session x partial redeclare | `V` empty first; exact fresh revalidated route only; a changed confirmed authority ref cannot issue a context |
| Same-store restart x snapshot compaction | Current FSM restored, no bootstrap |
| Lost controller store x replacement | Old `NodeId` not reused; replacement catches up before old member removal |
| Membership commit x response loss x retry | Exact Add/Remove retry returns one current configuration; identity/address conflict never aliases members |
| Session end x mutation ACK loss x reconnect | `V` stays false; reconnect snapshot or grace cleanup converges `C`; no mutation response replay |
| Credential removal x config skew x presence | Reloaded process retires only local state; presence does not prove cluster revocation |
| Listener accept x confirmation loss x owner crash | Caller `Unknown`; no outcome/Pipe recovery |
| Duplicate attempt x expiry x response loss | One O/offer max; no prior result replay |
| Backpressure x cancel x participant crash | Bounded terminal, all waiters end, slots drain |
| SDK session loss x Listener rebind x Open | New session/binding only; outage Open no queue; old Pipe/response/payload replay 0 |

## Release Commands

```bash
GOWORK=off go test -shuffle=on ./...
GOWORK=off go vet ./...
GOWORK=off go test -race ./...
(cd sdk/go && GOWORK=off go test -race ./... && GOWORK=off go vet ./...)
(cd examples/echo/go && GOWORK=off go test ./... && GOWORK=off go vet ./...)
cargo fmt --all --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./scripts/compose-smoke.sh
```

## Missing External Evidence

These are not satisfied by local test pass:

- Production PVC/storage-class and backup/restore evidence for controller volumes
- Production operator runbook evidence for lost-store replacement with new `NodeId`
- Disaster reset evidence: all old controller/control/gateway paths fenced before new epoch bootstrap
- `ClockSkewBound < relay.open_timeout` evidence
- Internal control/peer/Raft authentication or mTLS evidence
