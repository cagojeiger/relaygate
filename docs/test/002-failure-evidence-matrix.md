# TEST 002: Current Failure Evidence

This file records what current automation has executed and what remains external/operator evidence. It does not mark missing production evidence as passed.

## Status Meaning

| Status | Meaning |
| --- | --- |
| `executed` | A test/harness directly observes the fault order and oracle |
| `representative` | Deterministic automation observes the stated local boundary, but not every OS/network/deployment realization of that failure class |
| `invariant` | Ownership/API boundaries make the interaction unreachable or independent, with boundary tests |
| `missing-evidence` | Required, but not proven by current automated tests |
| `external-blocked` | Requires deployment/operator evidence outside this repository |

## Failure Axes

| Axis | Cases | Status | Evidence |
| --- | --- | --- | --- |
| Controller restart | same durable store, same `NodeId`, no bootstrap | `executed` | `TestSameStoreRestartRestoresStateWithoutBootstrap` |
| Snapshot recovery | compacted log restores current FSM | `executed` | `TestSnapshotRecoveryRestoresCurrentState`, FSM snapshot restore tests |
| Controller replacement primitive | fresh `NodeId` add, current-FSM catch-up, remove; old identity reuse rejected | `executed` | `TestAddCatchUpAndRemoveVoter`, `TestExistingStoreRejectsDifferentNodeID` |
| Local membership operator | controller-local Unix-socket list/add/remove, leader-only guard, state-idempotent retry | `executed` | membership service/client tests and Compose operator stage |
| Production replacement operation | deployed runbook drives start/add/readiness/remove safely | `external-blocked` | production operator evidence required |
| Initial bootstrap validation | one-shot bootstrap requires voter manifest and is removed from steady Compose services | `executed` | config bootstrap tests; the command-scoped Compose input is removed by recreating only the bootstrap controller with `bootstrap=false` before fault stages |
| Production PVC/runbook | actual storage class, backup, replacement procedure | `external-blocked` | operator evidence required |
| Disaster reset fence | old controller/control/gateway paths fenced before new epoch | `external-blocked` | operator evidence required; not covered by Compose stop |
| Authority | current, caller cancel, term/ref change, follower, definitive verify loss, steady point-lookup admission | `executed` | authority manager tests including `TestAdmissionRejectsChangedAuthorityRef` and `TestSteadyStateConfirmAndAdmitOpenDoNotCopyFullState` |
| Control session | syncing, revalidated, timeout, replacement, stale message | `executed` | control client/server tests and blackhole integration |
| Directory `C` | exact, absent, conflict, churn, max snapshot, cascade delete | `executed` | FSM/authority directory tests |
| Open | all six gates, reject, cancel, deadline, ACK loss, Unknown | `executed` | 64-vector and opening manager race tests |
| Remote hop | exact provenance, replay, expiry, and representative stream loss | `representative` | admission decoder, peer relay, forwarded-attempt tests; no arbitrary packet-loss/partition campaign |
| Payload | both directions, bound, pressure, close, and in-process participant termination | `representative` | opening/public/peer/SDK payload tests; no OS process-kill payload crash cut |
| Public error scope | Bind/Unbind stable failures stay operation-local; payload rejection ends only the exact Pipe; every unknown/unspecified enum and malformed/foreign correlation fails closed | `executed` | public Relay plus Go/Rust SDK strict-enum, error-scope, and terminal-order tests |
| SDK retry state | Protocol failure in connect, rebind, or ready enters terminal Failed; only transient transport/availability enters Backoff | `executed` | Go wrapped rebind-protocol regression and Go/Rust managed classification tests |
| Auth config | current, invalid candidate, removal, process skew | `executed` | auth/runtime/admin tests |
| Runtime role | controller owns Raft/control; gateway owns relay and no Raft/store/control server | `representative` | runtime composition and config/admin tests; Compose verifies the Gateway-side closed controller ports |
| Presence | committed `C` counters, leader-local `V` counters, no completeness/revocation claim | `representative` | authority/admin tests verify exact local values; no deployment-wide expected-roster proof |
| Data-plane process crash | OS/container kill during an active Pipe or payload write | `missing-evidence` | in-process cancellation tests are not a process-crash harness |
| Arbitrary network fault campaign | packet loss, duplication, reorder, delay, and partition at every peer/control cut | `missing-evidence` | representative stream-loss and timeout tests only |
| Remote clock bound | real node clock skew | `external-blocked` | operational evidence required |
| Internal identity | untrusted/shared network | `external-blocked` | peer/control/Raft authentication or mTLS required |
| Go SDK module | server module/workspace-free build/test | `executed` | `sdk/go` `GOWORK=off` test/vet |

## Compound Evidence

| Interaction | Direct evidence | Result |
| --- | --- | --- |
| Authority change x stale session x partial redeclare | `TestAuthorityFailoverRetainsCommittedDirectoryButDropsV`, `TestStaleGraceCleanupCannotDeleteReplacementInstance` | `V` empty first, fresh exact routes only |
| Same-store restart x persisted FSM | raft node restart/snapshot tests | Durable `C` survives restart |
| Lost store x replacement | raft node add/remove tests | New `NodeId` required; old identity reuse rejected |
| Session end x declare ACK loss x reconnect | `TestEndSessionRetainsCAndReconnectCancelsGraceCleanup`, `TestControlKeepaliveBlackholeDeletesAndRedeclaresCurrentRoutes` | `V` clears immediately; reconnect snapshot or grace cleanup converges `C`; no history/replay |
| Credential removal x config skew | `TestX03CredentialRemovalDuringGatewayConfigSkewRemainsProcessLocal` | Process-local retirement only |
| Listener accept x ACK loss x owner shutdown | `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown` | Caller `Unknown`, active 0 |
| Replay x expiry x response loss | `TestForwardedOwnerSingleUseExpiryAndFailedGuard` | One O, no result replay |
| Backpressure x cancel x crash | `TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots` | Bounded terminal and slot drain |

## Runtime Evidence

`./scripts/compose-smoke.sh` validates the local multi-container shape: command-scoped one-shot bootstrap retirement, controller named volumes, gateway services without Raft data volume, local leader-only membership socket, same/cross-Gateway relay, SDK combinations, leader failover, and fail-closed behavior under insufficient quorum.

Local Compose is not production evidence for PVC durability, backup/restore, mTLS, clock skew, or disaster reset fencing.
