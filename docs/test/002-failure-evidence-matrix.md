# TEST 002: Current Failure Evidence

이 문서는 [TEST 001](001-core-correctness-test-plan.md)의 현재 자동화 증거와 외부 blocker만 기록한다.

## Status meaning

| 상태 | 의미 |
| --- | --- |
| `executed` | 한 test/harness가 exact fault order와 oracle을 직접 관찰 |
| `invariant` | API/ownership 경계와 그 경계 test가 interaction을 unreachable/독립으로 증명 |
| `external-blocked` | 배포/operator evidence가 필요하며 passed가 아님 |

## Failure axes

| Axis | Cases | 상태 | Evidence |
| --- | --- | --- | --- |
| Authority | current, caller cancel, term change, follower, definitive verify loss | `executed` | Authority manager tests including `TestCallerVerificationCancellationDoesNotFenceCurrentAuthority`, `TestDefinitiveLeadershipLossFencesAuthorityAndDirectory` |
| Control session | syncing, revalidated, timeout, replacement, stale message | `executed` | Control client/server tests and blackhole integration |
| Directory | exact, absent, conflict, churn, max snapshot | `executed` | Directory tests plus `TestSnapshotEnvelopeAcceptsMaximumLegalSetAndRejectsExcess` |
| Open | all six gates, reject, cancel, deadline, ACK loss, Unknown | `executed` | 64-vector and Opening manager race tests |
| Remote hop | exact, provenance mismatch, replay, expiry, loss | `executed` | Admission decoder, peer relay, forwarded-attempt tests |
| Payload | both directions, bound, pressure, close/crash | `executed` | Opening/public/peer/SDK payload tests |
| Auth config | current, invalid candidate, removal, process skew | `executed` | Auth/runtime/admin tests |
| Raft storage | intact restart, quorum loss, epoch mismatch | `executed` | Raft node/state tests and 3-node Compose |
| Voter store loss replacement | new NodeId membership replacement | `external-blocked` | Dynamic membership flow 없음 |
| Fresh epoch external fence | partition된 모든 old path 차단 | `external-blocked` | Deployment evidence 필요 |
| Remote clock bound | real node clock skew | `external-blocked` | Operational evidence 필요 |
| Internal peer identity | untrusted/shared network | `external-blocked` | Peer authentication/mTLS 필요 |

## Compound evidence

| Interaction | Direct evidence | Result |
| --- | --- | --- |
| Authority change × stale session × partial redeclare | `TestX01AuthorityChangeRejectsStaleSessionAndRoutesPartialRedeclaration`, Compose failover | Empty first, fresh exact routes only |
| Session end × declare ACK loss × reconnect | `TestX02SessionEndAfterUnknownDeclareRedeclaresCurrentSnapshotOnly` | No history/replay; current snapshot only |
| Credential removal × config skew | `TestX03CredentialRemovalDuringGatewayConfigSkewRemainsProcessLocal` | Process-local retirement only |
| Listener accept × ACK loss × owner shutdown | `TestX04ListenerAcceptThenConfirmationLossAndOwnerShutdownIsUnknown` | Caller Unknown, active 0 |
| Replay × expiry × response loss | `TestForwardedOwnerSingleUseExpiryAndFailedGuard` | One O, no result replay |
| Backpressure × cancel × crash | `TestX07BackpressureCancelAndParticipantCrashReleaseAllPayloadSlots` | Bounded terminal and slot drain |

## Runtime evidence

`./scripts/compose-smoke.sh`는 격리된 3-node project에서 다음을 한 번에 검증하고 자기
container/volume/network/test image를 정리한다.

1. 3-voter leader/quorum/readiness
2. Same-Gateway와 Cross-Gateway Bind → Open → bidirectional payload → participant close
3. Go/Rust SDK 네 조합
4. Leader stop 뒤 2-node election
5. New authority의 empty directory와 Gateway fresh full redeclare

실행하지 않은 외부 blocker와 unsupported feature를 success로 기록하지 않는다.
