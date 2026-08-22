# TEST 001: 핵심 정확성 검증 계획

## 근거 규칙

각 테스트는 최초 식별자·상태, exact 사건 순서 또는 장애 지점, 관찰 가능한 결과, 남은 상태 개수를 정의해야 한다. 제한 시간을 늘리거나 숨은 재시도·재생 또는 log 문구만으로 통과하는 것은 근거가 아니다.

`SPEC 004`가 정규 상태표이며 아래 항목은 각 행 계열을 대표 자동화 또는 명시적인 미확보 근거에 연결한다.

## 필수 검증표

| ID | 계약 | 필수 판정 기준 | 현재 대표 자동화 |
| --- | --- | --- | --- |
| `R01` | Controller same-store restart | 기존 store를 bootstrap 없이 reopen하고 initialized/current FSM 보존 | `TestSameStoreRestartRestoresStateWithoutBootstrap`, `TestSnapshotRecoveryRestoresCurrentState` |
| `R02` | Lost store replacement | Fresh `NodeId`를 AddVoter/catch-up한 뒤 lost server 제거 | `TestAddCatchUpAndRemoveVoter`, `TestExistingStoreRejectsDifferentNodeID` |
| `R03` | Quorum fail closed | Quorum 없이 confirmed authority/admission 없음 | Node/authority quorum test와 Compose stage |
| `R04` | Bootstrap initial-only | Empty store는 valid voter manifest 필요, same-store restart는 bootstrap 없음 | Bootstrap config test |
| `R05` | Runtime role boundary | Gateway에는 Raft/store/control listener가 없고 Controller에는 Relay가 없음 | Runtime/config/admin test와 Compose port 검증 |
| `R06` | Local membership operator | Leader-only Unix socket list/add/remove, exact retry 수렴, conflict/limit 거부 | Membership test와 Compose operator stage |
| `R07` | 마지막 voter 제거 거부 | Voter 1개만 남은 상태에서 `RemoveServer`가 `FailedPrecondition`으로 거부되고 membership 불변 | `TestServiceRefusesToRemoveLastVoter` |
| `R08` | Draining 중 쓰기·검증 거부 | `BeginShutdown` 이후 신규 `Apply`/`VerifyLeader`/`AddVoter`/`RemoveServer`가 즉시 실패 | 미확보, draining 상태 주입 test 신규 필요 |
| `D01` | Atomic full snapshot | Invalid/conflict/over-cap snapshot partial install 0 | FSM/authority snapshot test |
| `D02` | Current-only cardinality | Bind/unbind churn 뒤 directory size=current live set | `TestFSMChurnDoesNotConsumeCapacityAndTrueDeleteReclaimsIt` |
| `D03` | True delete/cascade | Withdraw/remove/replacement 뒤 tombstone/history 없음 | FSM/authority stale/replacement test |
| `D04` | Exact stale fence | Old session mutation이 replacement state 생성/삭제 불가 | FSM ABA와 stale grace cleanup test |
| `D05` | ACK-loss convergence | Session end에서 `V` clear, reconnect snapshot 또는 grace cleanup으로 `C` 수렴, response replay 없음 | Control keepalive/reconnect test |
| `D06` | Maximum snapshot wire | 512 binding 허용, 513은 state change 전 거부 | Maximum envelope test |
| `G01` | Grace deadline 부여와 revalidation 면제 | 새 authority 확립 시 committed `C`의 모든 Gateway에 grace deadline이 부여되고, 재검증한 Gateway는 만료되지 않음 | `TestNewLeaderCleansPersistedGatewayThatNeverRevalidates`, `TestEndSessionRetainsCAndReconnectCancelsGraceCleanup` |
| `G02` | Grace 만료의 exact instance cascade | Deadline 경과 시 정확히 그 instance만 `RemoveGateway`되고 owned route가 cascade delete되며, 교체되거나 재검증된 instance는 삭제되지 않음 | `TestGraceCleanupDeletesOnlyUnrevalidatedCurrentGateway`, `TestStaleGraceCleanupCannotDeleteReplacementInstance` |
| `A01` | Six-gate admission | 64개 `A,L,Q,C,V,O` 조합 중 `111111`만 admit | `TestSixGateAdmissionComposition` |
| `A02` | Authority call cancellation | Caller cancel/deadline은 해당 call만 영향 | Authority call-scoped test |
| `A03` | 확정 리더 상실 | `V` 제거, 허용 판정 닫힌 실패 | 권한 주체 장애 전환 테스트 |
| `A04` | One confirmed boundary | VerifyLeader+Barrier 하나가 exact authority `C/V`를 묶고 steady Open은 full FSM copy 없음 | Admission ref/state-call test |
| `A05` | Apply 전송 실패 fence | 쓰기 명령 Apply 전송 실패 시 `V` 전체가 즉시 fence되고 `C`는 무손상으로 남음 | 미확보, Apply 전송 실패 주입 test 신규 필요 |
| `O01` | Open 선형화 순서 | O → 제안 → 수락·PipeId → 확인 ACK → 호출자 활성화 | Opening·공개 통합 테스트 |
| `O02` | Accept/cancel/unbind race | First LP wins, late success revival 없음, pre-O failure offer 없음 | Both-order/retirement test |
| `O03` | Unknown boundary | Post-LP response/hop loss는 retry/resume 없이 `Unknown` 가능 | Confirmation-loss/peer-loss test |
| `O04` | Existing Pipe independence | Future authority/admission failure가 accepted Pipe payload를 종료하지 않음 | `TestAcceptedPipeContinuesWhenFutureAdmissionIsUnavailable` |
| `H01` | Forward provenance | Mismatched ingress/auth/binding/owner field는 forwarding 전 거부 | OpenContext decoder test |
| `H02` | Replay/expiry fence | `AttemptId`당 O 최대 하나, strict expiry, outcome/PipeId replay 없음 | Forwarded owner fence test |
| `H03` | Remote stream isolation | Remote Pipe마다 stream 하나, stream failure는 sibling stream/shared connection을 종료하지 않음 | Peer lifecycle/connection pool test와 Compose smoke |
| `H04` | Peer connection sharing | Same exact owner의 concurrent/serial Pipe가 ClientConn 하나를 공유; changed owner identity/address는 새 connection, old connection은 ref drain 뒤 close; idle cache는 bounded LRU | `TestGatewayRelaySharesOneConnectionAcrossOwnerPipes`, `TestGatewayRelayReplacesChangedOwnerIdentityAfterOldPipesDrain`, `TestGatewayRelayIdleConnectionCacheIsBounded` |
| `P01` | Payload boundary/FIFO | 1..60 KiB exact bytes, per-direction FIFO, cross-direction order claim 없음 | Opening/public/peer/SDK payload test |
| `P02` | Bounded pressure | Full queue/timeout/write failure에서 silent drop 없고 slot 반환 | X07 test |
| `P03` | Pressure 중 bounded terminal | Public control/terminal lane은 payload 우회, peer blocked stream은 해당 Pipe만 cancel, worker join | Actor/peer/SDK race test |
| `P04` | Receipt LP | Receiver queue admission 뒤에만 receipt, exact receipt 뒤에만 `Send` 성공 | Public + Go/Rust SDK receipt test |
| `P05` | NotSent/Unknown cut | Handoff 전 deadline=`NotSent`, 이후 receipt 전=`Unknown` | Blocked writer/receipt test |
| `P06` | Exact receipt correlation | Current `PipeId + PayloadId`만 ACK/rejection, malformed/foreign/wrong-phase/conflict는 fatal | Public/peer/SDK strict decode test |
| `P07` | Duplicate receipt/payload | Exact duplicate는 bounded NoOp/re-ACK, same ID different bytes는 fatal, late result가 `Unknown` 수정 불가 | SDK history/fingerprint test |
| `P08` | Receipt pressure | Queue-full payload는 ACK/drop 없이 reject하고 exact Pipe만 terminal | Public/peer/SDK pressure test |
| `P09` | Terminal race | Receipt/close/deadline/session end 양순서가 absorbing result 하나와 slot drain | Public/SDK race test |
| `P10` | Same/cross-Gateway parity | 두 path 모두 destination SDK queue admission 뒤 exact receipt | Compose/peer integration |
| `P11` | No durable delivery state | Receipt pending/history는 bounded memory이고 FSM/log/snapshot에 없음 | FSM inspection/SDK capacity test |
| `K01` | Auth/reload | Invalid는 old 유지, valid removal은 swap 뒤 local child retire | Auth/runtime reload test |
| `K02` | Strict namespace | 다른 `ClientId`의 same endpoint/target fallback 없음 | Auth/authority/routing test |
| `K03` | Observed-only C/V | Committed와 revalidated/eligible counter 분리, completeness/revocation claim 없음 | Admin/authority test |
| `S01` | SDK parity | Go-Go/Go-Rust/Rust-Go/Rust-Rust exact Open/payload/close | SDK conformance Compose |
| `S02` | Go SDK isolation | `GOWORK=off` build/test가 server/internal API import 없음 | SDK test/vet/import scan |
| `S03` | SDK supervision | Fresh auth/current Listener rebind, outage Open=`NotReady`, old Pipe replay 없음 | Go/Rust managed test |
| `E01` | Bind/Unbind error scope | Stable operation-local failure 뒤 같은 stream에서 다음 request 성공, session/protocol failure는 stream end | Public/SDK binding test |
| `E02` | Managed retry parity | Permanent error는 Failed, transient transport만 bounded Backoff | Go/Rust classification test |
| `E03` | Payload rejection scope | SDK exact Pipe terminal, server는 exact owned Pipe만 변경 | Public/Go/Rust rejection test |
| `E04` | Strict response decode | Known nonzero enum만 허용, malformed/foreign/conflict는 session fatal, exact bounded replay는 NoOp | Go/Rust strict enum test |
| `E05` | Open/close SDK parity | Duplicate-in-flight는 distinct Rejected, `owned=false`는 NotOwned이며 Go는 `ErrPipeClosed` 호환 | Go/Rust outcome test |

## 복합 장애 지점

| 상호작용 | 판정 기준 |
| --- | --- |
| Same-epoch failover × stale session × partial redeclare | 먼저 빈 `V`, fresh exact revalidated route만 가능, changed authority ref context issuance 불가 |
| Same-store restart × snapshot compaction | Current FSM 복구, bootstrap 없음 |
| Lost store × replacement | Old `NodeId` 재사용 금지, replacement catch-up 뒤 remove |
| Membership commit × response loss × retry | Exact Add/Remove가 current config 반환, identity/address conflict 구분 |
| Session end × mutation ACK loss × reconnect | `V=false`, snapshot/cleanup으로 `C` 수렴, response replay 없음 |
| Credential removal × config skew × presence | Reload process local만 retire, presence는 cluster revocation 증명 안 함 |
| Listener accept × confirmation loss × owner crash | Caller `Unknown`, outcome/Pipe recovery 없음 |
| Duplicate attempt × expiry × response loss | O/offer 최대 하나, prior result replay 없음 |
| Shared connection × sibling stream failure × owner replacement | Sibling 유지, new identity는 new connection, old는 last ref 뒤 close |
| Backpressure × cancel × participant crash | Bounded terminal, waiter 종료, slot drain |
| SDK session loss × Listener rebind × Open | New session/binding만, outage Open queue 없음, old state replay 0 |

## 릴리스 검증 명령

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

## 아직 없는 외부 근거

로컬 테스트 통과만으로 다음 항목을 충족했다고 볼 수 없다.

- Controller volume의 production PVC/storage-class/backup/restore 근거
- Fresh `NodeId` lost-store replacement production runbook
- New epoch bootstrap 전 모든 old path fencing 근거
- `ClockSkewBound < relay.open_timeout` 근거
- Internal control/peer/Raft authentication 또는 mTLS 근거
