# SPEC 007: 오류, canonical 상태와 장애 전파

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 역할 | 외부 오류 code, 상태·전이와 observation 의미의 단일 기준 |

## 오류 계약

끝까지 await한 operation은 하나의 성공 또는 하나의 오류로 끝난다. Rust future를 drop 또는 abort하면 해당 호출에는 SDK 반환값이 없지만 operation owner는 내부 attempt를 정확히 한 번 terminal로 정리해야 한다. 아래의 재시도는 같은 attempt를 되살리는 것이 아니라 새로운 operation을 시작한다는 뜻이다.

| ID | Code | 의미 | 새 operation 재시도 |
| --- | --- | --- | --- |
| `ERR-001` | `INVALID_ARGUMENT` | 식별자, snapshot scope, authority 또는 요청 형식이 유효하지 않음 | 입력 수정 뒤 가능 |
| `ERR-002` | `UNAUTHENTICATED` | ClientKey 또는 필요한 transport/component identity 검증 실패 | 새 credential 또는 새 deployment configuration 뒤 가능 |
| `ERR-003` | `PERMISSION_DENIED` | 검증된 주체에 해당 등록 권한이 없음 | 권한 변경 뒤 가능 |
| `ERR-004` | `NOT_FOUND` | READY RT의 current view에 해당 `ClientId`의 live binding이 없음 | 상태가 바뀐 뒤 가능 |
| `ERR-005` | `FAILED_PRECONDITION` | `ShardDirectoryGeneration` mismatch, stale `BindingId`·`LeaseId`·registration revision, 정상 종료된 Pipe 또는 shutdown된 write direction에 대한 write처럼 현재 상태가 operation을 허용하지 않음 | compatible configuration 또는 current state로 다시 시작한 뒤 가능 |
| `ERR-006` | `UNAVAILABLE` | RT shard, Gateway, peer path 또는 Resolve 뒤 선택한 ListenerSession을 현재 사용할 수 없음 | backoff 뒤 가능 |
| `ERR-007` | `DEADLINE_EXCEEDED` | operation이 configured deadline 안에 terminal 결과를 내지 못함 | 새 operation으로 가능 |
| `ERR-008` | `RESOURCE_EXHAUSTED` | queue, frame, Pipe, stream 또는 connection의 bounded limit 초과 | 부하 감소 뒤 가능 |
| `ERR-009` | `CANCELLED` | await 중인 SDK control operation이 명시적으로 닫힌 Connector·Listener runtime 또는 Listener handle을 관찰하거나, live operation·Pipe가 terminal cancellation failure를 관찰함. future drop·abort의 내부 취소에는 사용자 반환값이 없음 | caller 결정으로 새 runtime·handle 또는 새 operation 가능 |
| `ERR-010` | `PROTOCOL_ERROR` | peer가 허용되지 않은 role, ownership 또는 상태 전이의 frame을 보냄 | owner의 Pipe-local 상태 위반은 해당 Pipe만 `RESET`; current Pipe의 non-owner frame과 role 위반은 offending session을 닫은 뒤 새 operation 가능 |
| `ERR-011` | `INTERNAL` | 위 code로 분류할 수 없는 RelayGate 내부 실패. 실패를 operation scope로 격리할 수 있으면 그 operation의 partial state만 terminal 정리하고 sibling state를 유지한다. 안전하게 격리할 수 없으면 enclosing session, runtime 또는 service를 terminal 종료한다. | backoff 뒤 새 operation으로만 가능 |
| `ERR-012` | `ALREADY_EXISTS` | 같은 Listener SDK runtime에 동일 `ClientId`의 pending `ListenAttempt` 또는 non-`CLOSED` Listener handle이 이미 존재함 | attempt가 terminal로 끝나거나 기존 handle이 `CLOSED`가 된 뒤 가능 |

`ALREADY_REGISTERED`는 오류 code가 아니다. 다른 `ListenerSession`이 같은 `ClientId`를 등록하는 것은 many-to-many model의 정상 동작이고, 하나의 existing Listener handle이 동일 session의 current binding 등록을 반복하는 것은 idempotent operation이다. `ALREADY_EXISTS`는 이들과 달리 한 SDK runtime 안에서 동일 `ClientId`의 pending attempt 또는 handle을 중복 생성한 경우에만 사용한다.

`NOT_FOUND`는 READY shard의 current view가 비어 있다는 뜻이며 전 세계에 Listener가 영원히 없다는 증명이 아니다. RT restart 뒤 재등록·갱신 수렴 구간에도 발생할 수 있다. `UNAVAILABLE`을 `NOT_FOUND`로 바꾸어서는 안 된다.

## PeerObservation

오류 이유와 상대방이 operation을 관찰했는지는 다른 축이다. generic `UNKNOWN` 상태나 오류 code를 추가하지 않고 terminal open 결과에 다음 증명 기반 observation을 함께 사용한다.

| 값 | 의미 | 자동 replay 또는 reroute |
| --- | --- | --- |
| `NOT_OBSERVED` | Listener queue admission과 Pipe 생성이 일어나지 않았음을 Connector가 증명할 수 있음 | 같은 attempt는 금지, 새 operation은 caller 정책 |
| `MAYBE_OBSERVED` | queue 미도달과 `OPENED` 확인 중 어느 것도 증명할 수 없음. 실제 queue 적재 여부는 알 수 없음 | 금지 |
| `OBSERVED` | Connector가 `OPENED`를 확인한 성공 | 해당 Pipe 사용 |

`MAYBE_OBSERVED`는 queue 적재가 실제로 일어났다는 단정이 아니라, open request commit 뒤 응답 유실·deadline·SDK-Gateway session loss 때문에 Connector가 안전하게 부정할 수 없다는 뜻이다.

remote path에서도 같은 증명 기준을 사용한다. peer `OPEN`이 ordered writer queue에 commit되기
전의 connect·handshake·queue 실패는 `NOT_OBSERVED`, commit 뒤 terminal 결과를 확인하지 못한
deadline·transport loss·cancel은 `MAYBE_OBSERVED`다. peer `OPENED`는 Listener queue admission을
확인하지만 external `OBSERVED`는 Connector SDK가 `OPENED`를 확인한 뒤에만 성립한다. 내부
component가 더 강한 증명을 갖더라도 Connector SDK가 그 결과를 확인하기 전에 caller가
future를 drop 또는 abort했다면 해당 호출에는 SDK 반환값이 없다. 다만 commit 뒤 내부 attempt는
cleanup과 replay 금지를 위해 보수적으로 `MAYBE_OBSERVED`로 분류한다.

- **`ERR-013`**: terminal open 결과는 `PeerObservation`을 함께 결정해야 한다.
- **`ERR-014`**: `MAYBE_OBSERVED` 결과는 같은 attempt의 자동 replay, 다른 binding 선택, Pipe resume를 발생시켜서는 안 된다.
- **`ERR-015`**: 알 수 없는 `ConnectorSessionId`, active state에 없는 terminal open/stream identity의 늦은 frame은 새 live state를 만들지 않고 제거해야 한다. current Pipe identity를 다른 session이 사용한 frame은 target state를 변경하지 않고 offending session의 `PROTOCOL_ERROR`가 된다. 지연·재사용 SDK `OPEN`은 SDK→Entry `ConnectorSession`의 `ConnectionId` high-watermark로, peer `OPEN`은 `PeerTransport`의 `StreamId` high-watermark로 거절한다. Owner Gateway는 current RelayStream에 결합된 `OpenIdentity`만 조회하며 종료된 identity의 remote ConnectorSession high-watermark나 tombstone을 만들지 않는다. 종료된 RT lease의 `Update`와 `KeepAlive`도 새 live state를 만들지 않는다.
- **`ERR-016`**: 현재 Rust SDK의 `Error::is_retryable()`은 code가 `UNAVAILABLE`, `DEADLINE_EXCEEDED` 또는 `RESOURCE_EXHAUSTED`이고 observation이 `NOT_OBSERVED`일 때만 `true`여야 한다. `MAYBE_OBSERVED`와 `OBSERVED`는 code와 관계없이 `false`다. 이 값은 application이 새 operation을 시작할 수 있다는 보수적 힌트이며 SDK의 자동 replay 명령이 아니다.

`OFFER`는 Listener 쪽 Pipe 생성 frame이지만 conforming Gateway는 같은 open attempt에서 한 번만 보내고 같은 `PipeId`를 재사용하지 않는다. SDK-Gateway transport의 방향별 순서와 `ConnectorSessionId`를 포함한 `PipeId` incarnation을 신뢰하므로 Listener SDK는 종료된 Pipe마다 `OFFER` tombstone을 보관할 필요가 없다. `OFFER` 재전송에 대한 복구 동작은 protocol 계약이 아니며, Gateway는 결과가 불확실하면 재전송 대신 selected `ListenerSession`을 종료한다.

Pipe가 열린 뒤 read EOF, write 실패와 transport close는 application payload 결과가 아니라 Pipe I/O 종료다. RelayGate 오류와 `PeerObservation`은 payload 처리 성공이나 delivery acknowledgement를 의미하지 않는다.

## 장애 처리 소유권

상태를 소유하고 불확실성을 감지한 component가 cleanup 범위를 결정한다. 다른 component의 내부 state를 직접 복구하지 않는다.

| 감지 owner | 불확실하거나 실패한 상태 | terminal 처리 | 자동 복구 범위 |
| --- | --- | --- | --- |
| Connector SDK | commit된 `OPEN`의 terminal 결과 불확실 | current `ConnectorSession` 종료, 소유 attempt·Pipe 실패 | 새 SDK-Gateway session만 연결; 과거 OPEN·Pipe·payload replay 없음 |
| Listener SDK | commit된 `REGISTER`의 terminal 결과 불확실, 또는 committed recovery `REGISTER` 중 returned Listener 제거 | current `ListenerSession` 종료, pending 최초 attempt 또는 closing Listener 종료 | 새 session에서 closing Listener를 제외한 이미 반환된 Listener만 재등록 |
| Gateway | 보낸 `OFFER`의 terminal 결과 불확실 | selected `ListenerSession` 종료, 소유 binding·pending Pipe·relay cleanup | 같은 attempt fallback 없음; Listener SDK가 새 session에서 returned Listener 재등록 |
| Entry Gateway | ConnectorSession 상실 | 해당 session의 current peer stream에 `RESET(CANCELLED)` commit; commit 불가 transport는 close | 과거 stream replay 없음; 성공한 RESET은 sibling session stream과 transport 유지, transport close는 그 transport의 모든 stream cleanup |
| Entry Gateway | commit된 peer `OPEN`의 terminal 결과 불확실 | `OPENING` RelayStream `RESET`, remote attempt와 Connector-side relay cleanup | 같은 attempt replay·reroute 없음; caller가 새 open을 결정 |
| Gateway 또는 SDK endpoint | identity로 격리 가능한 Pipe-local 오류 | 해당 Pipe `RESET`과 bounded cleanup | 기존 Pipe의 reroute·resume 없음 |

Gateway 내부 registry, `OFFER`, pending Pipe와 relay cleanup은 Gateway 책임이다. SDK는 자기 SDK-Gateway session과 local Pipe endpoint를 정리한다. 애플리케이션은 terminal Pipe 오류를 받은 뒤 새 `open(ClientId)`, application handshake, payload replay와 업무 retry 여부를 결정한다.

## 상태 scope와 identity

```text
Connector SDK runtime
  └── current ConnectorSession 0..1
        └── ConnectionId/Pipe 0..N

Listener SDK runtime
  ├── current ListenerSession 0..1
  ├── pending ListenAttempt 0..N
  └── Listener handle 0..N
        └── current ListenerBinding 0..1

Gateway route registration
  └── RegistrationKey
        └── current LeaseId 0..1
              └── MappingEntry 0..N

ShardDirectoryGeneration 1 ──► RT shard ── Resolve ──► Open attempt ──► Pipe
Gateway pair
  ├── PeerTransportSlot(dialer A) ──► PeerTransport 0..1 ──► RelayStream/Pipe
  └── PeerTransportSlot(dialer B) ──► PeerTransport 0..1 ──► RelayStream/Pipe
                                                               └── current OpenIdentity
```

| 대상 | State identity |
| --- | --- |
| ConnectorSession | cluster-global `ConnectorSessionId`; 현재 owner/provenance는 `EntryGatewayId` |
| ListenAttempt | `(Listener SDK runtime, reserved ClientId)`의 현재 pending operation |
| returned Listener handle | `(Listener SDK runtime, ClientId)`의 current non-`CLOSED` handle |
| local binding | `(GatewayId, ListenerSessionId, BindingId)` |
| RT mapping entry | `(GatewayId, ListenerSessionId, BindingId)` 안의 `ClientId` mapping |
| registration key | `(GatewayId, ListenerSessionId, ShardId)` |
| registration lease | `(RegistrationKey, LeaseId)` |
| RT shard state | `(ShardDirectoryGeneration, ShardId)` |
| Pipe | `PipeId = (ConnectorSessionId, ConnectionId)` |
| open attempt | `(EntryGatewayId, ConnectorSessionId, ConnectionId)` |
| peer open correlation | current `RelayStream` 안의 `OpenIdentity = (EntryGatewayId, ConnectorSessionId, ConnectionId)` |
| peer transport slot | `(unordered Gateway pair, DialerGatewayId)` |
| peer transport candidate | `(DialerGatewayId, PeerTransportId)` |
| relay stream | `(PeerTransport identity, StreamId)` |
| Pipe direction | `(Pipe identity, sender endpoint)` |

Gateway와 SDK가 종료한 object는 다시 활성화하지 않는다. 같은 logical 대상이 다시 필요하면 새 `BindingId`, `LeaseId`, `ConnectorSessionId`, `ConnectionId`, `PeerTransportId` 또는 transport-local `StreamId` identity를 만든다. RT에서도 `Deregister`, expiry 또는 restart로 종료된 lease의 operation은 registration이나 mapping을 다시 만들지 않는다.

## Canonical 전이표

| ID | 대상 | From | Event / guard | To | 관찰 결과 |
| --- | --- | --- | --- | --- | --- |
| `STATE-SDK-001` | SDK session | `DISCONNECTED` | 연결 시작 | `CONNECTING` | Gateway 연결 시도 |
| `STATE-SDK-002` | SDK session | `CONNECTING` | transport 성공 | `READY` | SDK operation 가능 |
| `STATE-SDK-003` | SDK session | `CONNECTING` | 연결 실패 | `DISCONNECTED` | managed backoff 뒤 재시도 가능 |
| `STATE-SDK-004` | SDK session | `READY` | transport loss, heartbeat timeout 또는 bounded outbound write 실패·deadline | `DISCONNECTED` | 기존 Pipe 종료, 새 session 재연결 가능 |
| `STATE-SDK-005` | SDK session | non-terminal | explicit close 또는 runtime의 마지막 public owner 소멸 | `CLOSED` | terminal, 자동 재연결 중단 |
| `STATE-CS-001` | ConnectorSession | `ABSENT` | Connector SDK transport 성공 | `LIVE` | 새 ConnectorSessionId와 attempt ownership 생성 |
| `STATE-CS-002` | ConnectorSession | `LIVE` | transport loss, explicit close, bounded outbound write failure 또는 commit된 OPEN terminal-response deadline | `CLOSED` | terminal, 소유 attempt와 Pipe 정리. current remote RelayStream마다 `RESET(CANCELLED)`을 commit하고 commit 불가 transport는 닫음 |
| `STATE-LA-001` | ListenAttempt | `ABSENT` | 같은 runtime에 동일 ClientId reservation/handle 없음 | `PENDING` | ClientId reservation 생성, live session의 bounded outbound path 대기 가능 |
| `STATE-LA-002` | ListenAttempt | `PENDING` | `REGISTER`가 current ListenerSession에 commit | `COMMITTED` | 같은 attempt를 다른 session으로 이동하거나 replay하지 않음 |
| `STATE-LA-003` | ListenAttempt | `COMMITTED` | `REGISTERED`, ClientKey 승인과 local binding 설치 확인 | `REGISTERED` | non-terminal, binding은 `ACTIVE`지만 public Listener handle은 아직 없고 reservation은 ListenAttempt에 남음 |
| `STATE-LA-004` | ListenAttempt | `PENDING` | commit 전 deadline, future drop·abort 또는 runtime close | `FAILED` | terminal, reservation과 내부 state 제거. future drop·abort이면 사용자 반환값 없음 |
| `STATE-LA-005` | ListenAttempt | `COMMITTED` | 등록 거절, response deadline, session 상실 또는 future drop·abort | `FAILED` | terminal, reservation 제거, 자동 재등록 금지; response deadline이나 미확정 내부 취소는 current ListenerSession 종료. future drop·abort이면 사용자 반환값 없음 |
| `STATE-LA-006` | ListenAttempt | `ABSENT` | 같은 runtime에 동일 ClientId reservation/handle 존재 | `FAILED` | `ALREADY_EXISTS`, object·registration·queue 생성 없음 |
| `STATE-LA-007` | ListenAttempt | `REGISTERED` | `listen` future가 `ACTIVE`를 확인하고 Listener handle을 반환 | `SUCCEEDED` | terminal, reservation을 returned Listener handle로 승계 |
| `STATE-LA-008` | ListenAttempt | `REGISTERED` | Listener handle 반환 전 future drop·abort, runtime close 또는 session 상실 | `FAILED` | terminal, reservation 제거와 자동 재등록 금지. live session이면 known binding을 `UNREGISTER`, session 상실이면 transport cleanup; future drop·abort이면 사용자 반환값 없음 |
| `STATE-LH-001` | returned Listener handle | `ABSENT` | ListenAttempt 성공과 handle 반환 | `ACTIVE` | local incoming Pipe 수신 가능, RT registration은 별도 상태 |
| `STATE-LH-002` | returned Listener handle | `ACTIVE` | SDK session 상실 | `SUSPENDED` | local binding이 있으면 제거, 신규 incoming 중단, accepted Pipe 실패와 old session의 미수락 queue 제거; RT registration 실패만으로는 전이하지 않음 |
| `STATE-LH-003` | returned Listener handle | `SUSPENDED` | 새 SDK session READY 또는 retry backoff 종료 | `REGISTERING` | 새 request identity로 recovery registration 선언 |
| `STATE-LH-004` | returned Listener handle | `REGISTERING` | local binding 설치 확인 | `ACTIVE` | 신규 incoming 재개, reconnect failure streak 초기화 가능 |
| `STATE-LH-005` | returned Listener handle | `REGISTERING` | transient 등록 실패 또는 recovery session 상실 | `SUSPENDED` | bounded backoff 뒤 새 recovery registration 가능 |
| `STATE-LH-006` | returned Listener handle | `REGISTERING/ACTIVE/SUSPENDED/BLOCKED` | explicit close | `CLOSED` | terminal, desired state·재등록과 pending accept 종료. 정상 ACTIVE close는 개별 UNREGISTER; committed recovery REGISTER 결과가 불확실하면 actor가 current ListenerSession reset |
| `STATE-LH-007` | returned Listener handle | `REGISTERING` | recovery `REGISTER`의 credential·permission terminal 거절 | `BLOCKED` | terminal registration state, 새 binding과 신규 admission 없음, old session 미수락 queue 제거와 pending·후속 accept 등록 오류, 자동 재등록·동일 handle 재활성화 금지, sibling handle과 shared session 유지; 새 credential은 close 뒤 새 `listen`으로 적용 |
| `STATE-LS-001` | ListenerSession | `ABSENT` | Gateway가 새 live session 생성 | `LIVE` | 같은 Gateway incarnation에서 재사용하지 않은 새 ListenerSessionId와 local registry 소유 |
| `STATE-LS-002` | ListenerSession | `LIVE` | transport loss, runtime explicit close, bounded outbound write failure, commit된 `REGISTER` terminal-response deadline, committed recovery `REGISTER` 중 returned Listener 제거 또는 Gateway `OFFER` terminal-response deadline | `CLOSED` | terminal, 소유 binding·Pipe와 session에 귀속된 미수락 queue 제거 및 lease deregister 시도; closing Listener를 제외한 returned sibling은 새 session 재등록 가능 |
| `STATE-BIND-001` | local ListenerBinding | `ABSENT` | ClientKey 승인, 새 BindingId 생성과 local 설치 | `ACTIVE` | 등록 성공, local OPEN 후보, route registration `UNSYNCED` 가능 |
| `STATE-BIND-002` | local ListenerBinding | `ACTIVE` | unregister 또는 session close | `REMOVED` | terminal, local 후보와 다음 snapshot에서 제외 |
| `STATE-REG-001` | Gateway RegistrationKey | `ABSENT` | shard에 첫 current binding 생김 | `UNSYNCED` | `Register`와 current snapshot `Update` 필요 |
| `STATE-REG-002` | Gateway RegistrationKey | `UNSYNCED` | active lease의 current snapshot 승인 | `SYNCED` | remote discovery registration 확인 |
| `STATE-REG-003` | Gateway RegistrationKey | `SYNCED` | local set 변경 | `UNSYNCED` | active lease revision 증가와 snapshot `Update` 필요 |
| `STATE-REG-004` | Gateway RegistrationKey | `SYNCED` | 해당 key의 `KeepAlive` 실패·stale lease 또는 shard의 공유 RT 연결 상실 | `UNSYNCED` | 공유 RT 연결 상실이면 그 연결을 공유하는 해당 shard의 active registration 각각에 이 전이를 적용한다. local binding은 유지한다. lease-bound `FAILED_PRECONDITION`은 의심 lease를 버리고 idempotent `Register` 판별을 한 번 수행한 뒤 성공한 current lease로 snapshot `Update`. 성공한 lease-bound operation만 판별 한도를 초기화 |
| `STATE-REG-005` | Gateway RegistrationKey | `UNSYNCED/SYNCED` | key binding 없음 | `ABSENT` | `Deregister` best effort와 local registration state 제거 |
| `STATE-REG-006` | Gateway RegistrationKey | non-terminal | `ListenerSession` 종료 | `REMOVED` | terminal, `Deregister` best effort와 local registration state 제거 |
| `STATE-REG-007` | Gateway RegistrationKey | `UNSYNCED/SYNCED` | 최초 `Register` mismatch, 판별 `Register` 실패 또는 판별 성공 뒤 다음 lease-bound operation도 `FAILED_PRECONDITION` | `UNSYNCED` | local binding 유지, 판별 `Register`는 복구 episode당 1회로 제한하고 terminal, 같은 process configuration으로 추가 재시도 금지 |
| `STATE-REG-008` | Gateway RegistrationKey | `UNSYNCED/SYNCED` | RT transport identity 또는 GatewayId authorization 실패 | `UNSYNCED` | local binding 유지, trust/authorization configuration 변경 전 자동 재시도 금지 |
| `STATE-MAP-001` | RT MappingEntry | `ABSENT` | active lease의 current snapshot에 포함 | `ACTIVE` | Resolve의 live BindingSet에 포함 |
| `STATE-MAP-002` | RT MappingEntry | `ACTIVE` | 새 snapshot에서 생략, deregister, expiry 또는 restart | `ABSENT` | 해당 mapping만 Resolve에서 제외 |
| `STATE-LEASE-001` | registration lease | `ABSENT` | `Register` 승인과 RT의 LeaseId 발급 | `VALID` | mapping이 없는 active lease 시작 |
| `STATE-LEASE-002` | registration lease | `VALID` | 같은 authenticated Gateway의 duplicate `Register` | `VALID` | current LeaseId 반환, mapping·revision·deadline 불변 |
| `STATE-LEASE-003` | registration lease | `VALID` | valid revision의 `Update` | `VALID` | mapping snapshot atomic replace, deadline 불변 |
| `STATE-LEASE-004` | registration lease | `VALID` | 유효한 `KeepAlive` | `VALID` | deadline 연장, mapping 불변 |
| `STATE-LEASE-005` | registration lease | `VALID` | `Deregister`, deadline 또는 RT restart | `ABSENT` | 해당 lease mapping 제거, tombstone과 revision history 없음 |
| `STATE-LEASE-006` | registration lease | `ABSENT` | 종료된 LeaseId의 늦은 `Update`, `KeepAlive` 또는 `Deregister` | `ABSENT` | update·keepalive는 `FAILED_PRECONDITION`, deregister는 idempotent success, state 생성 없음 |
| `STATE-LEASE-007` | current registration lease L2 | `VALID` | 과거 L1의 `Update`, `KeepAlive` 또는 `Deregister` | `VALID` | `FAILED_PRECONDITION`, L2와 mapping·deadline 불변 |
| `STATE-RT-001` | RT shard | `UNAVAILABLE` | process 시작 또는 restart 완료 | `READY` | configured generation의 memory-empty 상태로 요청 처리 |
| `STATE-RT-002` | RT shard | `READY` | process loss 또는 restart 시작 | `UNAVAILABLE` | memory state 소실, 기존 Pipe에는 영향 없음 |
| `STATE-OPEN-001` | Open attempt | `REQUESTED` | live ConnectorSession의 `open(ClientId)` | `RESOLVING` | full connection identity 생성, local 후보 확인 또는 RT Resolve |
| `STATE-OPEN-002` | Open attempt | `RESOLVING` | live binding 하나 선택 | `OPENING` | selected Owner Gateway에 OPEN |
| `STATE-OPEN-003` | Open attempt | `OPENING` | Listener incoming queue admission | `QUEUED` | Pipe 생성, Listener endpoint는 queue 또는 application에 존재 |
| `STATE-OPEN-004` | Open attempt | `QUEUED` | Connector가 OPENED 확인 | `SUCCEEDED` | `OBSERVED`, Connector Pipe 반환 |
| `STATE-OPEN-005` | Open attempt | non-terminal | FAILED, future drop·abort에 따른 내부 cancel 또는 deadline | `FAILED` | terminal 오류와 observation. future drop·abort이면 사용자 반환값 없이 내부 state만 terminal이 되며, `QUEUED`에서 실패하면 생성된 Pipe도 bounded cleanup |
| `STATE-PIPE-001` | Pipe | creation | Listener incoming queue admission | `OPEN` | Listener endpoint 사용 가능, Connector endpoint는 OPENED 확인 뒤 application에 반환 |
| `STATE-PDIR-001` | Pipe sender direction | `OPEN` | local `shutdown(write)` 또는 remote `FIN` | `FINISHED` | 먼저 수락한 bytes 뒤 peer read EOF, 반대 방향 불변 |
| `STATE-PDIR-002` | Pipe sender direction | `FINISHED` | duplicate `FIN` | `FINISHED` | idempotent no-op |
| `STATE-PDIR-003` | Pipe sender direction | `FINISHED` | 같은 sender의 `DATA` 수신 | `FINISHED` | `PROTOCOL_ERROR`, 해당 Pipe를 `RESET`하고 다른 stream은 유지 |
| `STATE-PIPE-002` | Pipe | `OPEN` | 양방향 `FINISHED` 또는 `CLOSE` | `CLOSED` | 정상 terminal, 양쪽 I/O와 state 제거 |
| `STATE-PIPE-003` | Pipe | `OPEN` | queue admission 뒤 open attempt 실패·OPENED 확인 유실, Connector SDK의 owner `CANCEL`, `RESET`, 소유 SDK session 종료 또는 필요한 transport loss | `CLOSED` | 실패 terminal, pending I/O와 accepted endpoint 실패; owner `CANCEL`은 해당 Pipe만 `RESET(CANCELLED)`로 닫고 sibling Pipe를 유지하며, 아직 accept되지 않은 endpoint는 incoming queue와 capacity에서 제거 |
| `STATE-PAIR-001` | PeerTransportSlot | `IDLE` | pair에 READY가 없고 slot의 dialer가 remote OPEN을 처리 | `CONNECTING` | 자기 방향 candidate 하나를 lazy 연결 |
| `STATE-PAIR-002` | PeerTransportSlot | `CONNECTING` | candidate handshake 성공 | `READY` | 방향별 reusable transport 사용 |
| `STATE-PAIR-003` | PeerTransportSlot | `CONNECTING` | candidate 연결 또는 handshake 실패 | `IDLE` | 해당 OPEN 실패, 이후 새 연결 가능 |
| `STATE-PAIR-004` | PeerTransportSlot | `READY` | slot transport loss | `IDLE` | 그 transport의 Pipe만 종료, 반대 slot은 불변 |
| `STATE-PAIR-005` | PeerTransportSlot | non-terminal | 참여 Gateway incarnation 종료 | `CLOSED` | terminal, 해당 slot의 candidate와 stream 정리 |
| `STATE-PT-001` | PeerTransport | `CONNECTING` | pair와 방향 handshake 성공 | `READY` | RelayStream multiplex 가능 |
| `STATE-PT-002` | PeerTransport | `CONNECTING` | 같은 slot duplicate 또는 연결·handshake 실패 | `CLOSED` | terminal, stream을 싣지 않음 |
| `STATE-PT-003` | PeerTransport | `READY` | transport loss, active heartbeat timeout 또는 shutdown | `CLOSED` | terminal, 포함된 Pipe 모두 종료 |
| `STATE-PT-004` | PeerTransport | `READY(stream_count=0)` | idle-retirement timeout | `CLOSED` | 정상 terminal, 이후 새 operation은 surviving transport를 쓰거나 lazy reconnect 가능 |
| `STATE-RS-001` | RelayStream | `ABSENT` | local actor가 StreamId를 할당하고 peer `OPEN`을 ordered writer queue에 commit하거나 valid remote `OPEN` 수신 | `OPENING` | full connection/binding identity와 bounded stream state 생성 |
| `STATE-RS-002` | RelayStream | `OPENING` | peer `OPENED` 수신 | `OPEN` | Listener queue admission 확인과 Pipe byte relay 가능; external observation은 아직 `MAYBE_OBSERVED`일 수 있음 |
| `STATE-RS-003` | RelayStream | `OPENING` | `FAILED`, `RESET`, stream-local protocol violation, terminal deadline 또는 PeerTransport loss | `CLOSED` | terminal cleanup. local `OPENING`에서 받은 `RESET` 또는 stream-local protocol violation은 Entry의 current attempt를 `FAILED(code, MAYBE_OBSERVED)`로 끝낸다. peer OPEN commit 전 실패는 `NOT_OBSERVED`, commit 뒤 결과 불명은 `MAYBE_OBSERVED` |
| `STATE-RS-004` | RelayStream | `OPEN` | 한 방향 `FIN` | `OPEN` | 해당 Pipe direction만 `FINISHED`, 반대 방향 relay 유지 |
| `STATE-RS-005` | RelayStream | `OPENING/OPEN` | 양방향 `FIN`, `CLOSE`, `RESET`, 소유 ConnectorSession 또는 PeerTransport 종료 | `CLOSED` | terminal, stream과 대응 Pipe state 제거. ConnectorSession 종료의 RESET commit 성공은 다른 stream과 transport를 유지하고, commit 불가로 transport를 닫으면 그 transport의 stream만 함께 종료 |

## 장애 전파

| 최초 장애 | 직접 변경되는 상태 | 유지되는 상태 | 신규 operation 결과 |
| --- | --- | --- | --- |
| Connector SDK-Gateway 단절 | ConnectorSession 종료, 소유 pending attempt 실패와 Pipe 정리, current remote RelayStream마다 `RESET(CANCELLED)` commit. commit 불가 PeerTransport는 close | Listener binding, RT mapping과 다른 ConnectorSession. RESET commit에 성공한 shared PeerTransport와 sibling stream | commit 전 open request는 새 session 준비를 기다릴 수 있음. commit 뒤 미확정 open request는 `MAYBE_OBSERVED`이며 replay 금지. transport close 시 그 transport의 다른 stream도 terminal |
| commit된 Connector OPEN terminal 응답 deadline 초과 | current ConnectorSession 종료, 해당 attempt는 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`, 소유 attempt·Pipe 정리 | Listener binding, RT mapping과 다른 ConnectorSession | 새 ConnectorSessionId로 재연결; 이전 OPEN·Pipe·payload replay 금지 |
| 동일 runtime의 Listener 중복 생성 | 새 object와 상태 없음 | 기존 pending reservation, Listener handle, binding, queue와 sibling handle | `ALREADY_EXISTS`; attempt 종료 또는 기존 handle 종료 뒤 새 생성 가능 |
| 정상 `ACTIVE` Listener handle close | 해당 desired와 binding 제거, pending accept와 미수락 Pipe 종료 | shared ListenerSession, sibling Listener와 이미 accept된 Pipe | 같은 ClientId는 handle 종료 뒤 새 `listen` 가능 |
| committed recovery `REGISTER` 중 returned Listener close/drop | closing Listener `CLOSED`, SDK actor가 current ListenerSession 종료, 그 session의 binding·Pipe 제거 | closing Listener를 제외한 returned sibling의 desired state | sibling만 새 ListenerSessionId로 재등록; closing Listener와 old Pipe replay 없음 |
| Listener SDK-Gateway 단절 | ListenerSession과 local binding 종료, accepted Pipe 실패, old session의 미수락 queue 제거, lease deregister 시도. commit된 pending ListenAttempt는 terminal 실패 | 다른 ListenerSession과 다른 Gateway binding | 새 ListenerSessionId로 재연결한 뒤 이미 반환된 Listener registration만 재등록. pending accept는 새 Pipe를 기다릴 수 있음 |
| commit된 Listener `REGISTER` terminal 응답 deadline 초과 | SDK의 current ListenerSession 종료, pending ListenAttempt terminal 실패; Gateway가 종료를 관찰하면 그 session의 binding·Pipe·route registration 제거 | 이미 반환된 Listener handle, 다른 ListenerSession과 다른 Gateway binding | 반환된 current Listener만 새 session·request identity로 재등록; 실패한 최초 attempt는 애플리케이션이 새로 시작할 수 있음 |
| selected ListenerSession의 `OFFER` terminal 응답 deadline 초과 | 해당 ListenerSession과 그 session의 모든 binding·Pipe 종료, current attempt는 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED` | 다른 ListenerSession, sibling binding과 그 Pipe | 새 ListenerSessionId로 반환된 current Listener만 재등록; 같은 attempt reroute 금지 |
| Listener의 명시적 `OFFER_REJECTED` | 해당 open attempt만 실패 | selected ListenerSession, sibling binding과 기존 Pipe | caller가 원하면 새 open 가능 |
| recovery `REGISTER`의 ClientKey 영구 거절 | 해당 returned Listener handle `BLOCKED`, old session 미수락 queue 제거, pending·후속 accept 등록 오류, 새 binding과 신규 admission 없음 | sibling handle과 shared session | 기존 handle을 닫고 새 credential로 새 `listen` 가능; 자동 재등록 없음 |
| Gateway-RT shard 연결 단절 | 해당 shard 연결을 공유하는 active registration 각각 `UNSYNCED` | local binding, ListenerSession, established Pipe와 다른 shard registration | local shortcut 가능, 해당 shard의 remote resolve/register/update는 `UNAVAILABLE` 가능. 연결 복구 뒤 보존한 current desired state로 각 registration을 검증·publish |
| Gateway-RT component identity 또는 channel integrity 실패 | 영향받은 registration `UNSYNCED`, 진행 중인 `Resolve` 실패, RT state 변경 없음 | local binding, ListenerSession, established Pipe와 기존 RT state | registration은 deployment trust 수정 전 재시도 금지. remote open은 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`, `NOT_OBSERVED`이며 새 operation만 가능 |
| RegistrationKey의 GatewayId와 authenticated Gateway identity mismatch | 해당 registration `UNSYNCED`, RT mutation 없음 | local binding, ListenerSession, established Pipe와 기존 RT state | `PERMISSION_DENIED`; authorization configuration 수정 전 registration 재시도 금지 |
| Gateway-RT generation mismatch | registration `UNSYNCED`, remote open attempt 실패 | local binding, ListenerSession, established Pipe와 RT state | 최초 `Register`면 즉시 terminal. lease-bound operation에서 처음 관찰되면 idempotent `Register` 판별 1회만 허용하고, 판별 실패 또는 판별 성공 뒤 다음 lease-bound 실패면 terminal; compatible generation으로 process restart 필요 |
| RT restart | RT lease와 mapping 소실 | Gateway local binding, established Pipe | READY-empty 수렴 중 `NOT_FOUND`, 새 lease 등록과 current snapshot update 뒤 복구 |
| Gateway process loss | 해당 Gateway의 session, Pipe와 peer transport 종료 | 다른 Gateway와 RT의 다른 registration | lease expiry까지 stale mapping 가능, Owner OPEN은 실패 |
| 종료된 lease의 늦은 `Update` 또는 `KeepAlive` | RT state 불변, operation 실패 | Gateway-local state와 다른 active registration | `FAILED_PRECONDITION`; 새 mapping은 새 lease를 `Register`한 뒤 current snapshot으로만 구성 |
| stale mapping 선택 | 해당 open attempt 실패 | local binding truth와 다른 candidate | `UNAVAILABLE`, `NOT_OBSERVED`, 같은 attempt reroute 없음 |
| Listener queue admission 뒤 OPENED 유실 | 생성된 queued 또는 accept된 Pipe 종료와 cleanup | 다른 attempt와 binding | terminal 오류, `MAYBE_OBSERVED`, 자동 replay 없음 |
| PeerTransport credential/name 실패 또는 authenticated claim mismatch | candidate `CLOSED`, 해당 open attempt 실패 | 기존 PeerTransport, 다른 stream, binding과 RT mapping | credential/name 실패는 `UNAUTHENTICATED`, 인증 뒤 runtime owner·pair·direction mismatch는 `PERMISSION_DENIED`; 둘 다 `NOT_OBSERVED`, stream 생성 없음 |
| peer connect 또는 handshake deadline | candidate `CLOSED`, 해당 방향 slot `IDLE`, attempt state 제거 | 반대 방향 slot, 기존 PeerTransport·Pipe, binding과 RT mapping | `DEADLINE_EXCEEDED`, `NOT_OBSERVED`; 이후 새 operation만 lazy connect 가능 |
| peer `OPEN` commit 뒤 terminal 결과 deadline | 해당 RelayStream과 attempt `CLOSED`, best-effort `RESET` | 같은 transport의 다른 stream, sibling binding과 RT mapping | `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`; 같은 attempt replay·reroute 금지 |
| `OPENING` remote attempt cancel | commit 전 state 제거 또는 commit 뒤 `RESET(CANCELLED)`과 RelayStream cleanup | 같은 transport의 다른 stream과 다른 attempt | 별도 peer `CANCEL` 없음; 늦은 OPENED/FAILED/RESET은 state를 부활시키지 않음 |
| PeerTransport loss | 포함된 RelayStream과 Pipe 종료, 해당 방향 slot `IDLE` | RT mapping, Listener registration, 반대 방향 PeerTransport와 그 stream | 이후 open은 살아 있는 transport를 재사용하거나 새 transport 연결 |
| 실행 중 `PeerEvent` queue Full 또는 receiver 종료 | peer runtime terminal, distributed Gateway fail-closed shutdown, current PeerTransport·SDK session·attempt·Pipe 정리 | 다른 Gateway process와 RT의 current state | 해당 Gateway의 새 operation은 `RESOURCE_EXHAUSTED` 또는 `UNAVAILABLE`; supervisor가 새 process를 시작하고 Listener SDK가 current state를 재등록한 뒤 새 operation 가능. 이미 시작된 정상 shutdown의 Full/Closed는 추가 장애가 아님 |
| stream `FIN` 뒤 같은 방향 `DATA` | 해당 RelayStream과 Pipe `RESET` | 다른 RelayStream과 PeerTransport | `PROTOCOL_ERROR`; 새 open만 가능 |
| SDK-Gateway heartbeat timeout | current SDK session과 그 session의 pending attempt·Pipe 종료 | 다른 SDK session, Gateway local binding, RT mapping | SDK runtime은 새 session으로 managed reconnect 가능. commit된 open, Pipe와 payload replay 없음 |
| active PeerTransport heartbeat timeout | 해당 PeerTransport와 포함된 RelayStream·Pipe 종료, 해당 방향 slot `IDLE` | 반대 방향 PeerTransport, 다른 Gateway pair, RT mapping과 Listener binding | 이후 새 open은 surviving transport 또는 새 lazy transport만 사용. 기존 Pipe replay·reroute 없음 |
| zero-stream PeerTransport idle-retirement timeout | 빈 PeerTransport 정상 종료, 해당 방향 slot `IDLE` | Listener binding, RT mapping, established Pipe 없음 | 이후 필요할 때 lazy reconnect. keepalive traffic 없음 |

## 공통 불변식

1. SDK와 Gateway의 `CLOSED`, `REMOVED`, `SUCCEEDED`, `FAILED` object는 다시 활성화하지 않는다. RT의 종료된 lease도 `Update`나 `KeepAlive`로 다시 활성화하지 않는다.
2. 끝까지 await한 operation은 terminal 결과를 한 번만 외부에 노출한다. future를 drop 또는 abort하면 외부 결과는 없고 내부 attempt만 정확히 한 번 terminal이 된다.
3. close, cancel, deregister, expiry와 cleanup은 중복·순서 변경에도 다른 live identity를 변경하지 않는다.
4. 다른 ListenerSession의 같은 `ClientId` binding은 서로 충돌하지 않는다.
5. established Pipe는 RT, registration 또는 peer transport 재구성의 복구 대상이 아니다.
6. terminal state의 live entry와 buffer는 configured bound 안에 제거한다.
7. generic `UNKNOWN`으로 오류 원인, registration sync와 peer observation을 합치지 않는다.
8. ConnectorSession보다 attempt와 Connector Pipe endpoint가 오래 존재할 수 없고, 새 session은 이전 session의 identity나 결과를 승계하지 않는다.
9. Gateway와 RT process의 `ShardDirectoryGeneration`은 process 수명 동안 불변이고, mismatch operation은 RT state를 읽거나 변경하지 못한다.
10. 한 Listener SDK runtime에서 같은 `ClientId`의 pending reservation과 non-`CLOSED` handle은 합쳐서 최대 하나이며 실패한 attempt는 reservation이나 live state를 남기지 않는다.
11. `FIN`은 한 방향만 닫고, 양방향 `FIN` 또는 `CLOSE`는 정상 전체 종료이며, `RESET`은 실패 전체 종료다. stream-local protocol 위반은 다른 stream이나 shared transport로 전파하지 않는다.
12. `ConnectorSessionId`는 cluster-global unique로 취급한다. `GatewayId`, `ListenerSessionId`와 `BindingId`를 포함한 다른 incarnation identity는 정의된 부모 scope의 lifetime 동안 재사용하지 않는다. stale frame이나 mapping은 새 incarnation과 같은 identity가 될 수 없다.
13. internal RT mutation과 PeerTransport handshake의 `GatewayId`는 authenticated transport identity에 결합되어야 하며 claimed identifier만으로 authority를 얻지 못한다. peer `OPEN`의 `OpenIdentity.EntryGatewayId`도 authenticated peer `GatewayId`와 일치해야 한다.
14. local `ListenerBinding`의 `ACTIVE/REMOVED` 상태는 RT registration의 `SYNCED/UNSYNCED` 상태와 독립적이다. RT 장애는 remote discovery를 약화시킬 수 있지만 live local binding을 비활성화하지 않는다.
15. 한 `ListenerSession`의 liveness failure는 그 session이 공유하는 모든 binding과 Pipe에 전파될 수 있지만 다른 ListenerSession으로 전파되어서는 안 된다.
16. SDK-Gateway session과 `stream_count > 0`인 PeerTransport는 activity-aware heartbeat를 사용한다. commit된 probe는 response deadline 전 matching `PONG`만 만족시킨다. heartbeat timeout은 transport loss와 같이 current session 또는 transport를 닫고 기존 cleanup 경로로 수렴한다. application Pipe read idle은 heartbeat failure가 아니며, RT registration의 `KeepAlive`는 별도 계약이다.
17. `REGISTER` response timeout 뒤 SDK는 old session request를 replay하지 않는다. pending `ListenAttempt`는 terminal 실패하고, new session에는 이미 반환된 current Listener만 새 registration operation으로 선언한다.
18. silent partition에서는 SDK와 Gateway가 같은 `ListenerSession`의 종료를 동시에 관찰한다고 보장하지 않는다. Gateway가 아직 보유한 old binding은 live 성공의 증명이 아니며 transport event 또는 그 binding에 대한 `OFFER` deadline에서 제거된다.
19. 명확한 Pipe-local 오류는 해당 Pipe의 `RESET`으로 격리하고, commit된 control operation의 terminal 결과를 owner가 확정할 수 없으면 그 owner가 current session을 종료한다. session reset은 과거 OPEN, Pipe 또는 payload replay의 근거가 아니다.
20. conforming Gateway는 같은 open attempt의 `OFFER`를 재전송하거나 `PipeId`를 재사용하지 않는다. Listener SDK의 memory 크기는 현재 live Pipe에 비례하며 종료된 `PipeId`별 tombstone에 비례해서는 안 된다.
21. Gateway의 `ClientId -> ClientKey` startup configuration은 process 수명 동안 불변이다. runtime key 발급·rotation·revocation event는 없으며 replacement Gateway의 recovery `REGISTER` 거절만 returned Listener를 `BLOCKED`로 만들 수 있다.
22. Listener handle은 shared session token을 직접 취소하지 않는다. 정상 ACTIVE close는 개별 binding cleanup이고, committed recovery REGISTER 중 desired 제거처럼 wire 결과가 불확실한 경우에만 shared Listener actor가 session reset을 결정한다.
23. Gateway는 current Pipe를 변경하기 전에 frame role과 sender ownership을 phase보다 먼저 검증한다. unknown·stale identity는 no-op이고 current identity의 non-owner frame은 target Pipe를 변경하지 않은 채 offending session을 종료한다. owner의 invalid phase frame은 해당 Pipe만 `RESET`한다. Connector SDK의 owner `CANCEL`은 `OPENED` application 반환 경합을 닫는 terminal frame이므로 Gateway Pipe가 `OPEN`이어도 유효하다.
24. Listener `accept`와 session 종료는 dequeue 뒤 마지막 `ACTIVE` 및 Pipe non-terminal 확인을 성공 선형화점으로 삼아 한 순서로 직렬화한다. 이 확인이 먼저면 반환된 Pipe가 직후 failure를 관찰할 수 있고, session 종료가 먼저면 old session의 미수락 Pipe를 제거한다. 성공 확인 전에 관찰된 `BLOCKED/CLOSED`는 queue보다 우선하며 terminal 오류를 반환한다.
25. PeerTransport endpoint의 단일 actor는 local `StreamId` counter 할당과 peer `OPEN` writer-queue commit을 같은 순서로 직렬화한다. 할당한 counter는 commit 실패 뒤에도 재사용하지 않는다.
26. remote OPEN cancel은 `OPENING` RelayStream의 `RESET(CANCELLED)`로 표현하고 별도 peer `CANCEL` frame을 두지 않는다.
27. local/CI `InternalGatewayKey`는 transport adapter의 startup credential일 뿐 mapping, lease, stream 또는 로그 state가 아니다. claimed `GatewayId`만으로 RT 또는 peer authority를 얻을 수 없고 plain TCP adapter를 production confidentiality/integrity 보장으로 취급하지 않는다.
28. SDK-origin `ConnectionId` high-watermark는 ordered SDK→Entry `ConnectorSession`에서만 유지한다. peer leg는 transport-local initiator-bit `StreamId` high-watermark를 사용하며 Owner Gateway는 current `RelayStream`의 `OpenIdentity`만 보유한다. 종료 뒤 remote ConnectorSession high-watermark, `OpenIdentity` tombstone과 replay table을 남기지 않는다.
29. ConnectorSession 종료 cleanup의 peer `RESET`을 bounded writer queue에 commit할 수 없으면 해당 PeerTransport를 닫아 transport-loss cleanup으로 수렴한다. 이미 수립된 stream을 다른 transport로 이동하거나 replay하지 않는다.
30. peer authentication 뒤에는 conforming Gateway가 frame과 identity scope를 지킨다고 전제한다. Byzantine Gateway의 semantic replay 방어는 범위 밖이며 `StreamId` high-watermark를 durable replay protection으로 해석하지 않는다.
31. 실행 중 내부 `PeerEvent`는 Gateway 상태 전이 입력이므로 관측성 event처럼 버리지 않는다. bounded queue 포화나 receiver 종료는 distributed Gateway를 fail-closed로 끝내고, 정상 shutdown과 경쟁한 전달 실패만 idempotent cleanup으로 처리한다.
32. `stream_count == 0`인 PeerTransport는 heartbeat를 보내지 않고 idle-retirement timeout 뒤 정상 종료한다. 새 stream이 재사용하면 retirement timer를 취소한다.

shard lease expiry나 RT restart로 mapping이 사라져도 Gateway-local `ListenerBinding`이나 live `ListenerSession`은 닫히지 않는다. `ACTIVE` local binding은 local OPEN에 계속 사용할 수 있고, Gateway가 새 lease를 `Register`한 뒤 current snapshot을 `Update`하여 RT mapping을 다시 구성한다.
