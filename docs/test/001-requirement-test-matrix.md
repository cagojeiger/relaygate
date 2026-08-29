# TEST 001: 요구사항 검증 매트릭스

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 기준 | [SPEC 001](../spec/001-terminology-and-object-model.md) ~ [SPEC 007](../spec/007-error-and-state-model.md) |

이 문서는 SPEC 요구사항을 검증 시나리오에 연결한다. 새로운 동작 규칙은 정의하지 않는다.

## 검증 원칙

1. 시간 기반 전이는 fake monotonic clock으로 결정적으로 검증한다.
2. publish, refresh, release, cancel, close, expiry와 late response의 순서를 바꿔 반복한다.
3. terminal 결과 뒤에는 해당 attempt, session, binding, lease, stream과 buffer의 live state가 남지 않는지 확인한다.
4. timeout, queue와 buffer의 구체적인 기본값이 아니라 configured bound 준수 여부를 검증한다.
5. multi-index의 forward/reverse view와 Gateway local/publication view가 같은 live set을 나타내는지 확인한다.

## 용어와 SDK

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-TERM-01` | `TERM-001`, `TERM-002`, `TERM-003`, `TERM-029` | Connector/Listener 역할, 위치와 분리된 non-empty UTF-8 ClientId, 등록 전용 ClientKey 경계가 유지된다. 정규화 형태나 case가 다른 ClientId bytes를 암묵적으로 합치지 않는다. |
| `T-TERM-02` | `TERM-004`, `TERM-011`, `TERM-025` | Listener SDK runtime은 current ListenerSession을 `0..1`개 공유하고 각 handle은 desired ClientId 하나와 current binding `0..1`개만 가진다. 같은 runtime의 non-CLOSED handle 사이에서 desired ClientId는 unique다. |
| `T-TERM-03` | `TERM-005`, `TERM-006`, `TERM-007`, `TERM-008`, `TERM-015` | 여러 session과 ClientId를 교차 등록하고 BindingId가 같은 ListenerSession의 서로 다른 ClientId와 제거된 incarnation 사이에서 unique하며 재사용되지 않는지 확인한다. N:M cardinality, 중복 방지와 live-only BindingSet도 확인한다. |
| `T-TERM-04` | `TERM-009`, `TERM-010` | 후보가 여러 개여도 한 connect가 Pipe 하나와 Listener endpoint 하나만 만든다. |
| `T-TERM-05` | `TERM-012`, `TERM-028` | 일반 identifier는 equality 외의 구조·정렬·위치 의미에 의존하지 않는다. ConnectionId는 ConnectorSession-local 증가 순서만, StreamId는 transport-local initiator bit와 counter 규칙만 해석하며 application 의미를 부여하지 않는다. |
| `T-TERM-06` | `TERM-013`, `TERM-014`, `TERM-016`, `TERM-026` | Gateway restart가 같은 locator를 재사용해도 새 GatewayId를 만들고 old incarnation과 구분하는지 확인한다. dialer별 PeerTransportSlot, pair당 READY 최대 두 개, RelayStream cardinality와 lifetime도 확인한다. |
| `T-TERM-07` | `TERM-017`, `TERM-023`, `TERM-024` | 한 ConnectorSession의 concurrent connect가 전송 순서상 strictly increasing ConnectionId를 사용하고 Gateway는 high-watermark 하나만 유지한다. 재연결마다 새 ConnectorSessionId가 생겨 같은 ConnectionId 값도 full identity로 분리되며 application 인증이나 delivery identity로 사용하지 않는다. |
| `T-TERM-08` | `TERM-018`, `TERM-019`, `TERM-020`, `TERM-021`, `TERM-022`, `TERM-027` | ShardDirectory가 shard별 stable logical endpoint를 정확히 하나만 포함하고 mapping을 포함하지 않으며 exact artifact bytes의 SHA-256 generation으로 process 동안 불변인지 확인한다. Gateway의 LeaseId 비재사용과 늦은 기존 Publish의 RT 재관찰을 구분하고, publication scope당 current lease 하나, revision 단조 증가와 Gateway remote mapping 비보관도 검증한다. |
| `T-SDK-01` | `SDK-001`, `SDK-002`, `SDK-003`, `SDK-004` | connect 성공 전에 Listener queue 적재를 확인하고 distinct Pipe가 accept 한 번에 하나씩 반환된다. queue full이면 기존 항목은 유지된다. |
| `T-SDK-02` | `SDK-005` | pending accept 취소가 다른 accept, sibling Listener handle과 queued Pipe에 영향을 주지 않는다. |
| `T-SDK-03` | `SDK-006` | Listener handle close를 queue admission·pending accept와 경쟁시킨다. 한 순서만 확정되고 close 뒤 미수락 Pipe는 반환되지 않으며 shared session, sibling handle과 이미 accept된 Pipe는 유지된다. |
| `T-SDK-04` | `SDK-007`, `SDK-009` | 양방향 byte 순서를 보존하고 write 성공을 peer application 처리 성공으로 보고하지 않는다. |
| `T-SDK-05` | `SDK-008` | incoming/Pipe capacity 소진 시 memory가 증가하지 않고 backpressure 또는 명시적 실패가 관찰된다. |
| `T-SDK-06` | `SDK-010`, `SDK-020`, `SDK-021` | write-direction shutdown은 먼저 수락한 bytes 뒤 EOF를 만들고 반대 방향 read는 계속된다. 양방향 FIN, CLOSE와 RESET을 구분하고 terminal 신호 반복은 같은 결과다. outbound capacity를 기다리는 local CLOSE와 remote RESET·session failure를 경쟁시켜 먼저 확정된 terminal 의미가 뒤집히지 않으며, CLOSE를 delivery acknowledgement로 보지 않는다. |
| `T-SDK-07` | `SDK-011`, `SDK-012`, `SDK-013`, `SDK-018`, `SDK-022` | Gateway 단절 뒤 Connector/Listener에 새 session identity를 만들고 live Listener handle의 current desired set만 다시 선언한다. 내부 outbound capacity보다 많은 desired handle도 누락하지 않는다. outbound path commit 전 connect만 새 session 준비를 기다릴 수 있고 commit 뒤 응답을 잃으면 미도달 증명이 없는 한 `MAYBE_OBSERVED`이며 이전 request, attempt, Pipe와 payload를 복구하거나 replay하지 않는다. |
| `T-SDK-08` | `SDK-014` | connect/accept 성공과 cancel을 경쟁시켜 terminal 결과가 한 번만 반환되는지 확인한다. |
| `T-SDK-09` | `SDK-015` | 필요한 SDK/peer transport를 끊으면 영향받은 Pipe가 종료되고 새 session으로 이동하지 않는다. |
| `T-SDK-10` | `SDK-017` | credential 거절이나 권한 폐기는 영향받은 handle만 `BLOCKED`로 만들고 configuration 변경 전 자동 재등록하지 않는다. 폐기 전에 admission을 마친 queued·accepted Pipe와 sibling handle, shared session은 유지하고 신규 Pipe만 중단한다. |
| `T-SDK-11` | `SDK-019` | 같은 runtime에서 동일 ClientId Listener를 동시에 생성하면 정확히 하나만 성공하고 나머지는 `ALREADY_EXISTS`다. handle, registration과 queue가 하나뿐인지 확인하고, close 뒤 재생성은 새 binding incarnation으로 성공한다. |

## Gateway 등록과 publication

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-REG-01` | `REG-001`, `REG-002`, `REG-003`, `REG-004` | 여러 session이 같은 ClientId를 등록하고 한 session은 여러 ClientId를 등록한다. current 반복 등록은 같은 BindingId이고 제거 후 재등록은 새 BindingId다. |
| `T-REG-02` | `REG-005`, `REG-006` | invalid/unauthorized ClientKey는 local binding과 RT projection을 만들지 않으며 application 인증으로 사용되지 않는다. |
| `T-REG-03` | `REG-007`, `REG-008`, `SDK-016` | ClientKey 승인과 local 설치 뒤 binding이 즉시 ACTIVE가 되고 등록 성공을 반환한다. 최초 RT publish를 실패시켜도 local OPEN은 가능하고 scope만 UNSYNCED이며, 등록 성공이 remote discovery 완료를 뜻하지 않는지 확인한다. |
| `T-REG-04` | `REG-009`, `REG-014` | binding 제거와 권한 폐기는 local index에서 즉시 제외되고 새 snapshot에서 빠진다. 다른 binding과 sibling handle은 유지된다. |
| `T-REG-05` | `REG-010`, `REG-011`, `REG-012` | 한 session의 binding을 여러 shard publication으로 나누고 scope별 LeaseId, revision과 직렬화가 독립적인지 확인한다. lease 교체 뒤에도 revision이 증가한다. |
| `T-REG-06` | `REG-013`, `REG-015`, `REG-016`, `REG-017`, `REG-020` | session 종료의 release 유실·늦은 Publish, RT 단절과 복구를 주입한다. local state가 되살아나지 않고 scope별 sync 상태와 stale projection 격리가 유지되며 current snapshot만 재게시하는지 확인한다. |
| `T-REG-07` | `REG-018`, `REG-019`, `REG-021` | Gateway가 process-fixed generation과 shard directory, local registry 및 자신의 publication state 외에 RT 전체 table이나 remote Resolve 결과를 복구 source로 보관하지 않는다. generation mismatch와 RT transport identity/GatewayId mismatch는 UNSYNCED와 local binding을 유지하고 관련 configuration 변경 전 같은 실패를 자동 재시도하지 않는다. |

## RouteTable

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-RT-01` | `RT-001`, `RT-002`, `RT-003`, `RT-004`, `RT-005`, `RT-006`, `RT-007`, `RT-008` | 동일 directory bytes의 content hash와 `sha256-modulo-v1` authority 결정성, shard별 key·scope 격리를 검증한다. endpoint가 없거나 독립 endpoint가 여러 개인 shard record는 invalid다. byte·순서 변경은 generation을 다시 계산하고 다른 generation operation을 거절한다. unauthenticated caller와 authenticated GatewayId가 scope와 다른 mutation은 state를 읽거나 바꾸지 않으며 replication 보장을 가정하지 않는다. |
| `T-RT-02` | `RT-010`, `RT-011`, `RT-012`, `RT-013`, `RT-014`, `RT-015`, `RT-016` | full snapshot의 atomic replace, equal revision idempotency, conflicting·lower revision 거절, 더 높은 revision의 lease 교체, stale LeaseId 격리와 다른 scope 보존을 확인한다. |
| `T-RT-03` | `RT-017`, `RT-020`, `RT-021`, `RT-022`, `RT-023`, `RT-024`, `RT-025` | Publish, Refresh, Release와 Expire를 중복·재정렬한다. scope가 비면 늦은 Publish를 수락하고 뒤따른 current Refresh만 연장하지만, tombstone·expiry record를 누적하지 않고 마지막 늦은 갱신 뒤 lease lifetime 안에 제거한다. |
| `T-RT-04` | `RT-030`, `RT-031`, `RT-032`, `RT-033`, `RT-034` | Resolve가 모든 current projection만 반환하고 선택·정렬·도달성 보장을 추가하지 않으며 READY-empty와 unavailable을 구분한다. |
| `T-RT-05` | `RT-040`, `RT-041`, `RT-042`, `RT-043` | process down의 unavailable과 restart 직후 READY-empty를 구분하고 current snapshot으로만 점진적으로 다시 구성한다. |
| `T-RT-06` | `RT-044` | RT 중단·restart·replace·release·expiry 중에도 필요한 data transport가 살아 있는 기존 Pipe는 유지된다. |
| `T-RT-07` | `RT-050`, `RT-051`, `RT-052`, `RT-053`, `RT-054` | local binding이 없는 신규 연결은 매번 authority를 조회한다. forward/reverse index mutation은 table scan과 partial/orphan state 없이 atomic하게 적용되며 ConnectorSession과 ConnectionId는 RT에 들어가지 않는다. |

## 연결 수립과 peer relay

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-OPEN-01` | `OPEN-001`, `OPEN-002` | Entry Gateway는 local ACTIVE set이 non-empty면 그 안에서 하나를 열고 RT를 호출하지 않는다. local set이 비었을 때만 authority를 조회하며 READY-empty와 RT unavailable을 구분한다. |
| `T-OPEN-02` | `OPEN-003`, `OPEN-009` | 여러 후보에서 하나만 선택하고 다른 Listener에는 OPEN이나 Pipe가 생기지 않는다. |
| `T-OPEN-03` | `OPEN-004`, `OPEN-005` | Resolve 뒤 binding 제거·재등록, handle close, 권한 폐기와 locator 재사용을 queue admission과 경쟁시킨다. Owner가 BindingId까지 재검증하고 admission과 제거를 한 순서로 직렬화한다. 제거가 먼저면 stale candidate를 `NOT_OBSERVED`로 거절하고 admission이 먼저면 이미 열린 Pipe만 기존 lifecycle을 따른다. |
| `T-OPEN-04` | `OPEN-006` | local과 remote one-hop 경로가 동일한 SDK Pipe 결과를 만든다. |
| `T-OPEN-05` | `OPEN-007`, `OPEN-008` | queue 적재 뒤 OPENED일 때만 connect가 성공하며 application accept·인증을 기다리지 않는다. |
| `T-OPEN-06` | `OPEN-010`, `OPEN-011` | OPEN 또는 Pipe 수립 뒤 route 변화와 transport 단절이 reroute, replay 또는 resume를 만들지 않는다. |
| `T-OPEN-07` | `OPEN-012`, `OPEN-013`, `OPEN-014`, `OPEN-019` | request commit 뒤 Gateway 수신 전·Owner queue 전·queue 후에 각각 응답 유실, cancel, deadline과 late frame을 경쟁시킨다. queue 미도달 증명이 있을 때만 `NOT_OBSERVED`, OPENED 확인만 `OBSERVED`, 어느 쪽도 증명할 수 없으면 실제 queue 여부와 관계없이 `MAYBE_OBSERVED`이며 terminal 결과 하나와 bounded cleanup을 확인한다. |
| `T-OPEN-08` | `OPEN-015` | Pipe 수립 뒤 RT를 중단해도 payload와 close가 RT를 요구하지 않는다. |
| `T-OPEN-09` | `OPEN-016`, `OPEN-017`, `OPEN-020`, `OPEN-021` | 같은 ConnectionId 값을 서로 다른 ConnectorSession과 Entry Gateway에서 사용하고 한 session에는 증가·중복·낮은 OPEN을 주입한다. Gateway는 remote high-watermark 하나로 중복·지연 OPEN을 제거하고 terminal history를 누적하지 않는다. full identity별 결과는 하나이며 session 단절은 소유 attempt와 Pipe만 정리하고 새 session은 이전 결과를 승계하지 않는다. |
| `T-OPEN-10` | `OPEN-018` | Resolve 결과는 해당 attempt가 선택 또는 종료된 뒤 폐기되고 다음 connect가 새 Resolve를 수행한다. |
| `T-OPEN-11` | `OPEN-022` | Entry Gateway와 RT의 generation을 다르게 하여 remote connect가 `FAILED_PRECONDITION`, `NOT_OBSERVED`로 한 번만 끝나고 다른 shard·binding으로 재시도하거나 Pipe를 만들지 않는지 확인한다. |
| `T-OPEN-12` | `OPEN-023` | local 후보가 없을 때 RT `Resolve` channel의 component identity와 authorization 실패를 주입한다. connect는 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`, `NOT_OBSERVED`로 한 번만 끝나고 binding 선택, peer OPEN, Listener queue와 Pipe를 만들지 않으며 같은 attempt를 재시도하지 않는다. |
| `T-PEER-01` | `PEER-001` | local path는 peer를 쓰지 않고 remote path는 선택된 Owner Gateway만 한 번 경유한다. |
| `T-PEER-02` | `PEER-002`, `PEER-003`, `PEER-004`, `PEER-005` | 같은 Gateway의 동시 OPEN은 자기 방향 candidate 하나를 공유한다. 양 Gateway가 동시에 처음 dial하면 winner 합의 없이 방향별 하나씩 최대 두 transport가 READY가 되고, 같은 방향 duplicate만 stream 전에 닫힌다. claimed GatewayId가 authenticated transport peer와 다르면 candidate는 READY가 되지 않고 해당 OPEN은 `UNAUTHENTICATED`, `NOT_OBSERVED`로 끝나며 stream이나 pair state를 남기지 않는다. |
| `T-PEER-03` | `PEER-006`, `PEER-007`, `PEER-015`, `PEER-016` | 양 endpoint가 같은 counter로 동시에 stream을 열어 짝수·홀수 StreamId가 충돌하지 않는지 확인한다. failed·closed ID의 delayed duplicate OPEN은 bounded remote high-watermark로 거절하고 counter exhaustion은 기존 stream을 유지한 채 새 OPEN만 실패한다. |
| `T-PEER-04` | `PEER-008`, `PEER-009` | stream 및 aggregate capacity를 각각 소진해 bounded memory, backpressure와 명시적 resource failure를 확인한다. |
| `T-PEER-05` | `PEER-010`, `PEER-017`, `PEER-018`, `PEER-019`, `PEER-020`, `PEER-021` | OPEN 결과와 DATA/FIN/CLOSE/RESET 순서를 바꾼다. FIN은 한 방향만 닫고, 양방향 FIN·CLOSE는 정상 종료, RESET은 실패 종료이며, FIN 뒤 DATA는 해당 stream만 reset한다. duplicate terminal·late frame은 재활성화나 unbounded history를 만들지 않는다. |
| `T-PEER-06` | `PEER-011`, `PEER-012` | 한 transport 단절은 그 transport의 Pipe만 닫고 반대 방향 transport와 stream을 유지한다. 이후 새 Pipe는 surviving transport 또는 새 lazy transport만 사용한다. |
| `T-PEER-07` | `PEER-013` | connection failure, 같은 방향 duplicate와 loss 뒤 candidate·stream·buffer가 configured bound 안에 제거되고 반대 방향 slot은 유지된다. |
| `T-PEER-08` | `PEER-014` | 같은 PeerTransport에 서로 다른 ConnectorSession의 stream을 싣고 한 session만 끊는다. 해당 stream만 닫히고 다른 stream과 shared transport는 유지된다. |

## 오류와 상태

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-ERR-01` | `ERR-001` | malformed identifier, mixed shard snapshot과 잘못된 scope는 `INVALID_ARGUMENT`이며 입력 수정 전 재시도하지 않는다. |
| `T-ERR-02` | `ERR-002`, `ERR-003` | ClientKey 및 component transport identity 검증 실패와 인증된 주체의 권한 부재를 구분하고 credential·deployment trust·권한 변경 전 같은 결과를 유지한다. |
| `T-ERR-03` | `ERR-004`, `ERR-005`, `ERR-006` | READY-empty, generation mismatch·stale lease/revision/object와 unavailable dependency를 `NOT_FOUND`, `FAILED_PRECONDITION`, `UNAVAILABLE`로 구분한다. |
| `T-ERR-04` | `ERR-007`, `ERR-008`, `ERR-009` | deadline, resource limit과 caller cancel을 각각 안정적인 code로 반환하고 같은 attempt를 되살리지 않는다. |
| `T-ERR-05` | `ERR-010`, `ERR-011` | invalid state/frame은 `PROTOCOL_ERROR`, 분류 불가 내부 실패는 `INTERNAL`이다. demultiplex 가능한 stream 위반은 해당 stream만 reset하고 transport-level 위반만 transport를 닫으며 partial live state가 없다. |
| `T-ERR-06` | `ERR-013`, `ERR-014`, `ERR-015` | queue 미도달 증명, OPENED 확인과 둘 다 불가능한 terminal failure를 구분한다. `MAYBE_OBSERVED`는 실제 queue 여부를 단정하지 않고 replay하지 않으며 unknown ConnectorSession/Connection/Stream identity frame이 live state를 만들지 않는다. |
| `T-ERR-07` | `ERR-012` | 같은 runtime의 동일 ClientId Listener 중복만 `ALREADY_EXISTS`이고, 다른 runtime/session의 같은 ClientId 등록과 existing handle의 wire retry는 정상임을 구분한다. |
| `T-STATE-SDK` | `STATE-SDK-001`, `STATE-SDK-002`, `STATE-SDK-003`, `STATE-SDK-004`, `STATE-SDK-005` | 연결 성공·실패·단절·명시적 close를 순회하고 CLOSED 뒤 retry가 없는지 확인한다. |
| `T-STATE-CONNECTOR` | `STATE-CS-001`, `STATE-CS-002` | Connector transport마다 새 session identity를 만들고 단절 시 그 session의 attempt·Pipe·RelayStream만 terminal cleanup되는지 확인한다. |
| `T-STATE-LISTENER` | `STATE-LH-000`, `STATE-LH-001`, `STATE-LH-002`, `STATE-LH-003`, `STATE-LH-004`, `STATE-LH-005`, `STATE-LH-006`, `STATE-LH-007`, `STATE-LS-001`, `STATE-LS-002` | duplicate create는 ABSENT를 유지하고, 정상 생성·transient failure에서는 suspend/re-register하며 credential failure에서는 BLOCKED가 된다. reconnect는 같은 Gateway incarnation에서 재사용하지 않은 새 ListenerSessionId를 만들고, BLOCKED 전 기존 Pipe는 유지한다. |
| `T-STATE-BIND` | `STATE-BIND-001`, `STATE-BIND-002` | binding은 ClientKey 승인과 local 설치로 ACTIVE가 되고 publication 상태와 독립적으로 local 후보가 된다. 제거 뒤에는 새 BindingId로만 재등록된다. |
| `T-STATE-PUBLICATION` | `STATE-PUB-001`, `STATE-PUB-002`, `STATE-PUB-003`, `STATE-PUB-004`, `STATE-PUB-005`, `STATE-PUB-006`, `STATE-PUB-007`, `STATE-PUB-008`, `STATE-PROJ-001`, `STATE-PROJ-002`, `STATE-LEASE-001`, `STATE-LEASE-002`, `STATE-LEASE-003`, `STATE-LEASE-004`, `STATE-LEASE-005` | local set 변경, RT loss, generation mismatch, transport identity/authorization failure, republish, lease 교체, release, session 종료, expiry와 늦은 Publish를 순회한다. RT observation이 다시 VALID가 되어도 local binding은 terminal이며 quiescence 뒤 projection이 사라지는지 확인한다. |
| `T-STATE-RT` | `STATE-RT-001`, `STATE-RT-002` | process 시작 즉시 READY-empty가 되고 process loss는 UNAVAILABLE이며 기존 Pipe와 local binding은 독립적이다. |
| `T-STATE-OPEN` | `STATE-OPEN-001`, `STATE-OPEN-002`, `STATE-OPEN-003`, `STATE-OPEN-004`, `STATE-OPEN-005` | 성공·실패·cancel race에서 Open attempt가 terminal 결과와 observation 하나로 끝난다. |
| `T-STATE-PIPE` | `STATE-PIPE-001`, `STATE-PDIR-001`, `STATE-PDIR-002`, `STATE-PDIR-003`, `STATE-PIPE-002`, `STATE-PIPE-003` | 성공한 OPEN 뒤 방향별 FIN과 duplicate FIN을 순회한다. 반대 방향은 유지되고 양방향 FIN/CLOSE는 정상 CLOSED, RESET·transport loss·FIN 뒤 DATA는 실패 CLOSED가 된다. |
| `T-STATE-PAIR` | `STATE-PAIR-001`, `STATE-PAIR-002`, `STATE-PAIR-003`, `STATE-PAIR-004`, `STATE-PAIR-005` | 두 방향 slot을 독립적으로 lazy connect, 실패, loss, 재사용과 shutdown까지 순회한다. 한 slot 전이는 sibling slot을 바꾸지 않는다. |
| `T-STATE-TRANSPORT` | `STATE-PT-001`, `STATE-PT-002`, `STATE-PT-003` | handshake 성공, 같은 방향 duplicate·실패와 READY loss에서 각 transport instance가 한 번만 terminal이 된다. |

## 조합 edge case

| Test ID | 장애 순서 | 유지해야 하는 SPEC 결과 |
| --- | --- | --- |
| `T-EDGE-01` | 같은 ClientId의 session 세 개 중 하나 종료 | 종료된 binding만 제거되고 나머지 두 binding은 Resolve에 남는다. |
| `T-EDGE-02` | revision 8 snapshot 적용 뒤 revision 7 지연 도착 | 낮은 revision은 current set, lease deadline과 Resolve 결과를 바꾸지 않는다. |
| `T-EDGE-03` | lease L1 revision 7 뒤 L2 revision 8과 과거 L1 Publish/Refresh/Release가 재정렬되어 도착 | L2가 current가 되고 L1 operation은 L2와 그 projection을 변경하지 않는다. |
| `T-EDGE-04` | unregister B1, 같은 ClientId를 B2로 재등록, 과거 B1 snapshot/OPEN 도착 | B1은 B2를 제거하거나 선택할 수 없고 Owner는 stale B1 OPEN을 거절한다. |
| `T-EDGE-05` | 한 session이 여러 shard에 게시된 채 한 shard Refresh 유실 | 해당 scope만 UNSYNCED/expired가 되고 local binding, 다른 shard와 다른 session은 유지된다. |
| `T-EDGE-06` | RT restart -> READY-empty -> Gateway current snapshot 게시 | 기존 Pipe와 local OPEN은 유지되고 remote connect는 수렴 중 NOT_FOUND일 수 있으며 게시 뒤 복구된다. |
| `T-EDGE-07` | Resolve -> 선택된 ListenerSession 종료 -> OPEN 도착 | `UNAVAILABLE`, `NOT_OBSERVED`로 끝나며 다른 후보에 자동 reroute하지 않는다. |
| `T-EDGE-08` | Listener queue 적재 -> OPENED 유실 -> application accept | Connector 결과는 `MAYBE_OBSERVED`이고 provisional Pipe도 bound 안에 종료되며 자동 replay하지 않는다. |
| `T-EDGE-09` | transport가 없는 A와 B가 서로 다른 remote OPEN을 동시에 시작해 양방향 peer dial | 방향별 transport가 하나씩 최대 두 개 READY가 되고 각 connect는 Pipe 하나만 만든다. 같은 방향 duplicate, winner 합의와 중복 Pipe는 없다. |
| `T-EDGE-10` | buffer full -> peer transport loss | pending I/O가 종료되고 모든 stream·Pipe buffer가 해제된다. |
| `T-EDGE-11` | session close -> Release 응답 유실 | local binding은 즉시 사라지고 RT projection은 release retry 또는 lease expiry로 제거된다. |
| `T-EDGE-12` | 같은 ClientId에 다수 binding과 concurrent connect | 각 connect는 Listener 하나에 Pipe 하나만 만들며 fan-out하지 않고 각 attempt의 Resolve 결과를 폐기한다. |
| `T-EDGE-13` | ConnectorSession S1에서 Listener queue 적재 -> S1 단절 -> S2 재연결과 같은 ConnectionId 값 사용 -> S1의 OPENED 지연 도착 | S1은 `MAYBE_OBSERVED`와 bounded cleanup으로 끝나고 늦은 응답은 S2의 attempt나 Pipe를 변경하지 않는다. |
| `T-EDGE-14` | 같은 PeerTransport를 공유하는 ConnectorSession S1·S2 중 S1 단절 | S1의 pending attempt, Pipe와 RelayStream만 닫히며 S2, shared PeerTransport, Listener binding과 RT projection은 유지된다. |
| `T-EDGE-15` | 같은 Listener SDK runtime에서 동일 ClientId handle을 concurrent create -> winner close -> 다시 create | 최초에는 하나만 성공하고 나머지는 `ALREADY_EXISTS`이며 binding과 queue가 하나다. close 뒤 새 handle과 재사용하지 않은 BindingId 하나가 생긴다. |
| `T-EDGE-16` | Gateway generation G1이 RT generation G2에 Publish/Resolve -> 모든 process를 G2로 coordinated restart -> current desired registration 재게시 | mismatch operation은 `FAILED_PRECONDITION`이고 RT state를 바꾸지 않는다. restart 직후 RT는 READY-empty이며 새 session의 current snapshot만으로 mapping이 다시 구성된다. |
| `T-EDGE-17` | 동일 directory artifact를 여러 process가 load -> 한 process의 artifact 공백 또는 shard 순서 한 byte 변경 | 동일 bytes는 같은 SHA-256 generation과 authority 결과를 만들고 변경된 artifact는 generation을 다시 계산한다. mixed generation operation은 state를 읽거나 바꾸지 않는다. |
| `T-EDGE-18` | live binding B2가 있는 상태에서 stale scope Publish L1 -> Release 또는 expiry -> 과거 Publish L1 -> 과거 Refresh L1 -> stale B1 선택/OPEN -> quiescence | RT는 tombstone 없이 L1을 다시 current로 관찰할 수 있지만 Gateway-local session은 살아나지 않고 Owner OPEN이 stale B1을 거절한다. B2가 있어도 같은 connect는 재선택하지 않으며 마지막 Refresh 뒤 한 lease lifetime 안에 B1 projection이 사라진다. |
| `T-EDGE-19` | 한 PeerTransport의 dialer와 acceptor가 counter 0으로 동시에 OPEN -> 한 OPEN 실패·cleanup -> 그 OPEN 지연 재도착 -> 다음 OPEN | StreamId 0과 1로 충돌하지 않고 remote high-watermark가 지연·재사용 ID를 거절한다. 실패한 ID는 재사용되지 않고 다른 stream과 transport는 유지된다. |
| `T-EDGE-20` | DATA -> 한 방향 FIN -> 반대 방향 DATA -> duplicate FIN -> 종료 방향 DATA -> RESET | FIN 이전 bytes 뒤 EOF가 보이고 반대 방향은 계속 전달된다. duplicate FIN은 no-op이고 FIN 뒤 DATA는 해당 stream과 Pipe만 실패 종료한다. |
| `T-EDGE-21` | Connector request가 S1 outbound path에 commit -> Gateway 수신 여부가 확인되기 전 S1 단절 -> S2 재연결 | S1 호출은 queue 미도달을 증명할 수 없으므로 실제 Gateway 수신 여부와 관계없이 `MAYBE_OBSERVED`로 끝난다. S2는 S1 request를 replay하지 않으며 caller가 원하면 새 operation만 시작한다. |
| `T-EDGE-22` | Gateway G1 종료 -> 같은 GatewayLocator에서 새 runtime G2 시작 -> 과거 G1 projection·OPEN·peer frame 도착 | G2는 G1과 다른 GatewayId를 사용하고 과거 `(G1, ListenerSessionId, BindingId)` OPEN과 G1 pair/transport frame을 거절한다. 새 ListenerSessionId, BindingId와 peer identity만 신규 admission에 사용되며 locator 재사용이 identity 재사용을 만들지 않는다. |
| `T-EDGE-23` | Listener Pipe가 queue에 적재되고 다른 Pipe가 accept됨 -> `ClientKey` 권한 폐기 -> 새 OPEN | handle과 binding은 `BLOCKED`/removed가 되어 새 OPEN을 거절한다. 폐기 전에 admission을 마친 queued·accepted Pipe, sibling handle과 shared session은 유지되고 기존 Pipe는 자체 close 규칙을 따른다. |
| `T-EDGE-24` | local binding 없음 -> RT `Resolve`의 component identity mismatch -> trust configuration 수정 | 최초 connect는 `UNAUTHENTICATED`, `NOT_OBSERVED`이고 peer·Listener state를 만들지 않는다. 기존 Pipe와 다른 local binding은 유지되며 configuration 수정 뒤 caller가 시작한 새 connect만 다시 Resolve한다. |

## 완료 기준

1. SPEC 001~007의 모든 requirement와 state transition ID가 최소 한 test에 연결된다.
2. 모든 실패 attempt와 terminal object의 live state·queue·buffer가 configured bound 안에 제거된다.
3. 상태 크기와 memory 사용량은 현재 live Connector/Listener session, binding, attempt, Pipe와 stream 및 RT의 current lease/projection observation 수에 비례한다.
4. 테스트 결과는 application payload 처리, RT replication 또는 구현 언어의 동작을 RelayGate 보장으로 확대하지 않는다.
