# TEST 001: 요구사항 검증 매트릭스

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 기준 | [SPEC 001](../spec/001-terminology-and-object-model.md) ~ [SPEC 008](../spec/008-runtime-observability-contract.md) |

이 문서는 SPEC 요구사항을 검증 시나리오에 연결한다. 새로운 동작 규칙은 정의하지 않는다.
각 시나리오의 현재 실행 증거와 공백은
[`001-executable-coverage.toml`](001-executable-coverage.toml)에서 관리하며 CI가 실제 Rust
test 목록과 대조한다.

## 검증 원칙

1. 시간 기반 전이는 fake monotonic clock으로 결정적으로 검증한다.
2. register, update, keepalive, deregister, cancel, close, expiry와 late response의 순서를 바꿔 반복한다.
3. terminal 결과 뒤에는 해당 attempt, session, binding, lease, stream과 buffer의 live state가 남지 않는지 확인한다.
4. timeout, queue와 buffer의 구체적인 기본값이 아니라 configured bound 준수 여부를 검증한다.
5. multi-index의 forward/reverse view와 Gateway local/registration view가 같은 live set을 나타내는지 확인한다.
6. peer `StreamId` counter 할당과 ordered writer commit은 같은 actor 순서로 검증하고, commit 전·후 failure observation을 분리한다.

## 용어와 SDK

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-TERM-01` | `TERM-001`, `TERM-002`, `TERM-003`, `TERM-029`, `TERM-030` | Connector/Listener 역할, 위치와 분리된 non-empty UTF-8 ClientId, 등록 전용 ClientKey와 local/CI component 전용 InternalGatewayKey 경계가 유지된다. Gateway startup config는 configured ClientId마다 ClientKey 하나만 허용하고 process 수명 동안 바뀌지 않는다. 두 key를 application identity나 서로의 권한으로 사용하지 않고 정규화 형태나 case가 다른 ClientId bytes를 암묵적으로 합치지 않는다. |
| `T-TERM-02` | `TERM-004`, `TERM-011`, `TERM-025` | Listener SDK runtime은 current ListenerSession을 `0..1`개 공유한다. pending ListenAttempt는 ClientId 하나를 예약하고 반환된 각 handle은 desired ClientId 하나와 current binding `0..1`개만 가진다. pending reservation과 non-CLOSED handle 사이에서 ClientId는 unique하고 attempt의 terminal 실패 뒤 reservation이 제거된다. |
| `T-TERM-03` | `TERM-005`, `TERM-006`, `TERM-007`, `TERM-008`, `TERM-015` | 여러 session과 ClientId를 교차 등록하고 BindingId가 같은 ListenerSession의 서로 다른 ClientId와 제거된 incarnation 사이에서 unique하며 재사용되지 않는지 확인한다. N:M cardinality, 중복 방지와 live-only BindingSet도 확인한다. |
| `T-TERM-04` | `TERM-009`, `TERM-010` | 후보가 여러 개여도 한 open은 Listener queue admission에서 Pipe 하나만 만든다. 성공하면 같은 Pipe를 유지하고 admission 뒤 실패하면 새 Pipe를 만들지 않은 채 생성된 Pipe를 닫는다. |
| `T-TERM-05` | `TERM-012`, `TERM-028` | 일반 identifier는 equality 외의 구조·정렬·위치 의미에 의존하지 않는다. ConnectionId는 SDK→Entry ConnectorSession-local 증가 순서만, StreamId는 peer transport-local initiator bit와 counter 규칙만 해석하며 application 의미를 부여하지 않는다. |
| `T-TERM-06` | `TERM-013`, `TERM-014`, `TERM-016`, `TERM-026` | Gateway restart가 같은 locator를 재사용해도 새 GatewayId를 만들고 old incarnation과 구분하는지 확인한다. dialer별 PeerTransportSlot, pair당 READY 최대 두 개, RelayStream cardinality와 lifetime도 확인한다. |
| `T-TERM-07` | `TERM-017`, `TERM-023`, `TERM-024` | 한 ConnectorSession의 concurrent open이 전송 순서상 strictly increasing ConnectionId를 사용하고 Entry Gateway만 session별 high-watermark 하나를 유지한다. 재연결마다 새 ConnectorSessionId가 생겨 같은 ConnectionId 값도 full identity로 분리되며 application 인증이나 delivery identity로 사용하지 않는다. Owner Gateway는 current RelayStream의 OpenIdentity만 보유하고 종료 뒤 remote ConnectorSession high-watermark나 tombstone을 남기지 않는다. |
| `T-TERM-08` | `TERM-018`, `TERM-019`, `TERM-020`, `TERM-021`, `TERM-022`, `TERM-027` | ShardDirectory가 shard별 stable logical endpoint를 정확히 하나만 포함하고 mapping을 포함하지 않으며 exact artifact bytes의 SHA-256 generation으로 process 동안 불변인지 확인한다. `RegistrationKey`당 active lease 하나, RT가 발급한 LeaseId 비재사용, lease-local revision과 종료 lease operation 거절 및 Gateway remote mapping 비보관도 검증한다. |
| `T-TERM-09` | `TERM-031` | Heartbeat가 transport liveness 전용이고 Pipe read idle, application response absence, delivery acknowledgement 또는 authorization 결과가 아닌지 확인한다. IdleRetirement는 stream 수가 0인 PeerTransport에만 적용되고 live Pipe를 닫지 않는다. |
| `T-SDK-01` | `SDK-001`, `SDK-002`, `SDK-003`, `SDK-004` | Rust API에서 `Connector::connect(Config)`의 session 준비와 `connector.open(ClientId)`를 구분한다. Listener queue admission이 Pipe를 생성하고 distinct current-session Pipe가 accept 한 번에 하나씩 반환되지만 Connector endpoint는 `OPENED` 전에는 반환되지 않는다. queue full이면 기존 non-terminal 항목은 유지한다. capacity 1에서 accept 전 terminal이 된 항목은 제거되어 다음 live OFFER가 애플리케이션의 중간 accept 없이 admission되는지 확인한다. accept와 session 종료를 경쟁시켜 종료가 먼저면 old queued Pipe를 반환하지 않는지 확인한다. |
| `T-SDK-02` | `SDK-005` | pending accept 취소가 다른 accept, sibling Listener handle과 queued Pipe에 영향을 주지 않는다. |
| `T-SDK-03` | `SDK-006` | 정상 ACTIVE Listener handle close를 queue admission·pending accept와 경쟁시키면 개별 binding과 미수락 Pipe만 종료되고 shared session, sibling handle과 이미 accept된 Pipe는 유지된다. 별도로 returned Listener의 recovery REGISTER를 commit한 뒤 그 handle을 close/drop하면 handle이 직접 session token을 취소하지 않고 actor가 current ListenerSession을 reset한다. old session Pipe는 실패하고 closing Listener는 제외되며 returned sibling만 새 session에 재등록된다. |
| `T-SDK-04` | `SDK-007`, `SDK-009` | 양방향 byte 순서를 보존하고 write 성공을 peer application 처리 성공으로 보고하지 않는다. |
| `T-SDK-05` | `SDK-008` | incoming/Pipe capacity 소진 시 memory가 증가하지 않고 backpressure 또는 명시적 실패가 관찰된다. application Pipe I/O에 caller timeout을 적용하면 future가 취소 가능하고, session failure는 capacity 대기를 terminal failure로 해제한다. |
| `T-SDK-06` | `SDK-010`, `SDK-020`, `SDK-021` | write-direction shutdown은 먼저 수락한 bytes 뒤 EOF를 만들고 반대 방향 read는 계속된다. 양방향 FIN, CLOSE와 RESET을 구분하고 terminal 신호 반복은 같은 결과다. outbound capacity를 기다리는 local CLOSE와 remote RESET·session failure를 경쟁시켜 먼저 확정된 terminal 의미가 뒤집히지 않으며, CLOSE를 delivery acknowledgement로 보지 않는다. |
| `T-SDK-07` | `SDK-011`, `SDK-012`, `SDK-013`, `SDK-018`, `SDK-022` | Gateway 단절 뒤 Connector/Listener에 새 session identity를 만들고 live Listener handle의 current desired set만 다시 선언한다. 내부 outbound capacity보다 많은 desired handle도 누락하지 않는다. outbound path commit 전 open만 새 session 준비를 기다릴 수 있고 commit 뒤 응답을 잃으면 미도달 증명이 없는 한 `MAYBE_OBSERVED`이며 이전 request, attempt, Pipe와 payload를 복구하거나 replay하지 않는다. 새 open과 업무 retry는 application이 명시적으로 시작한다. |
| `T-SDK-08` | `SDK-014` | open/accept 성공과 cancel을 경쟁시켜 terminal 결과가 한 번만 반환되는지 확인한다. |
| `T-SDK-09` | `SDK-015` | 필요한 SDK/peer transport를 끊으면 accepted Pipe가 종료되고 새 session으로 이동하지 않는다. ListenerSession 종료가 먼저 확정된 old 미수락 Pipe는 queue에서 제거되고 이후 session의 Pipe처럼 반환되지 않는다. |
| `T-SDK-10` | `SDK-017` | returned Listener의 recovery `REGISTER`를 credential·permission terminal error로 거절하면 해당 handle만 `BLOCKED`가 되고 old 미수락 queue를 제거하며 pending·후속 accept에 등록 오류를 반환한다. 자동 재등록·동일 handle 재활성화를 하지 않고 sibling handle과 shared session은 유지하며, 기존 handle을 닫은 뒤 새 credential의 새 `listen`은 새 binding incarnation으로 성공할 수 있다. |
| `T-SDK-11` | `SDK-019` | 같은 runtime에서 동일 ClientId의 pending listen과 handle 생성을 경쟁시키면 reservation 또는 handle 하나만 남고 나머지는 `ALREADY_EXISTS`다. attempt 실패나 handle close 뒤 새 listen은 새 binding incarnation으로 성공한다. 서로 다른 runtime/session에서 같은 ClientId를 등록하면 둘 다 정상 binding으로 공존한다. |
| `T-SDK-12` | `SDK-023` | commit된 open request의 terminal 응답을 blackhole 처리한다. deadline 뒤 호출은 `MAYBE_OBSERVED`로 한 번만 끝나고 current ConnectorSession의 다른 attempt·Pipe도 종료되며, 새 session은 새 identity를 쓰고 이전 OPEN을 replay하지 않는다. |
| `T-SDK-13` | `SDK-024` | Connector와 Listener 각각의 transport write를 정체시킨 뒤 cancellation과 configured deadline 안에 session actor가 종료되고 pending operation·Pipe·buffer가 정리되는지 확인한다. |
| `T-SDK-14` | `SDK-025` | public runtime owner clone 하나의 drop은 session을 유지한다. live Pipe만 남은 동안에는 I/O가 유지되고 마지막 Connector 또는 Listener-side owner와 Pipe가 사라지면 transport가 닫히며 reconnect가 중단된다. explicit runtime close는 owner 수와 무관하게 종료한다. |
| `T-SDK-15` | `SDK-026` | SDK-Gateway session을 idle 상태로 둔 뒤 valid inbound activity는 `PING` 전송 전 timer를 연장할 수 있고, `PING` commit 뒤에는 response deadline 전 matching `PONG`만 probe를 만족하는지 확인한다. 송신 부하 아래의 timely matching `PONG`은 수신되고, nonce가 다르거나 deadline 이후인 `PONG`은 무시해야 한다. timeout이면 current session 전체가 transport-loss cleanup과 managed reconnect로 수렴한다. 단순 Pipe read idle은 session failure가 아니며 commit된 open, Pipe와 payload는 replay하지 않는다. |
| `T-SDK-16` | `SDK-027` | 기존 반환 Listener A·B와 최초 ListenAttempt C가 한 session에 있을 때 C의 commit된 REGISTER terminal 응답을 blackhole 처리한다. current ListenerSession 전체와 기존 Pipe가 종료되고 C는 한 번만 terminal 실패하며 reservation이 제거된다. 새 session에는 A·B만 새 request identity로 재등록되고 C나 old request는 replay되지 않는다. 별도로 최초 attempt의 명시적 transient `REGISTER_FAILED`는 terminal인 반면 A·B recovery registration의 transient 실패는 bounded backoff 뒤 새 request로 복구되는지 확인한다. TCP 연결 성공만으로 backoff를 초기화하지 않고 A·B recovery 성공 뒤 초기화한다. |
| `T-SDK-17` | `SDK-028` | `Pipe`의 Tokio I/O trait와 consuming owned split을 public API로 사용한다. read/write half를 서로 다른 task에서 동시에 구동해 byte ordering과 half-close를 확인하고, outbound full 상태의 write가 capacity 또는 terminal failure에 깨어나는지 검증한다. half 하나의 drop은 frame을 만들지 않고 마지막 public owner drop만 cleanup을 한 번 발생시키며, `AsyncWrite::shutdown`은 `FIN`, shutdown 뒤 write는 오류이고 Tokio I/O 오류의 downcast 가능한 inner error에는 원래 RelayGate `Error`의 code와 observation이 남는다. |

## 관측성

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-OBS-01` | `OBS-001`, `OBS-002` | `text`, `json`, unset과 양수 interval을 승인하고 알 수 없는 format과 0 interval은 socket을 열기 전에 거절한다. |
| `T-OBS-02` | `OBS-003` | Listener/Connector session, binding, pending offer와 live Pipe를 생성·제거하며 local `GatewaySnapshot` 값이 같은 current state index와 일치하는지 확인한다. distributed mode에서는 worker가 관찰한 session-shard registration의 `SYNCED/UNSYNCED`, remote open attempt, connecting/ready PeerTransport와 current RelayStream 수가 publication·RT/peer 단절·복구·session 제거에 따라 최종 수렴하고 서로 원자적 시점은 가정하지 않는다. local-only mode에서는 분산 count가 0인지 확인한다. |
| `T-OBS-03` | `OBS-004`, `OBS-005`, `OBS-006` | session·registration·open·Pipe terminal event가 고정된 `component`와 `event`, current identity와 terminal error field를 갖는지 확인한다. configured `ClientKey`, `InternalGatewayKey`와 payload marker가 출력에 없고 DATA 반복 수에 비례한 event가 생기지 않아야 한다. |
| `T-OBS-04` | `OBS-007`, `OBS-008` | library만 포함해도 전역 subscriber나 listener가 생기지 않는다. server의 기본 설정은 snapshot event를 만들지 않고, 명시적으로 활성화하면 JSON current-state event를 남기되 Gateway protocol port 외 새 port를 열지 않는다. |
| `T-OBS-05` | `OBS-009` | SDK-Gateway heartbeat timeout, active PeerTransport heartbeat timeout과 zero-stream PeerTransport idle retirement event가 transport lifecycle로만 기록되고 payload bytes, application data와 delivery acknowledgement를 기록하지 않는지 확인한다. |

## Gateway 등록과 route registration

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-REG-01` | `REG-001`, `REG-002`, `REG-003`, `REG-004` | 여러 session이 같은 ClientId를 등록하고 한 session은 여러 ClientId를 등록한다. current 반복 등록은 같은 BindingId이고 제거 후 재등록은 새 BindingId다. |
| `T-REG-02` | `REG-005`, `REG-006` | invalid/unauthorized ClientKey는 local binding과 RT mapping을 만들지 않으며 application 인증으로 사용되지 않는다. |
| `T-REG-03` | `REG-007`, `REG-008`, `SDK-016` | ClientKey 승인과 local 설치 뒤 binding이 즉시 ACTIVE가 되고 등록 성공을 반환한다. 최초 RT `Register` 또는 `Update`를 실패시켜도 local OPEN은 가능하고 key만 UNSYNCED이며, 등록 성공이 remote discovery 완료를 뜻하지 않는지 확인한다. |
| `T-REG-04` | `REG-009`, `REG-014` | 명시적으로 제거한 binding은 local index와 새 snapshot에서 즉시 빠지고 해당 registration key의 마지막 binding이면 빈 `Update` 대신 `Deregister`한다. Gateway startup config는 ClientId당 key 하나만 허용하며 runtime 변경 경로가 없고, 다른 key는 replacement process에서만 적용된다. |
| `T-REG-05` | `REG-010`, `REG-011`, `REG-012` | 한 session의 binding을 여러 shard registration으로 나누고 key별 RT가 발급한 LeaseId, lease-local revision과 직렬화가 독립적인지 확인한다. 새 lease는 첫 revision부터 시작하고 terminal attempt의 늦은 응답을 사용하지 않는다. |
| `T-REG-06` | `REG-013`, `REG-015`, `REG-016`, `REG-017`, `REG-020` | session 종료의 `Deregister` 응답 유실, 종료 lease의 늦은 operation, RT 단절과 복구를 주입한다. local binding은 독립적으로 유지·제거되고 key별 sync 상태를 보존한다. 복구는 `Register`가 반환한 current 또는 새 active lease와 current snapshot만 사용하며 과거 mapping을 되살리지 않는다. |
| `T-REG-07` | `REG-018`, `REG-019`, `REG-021` | Gateway가 process-fixed generation과 shard directory, local registry 및 자신의 registration state 외에 RT 전체 table이나 remote Resolve 결과를 복구 source로 보관하지 않는다. lease-bound `FAILED_PRECONDITION`은 idempotent `Register` 판별을 복구 episode당 한 번만 허용한다. 판별 `Register`가 실패하거나 성공 뒤 다음 lease-bound operation도 실패하면 terminal이 된다. 성공한 lease-bound operation 뒤에는 이후 별도 episode의 판별 1회를 다시 허용한다. generation mismatch와 RT transport identity/GatewayId mismatch는 UNSYNCED와 local binding을 유지하고 관련 configuration 변경 전 추가 재시도하지 않는다. |
| `T-REG-08` | `REG-022` | 한 ListenerSession이 여러 ClientId와 Pipe를 소유할 때 하나의 unanswered OFFER를 만료시킨다. selected session의 binding·registration·Pipe는 전부 제거하고 같은 ClientId의 다른 ListenerSession binding과 다른 key는 유지하며, reconnect는 이미 반환된 Listener만 새 identity로 등록한다. |
| `T-REG-09` | `REG-023` | 한 ListenerSession에 기존 반환 Listener의 binding·Pipe와 pending ListenAttempt의 REGISTER가 함께 있을 때 REGISTER 응답을 유실한다. SDK session 종료 뒤 Gateway가 그 session의 모든 local state를 제거하고, pending attempt는 실패하며 새 session에는 반환된 Listener만 새 identity로 등록한다. permanent credential·permission failure는 자동 재시도를 만들지 않는다. |

## RouteTable

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-RT-01` | `RT-001`, `RT-002`, `RT-003`, `RT-004`, `RT-005`, `RT-006`, `RT-007`, `RT-008`, `RT-009` | 동일 JSON directory bytes의 content hash와 `sha256-modulo-v1` authority 결정성, shard별 key·scope 격리를 검증한다. unknown field, empty identifier·endpoint, endpoint가 없거나 복수인 record는 invalid다. 공백·byte·순서 변경은 generation을 다시 계산하고 다른 generation operation을 거절한다. local/CI adapter의 Gateway별 key allowlist에서 valid name/key만 fresh runtime GatewayId에 결합하고, 잘못된 key·다른 Gateway 이름·owner mismatch는 state를 읽거나 바꾸지 않는다. key가 로그와 protocol state에 없고 plain TCP를 production 보장으로 취급하지 않으며 replication 보장을 가정하지 않는다. |
| `T-RT-02` | `RT-010`, `RT-011`, `RT-012`, `RT-013`, `RT-014`, `RT-015`, `RT-016`, `RT-017` | `Register`가 mapping 없는 RT 발급 lease를 만들고 duplicate Register가 deadline을 포함한 state를 바꾸지 않는지 확인한다. 첫 revision `1`, active lease의 full snapshot atomic replace, equal revision·equal snapshot idempotency, first revision 위반, conflicting·lower revision과 stale LeaseId 거절 및 다른 registration 보존도 검증한다. higher revision이 current `MappingIdentity`의 `ClientId`나 `GatewayLocator`를 바꾸지 못하고 새 binding incarnation만 변경을 표현하는지도 확인한다. `RegistrationAck`의 revision과 상대 TTL이 실제 state와 일치해야 한다. |
| `T-RT-03` | `RT-020`, `RT-021`, `RT-022`, `RT-023`, `RT-024`, `RT-025` | `Update`, `KeepAlive`, `Deregister`와 expiry를 중복·재정렬한다. 종료된 lease의 operation은 registration이나 mapping을 만들지 않고 새 lease를 변경하지 않으며, tombstone이나 expiry record가 누적되지 않는다. |
| `T-RT-04` | `RT-030`, `RT-031`, `RT-032`, `RT-033`, `RT-034`, `RT-035` | Resolve가 active lease의 모든 current mapping만 반환하고 선택·정렬·도달성 보장을 추가하지 않으며 READY-empty와 unavailable을 구분한다. complete BindingSet이 frame 상한을 넘으면 partial 결과나 connection close 대신 `RESOURCE_EXHAUSTED`를 반환하고 같은 connection의 이후 작은 요청을 처리한다. |
| `T-RT-05` | `RT-040`, `RT-041`, `RT-042`, `RT-043` | process down의 unavailable과 restart 직후 READY-empty를 구분하고 새 lease `Register`와 current snapshot `Update`로만 점진적으로 다시 구성한다. |
| `T-RT-06` | `RT-044` | RT 중단·restart·replace·deregister·expiry 중에도 필요한 data transport가 살아 있는 기존 Pipe는 유지된다. |
| `T-RT-07` | `RT-050`, `RT-051`, `RT-052`, `RT-053`, `RT-054` | local binding이 없는 신규 연결은 매번 authority를 조회한다. forward/reverse index mutation은 table scan과 partial/orphan state 없이 atomic하게 적용되며 ConnectorSession과 ConnectionId는 RT에 들어가지 않는다. |

## 연결 수립과 peer relay

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-OPEN-01` | `OPEN-001`, `OPEN-002` | Entry Gateway는 local ACTIVE set이 non-empty면 그 안에서 하나를 열고 RT를 호출하지 않는다. local set이 비었을 때만 authority를 attempt당 정확히 한 번 조회하며 READY-empty와 RT unavailable을 구분한다. |
| `T-OPEN-02` | `OPEN-003`, `OPEN-009` | 여러 후보에서 하나만 선택하고 다른 Listener에는 OPEN이나 Pipe가 생기지 않는다. |
| `T-OPEN-03` | `OPEN-004`, `OPEN-005` | Resolve 뒤 binding 제거·재등록, handle close와 locator 재사용을 queue admission과 경쟁시킨다. Owner가 BindingId까지 재검증하고 admission과 제거를 한 순서로 직렬화한다. 제거가 먼저면 stale candidate를 `NOT_OBSERVED`로 거절하고 admission이 먼저면 이미 열린 Pipe만 기존 lifecycle을 따른다. |
| `T-OPEN-04` | `OPEN-006` | local과 remote one-hop 경로가 동일한 SDK Pipe 결과를 만든다. |
| `T-OPEN-05` | `OPEN-007`, `OPEN-008` | queue admission이 Pipe를 정확히 하나 만들지만 Connector endpoint는 `OPENED` 확인 뒤에만 반환된다. open 성공은 application accept·인증을 기다리지 않는다. |
| `T-OPEN-06` | `OPEN-010`, `OPEN-011` | OPEN 또는 Pipe 수립 뒤 route 변화와 transport 단절이 reroute, replay 또는 resume를 만들지 않는다. |
| `T-OPEN-07` | `OPEN-012`, `OPEN-013`, `OPEN-014`, `OPEN-019` | request commit 뒤 Gateway 수신 전·Owner queue 전·queue 후에 각각 응답 유실, cancel, deadline과 late frame을 경쟁시킨다. queue 미도달 증명이 있을 때만 `NOT_OBSERVED`, OPENED 확인만 `OBSERVED`, 어느 쪽도 증명할 수 없으면 실제 queue 여부와 관계없이 `MAYBE_OBSERVED`다. queue admission 뒤 실패하면 application이 이미 accept했어도 생성된 Pipe가 terminal로 닫히며 terminal 결과 하나와 bounded cleanup을 확인한다. |
| `T-OPEN-08` | `OPEN-015` | Pipe 수립 뒤 RT를 중단해도 payload와 close가 RT를 요구하지 않는다. |
| `T-OPEN-09` | `OPEN-016`, `OPEN-017`, `OPEN-020`, `OPEN-021` | 같은 ConnectionId 값을 서로 다른 ConnectorSession과 Entry Gateway에서 사용하고 한 SDK→Entry session에는 증가·중복·낮은 OPEN을 주입한다. Entry Gateway는 session별 ConnectionId high-watermark로 중복·지연 SDK OPEN을 제거하고, Owner Gateway는 peer StreamId high-watermark와 current OpenIdentity만 사용한다. 양 Gateway는 terminal OpenIdentity history를 누적하지 않는다. full identity별 결과는 하나이며 session 단절은 소유 attempt와 Pipe만 정리하고 새 session은 이전 결과를 승계하지 않는다. |
| `T-OPEN-10` | `OPEN-018` | Resolve 결과는 해당 attempt가 선택 또는 종료된 뒤 폐기되고 다음 open이 새 Resolve를 수행한다. |
| `T-OPEN-11` | `OPEN-022` | Entry Gateway와 RT의 generation을 다르게 하여 remote open이 `FAILED_PRECONDITION`, `NOT_OBSERVED`로 한 번만 끝나고 다른 shard·binding으로 재시도하거나 Pipe를 만들지 않는지 확인한다. |
| `T-OPEN-12` | `OPEN-023` | local 후보가 없을 때 RT `Resolve` channel의 component identity와 authorization 실패를 주입한다. open은 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`, `NOT_OBSERVED`로 한 번만 끝나고 binding 선택, peer OPEN, Listener queue와 Pipe를 만들지 않으며 같은 attempt를 재시도하지 않는다. |
| `T-OPEN-13` | `OPEN-024`, `OPEN-025` | unanswered OFFER와 명시적 OFFER_REJECTED를 분리한다. 무응답 deadline은 selected ListenerSession 전체를 종료하고 late accept가 state를 되살리지 못하게 하며, 명시적 거절은 attempt만 끝내고 session·sibling binding·기존 Pipe를 유지한다. 두 경우 모두 같은 attempt를 다른 binding으로 reroute하지 않는다. |
| `T-OPEN-14` | `OPEN-026` | commit된 OPEN의 Gateway terminal 응답을 blackhole 처리한다. Connector operation deadline이 current ConnectorSession 전체 cleanup을 일으키고 새 session이 이전 attempt·ConnectionId·Pipe·payload를 승계하지 않는지 확인한다. |
| `T-OPEN-15` | `OPEN-027`, `OPEN-028` | 한 ConnectorSession에서 성공·실패 open을 반복해 `PipeId`가 재사용되지 않고 각 attempt의 `OFFER`가 최대 한 번만 전달되는지 확인한다. 명확한 Pipe-local 오류는 그 Pipe만 RESET하고, unanswered OFFER는 selected ListenerSession과 Gateway 소유 registry·pending Pipe·relay state를 정리하며 sibling session으로 fallback하지 않는다. Listener SDK에는 종료 PipeId tombstone이 누적되지 않는다. |
| `T-OPEN-16` | `OPEN-029` | `OFFER_ACCEPTED`, `OFFER_REJECTED`, `CANCEL`, `DATA`, `FIN`, `CLOSE`, `RESET`을 Offered/Open/terminal Pipe와 owner·non-owner·unknown sender 조합으로 순회한다. unknown identity는 no-op이고, known non-owner는 target Pipe를 변경하지 않은 채 offending session만 종료한다. owner의 invalid phase는 해당 Pipe만 RESET하되, Connector owner `CANCEL`은 committed OPEN cleanup을 위해 `OPEN` Pipe에서도 유효하다. |
| `T-OPEN-17` | `OPEN-030`, `OPEN-031` | remote path를 peer connect 전, handshake 중, OPEN writer commit 전, commit 뒤, peer OPENED 수신 뒤와 Connector SDK OPENED 확인 뒤에서 각각 실패·deadline·cancel한다. pre-commit은 `NOT_OBSERVED`, post-commit 결과 불명과 Entry-to-SDK 성공 응답 유실은 `MAYBE_OBSERVED`, Connector SDK 확인만 `OBSERVED`다. commit 뒤 cancel은 같은 StreamId의 `RESET(CANCELLED)`만 보내고 writer queue commit 불가 시 해당 transport를 닫는다. 별도 peer CANCEL, same-attempt fallback·replay·resume가 없으며 late terminal frame이 state를 부활시키지 않는다. |
| `T-PEER-01` | `PEER-001` | local path는 peer를 쓰지 않고 remote path는 선택된 Owner Gateway만 한 번 경유한다. peer에서 OPEN을 받은 Owner는 RT Resolve나 다른 Gateway forwarding을 하지 않는다. |
| `T-PEER-02` | `PEER-002`, `PEER-003`, `PEER-004`, `PEER-005`, `PEER-024` | 같은 Gateway의 동시 OPEN은 자기 방향 candidate 하나를 공유한다. 양 Gateway가 동시에 처음 dial하면 winner 합의 없이 방향별 하나씩 최대 두 transport가 READY가 되고, 같은 방향 duplicate만 stream 전에 닫힌다. local/CI adapter가 Gateway별 key와 fresh runtime GatewayId를 결합하며 unknown name·잘못된 key는 `UNAUTHENTICATED`, 인증 뒤 다른 runtime owner·pair·direction claim은 `PERMISSION_DENIED`, `NOT_OBSERVED`로 끝난다. 어느 실패도 candidate를 READY로 만들지 않고 key는 로그·stream state에 남지 않는다. |
| `T-PEER-03` | `PEER-006`, `PEER-007`, `PEER-015`, `PEER-016`, `PEER-022` | 양 endpoint가 같은 counter로 동시에 stream을 열어 짝수·홀수 StreamId가 충돌하지 않는지 확인한다. 한 endpoint의 concurrent OPEN에서 actor가 counter 할당과 writer commit을 같은 순서로 직렬화하고 commit 실패 ID도 재사용하지 않는다. failed·closed ID의 delayed duplicate OPEN은 bounded remote high-watermark로 거절하고 counter exhaustion은 기존 stream을 유지한 채 새 OPEN만 실패한다. |
| `T-PEER-04` | `PEER-008`, `PEER-009`, `PEER-025` | stream 및 aggregate capacity를 각각 소진해 bounded memory, backpressure와 명시적 resource failure를 확인한다. capacity 1의 내부 PeerEvent queue를 포화시켜 cyclic wait 없이 `RESOURCE_EXHAUSTED` fail-closed와 transport/stream count 0 수렴을 검증한다. receiver 종료는 실행 중 `UNAVAILABLE`이고, 이미 취소된 정상 shutdown과 경쟁한 Full/Closed는 오류 없이 끝나야 한다. |
| `T-PEER-05` | `PEER-010`, `PEER-017`, `PEER-018`, `PEER-019`, `PEER-020`, `PEER-021` | OPEN 결과와 DATA/FIN/CLOSE/RESET 순서를 바꾼다. local OPENING 중 RESET 또는 stream-local protocol violation은 `FAILED(code, MAYBE_OBSERVED)`로 current Entry attempt만 끝낸다. FIN은 한 방향만 닫고, 양방향 FIN·CLOSE는 정상 종료, RESET은 실패 종료이며, FIN 뒤 DATA는 해당 stream만 reset한다. duplicate terminal·late frame은 재활성화나 unbounded history를 만들지 않는다. |
| `T-PEER-06` | `PEER-011`, `PEER-012` | 한 transport 단절은 그 transport의 Pipe만 닫고 반대 방향 transport와 stream을 유지한다. 이후 새 Pipe는 surviving transport 또는 새 lazy transport만 사용한다. |
| `T-PEER-07` | `PEER-013` | connection failure, 같은 방향 duplicate와 loss 뒤 candidate·stream·buffer가 configured bound 안에 제거되고 반대 방향 slot은 유지된다. |
| `T-PEER-08` | `PEER-014` | 같은 PeerTransport에 서로 다른 ConnectorSession의 stream을 싣고 한 session만 끊는다. 정상 writer에서는 그 session의 current stream마다 `RESET(CANCELLED)`이 commit되고 다른 stream과 shared transport는 유지된다. writer queue commit을 실패시키면 transport를 닫아 그 transport의 모든 stream이 terminal cleanup되며 반대 slot·RT mapping·binding은 유지된다. 두 경로 모두 Owner의 OpenIdentity state가 current stream 수로 돌아오고 remote ConnectorSession high-watermark나 terminal history가 남지 않는다. |
| `T-PEER-09` | `PEER-023` | connect·handshake deadline, OPEN commit 뒤 response deadline, OPENING cancel, peer OPENED와 Entry-to-SDK 응답 유실을 각각 주입한다. candidate/slot과 RelayStream이 정확한 scope로 닫히고 peer OPENED와 external `OBSERVED`를 구분하며 별도 peer CANCEL과 암묵적 재시도가 없다. |
| `T-PEER-10` | `PEER-026`, `PEER-027` | active PeerTransport에서 `PING` 전 valid inbound activity, `PING` commit, response deadline 전 matching `PONG`, nonce 불일치와 늦은 `PONG`, unrelated frame, timeout을 분리해 검증한다. timeout은 해당 transport의 stream과 Pipe만 terminal cleanup한다. stream 수가 0이 되면 heartbeat 없이 idle-retirement timer만 동작하고, 재사용은 timer를 취소하며 timeout은 빈 transport를 정상 종료한다. |

## 오류와 상태

| Test ID | Requirement | 시나리오와 기대 결과 |
| --- | --- | --- |
| `T-ERR-01` | `ERR-001` | malformed identifier, mixed shard snapshot과 잘못된 scope는 `INVALID_ARGUMENT`이며 입력 수정 전 재시도하지 않는다. |
| `T-ERR-02` | `ERR-002`, `ERR-003` | ClientKey 및 component transport identity 검증 실패와 인증된 주체의 권한 부재를 구분하고 새 credential·deployment configuration 전에는 같은 결과를 유지한다. |
| `T-ERR-03` | `ERR-004`, `ERR-005`, `ERR-006` | READY-empty, generation mismatch·stale lease/revision/object와 unavailable dependency를 `NOT_FOUND`, `FAILED_PRECONDITION`, `UNAVAILABLE`로 구분한다. |
| `T-ERR-04` | `ERR-007`, `ERR-008`, `ERR-009` | deadline, resource limit과 caller cancel을 각각 안정적인 code로 반환하고 같은 attempt를 되살리지 않는다. |
| `T-ERR-05` | `ERR-010`, `ERR-011` | owner의 invalid Pipe phase는 해당 Pipe만 `PROTOCOL_ERROR`로 reset하고 sibling state를 유지한다. role 위반과 current Pipe의 non-owner frame은 target state를 변경하지 않고 offending session만 닫는다. 분류 불가 내부 실패는 `INTERNAL`이며 partial live state가 없다. |
| `T-ERR-06` | `ERR-013`, `ERR-014`, `ERR-015` | queue 미도달 증명, OPENED 확인과 둘 다 불가능한 terminal failure를 구분한다. `MAYBE_OBSERVED`는 실제 queue 여부를 단정하지 않고 replay하지 않는다. SDK leg의 낮은 ConnectionId는 Entry session high-watermark로, peer leg의 낮거나 잘못된 role StreamId는 transport high-watermark로 거절한다. unknown terminal identity는 live state를 만들지 않고 current identity의 non-owner frame은 target을 변경하지 않는다. conforming Gateway는 OFFER를 재전송하지 않고 Listener SDK와 Owner Gateway는 종료 PipeId/OpenIdentity tombstone을 보관하지 않는다. |
| `T-ERR-07` | `ERR-012` | 같은 runtime의 동일 ClientId Listener 중복만 `ALREADY_EXISTS`이고, 다른 runtime/session의 같은 ClientId 등록과 existing handle의 wire retry는 정상임을 구분한다. |
| `T-ERR-08` | `ERR-016` | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` 각각에 세 observation을 조합한다. `is_retryable()`은 `NOT_OBSERVED` 조합만 true이고 `MAYBE_OBSERVED`/`OBSERVED`는 false이며 SDK가 operation을 자동 실행하지 않는다. |
| `T-STATE-SDK` | `STATE-SDK-001`, `STATE-SDK-002`, `STATE-SDK-003`, `STATE-SDK-004`, `STATE-SDK-005` | 연결 성공·실패, bounded write failure, 단절, 명시적 close와 마지막 public owner drop을 순회하고 CLOSED 뒤 retry가 없는지 확인한다. |
| `T-STATE-CONNECTOR` | `STATE-CS-001`, `STATE-CS-002` | Connector transport마다 새 session identity를 만들고 단절·bounded write failure·commit된 OPEN terminal-response deadline에서 current session의 attempt와 Pipe를 terminal cleanup하는지 확인한다. remote RelayStream RESET commit 성공은 sibling session stream과 transport를 유지하고, commit 실패는 해당 transport를 닫아 transport-loss cleanup으로 수렴한다. |
| `T-STATE-LISTENER` | `STATE-LA-001`, `STATE-LA-002`, `STATE-LA-003`, `STATE-LA-004`, `STATE-LA-005`, `STATE-LA-006`, `STATE-LH-001`, `STATE-LH-002`, `STATE-LH-003`, `STATE-LH-004`, `STATE-LH-005`, `STATE-LH-006`, `STATE-LH-007`, `STATE-LS-001`, `STATE-LS-002` | duplicate create는 state를 만들지 않는다. 최초 attempt는 commit 전 대기만 허용하고 commit 뒤 실패·deadline·session loss에는 terminal 실패한다. 성공해 반환된 Listener만 suspend/re-register하고 session 종료가 먼저면 old 미수락 queue를 제거한다. recovery credential terminal failure에서는 해당 handle만 BLOCKED가 되어 queue보다 terminal 오류가 우선하며 같은 handle은 부활하지 않는다. 정상 ACTIVE close는 개별 UNREGISTER지만 committed recovery REGISTER 중 close는 actor가 current session을 reset한다. Listener SDK와 Gateway가 감지한 불확실성은 각각 current/selected ListenerSession 전체만 닫고 새 identity로 재연결한다. |
| `T-STATE-BIND` | `STATE-BIND-001`, `STATE-BIND-002` | binding은 ClientKey 승인과 local 설치로 ACTIVE가 되고 RT registration 상태와 독립적으로 local 후보가 된다. 제거 뒤에는 새 BindingId로만 재등록된다. |
| `T-STATE-REGISTRATION` | `STATE-REG-001`, `STATE-REG-002`, `STATE-REG-003`, `STATE-REG-004`, `STATE-REG-005`, `STATE-REG-006`, `STATE-REG-007`, `STATE-REG-008`, `STATE-MAP-001`, `STATE-MAP-002`, `STATE-LEASE-001`, `STATE-LEASE-002`, `STATE-LEASE-003`, `STATE-LEASE-004`, `STATE-LEASE-005`, `STATE-LEASE-006`, `STATE-LEASE-007` | local set 변경, RT loss, generation mismatch, transport identity/authorization failure, 새 lease 등록, duplicate Register, update, keepalive, deregister, session 종료와 expiry를 순회한다. 종료 lease의 operation과 과거 lease operation이 `ABSENT`나 새 lease를 바꾸거나 mapping을 되살리지 않는지 확인한다. |
| `T-STATE-RT` | `STATE-RT-001`, `STATE-RT-002` | process 시작 즉시 READY-empty가 되고 process loss는 UNAVAILABLE이며 기존 Pipe와 local binding은 독립적이다. |
| `T-STATE-OPEN` | `STATE-OPEN-001`, `STATE-OPEN-002`, `STATE-OPEN-003`, `STATE-OPEN-004`, `STATE-OPEN-005` | queue admission은 Pipe를 생성한 채 attempt를 `QUEUED`로 만들고 `OPENED` 확인만 `SUCCEEDED`로 만든다. 성공·실패·cancel race에서 terminal 결과와 observation 하나로 끝난다. |
| `T-STATE-PIPE` | `STATE-PIPE-001`, `STATE-PDIR-001`, `STATE-PDIR-002`, `STATE-PDIR-003`, `STATE-PIPE-002`, `STATE-PIPE-003` | Pipe lifecycle은 queue admission에서 시작한다. admission 뒤 open failure와 OPENED 확인 유실은 실패 CLOSED가 되고 old 미수락 endpoint는 queue에서 제거된다. Connector SDK가 `OPENED`를 application에 반환하기 전 보내는 owner `CANCEL`은 Gateway의 `OPEN` Pipe를 `RESET(CANCELLED)`로 닫고 sibling Pipe를 유지한다. 성공한 OPEN 뒤 양방향 FIN/CLOSE는 정상 CLOSED, RESET·transport loss·FIN 뒤 DATA는 실패 CLOSED가 된다. 모든 state mutation은 owner 확인 뒤 수행한다. |
| `T-STATE-PAIR` | `STATE-PAIR-001`, `STATE-PAIR-002`, `STATE-PAIR-003`, `STATE-PAIR-004`, `STATE-PAIR-005` | 두 방향 slot을 독립적으로 lazy connect, 실패, loss, 재사용과 shutdown까지 순회한다. 한 slot 전이는 sibling slot을 바꾸지 않는다. |
| `T-STATE-TRANSPORT` | `STATE-PT-001`, `STATE-PT-002`, `STATE-PT-003`, `STATE-PT-004` | handshake 성공, 같은 방향 duplicate·실패, READY loss, active heartbeat timeout과 zero-stream idle-retirement timeout에서 각 transport instance가 한 번만 terminal이 된다. |
| `T-STATE-STREAM` | `STATE-RS-001`, `STATE-RS-002`, `STATE-RS-003`, `STATE-RS-004`, `STATE-RS-005` | local/remote OPEN으로 RelayStream을 OPENING에 만들고 OPENED, FAILED, RESET(CANCELLED), FIN/CLOSE/RESET, ConnectorSession·PeerTransport loss를 순회한다. terminal stream은 재활성화되지 않고 한 방향 FIN은 반대 방향과 sibling stream을 유지한다. ConnectorSession cleanup RESET commit 실패는 해당 transport를 닫고, cleanup 뒤 current OpenIdentity와 live stream state가 없다. |

## 조합 edge case

| Test ID | 장애 순서 | 유지해야 하는 SPEC 결과 |
| --- | --- | --- |
| `T-EDGE-01` | 같은 ClientId의 session 세 개 중 하나 종료 | 종료된 binding만 제거되고 나머지 두 binding은 Resolve에 남는다. |
| `T-EDGE-02` | revision 8 snapshot 적용 뒤 revision 7 지연 도착 | 낮은 revision은 current set, lease deadline과 Resolve 결과를 바꾸지 않는다. |
| `T-EDGE-03` | lease L1 deregister 뒤 새 lease L2를 register/update하고 과거 L1 Update/KeepAlive/Deregister가 재정렬되어 도착 | L1 operation은 실패하거나 idempotent하게 끝나며 L2와 그 mapping을 변경하지 않는다. |
| `T-EDGE-04` | unregister B1, 같은 ClientId를 B2로 재등록, 과거 B1 snapshot/OPEN 도착 | B1은 B2를 제거하거나 선택할 수 없고 Owner는 stale B1 OPEN을 거절한다. |
| `T-EDGE-05` | 한 session이 여러 shard에 등록된 채 한 shard의 `KeepAlive` 응답이 유실되어 RT 연결 종료 | 해당 shard의 registration은 보수적으로 `UNSYNCED`가 될 수 있지만 local binding과 current desired state, 다른 shard와 다른 session은 제거되지 않는다. 연결 복구 뒤 current state를 다시 검증·publish한다. |
| `T-EDGE-06` | RT restart -> READY-empty -> Gateway 새 lease Register -> current snapshot Update | 기존 Pipe와 local OPEN은 유지되고 remote open은 수렴 중 NOT_FOUND일 수 있으며 Update 뒤 복구된다. |
| `T-EDGE-07` | Resolve -> 선택된 ListenerSession 종료 -> OPEN 도착 | `UNAVAILABLE`, `NOT_OBSERVED`로 끝나며 다른 후보에 자동 reroute하지 않는다. |
| `T-EDGE-08` | Listener queue admission으로 Pipe 생성 -> application accept -> OPENED 유실 | Connector 결과는 `MAYBE_OBSERVED`이고 생성된 Pipe는 bound 안에 종료되며 자동 replay하지 않는다. |
| `T-EDGE-09` | transport가 없는 A와 B가 서로 다른 remote OPEN을 동시에 시작해 양방향 peer dial | 방향별 transport가 하나씩 최대 두 개 READY가 되고 각 open은 Pipe 하나만 만든다. 같은 방향 duplicate, winner 합의와 중복 Pipe는 없다. |
| `T-EDGE-10` | buffer full -> peer transport loss | pending I/O가 종료되고 모든 stream·Pipe buffer가 해제된다. |
| `T-EDGE-11` | session close -> Deregister 응답 유실 | local binding은 즉시 사라지고 RT mapping은 deregister retry 또는 lease expiry로 제거된다. |
| `T-EDGE-12` | 같은 ClientId에 다수 binding과 concurrent open | 각 open은 Listener 하나에 Pipe 하나만 만들며 fan-out하지 않고 각 attempt의 Resolve 결과를 폐기한다. |
| `T-EDGE-13` | ConnectorSession S1에서 Listener queue 적재 -> S1 단절 -> S2 재연결과 같은 ConnectionId 값 사용 -> S1의 OPENED 지연 도착 | S1은 `MAYBE_OBSERVED`와 bounded cleanup으로 끝나고 늦은 응답은 S2의 attempt나 Pipe를 변경하지 않는다. |
| `T-EDGE-14` | 같은 PeerTransport를 공유하는 ConnectorSession S1·S2 중 S1 단절 -> S1 RESET commit 성공 또는 writer queue commit 실패 | 성공 경로는 S1의 pending attempt, Pipe와 current RelayStream만 닫고 S2와 shared PeerTransport를 유지한다. commit 실패 경로는 PeerTransport를 닫아 S2 stream도 transport-loss로 끝내지만 반대 slot, Listener binding과 RT mapping은 유지한다. 어느 경로도 OpenIdentity tombstone, fallback 또는 replay를 만들지 않는다. |
| `T-EDGE-15` | 같은 Listener SDK runtime에서 동일 ClientId handle을 concurrent create -> winner close -> 다시 create | 최초에는 하나만 성공하고 나머지는 `ALREADY_EXISTS`이며 binding과 queue가 하나다. close 뒤 새 handle과 재사용하지 않은 BindingId 하나가 생긴다. |
| `T-EDGE-16` | Gateway generation G1이 RT generation G2에 Register/Resolve -> 모든 process를 G2로 coordinated restart -> current desired registration 재등록 | mismatch operation은 `FAILED_PRECONDITION`이고 RT state를 바꾸지 않는다. restart 직후 RT는 READY-empty이며 새 lease와 current snapshot만으로 mapping이 다시 구성된다. |
| `T-EDGE-17` | 동일 directory artifact를 여러 process가 load -> 한 process의 artifact 공백 또는 shard 순서 한 byte 변경 | 동일 bytes는 같은 SHA-256 generation과 authority 결과를 만들고 변경된 artifact는 generation을 다시 계산한다. mixed generation operation은 state를 읽거나 바꾸지 않는다. |
| `T-EDGE-18` | lease L1의 B1 mapping -> Deregister 또는 expiry -> 과거 Update/KeepAlive L1 -> 새 lease L2의 B2 mapping | 과거 L1 operation은 `FAILED_PRECONDITION`이고 B1을 다시 만들지 않는다. Resolve에는 L2의 B2만 나타나며 tombstone이나 과거 lease history는 누적하지 않는다. |
| `T-EDGE-19` | 한 PeerTransport의 dialer와 acceptor가 counter 0으로 동시에 OPEN -> 한 OPEN 실패·cleanup -> 그 OPEN 지연 재도착 -> 다음 OPEN | StreamId 0과 1로 충돌하지 않고 remote high-watermark가 지연·재사용 ID를 거절한다. 실패한 ID는 재사용되지 않고 다른 stream과 transport는 유지된다. |
| `T-EDGE-20` | DATA -> 한 방향 FIN -> 반대 방향 DATA -> duplicate FIN -> 종료 방향 DATA -> RESET | FIN 이전 bytes 뒤 EOF가 보이고 반대 방향은 계속 전달된다. duplicate FIN은 no-op이고 FIN 뒤 DATA는 해당 stream과 Pipe만 실패 종료한다. |
| `T-EDGE-21` | Connector request가 S1 outbound path에 commit -> Gateway 수신 여부가 확인되기 전 S1 단절 -> S2 재연결 | S1 호출은 queue 미도달을 증명할 수 없으므로 실제 Gateway 수신 여부와 관계없이 `MAYBE_OBSERVED`로 끝난다. S2는 S1 request를 replay하지 않으며 caller가 원하면 새 operation만 시작한다. |
| `T-EDGE-22` | Gateway G1 종료 -> 같은 GatewayLocator에서 새 runtime G2 시작 -> 과거 G1 mapping·OPEN·peer frame 도착 | G2는 G1과 다른 GatewayId를 사용하고 과거 `(G1, ListenerSessionId, BindingId)` OPEN과 G1 pair/transport frame을 거절한다. 새 ListenerSessionId, BindingId와 peer identity만 신규 admission에 사용되며 locator 재사용이 identity 재사용을 만들지 않는다. |
| `T-EDGE-23` | G1에서 returned Listener A·B 활성 -> G1 종료 -> 다른 고정 key의 replacement G2에서 A recovery `REGISTER` 영구 거절, B 승인 | A만 `BLOCKED`이고 새 binding을 만들지 않으며 B와 shared G2 session은 유지된다. A는 자동 재등록하지 않고 application이 A를 닫은 뒤 새 key의 새 `listen`을 시작해야 한다. |
| `T-EDGE-24` | local binding 없음 -> RT `Resolve`의 component identity mismatch -> trust configuration 수정 | 최초 open은 `UNAUTHENTICATED`, `NOT_OBSERVED`이고 peer·Listener state를 만들지 않는다. 기존 Pipe와 다른 local binding은 유지되며 configuration 수정 뒤 caller가 시작한 새 open만 다시 Resolve한다. |
| `T-EDGE-25` | ListenerSession L1이 ClientId A·B와 기존 Pipe를 소유 -> A의 OFFER 무응답 -> 같은 ClientId A의 sibling ListenerSession L2 존재 -> 늦은 L1 ACCEPT | L1의 모든 binding·Pipe와 registration state만 terminal cleanup되고 L2는 유지된다. current attempt는 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`이며 늦은 ACCEPT는 state를 만들지 않는다. 이후 caller의 새 open만 L2를 선택할 수 있다. |
| `T-EDGE-26` | ConnectorSession C1의 기존 Pipe 존재 -> 새 OPEN commit -> terminal 응답 무응답 -> deadline -> C2 재연결 -> C1 OPENED 지연 도착 | C1의 attempt와 Pipe가 모두 terminal cleanup되고 timeout 호출은 `MAYBE_OBSERVED`다. C2는 새 identity를 쓰고 C1 OPEN을 replay하지 않으며 지연 OPENED는 C2를 변경하지 않는다. |
| `T-EDGE-27` | ListenerSession L1에 반환된 A·B active, 최초 C REGISTER commit -> 응답 유실 -> L1 종료 -> 과거 REGISTERED 지연 도착 | C 호출은 terminal 실패하고 reservation이 제거된다. L2에는 A·B만 새 request·BindingId로 등록되며 C와 과거 L1 응답은 L2를 변경하지 않는다. 애플리케이션이 C를 원하면 새 listen operation을 시작한다. |
| `T-EDGE-28` | 반환된 Listener A의 recovery REGISTERED 응답과 SDK의 old ListenerSession 종료 신호가 silent partition에서 유실 -> SDK는 L2에 A를 새 identity로 등록 -> Gateway는 old L1과 new L2 binding을 잠시 함께 보유 -> L1에 OPEN/OFFER 무응답 | N:M 때문에 두 binding 공존은 허용되지만 L1은 성공을 만들지 못한다. L1 OFFER deadline이 L1의 binding·Pipe를 전부 제거하고 L2는 유지되며 같은 open은 reroute하지 않는다. |
| `T-EDGE-29` | RT가 `Register`로 L1을 만들고 응답 유실 -> 같은 authenticated Gateway가 같은 RegistrationKey를 다시 `Register` -> 첫 응답 지연 도착 | retry는 mapping·revision·deadline을 바꾸지 않고 같은 L1을 반환한다. Gateway는 current attempt의 응답만 사용해 첫 snapshot을 `Update`하고 terminal attempt의 늦은 응답은 후속 registration을 변경하지 않는다. |
| `T-EDGE-30` | 같은 ClientId의 ListenerSession L1·L2 공존 -> open이 L1 선택 -> L1이 명시적으로 거절하거나 OFFER deadline -> L2 생존 | 최초 attempt는 terminal 실패하고 L2로 fallback하지 않는다. Gateway와 SDK는 실패 scope만 정리하며, application이 시작한 새 open만 current live set에서 L2를 선택할 수 있다. 기존 attempt·Pipe·payload는 replay하지 않는다. |
| `T-EDGE-31` | returned Listener A·B가 L1에서 active -> L1 단절 -> L2에서 A recovery REGISTER commit, 응답 전 A close/drop -> actor가 L2 종료 -> L3 연결 | A는 CLOSED이고 재등록되지 않는다. B desired만 L3에 새 request·BindingId로 재등록된다. handle이 session token을 직접 소유하거나 old REGISTER·Pipe를 replay하지 않는다. |
| `T-EDGE-32` | Connector C와 Listener L의 Offered/Open PipeId를 제3 SDK session F가 사용해 OFFER response/CANCEL/DATA/FIN/CLOSE/RESET 전송 | F의 frame은 target Pipe와 C·L state를 변경하지 않고 F session만 `PROTOCOL_ERROR`로 종료한다. 같은 frame의 unknown·terminal PipeId는 no-op이며 tombstone을 만들지 않는다. |
| `T-EDGE-33` | L1 queue admission과 application accept를 L1 단절과 경쟁 -> L2 recovery에서 영구 등록 거절 | accept가 먼저면 반환된 Pipe가 failure를 관찰하고, 단절이 먼저면 old 미수락 Pipe는 제거된다. L2 거절 뒤 handle은 BLOCKED이고 pending·후속 accept는 등록 오류를 반환한다. close 뒤 새 key의 새 listen만 새 binding으로 성공한다. |
| `T-EDGE-34` | peer OPEN writer commit 직전 cancel -> commit 직후 cancel -> peer OPENED·Connector SDK OPENED 확인과 cancel 경쟁 | Gateway가 pre-commit 미도달을 확인해 terminal 응답하면 `NOT_OBSERVED`이고 post-commit은 `RESET(CANCELLED)`과 `MAYBE_OBSERVED`다. RESET writer commit 실패는 해당 transport close로 수렴한다. caller가 Gateway 증명을 기다리지 않고 SDK operation을 취소하면 pre-commit이어도 보수적으로 `MAYBE_OBSERVED`일 수 있다. Connector SDK OPENED 확인이 먼저면 established Pipe의 terminal cleanup으로 끝나며 어떤 순서도 별도 peer CANCEL, fallback 또는 state resurrection을 만들지 않는다. |
| `T-EDGE-35` | 같은 PeerTransport endpoint의 concurrent OPEN A·B에서 A counter를 먼저 할당한 뒤 A writer를 지연하고 B를 진행 | 단일 actor 때문에 A OPEN이 B보다 먼저 commit되거나 A가 terminal 실패한 뒤 B가 commit된다. receiver high-watermark가 정상 B 뒤 지연 A를 보는 순서는 생기지 않고 두 counter 모두 재사용되지 않는다. |
| `T-EDGE-36` | gw-a key로 gw-b 이름·runtime id 주장 -> valid gw-b 연결 -> 과거 gw-b runtime id frame 지연 도착 | 잘못된 name/key 결합은 state 생성 없이 인증 실패한다. valid connection만 fresh GatewayId에 결합되고 과거 incarnation frame은 새 RT registration·PeerTransport·RelayStream을 변경하지 않으며 key는 로그에 없다. |
| `T-EDGE-37` | SDK-Gateway session idle -> heartbeat PING commit -> unrelated frame 또는 늦은 PONG -> timeout | unrelated frame과 response deadline 이후의 `PONG`은 commit된 probe를 만족시키지 않는다. current session과 그 session의 pending attempt·Pipe만 종료되고 managed reconnect는 새 session identity로 시작한다. application Pipe read idle이나 payload 무응답을 heartbeat failure로 오해하지 않는다. |
| `T-EDGE-38` | PeerTransport stream_count 1 -> heartbeat timeout, 그리고 별도 경로에서 stream_count 0 -> idle-retirement timeout | active timeout은 해당 transport의 stream/Pipe 실패로 수렴하고 zero-stream timeout은 빈 transport 정상 종료로 끝난다. 두 경우 모두 RT mapping, Listener binding, 반대 방향 transport와 다른 Gateway pair는 유지된다. |

## 완료 기준

1. SPEC 001~008의 모든 requirement와 state transition ID가 최소 한 test에 연결된다.
2. 모든 실패 attempt와 terminal object의 live state·queue·buffer가 configured bound 안에 제거된다.
3. 상태 크기와 memory 사용량은 현재 live Connector/Listener session, binding, attempt, Pipe와 stream 및 RT의 active lease/mapping 수에 비례한다.
4. 테스트 결과는 application payload 처리, RT replication 또는 구현 언어의 동작을 RelayGate 보장으로 확대하지 않는다.
