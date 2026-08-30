# SPEC 005: 연결 수립 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 001](../adr/001-relayed-pipe-responsibility-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 006](../adr/006-one-hop-peer-multiplexing.md) |
| 관련 계약 | [SPEC 002](002-sdk-pipe-contract.md), [SPEC 004](004-route-table-contract.md), [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 `open(ClientId)`, 즉 Phase 1 Rust API의 `connector.open(ClientId)` 한 번이 live `ListenerBinding` 하나를 선택하고, 정확히 하나의 Listener SDK runtime과 양방향 `Pipe`를 수립하는 과정을 정의한다. `Connector::connect(Config)`는 SDK-Gateway session을 만들 뿐 이 연결 시도와 구분된다.

```text
registration : ClientId * <── ListenerBinding ──> * ListenerSession
one open     : Connector 1 <══════ Pipe ══════> 1 Listener
```

## 연결 흐름

```text
Connector ── ConnectorSession ── open(ClientId) ──► Entry Gateway
                                      │
                                      ├── owned local live binding
                                      │
                                      └── Resolve(AuthenticatedGatewayId,
                                                  Generation, ClientId) ──► RT shard
                                                        │ BindingSet
                                                        ▼
                                                request-local BindingSet
                                                        │
                                                select one binding
                                      │
                                      ├── local  ───────────────► Listener SDK
                                      └── remote ─► Owner GW ──► Listener SDK
                                                           queue Pipe

Connector ◄──────────── OPENED + Pipe ───────────────────────────┘
```

## Gateway data 경계

```text
Gateway
  ├── immutable ShardDirectory + ShardDirectoryGeneration
  ├── ConnectorSessions                 # Entry Gateway만 소유
  │     └── (EntryGatewayId, ConnectorSessionId)
  │           -> SDK-origin ConnectionId high-watermark + active attempts
  ├── LocalRegistry
  │     ├── ClientId  -> local BindingId set
  │     └── SessionId -> local BindingId set
  ├── OpenAttempts
  │     └── (EntryGatewayId, ConnectorSessionId, ConnectionId)
  │           -> selected binding + phase
  └── PeerPool
        └── PeerGatewayId
              ├── locally dialed PeerTransport  0..1
              ├── remotely dialed PeerTransport 0..1
              └── transport-local StreamId high-watermark
                    └── current RelayStream -> OpenIdentity
```

Gateway는 RT 전체 table을 받지 않는다. Entry Gateway에 해당 `ClientId`의 `ACTIVE` local binding이 하나 이상 있으면 그 local set에서 하나를 선택하고 RT를 조회하지 않는다. local set이 비었을 때만 불변 shard directory로 authority를 찾고 internal channel이 검증한 자기 `AuthenticatedGatewayId`와 함께 `Resolve(AuthenticatedGatewayId, ShardDirectoryGeneration, ClientId)`를 정확히 한 번 보낸다. 결과는 해당 open attempt 안에서만 사용한다. 이는 최초 버전의 명시적인 local-first candidate-source 규칙이며, global N:M binding 사이의 fairness나 load-balancing 품질을 보장하지 않는다. 후보 하나를 선택한 뒤 나머지 결과를 routing cache로 보관하지 않는다. `PeerPool`은 remote mapping cache가 아니라 이미 통신하는 Gateway pair의 transport 재사용 상태다. pair의 두 방향 slot은 `DialerGatewayId`로 구분하며 각 slot에는 `READY` transport가 최대 하나다. remote path는 선택한 Owner Gateway에서 끝나고 peer에서 받은 `OPEN`을 다시 Resolve하거나 다른 Gateway로 전달하지 않는다.

`ConnectionId`의 순서와 중복은 ordered SDK→Entry `ConnectorSession`에서 Entry Gateway가 검증한다. peer leg는 이 application-facing counter를 다시 검증하지 않고 `PeerTransport`별 initiator-bit `StreamId` 순서와 중복을 검증한다. Owner Gateway가 받은 `OpenIdentity`는 현재 `RelayStream`과 open/Pipe cleanup을 상관시키는 동안만 존재하며 종료 뒤 remote `ConnectorSession` high-watermark나 terminal `OpenIdentity` history로 남지 않는다.

Gateway는 `LocalRegistry`, `OFFER`, pending open/Pipe, relay와 terminal signal을 소유한다. 명확한 Pipe-local 오류는 해당 Pipe의 `RESET`과 bounded cleanup으로 끝내고, Gateway가 보낸 `OFFER`의 terminal 결과를 알 수 없는 경우는 selected `ListenerSession`을 종료하여 그 session의 relay state를 일괄 정리한다. SDK가 Gateway 내부 registry나 pending state를 대신 정리하지 않는다.

Gateway는 Pipe와 open state를 변경하기 전에 frame role, 현재 identity의 존재, sender ownership, 허용된 phase 순서로 검증한다. 종료되어 현재 state에 없는 identity의 늦은 frame은 no-op이다. 현재 Pipe가 존재하지만 sender가 Connector 또는 selected Listener owner가 아니면 target Pipe를 변경하지 않고 offending SDK session을 `PROTOCOL_ERROR`로 종료한다. owner가 현재 Pipe phase에서 허용되지 않은 frame을 보내면 해당 Pipe만 `RESET`한다.

후보 선택은 attempt당 한 번뿐이다. selected Listener가 실패하면 해당 attempt는 terminal 실패하고, sibling binding이 남아 있어도 같은 attempt 안에서 fallback하지 않는다. 애플리케이션이 새 `open(ClientId)`를 호출해야 그 시점의 live candidate set에서 다시 하나를 선택할 수 있다.

실패 경로는 하나의 terminal 결과로 끝난다.

```text
OPEN ──► OPENED
   └──► FAILED(error)
```

remote path에서 Entry Gateway가 아직 peer `OPEN`을 bounded writer queue에 commit하지 못한
실패는 Listener queue 미도달이 증명되므로 `NOT_OBSERVED`다. commit 뒤 peer `OPENED` 또는
`FAILED`를 확인하지 못한 deadline·transport loss·cancel은 `MAYBE_OBSERVED`이며,
Entry Gateway는 `OPENING` RelayStream에 `RESET(CANCELLED)`을 보내 best-effort cleanup한다.
별도 peer `CANCEL` frame은 두지 않는다. Connector SDK가 자신의 SDK-Gateway commit 뒤 응답을
기다리지 않고 호출을 취소한 경우에는 이 내부 증명을 아직 받지 못하므로 기존 SDK 계약대로
보수적인 `MAYBE_OBSERVED`를 반환할 수 있다.

## 요구사항

| ID | 요구사항 |
| --- | --- |
| `OPEN-001` | Entry Gateway에 해당 `ClientId`의 `ACTIVE` local binding이 하나 이상 있으면 local set에서 후보 하나를 선택하고 RT를 조회하지 않아야 한다. local set이 비었을 때만 configured generation의 `Authority(ClientId)`인 RT shard에 `Resolve(AuthenticatedGatewayId, ShardDirectoryGeneration, ClientId)`를 attempt당 정확히 한 번 보내 후보를 구한다. RT mapping 상실만으로 live local binding을 제외하지 않는다. |
| `OPEN-002` | live 후보가 없으면 `open`은 `NOT_FOUND`로 끝나며 Pipe를 만들지 않는다. RT shard 자체를 사용할 수 없으면 `UNAVAILABLE`로 끝난다. |
| `OPEN-003` | 하나의 연결 시도는 `OPEN-001`이 정한 local set 또는 RT `BindingSet` 중 하나의 candidate set에서 정확히 하나의 live binding만 선택한다. local-first 외의 selection 품질, 순서나 공정성은 보장하지 않는다. |
| `OPEN-004` | 선택된 binding의 Owner Gateway는 `(GatewayId, ListenerSessionId, BindingId, ClientId)`가 자신의 current `ACTIVE` local binding과 일치하고 session이 살아 있는지 OPEN 처리 시점에 다시 확인한다. `GatewayLocator`만으로 identity를 판단하지 않는다. 이 revalidation과 Listener queue admission은 binding 제거·handle close와 하나의 순서로 직렬화되어야 한다. admission이 먼저면 기존 Pipe lifecycle을 따르고 제거가 먼저면 Pipe를 만들지 않는다. |
| `OPEN-005` | 재확인에 실패한 stale binding은 `FAILED(UNAVAILABLE, NOT_OBSERVED)`로 끝난다. 같은 시도에서 다른 binding으로 자동 재선택하지 않는다. |
| `OPEN-006` | local binding은 Entry Gateway가 Listener에 직접 연다. remote binding은 선택된 Owner Gateway 하나를 거쳐 연다. 두 경로의 SDK 결과는 같다. |
| `OPEN-007` | Owner Gateway는 Listener SDK runtime의 bounded incoming queue admission으로 Pipe를 정확히 하나 만든 뒤에만 `OPENED`를 반환한다. application의 `accept`, payload 해석 또는 peer 인증은 기다리지 않는다. |
| `OPEN-008` | Connector의 `open`은 `OPENED`를 확인한 뒤에만 사용할 수 있는 Pipe를 반환한다. `FAILED`를 관찰한 시도는 Pipe를 반환하지 않는다. |
| `OPEN-009` | 하나의 연결 시도는 하나의 Listener에만 OPEN을 전달한다. broadcast, fan-out 및 여러 Listener에 대한 경쟁 OPEN을 하지 않는다. |
| `OPEN-010` | OPEN 이후 route 변경, binding 제거 또는 더 좋은 후보 발견은 해당 시도를 다른 Listener나 Gateway로 이동시키지 않는다. |
| `OPEN-011` | 성공한 Pipe가 끊어져도 RelayGate는 payload를 replay하거나 Pipe를 reroute 또는 resume하지 않는다. 다시 연결하려면 새로운 `open`이 필요하다. |
| `OPEN-012` | cancel, deadline, resource limit, OPEN 전달 실패 및 성공 확인 유실은 성공 또는 `FAILED` 중 하나의 terminal 결과로 끝나야 한다. Connector가 Listener queue 미도달을 증명하지 못한 채 성공 확인을 잃은 실패는 실제 queue 적재 여부와 관계없이 `MAYBE_OBSERVED`여야 하며 같은 attempt를 replay하거나 다른 binding으로 reroute해서는 안 된다. |
| `OPEN-013` | queue admission 뒤 시도가 실패하면 이미 생성된 Pipe와 양쪽 상태는 configured bound 안에 `CLOSED` 또는 `RESET`이 되어야 한다. Listener application이 이미 `accept`한 경우에도 같다. |
| `OPEN-014` | 늦거나 중복된 `OPENED`, `FAILED`, cancel 및 close는 terminal 결과를 바꾸거나 Pipe를 다시 살리지 못한다. |
| `OPEN-015` | Pipe 수립 뒤에는 RT가 해당 Pipe의 payload, close 또는 lifetime에 관여하지 않는다. |
| `OPEN-016` | Entry Gateway는 Connector SDK와 새 live session을 만들 때 자신의 `GatewayId` 범위에서 재사용하지 않는 `ConnectorSessionId`를 부여해야 한다. Connector SDK는 session 안에서 전송 순서가 strictly increasing인 `ConnectionId`를 할당한다. Entry Gateway는 `(EntryGatewayId, ConnectorSessionId, ConnectionId)`를 `OpenIdentity`로 Owner Gateway까지 전달하고 양 Gateway는 현재 OPEN, terminal 응답과 cleanup을 상관시켜야 한다. |
| `OPEN-017` | Entry Gateway는 SDK→Entry `ConnectorSession`마다 remote `ConnectionId` high-watermark 하나를 유지하고 valid SDK `OPEN`을 처리하기 전에 전진시켜야 한다. high-watermark 이하의 중복·지연 SDK `OPEN`은 두 번째 Pipe나 terminal 결과를 만들지 않고 제거한다. Owner Gateway는 peer leg에서 `StreamId` high-watermark를 사용하고 current `RelayStream`에 결합된 `OpenIdentity`만 보유해야 하며, remote `ConnectorSession` high-watermark나 terminal `OpenIdentity` tombstone을 보관해서는 안 된다. 이미 terminal이거나 알 수 없는 identity의 늦은 frame은 기존 Pipe를 변경하거나 새 live state를 만들지 않는다. |
| `OPEN-018` | RT `BindingSet`은 open attempt 안에서만 사용해야 한다. 선택 또는 terminal 결과 뒤 남은 후보를 이후 신규 open의 authority로 보관해서는 안 된다. |
| `OPEN-019` | `PeerObservation`은 오류 이유와 별도의 증명 축이어야 한다. Listener queue에 들어가지 않았음을 Connector가 증명할 수 있는 실패만 `NOT_OBSERVED`, `OPENED`를 확인한 성공만 `OBSERVED`, 어느 쪽도 증명할 수 없는 terminal 실패는 실제 queue 적재 여부와 관계없이 `MAYBE_OBSERVED`로 분류해야 한다. |
| `OPEN-020` | 모든 open attempt와 Connector Pipe endpoint는 하나의 live `ConnectorSession`에 속해야 한다. session 단절은 non-terminal attempt를 실패시키고 그 session의 established Pipe를 닫으며, commit된 remote open의 current RelayStream마다 `RESET(CANCELLED)`을 보내 configured bound 안에 정리해야 한다. `RESET`을 peer transport의 bounded writer queue에 commit할 수 없으면 Entry Gateway는 그 `PeerTransport`를 닫아 transport-loss cleanup으로 수렴시켜야 한다. Connector에 terminal 결과를 전달할 수 없고 queue 미도달도 증명할 수 없으면 결과는 `MAYBE_OBSERVED`다. |
| `OPEN-021` | 재연결로 만든 새 `ConnectorSession`은 이전 session의 ConnectionId, attempt, Pipe 또는 terminal 결과를 승계하지 않는다. 이전 session identity의 늦은 frame은 새 session state를 변경할 수 없다. |
| `OPEN-022` | RT가 `ShardDirectoryGeneration` mismatch를 반환하면 해당 remote open attempt는 `FAILED_PRECONDITION`, `NOT_OBSERVED`로 끝나고 Pipe를 만들거나 다른 shard 또는 binding으로 재시도해서는 안 된다. Gateway는 같은 process에서 directory generation을 바꾸지 않는다. |
| `OPEN-023` | local 후보가 없는 상태에서 RT `Resolve`의 component identity 또는 authorization 검증이 실패하면 해당 open attempt는 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`, `NOT_OBSERVED`로 끝나야 한다. binding 선택, peer OPEN과 Listener queue admission은 일어나지 않으며 같은 attempt를 자동 재시도해서는 안 된다. |
| `OPEN-024` | Owner Gateway가 selected ListenerSession에 `OFFER`를 보낸 뒤 configured deadline까지 `OFFER_ACCEPTED` 또는 `OFFER_REJECTED`를 받지 못하면 attempt는 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`로 끝나야 한다. Gateway는 selected ListenerSession 전체를 종료하고 그 session의 모든 binding·Pipe를 정리하되 다른 ListenerSession은 유지해야 한다. 같은 attempt를 sibling binding으로 reroute해서는 안 된다. |
| `OPEN-025` | Listener SDK가 deadline 안에 명시적인 `OFFER_REJECTED`를 반환한 것은 session liveness failure가 아니다. 해당 attempt만 전달된 code로 실패시키고 selected ListenerSession, sibling binding과 기존 Pipe는 유지해야 한다. |
| `OPEN-026` | Connector SDK가 commit된 OPEN의 terminal Gateway 응답을 자신의 operation deadline까지 받지 못하면 해당 attempt는 `DEADLINE_EXCEEDED`, `MAYBE_OBSERVED`로 끝나고 current ConnectorSession을 종료해야 한다. 그 session의 다른 attempt와 Pipe도 terminal cleanup하며 새 session으로 자동 replay하지 않는다. |
| `OPEN-027` | conforming Connector SDK는 한 `ConnectorSession` 안에서 `ConnectionId`를 재사용하지 않고, conforming Gateway는 그 full `PipeId`를 재사용하거나 같은 attempt의 `OFFER`를 재전송해서는 안 된다. SDK-Gateway transport의 순서 보장과 session identity를 이 계약의 전제로 삼으므로 Listener SDK에 종료된 `PipeId`별 `OFFER` tombstone 보관을 요구하지 않는다. |
| `OPEN-028` | Gateway는 자신이 소유한 local registry, selected binding, `OFFER`, pending Pipe와 relay state를 bounded하게 정리해야 한다. 대상 SDK session이 writable이면 `OPENED`, `OPEN_FAILED` 또는 `RESET` terminal signal을 보내고, transport 상실로 보낼 수 없으면 session close가 SDK의 terminal failure 관찰로 이어져야 한다. 명확한 Pipe-local 오류는 해당 Pipe만 `RESET`하고, `OFFER` terminal 결과가 불확실하면 selected `ListenerSession`을 종료하되 sibling `ListenerSession`으로 전파하거나 같은 attempt를 fallback해서는 안 된다. |
| `OPEN-029` | Gateway는 `OFFER_ACCEPTED`, `OFFER_REJECTED`, `CANCEL`, `DATA`, `FIN`, `CLOSE`, `RESET`을 처리할 때 frame role과 sender ownership을 Pipe phase보다 먼저 검증해야 한다. unknown/stale identity는 live state를 바꾸지 않는 no-op이고, current Pipe의 non-owner frame은 target Pipe를 바꾸지 않은 채 offending SDK session의 `PROTOCOL_ERROR`로 끝나야 한다. owner의 invalid phase frame은 해당 Pipe만 `RESET`하고 sibling Pipe와 session은 유지한다. |
| `OPEN-030` | remote path의 peer connect·handshake·writer-queue commit 전 실패는 candidate와 attempt state를 제거하고 `UNAVAILABLE` 또는 `DEADLINE_EXCEEDED`, `NOT_OBSERVED`로 끝나야 한다. peer `OPEN` commit 뒤 terminal 결과를 확인하지 못한 deadline·transport loss는 `DEADLINE_EXCEEDED` 또는 `UNAVAILABLE`, `MAYBE_OBSERVED`로 끝나야 한다. peer `OPENED`는 Listener queue admission을 확인하지만 external `OBSERVED`는 Connector SDK가 `OPENED`를 확인했을 때만 성립한다. Entry-to-SDK 성공 응답을 잃으면 `MAYBE_OBSERVED`다. 어느 경우도 같은 attempt를 replay, reroute 또는 resume해서는 안 된다. |
| `OPEN-031` | Entry Gateway가 `OPENING` remote attempt의 cancel을 처리할 때 peer `OPEN` commit 전이면 state만 제거하고, commit 뒤면 같은 RelayStream에 `RESET(CANCELLED)`을 보내야 한다. `RESET`을 writer queue에 commit할 수 없으면 해당 PeerTransport를 닫아야 한다. 별도 peer `CANCEL` frame을 만들거나 sibling binding으로 fallback해서는 안 된다. Owner Gateway는 current stream/Pipe가 있으면 terminal cleanup하고, 늦거나 중복된 RESET은 state를 부활시키지 않아야 한다. |

## 연결 시도 불변식

```text
한 시도       -> terminal 결과 정확히 하나
성공한 시도   -> Connector Pipe 하나 + Listener Pipe 하나
실패한 시도   -> 사용 가능한 Pipe 없음 + bounded cleanup
선택한 binding -> 다른 binding으로 암묵적 이동 없음
MAYBE_OBSERVED -> 자동 replay 또는 reroute 없음
ConnectorSession 종료 -> 소유 attempt와 Pipe 종료, 새 session으로 이동 없음
identity ordering   -> SDK leg는 ConnectionId, peer leg는 transport-local StreamId
Owner remote state -> current RelayStream/OpenIdentity만, terminal history 없음
OFFER             -> attempt당 최대 한 번, terminal PipeId history 없음
Pipe state mutation -> role valid + current identity + sender ownership + valid phase
remote cancel      -> pre-commit cleanup | post-commit RESET(CANCELLED), peer CANCEL 없음
```

timeout 값, selection algorithm과 resource limit 기본값은 배포 설정으로 정한다. 어떤 값을
사용하더라도 위 결과와 [SPEC 007](007-error-and-state-model.md)의 상태·오류 의미는 바뀌지 않는다.
