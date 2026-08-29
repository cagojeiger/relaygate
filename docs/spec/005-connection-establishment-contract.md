# SPEC 005: 연결 수립 계약

| 항목 | 값 |
| --- | --- |
| 상태 | Draft |
| 근거 | [ADR 001](../adr/001-relayed-pipe-responsibility-boundary.md), [ADR 003](../adr/003-client-id-listener-binding.md), [ADR 004](../adr/004-current-state-routing-topology.md), [ADR 006](../adr/006-one-hop-peer-multiplexing.md) |
| 관련 계약 | [SPEC 002](002-sdk-pipe-contract.md), [SPEC 004](004-route-table-contract.md), [SPEC 007](007-error-and-state-model.md) |

## 범위

이 문서는 `connect(ClientId)` 한 번이 live `ListenerBinding` 하나를 선택하고, 정확히 하나의
Listener SDK runtime과 양방향 `Pipe`를 수립하는 과정을 정의한다.

```text
registration : ClientId * <── ListenerBinding ──> * ListenerSession
one connect  : Connector 1 <══════ Pipe ══════> 1 Listener
```

## 연결 흐름

```text
Connector ── ConnectorSession ── connect(ClientId) ──► Entry Gateway
                                      │
                                      ├── owned local live binding
                                      │
                                      └── Resolve(Generation, ClientId) ──► RT shard
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
  ├── ConnectorSessions
  │     └── (EntryGatewayId, ConnectorSessionId)
  │           -> remote ConnectionId high-watermark + active ConnectionId set
  ├── LocalRegistry
  │     ├── ClientId  -> local BindingId set
  │     └── SessionId -> local BindingId set
  ├── OpenAttempts
  │     └── (EntryGatewayId, ConnectorSessionId, ConnectionId)
  │           -> selected binding + phase
  └── PeerPool
        └── PeerGatewayId
              ├── locally dialed PeerTransport  0..1
              └── remotely dialed PeerTransport 0..1
```

Gateway는 RT 전체 table을 받지 않는다. Entry Gateway에 해당 `ClientId`의 `ACTIVE` local binding이 하나 이상 있으면 그 local set에서 하나를 선택하고 RT를 조회하지 않는다. local set이 비었을 때만 불변 shard directory로 authority를 찾고 `Resolve(ShardDirectoryGeneration, ClientId)` 결과를 해당 connect attempt 안에서 사용한다. 이는 최초 버전의 명시적인 local-first candidate-source 규칙이며, global N:M binding 사이의 fairness나 load-balancing 품질을 보장하지 않는다. 후보 하나를 선택한 뒤 나머지 결과를 routing cache로 보관하지 않는다. `PeerPool`은 remote mapping cache가 아니라 이미 통신하는 Gateway pair의 transport 재사용 상태다. pair의 두 방향 slot은 `DialerGatewayId`로 구분하며 각 slot에는 `READY` transport가 최대 하나다.

실패 경로는 하나의 terminal 결과로 끝난다.

```text
OPEN ──► OPENED
   └──► FAILED(error)
```

## 요구사항

| ID | 요구사항 |
| --- | --- |
| `OPEN-001` | Entry Gateway에 해당 `ClientId`의 `ACTIVE` local binding이 하나 이상 있으면 local set에서 후보 하나를 선택하고 RT를 조회하지 않아야 한다. local set이 비었을 때만 configured generation의 `Authority(ClientId)`인 RT shard에 `Resolve(ShardDirectoryGeneration, ClientId)`를 보내 후보를 구한다. RT projection 상실만으로 live local binding을 제외하지 않는다. |
| `OPEN-002` | live 후보가 없으면 `connect`는 `NOT_FOUND`로 끝나며 Pipe를 만들지 않는다. RT shard 자체를 사용할 수 없으면 `UNAVAILABLE`로 끝난다. |
| `OPEN-003` | 하나의 연결 시도는 `OPEN-001`이 정한 local set 또는 RT `BindingSet` 중 하나의 candidate set에서 정확히 하나의 live binding만 선택한다. local-first 외의 selection 품질, 순서나 공정성은 보장하지 않는다. |
| `OPEN-004` | 선택된 binding의 Owner Gateway는 `(GatewayId, ListenerSessionId, BindingId, ClientId)`가 자신의 current `ACTIVE` local binding과 일치하고 session이 살아 있는지 OPEN 처리 시점에 다시 확인한다. `GatewayLocator`만으로 identity를 판단하지 않는다. 이 revalidation과 Listener queue admission은 binding 제거·handle close·권한 폐기와 하나의 순서로 직렬화되어야 한다. admission이 먼저면 기존 Pipe lifecycle을 따르고 제거가 먼저면 Pipe를 만들지 않는다. |
| `OPEN-005` | 재확인에 실패한 stale binding은 `FAILED(UNAVAILABLE, NOT_OBSERVED)`로 끝난다. 같은 시도에서 다른 binding으로 자동 재선택하지 않는다. |
| `OPEN-006` | local binding은 Entry Gateway가 Listener에 직접 연다. remote binding은 선택된 Owner Gateway 하나를 거쳐 연다. 두 경로의 SDK 결과는 같다. |
| `OPEN-007` | Owner Gateway는 Listener SDK runtime의 bounded incoming queue에 provisional Pipe를 넣은 뒤에만 `OPENED`를 반환한다. application의 `accept`, payload 해석 또는 peer 인증은 기다리지 않는다. |
| `OPEN-008` | Connector의 `connect`는 `OPENED`를 확인한 뒤에만 사용할 수 있는 Pipe를 반환한다. `FAILED`를 관찰한 시도는 Pipe를 반환하지 않는다. |
| `OPEN-009` | 하나의 연결 시도는 하나의 Listener에만 OPEN을 전달한다. broadcast, fan-out 및 여러 Listener에 대한 경쟁 OPEN을 하지 않는다. |
| `OPEN-010` | OPEN 이후 route 변경, binding 제거 또는 더 좋은 후보 발견은 해당 시도를 다른 Listener나 Gateway로 이동시키지 않는다. |
| `OPEN-011` | 성공한 Pipe가 끊어져도 RelayGate는 payload를 replay하거나 Pipe를 reroute 또는 resume하지 않는다. 다시 연결하려면 새로운 `connect`가 필요하다. |
| `OPEN-012` | cancel, deadline, resource limit, OPEN 전달 실패 및 성공 확인 유실은 성공 또는 `FAILED` 중 하나의 terminal 결과로 끝나야 한다. Connector가 Listener queue 미도달을 증명하지 못한 채 성공 확인을 잃은 실패는 실제 queue 적재 여부와 관계없이 `MAYBE_OBSERVED`여야 하며 같은 attempt를 replay하거나 다른 binding으로 reroute해서는 안 된다. |
| `OPEN-013` | 실패한 시도의 provisional Pipe와 양쪽 임시 상태는 configured bound 안에 제거되거나 `CLOSED`가 되어야 한다. application이 이미 `accept`한 경우에도 같다. |
| `OPEN-014` | 늦거나 중복된 `OPENED`, `FAILED`, cancel 및 close는 terminal 결과를 바꾸거나 Pipe를 다시 살리지 못한다. |
| `OPEN-015` | Pipe 수립 뒤에는 RT가 해당 Pipe의 payload, close 또는 lifetime에 관여하지 않는다. |
| `OPEN-016` | Entry Gateway는 Connector SDK와 새 live session을 만들 때 자신의 `GatewayId` 범위에서 재사용하지 않는 `ConnectorSessionId`를 부여해야 한다. Connector SDK는 session 안에서 전송 순서가 strictly increasing인 `ConnectionId`를 할당한다. `(EntryGatewayId, ConnectorSessionId, ConnectionId)`를 Owner Gateway까지 전달하고 양 Gateway는 이 identity로 OPEN, cancel, terminal 응답과 cleanup을 상관시켜야 한다. |
| `OPEN-017` | Gateway는 ConnectorSession마다 remote `ConnectionId` high-watermark 하나를 유지하고 valid OPEN을 처리하기 전에 전진시켜야 한다. high-watermark 이하의 중복·지연 OPEN은 두 번째 provisional Pipe나 terminal 결과를 만들지 않고 제거한다. terminal identity별 tombstone은 보관하지 않으며, 이미 terminal이거나 알 수 없는 identity의 다른 늦은 frame도 기존 Pipe를 변경하거나 새 live state를 만들지 않는다. |
| `OPEN-018` | RT `BindingSet`은 connect attempt 안에서만 사용해야 한다. 선택 또는 terminal 결과 뒤 남은 후보를 이후 신규 connect의 authority로 보관해서는 안 된다. |
| `OPEN-019` | `PeerObservation`은 오류 이유와 별도의 증명 축이어야 한다. Listener queue에 들어가지 않았음을 Connector가 증명할 수 있는 실패만 `NOT_OBSERVED`, `OPENED`를 확인한 성공만 `OBSERVED`, 어느 쪽도 증명할 수 없는 terminal 실패는 실제 queue 적재 여부와 관계없이 `MAYBE_OBSERVED`로 분류해야 한다. |
| `OPEN-020` | 모든 connect attempt와 Connector Pipe endpoint는 하나의 live `ConnectorSession`에 속해야 한다. session 단절은 non-terminal attempt를 실패시키고 그 session의 established Pipe를 닫으며, remote provisional state에는 best-effort cancel 또는 close를 보내고 configured bound 안에 정리해야 한다. Connector에 terminal 결과를 전달할 수 없고 queue 미도달도 증명할 수 없으면 결과는 `MAYBE_OBSERVED`다. |
| `OPEN-021` | 재연결로 만든 새 `ConnectorSession`은 이전 session의 ConnectionId, attempt, Pipe 또는 terminal 결과를 승계하지 않는다. 이전 session identity의 늦은 frame은 새 session state를 변경할 수 없다. |
| `OPEN-022` | RT가 `ShardDirectoryGeneration` mismatch를 반환하면 해당 remote connect attempt는 `FAILED_PRECONDITION`, `NOT_OBSERVED`로 끝나고 Pipe를 만들거나 다른 shard 또는 binding으로 재시도해서는 안 된다. Gateway는 같은 process에서 directory generation을 바꾸지 않는다. |
| `OPEN-023` | local 후보가 없는 상태에서 RT `Resolve`의 component identity 또는 authorization 검증이 실패하면 해당 connect attempt는 `UNAUTHENTICATED` 또는 `PERMISSION_DENIED`, `NOT_OBSERVED`로 끝나야 한다. binding 선택, peer OPEN과 Listener queue admission은 일어나지 않으며 같은 attempt를 자동 재시도해서는 안 된다. |

## 연결 시도 불변식

```text
한 시도       -> terminal 결과 정확히 하나
성공한 시도   -> Connector Pipe 하나 + Listener Pipe 하나
실패한 시도   -> 사용 가능한 Pipe 없음 + bounded cleanup
선택한 binding -> 다른 binding으로 암묵적 이동 없음
MAYBE_OBSERVED -> 자동 replay 또는 reroute 없음
ConnectorSession 종료 -> 소유 attempt와 Pipe 종료, 새 session으로 이동 없음
```

timeout 값, selection algorithm과 resource limit 기본값은 배포 설정으로 정한다. 어떤 값을
사용하더라도 위 결과와 [SPEC 007](007-error-and-state-model.md)의 상태·오류 의미는 바뀌지 않는다.
