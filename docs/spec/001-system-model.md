# SPEC 001: 시스템 모델

## 범위

RelayGate는 인증된 호출자와 현재 연결 가능한 Listener 사이에 임시 양방향 Pipe를 만든다. 오프라인 저장소, 영속 대기열, 발행·구독, 재시도, 재개, 중복 제거, 작업 흐름은 애플리케이션 책임이다.

```mermaid
flowchart LR
    SDK["Go / Rust SDK"] <--> GW["Gateway\npublic Relay"]
    GW <--> CTL["Controller leader\ncontrol gRPC"]
    CTL <--> RAFT["Durable Raft quorum\ncurrent FSM"]
    GW <--> OWNER["Owner Gateway\npeer relay"]
    CTL --> REST["Read-only admin"]
    GW --> GREST["Read-only admin"]
```

공개 Relay, 제어, Peer Relay, Raft TCP, REST는 서로 분리된 프로토콜·신뢰 경계다.

## 식별자와 소유권

```text
BindingKey       = (ClientId, EndpointPattern, TargetId)
GatewaySession   = (GatewayId, GatewayInstanceId)
ControlSession   = (ClusterEpoch, AuthorityId, ControlSessionId,
                    GatewayId, GatewayInstanceId)
ListenerBinding = (GatewayId, GatewayInstanceId, ListenerBindingId)
Pipe participant = exact ClientSessionRef
```

`ClientId`는 인증으로 결정되는 엄격한 이름 공간이다. v0 Open은 문자 그대로 일치하는 endpoint와 필수 exact target을 사용한다. 와일드카드, 우선순위, target 생략, `OpenAll`은 범위 밖이다.

| 소유자 | 상태 | 영속성 |
| --- | --- | --- |
| Controller Raft | term, vote, log, membership, stable state, snapshot, `NodeId` | 영속 `raft.data_dir` |
| Controller FSM | `ClusterEpoch`, 현재 `GatewaySession`, exact current route | 영속 Raft log/snapshot |
| 현재 권한 주체 | `AuthorityId`, 제어 세션, 재검증된 거울, owner relay address | 리더 로컬 메모리 |
| Gateway | 인증·세션, 로컬 바인딩, 시도 차단, Pipe 구간, 버퍼, payload | 프로세스 메모리 |
| 외부 설정 | Client/API key 검증기 | 외부 YAML |

FSM은 현재 Gateway 세션과 exact route만 저장한다. 부재가 삭제를 뜻하며 제어 세션 ID, owner relay address, route tombstone·이력, 인증 정보, Pipe, payload, 재생, 재개 상태는 저장하지 않는다.

## 실행 역할

| 역할 | 로컬 소유 항목 | 소유하지 않는 항목 |
| --- | --- | --- |
| `controller` | Raft 투표자·저장소, 현재 FSM, 권한·제어 서버, 관리 API | 공개 Relay, Peer Relay, SDK 세션 |
| `gateway` | 제어 클라이언트, 공개·Peer Relay, 인증·세션·바인딩·Pipe 실행 상태, 관리 API | Raft 노드·저장소, 권한 주체, 제어 listener |

역할은 프로세스 시작 때 고정된다. Gateway 준비 상태는 현재 제어 연결을 요구한다. Controller `/healthz/ready`는 구성원 준비 상태다. 로컬 FSM에 `ClusterEpoch`가 초기화되고 Raft leader가 보이면 정상 follower도 준비 상태다. 권한 주체 전용 관찰은 `/status`이며 follower나 quorum 상실에서는 `503/NoAuthority`를 반환한다.

## Controller 집합 수명주기

최초 bootstrap은 비어 있는 Controller 저장소를 위한 외부 일회성 작업이다. 이후에는 합의된 Raft 구성원이 기준 상태다.

1. Controller는 Raft 식별자, log, stable state, 구성원, snapshot을 영속 볼륨에 저장한다.
2. 같은 저장소를 사용한 재시작은 bootstrap 없이 기존 `NodeId`와 상태를 다시 연다.
3. 같은 epoch의 leader 장애 전환은 새 권한 주체를 만들고 리더 로컬 `V`를 초기화한다.
4. Gateway가 다시 연결하고 현재 바인딩 전체 snapshot으로 `V`를 재구축한다.
5. Controller 저장소 유실은 살아 있는 quorum에서 새 `NodeId`를 leader 전용 add/catch-up/remove 절차로 교체한다. 변경 인터페이스는 실행 중인 Controller data directory의 권한 제한 Unix socket이며 관리 REST는 읽기 전용이다.
6. Quorum 상실에서는 새 권한·제어·허용 판정을 닫힌 실패로 처리한다.

재해 초기화는 기존 Raft 상태 기계의 복구가 아니다. 운영자는 이전 Controller·제어·Gateway 경로를 차단하고 새 epoch와 집합을 빈 현재 애플리케이션 상태에서 bootstrap해야 한다. `bootstrap=true`를 구성원 교체에 사용하면 안 된다.

운영 Controller는 영속 PVC 또는 동등한 영속 볼륨을 사용한다. Compose는 이름 있는 Controller 볼륨을 사용하고 `emptyDir`은 폐기 가능한 개발용 저장소로만 허용한다.

## 제어 세션과 경로 목록

```mermaid
sequenceDiagram
    participant G as Gateway
    participant A as Controller authority
    participant R as Raft FSM
    G->>A: Hello(epoch, gateway, instance, relay address)
    A->>R: RegisterGateway
    A-->>G: SessionOpened(exact ControlSessionRef)
    G->>A: FullSnapshot(current LiveBinding only)
    A->>R: ReplaceSnapshot
    A-->>G: SnapshotAccepted
    G->>A: serial Declare / Withdraw
    A->>R: DeclareRoute / WithdrawRoute
```

- `C`는 현재 Gateway 세션과 exact route로 구성된 합의 완료 FSM 상태다.
- `V`는 현재 제어 세션, 수락된 전체 snapshot, owner relay address로 구성된 리더 로컬 검증 상태다.
- Exact `C`와 exact `V`가 모두 존재해야 route를 사용할 수 있다.
- `Syncing` 세션은 사용할 수 없다.
- 전체 snapshot 설치는 원자적이다. 유효하지 않거나 충돌하거나 용량을 넘는 snapshot은 아무것도 설치하지 않는다.
- 동일 세션의 동일 선언은 멱등이다.
- 동일 route key의 다른 owner/ref는 충돌이다.
- Withdraw는 exact current route를 삭제한다.
- Gateway 교체는 새 전체 snapshot 전에 이전 instance 소유 route를 삭제한다.
- Gateway 제거는 해당 Gateway가 소유한 exact route를 연쇄 삭제한다.
- 권한 주체 변경은 영속 `C`가 아니라 `V`를 초기화한다. 재연결과 전체 snapshot이 사용 가능 상태를 복구한다.

## 새 Pipe 허용 판정

```text
Admit = A ∧ L ∧ Q ∧ C ∧ V ∧ O
```

| 판정 조건 | 충족 조건 |
| --- | --- |
| `A` | caller auth/session이 current |
| `L` | current authority가 epoch의 confirmed leader |
| `Q` | quorum verification과 read barrier 성공 |
| `C` | committed current FSM에 exact `(ClientId, endpoint, target)` route 존재 |
| `V` | exact owner control session이 current/revalidated이고 relay address 보유 |
| `O` | owner가 authority/session/auth/binding/expiry/capacity를 재검사하고 attempt reserve |

`111111`만 Listener offer를 만든다. Context issuance는 reservation이나 Pipe가 아니다. O와 성공한 `AttemptId` fence insertion은 하나의 atomic owner effect다.

## Bind, Open, Pipe, SDK

- Bind는 local pending binding을 만들고 Controller ACK 뒤에만 live가 된다.
- Unbind/revocation/session end는 먼저 local binding을 ineligible하게 만들고 exact withdraw를 시도한다.
- Bind/Unbind의 validation, capacity, conflict, control-unavailable은 operation-local response다. Valid Relay session을 종료하지 않는다.
- 인증·세션 종료, 잘못된 프로토콜 상태, stream 전송 실패는 세션을 종료하는 gRPC 오류다.
- Listener 수락이 Open 선형화 지점이며 `PipeId`를 만든다.
- 선형화 뒤 응답·구간 손실은 호출자에게 `Unknown`이 될 수 있다.
- 진입 Gateway는 exact 소유 Gateway 식별자·주소마다 공유 gRPC/HTTP2 연결 하나를 유지한다. 원격 Pipe마다 이 연결 위에 독립 양방향 stream 하나를 연다.
- 소유자 식별자·주소 변경은 새 연결로 교체한다. 이전 연결은 기존 Pipe stream이 모두 끝난 뒤 닫는다.
- 유휴 공유 연결은 최대 64개 또는 `max_pipes` 중 작은 값으로 제한하며 LRU로 제거한다. 따라서 과거 Gateway ID 변동으로 socket이 무한히 쌓이지 않는다.
- Peer stream 하나의 제한 시간 초과·취소는 그 Pipe만 종료하고 공유 연결과 다른 stream은 유지한다. 연결 수준 실패는 해당 연결의 stream 모두를 끝낸다.
- Payload는 opaque, bounded, per-direction FIFO이며 exact `PayloadId`를 가진다. `Send`는 peer SDK bounded receive queue admission과 exact receipt 반환 뒤에만 성공한다. Peer application processing이나 durable commit은 아니다. Pre-handoff failure=`NotSent`, exact refusal=`Rejected`, post-handoff receipt loss=`Unknown`이다.
- 다중화된 공개 Relay stream은 제어·종료와 payload에 별도 상한 대기열을 사용한다.
- Pipe별 Peer stream은 상한 대기열 하나에서 전송을 직렬화한다. 전송 제한 시간 초과·취소는 Pipe와 stream을 종료하며 막힌 gRPC 쓰기를 우선순위로 우회하거나 조용히 버리거나 재시도·재생하지 않는다.
- `ManagedClient`는 세션과 현재 Listener 선언만 재연결한다. 준비되지 않은 상태의 Open을 거부하고 Open·Pipe·payload 상태를 재생하지 않는다.

## 상태 관찰

`/status`는 관찰 전용이다. Controller는 합의된 `C`의 `committed_gateways`, `committed_routes`, `V`의 `revalidated_gateways`, exact `C/V`가 일치하는 `eligible_routes`를 분리해 보고한다. Gateway 상태는 제어 클라이언트 준비 상태를 노출할 수 있다. 이 값은 현재 관찰 개수일 뿐 완전성, 폐기 증명, 허용 성공을 뜻하지 않는다. Follower나 quorum 불확실성은 권한 관찰·허용 판정을 닫힌 실패로 처리하지만 정상 follower는 `/healthz/ready`에서 구성원 준비 상태일 수 있다.

## 불변식

1. 모든 상태 전진은 exact epoch·세션·instance·바인딩·참여자 식별자를 요구한다.
2. 오래된 식별자는 현재 상태를 생성하거나 삭제할 수 없다.
3. 영속 FSM은 현재 상태만 가지며 삭제는 tombstone·이력을 남기지 않는다.
4. 새 Open은 여섯 조건을 모두 요구한다. 이후 권한·quorum 허용 실패만으로 수락된 Pipe를 종료하지 않는다.
5. 용량 초과는 새 작업을 거부하며 기존 실행 상태를 축출하지 않는다.
6. 세션 재연결은 현재 Listener만 새로 Bind한다. Open 재시도, 응답 재생, Pipe 재개·연결, payload 재생은 없다.
7. Payload 확인 상태는 Pipe 로컬 상한 메모리이며 Controller Raft에 들어가지 않고 관찰하지 못한 확인을 확정 성공·실패로 바꾸지 않는다.
