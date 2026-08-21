# ADR 002: 영속 Controller 집합과 현재 상태 복구

## 배경

RelayGate에는 현재 연결 가능한 Gateway session과 exact route의 authoritative directory가 필요하다. Controller가 재시작할 때마다 directory를 잃으면 일반적인 HA가 불가능하다. 반대로 route history, tombstone, Pipe state, payload, replay metadata까지 보존하면 durable broker가 된다.

이 ADR은 과거의 volatile cohort 방향을 대체한다. 이미 수용된 의미 변경을 이전 문장에 숨기지 않고 이 결정에 기록한다.

## 결정

운영 제어 상태는 영속 내장 HashiCorp Raft를 사용하는 `controller` 역할이 소유한다.

- 각 Controller는 durable `raft.data_dir`를 가진 persistent Raft voter다.
- Raft log는 protocol history이며 snapshot으로 compact될 수 있다.
- 애플리케이션 FSM은 현재 `GatewaySession` record와 exact route만 저장한다.
- Snapshot에는 현재 FSM만 들어간다. 성공적인 snapshot compaction은 오래된 logical log entry를 제거하지만 Bolt file은 즉시 축소되지 않고 high-water page를 재사용할 수 있다.
- Route withdraw, Gateway 교체, Gateway 제거는 소유 route의 연쇄 삭제를 포함한 실제 삭제다.
- FSM은 tombstone, generation history, credential, control-session ID, relay address, Pipe, payload, replay, resume state를 저장하지 않는다.
- 현재 권한 주체, 재검증된 제어 세션, 광고된 소유자 Relay 주소, O 이전 context 발급 작업은 리더 로컬 휘발 상태다.
- O 성공 뒤 owner Gateway는 context expiry까지 bounded `AttemptId` replay fence만 유지하며 outcome이나 `PipeId`를 replay용으로 저장하지 않는다.
- 권한 주체가 바뀌면 Gateway는 현재 리더에 다시 연결하고 현재 바인딩 전체 snapshot을 보내 리더 로컬 상태를 재구축한다.

정상 복구는 같은 epoch와 같은 Raft cluster 안에서 수행한다.

| 조건 | 필수 동작 |
| --- | --- |
| 기존 durable store를 가진 Controller 재시작 | 같은 `NodeId`, log, stable state, snapshot을 다시 연다 |
| Quorum이 생존한 상태에서 leader 장애 | 같은 epoch의 leader를 선출하고 leader-local authority/session/address를 초기화한 뒤 Gateway가 reconnect/revalidate한다 |
| Controller store 유실 | 새 `NodeId`로 replacement를 시작하고 voter로 추가해 catch-up한 뒤 유실된 server를 제거한다 |
| Quorum 사용 불가 | Quorum이 복구될 때까지 새 authority/control/admission을 fail closed한다 |

`raft.bootstrap=true`는 비어 있는 cluster의 최초 형성을 위한 외부 one-shot이다. Production recovery 수단이 아니다. Disaster reset은 operator의 명시적 작업이다. 모든 과거 controller/control/gateway path를 fence하고 새 epoch/cohort를 선택한 뒤 빈 current application state에서 별도 cluster를 bootstrap한다.

정상 membership 변경은 살아 있는 leader의 controller-local Unix socket operator API로 제출한다. Exact Add/Remove retry는 현재 membership에 수렴하는 state-idempotent 동작이지만 Raft protocol log index 자체는 application idempotency contract가 아니다.

운영 Controller는 영속 volume/PVC가 필요하다. Compose 이름 있는 volume은 로컬 영속 구성이다. `emptyDir`은 폐기 가능한 개발용 저장소일 뿐 고가용성 구성이 아니다.

## 결과

- Controller 저장소는 운영상 중요한 상태이므로 백업, 감시, Raft 구성원 기반 교체가 필요하다.
- 논리 애플리케이션 상태 개수는 현재 Gateway·route 수에 비례한다. 물리 volume은 Raft log 급증, snapshot, Bolt 최대 도달 크기도 고려한다.
- Quorum이 생존하면 add/catch-up/remove로 Controller 하나의 storage loss를 복구할 수 있다.
- Quorum 상실에서는 권한 주체를 임의로 만들지 않고 새 허용 판정을 중단한다.
- Same-epoch failover는 committed current FSM을 보존하지만 leader-local control session, address, unfinished context issuance를 폐기한다. 성공한 owner O fence는 expiry까지 Gateway-local로 남는다.
- Open 결과, Pipe handle, payload 위치, SDK 전달 상태는 의도적으로 복구하지 않는다.
- Relay 용량은 영속 저장소가 없는 `gateway` replica로 확장한다.
