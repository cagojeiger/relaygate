# ADR 005: Route mapping은 active lease에 연결된 soft state다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 003](003-client-id-listener-binding.md), [ADR 004](004-current-state-routing-topology.md) |

## 맥락

RouteTable의 목적은 과거를 복원하는 것이 아니라 현재 연결 가능한 Listener 위치를 찾는 것이다. durable history나 mutation replay를 관리하면 live session보다 저장 상태가 앞서는 별도 복구 문제가 생긴다.

## 결정

```text
Gateway current ListenerBinding set
             │ Register / Update / KeepAlive
             ▼
RouteTable current MappingEntry set
             │ replace / Deregister / expire
             ▼
           removed
```

Gateway가 현재 소유한 live `ListenerSession`과 `ListenerBinding`이 truth다. Gateway는 session-shard별 registration lease를 얻고 current binding snapshot으로 mapping을 갱신한다. RouteTable은 active lease에 연결된 `MappingEntry`만 memory-only soft state로 보관하며, 갱신되지 않은 mapping은 제거한다.

새 mapping state는 `Register`만 만들 수 있고 RT가 새 `LeaseId`를 발급한다. `Update`와 `KeepAlive`는 현재 active lease에만 적용된다. `Deregister`, expiry 또는 RT restart로 종료된 lease의 늦은 operation은 새 mapping을 만들거나 과거 mapping을 되살리지 못한다. Gateway는 새 lease를 등록한 뒤 현재 snapshot을 다시 보낸다.

여기서 `KeepAlive`는 Gateway가 RT registration lease를 갱신하는 control-plane operation이다. SDK와 Gateway 사이의 session liveness를 확인하는 periodic heartbeat와는 다른 계약이다. Owner Gateway는 lease가 active여도 process loss나 binding 제거와 lookup 사이의 경쟁을 막기 위해 `OPEN` 시점에 binding identity를 다시 확인한다.

## 결과

- RT state는 현재 live mapping과 registration lease 수에 비례한다.
- RT는 binding history, mutation log, Pipe와 payload를 저장하지 않는다.
- RT가 state를 잃으면 live Gateway의 current snapshot으로 다시 구성한다.
- current lease가 존재하는 동안 오래되거나 충돌하는 update는 그 lease와 revision을 바꾸지 못한다.
- 종료되거나 알려지지 않은 lease의 `Update`와 `KeepAlive`는 state를 만들지 않고 거절된다.
- RT는 tombstone이나 종료된 lease history를 보관하지 않으며 새 state 생성과 active lease 갱신을 분리한다.
- stale mapping이 선택되면 Owner Gateway가 binding을 재검증하고 같은 open은 실패할 수 있다. 새 후보 선택은 새 open의 책임이다.
- 기존 Pipe의 lifecycle은 route mapping 복구와 분리된다.

## 이 ADR에서 정하지 않는 것

- registration key, snapshot schema, lease identity와 revision 규칙
- lease 시간, keepalive 주기와 clock handling
- RT restart 중 외부에 보이는 상태와 오류

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [RFC 2205](../rfc/rfc-2205-rsvp-soft-state.md)
- [RFC 8656](../rfc/rfc-8656-turn-lifetime.md)
- [RFC 9301](../rfc/rfc-9301-lisp-control-plane.md)
