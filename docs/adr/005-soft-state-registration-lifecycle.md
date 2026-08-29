# ADR 005: Route mapping은 current snapshot에서 파생한 soft state다

| 항목 | 값 |
| --- | --- |
| 상태 | Proposed |
| 전제 | [ADR 003](003-client-id-listener-binding.md), [ADR 004](004-current-state-routing-topology.md) |

## 맥락

RouteTable의 목적은 과거를 복원하는 것이 아니라 현재 연결 가능한 Listener 위치를 찾는 것이다. durable history나 mutation replay를 관리하면 live session보다 저장 상태가 앞서는 별도 복구 문제가 생긴다.

## 결정

```text
Gateway current ListenerBinding set
             │ publish / refresh
             ▼
RouteTable current BindingProjection set
             │ replace / release / expire
             ▼
           removed
```

Gateway가 현재 소유한 live `ListenerSession`과 `ListenerBinding`이 truth다. Gateway는 session-shard별 current binding snapshot을 게시한다. RouteTable은 그 snapshot과 lease에서 파생한 projection만 memory-only soft state로 보관하며, 갱신되지 않은 projection은 제거한다.

`Release`와 expiry는 tombstone을 남기는 hard fence가 아니다. RT가 한 scope를 이미 잊은 뒤 늦은 `PublishCurrent`가 도착하면 그 snapshot을 새 current observation으로 다시 받아들일 수 있다. 이 projection은 live binding의 증명이 아니므로 Owner Gateway가 `OPEN` 시점에 binding identity를 다시 확인한다. 더 이상 늦은 publish나 refresh가 도착하지 않는 quiescent 상태에서는 마지막으로 수락한 갱신의 lease가 만료된 뒤 projection이 사라진다.

## 결과

- RT state는 현재 live registration과 publication lease 수에 비례한다.
- RT는 binding history, mutation log, Pipe와 payload를 저장하지 않는다.
- RT가 state를 잃으면 live Gateway의 current snapshot으로 다시 구성한다.
- current lease가 존재하는 동안 오래되거나 충돌하는 publication은 그 lease와 revision을 바꾸지 못한다.
- release 직후 늦은 publication이 projection을 잠시 되살릴 수 있지만 잘못된 Pipe를 열 수는 없다.
- stale projection이 선택되면 다른 live binding이 함께 있어도 같은 connect는 실패할 수 있다. 새 후보 선택은 새 connect의 책임이다.
- RT는 tombstone이나 종료된 lease history를 보관하지 않으므로 상태 크기는 과거 operation 수에 비례하지 않는다.
- 기존 Pipe의 lifecycle은 route mapping 복구와 분리된다.

## 이 ADR에서 정하지 않는 것

- snapshot schema, lease identity와 revision 규칙
- lease 시간, refresh 주기와 clock handling
- RT restart 중 외부에 보이는 상태와 오류

## 참고

- [RFC 1958](../rfc/rfc-1958-internet-architecture.md)
- [RFC 2205](../rfc/rfc-2205-rsvp-soft-state.md)
- [RFC 8656](../rfc/rfc-8656-turn-lifetime.md)
- [RFC 9301](../rfc/rfc-9301-lisp-control-plane.md)
