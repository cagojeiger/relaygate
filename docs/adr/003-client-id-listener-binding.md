# ADR 003: ClientId와 ListenerSession은 many-to-many다

| 항목 | 값 |
| --- | --- |
| 상태 | Accepted |
| 전제 | [ADR 001](001-relayed-pipe-responsibility-boundary.md) |

## 맥락

`ClientId`를 특정 Listener process나 Gateway 위치와 동일시하면 logical destination마다 단일 runtime owner가 생기고 수평 분산이 제한된다.

## 결정

`ClientId`는 위치가 아닌 logical destination identifier다. `ListenerBinding`은 하나의 `ClientId`와 현재 하나의 `ListenerSession`을 연결하는 live association이다.

```text
ClientId *  ◄──── ListenerBinding ────►  * ListenerSession

ClientId          -> 0..N live ListenerBinding locations
one SDK runtime   -> ClientId별 non-CLOSED Listener handle 0..1
one open          -> one live ListenerBinding
established Pipe  -> Connector 1 : 1 Listener
```

many-to-many는 identity와 runtime location을 분리하는 모델이다. broadcast, fan-out 또는 load-balancing 품질을 의미하지 않는다.

여러 runtime과 session은 같은 `ClientId`를 동시에 제공할 수 있지만, 하나의 Listener SDK runtime은 같은 `ClientId`의 non-closed Listener handle을 중복 생성하지 않는다. 중복 생성은 합치거나 별도 binding으로 바꾸지 않고 거절한다.

## 결과

- 같은 `ClientId`를 여러 ListenerSession이 동시에 제공할 수 있다.
- 하나의 ListenerSession이 여러 `ClientId`를 제공할 수 있다.
- 한 SDK runtime 안에서는 `ClientId`와 Listener handle의 관계가 명확하게 하나로 유지된다.
- 하나의 session이 사라져도 다른 live binding은 독립적으로 남을 수 있다.
- 연결마다 후보 중 하나를 선택해야 하지만 selection policy는 별도 계약이다.

## 참고

- [RFC 9299](../rfc/rfc-9299-lisp-architecture.md)
