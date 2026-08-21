# ADR 011: SDK session supervision

## 배경

Application이 session마다 authentication과 Bind를 직접 반복하면 transient connection recovery가 어렵다. SDK가 Open, Pipe, payload state를 retry하면 RelayGate가 queue/replay layer가 된다.

## 결정

Go/Rust SDK는 두 계층을 제공한다.

- `Client`는 authenticated Relay session 하나를 소유한다. Session 종료는 모든 child handle의 terminal이다.
- 권장 `ManagedClient`는 SDK 내부 supervisor 하나로 fresh `Client`를 reconnect한다. Raw `Client`는 session reconnect와 Listener redeclaration을 직접 소유하려는 advanced API다.

`ManagedClient`는 current logical Listener declaration만 memory에 유지한다. Session이 끝나면 old Listener, Offer, Open, Pipe를 종료한다. Bounded backoff 뒤 새 session을 인증하고 current Listener를 fresh Bind한다. 모든 Listener Bind가 완료된 뒤에만 `Ready`다.

`Open`은 `Ready` session에 정확히 한 번 제출한다. Reconnect/rebind 중에는 queue하지 않고 `NotReady`로 거부한다. Open outcome, Pipe, payload를 다음 session에서 retry, replay, resume하지 않는다. Permanent auth/config/protocol error는 supervisor를 종료한다.

## 결과

- 별도 daemon이나 server state 없이 SDK reconnect를 지원한다.
- Supervisor memory는 current logical Listener 수에만 비례한다.
- Credential 변경에는 새 `ManagedClient`가 필요하다.
- `Close`는 connect/backoff를 취소하고 supervisor를 join한다.
