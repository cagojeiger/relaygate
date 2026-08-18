# ADR 011: SDK session supervision

## Context

Application이 인증과 Bind를 매번 직접 반복하면 일시적 연결 장애 복구가 어렵다. 반대로 SDK가 Open, Pipe나
payload를 재시도하면 RelayGate가 queue/replay 계층이 된다.

## Decision

Go/Rust SDK는 두 계층을 제공한다.

- `Client`는 인증된 Relay session 하나를 소유한다. Session 종료는 모든 child handle의 terminal이다.
- Opt-in `ManagedClient`는 SDK 내부 supervisor 하나로 fresh `Client`를 재접속한다.

`ManagedClient`는 current logical Listener 선언만 memory에 둔다. Session이 끝나면 old Listener, Offer, Open과
Pipe를 종료하고 bounded backoff 뒤 새 session을 인증한 다음 현재 Listener를 fresh Bind한다. 모든 Listener가
Bind되어야 `Ready`다.

`Open`은 `Ready` session에 한 번만 제출한다. Reconnect와 rebind 중에는 queue하지 않고 `NotReady`로 거부한다.
Open outcome, Pipe와 payload는 다음 session에서 retry, replay 또는 resume하지 않는다. Permanent auth,
configuration 또는 protocol error는 supervisor를 종료한다.

## Consequences

- SDK reconnect는 별도 daemon이나 server state 없이 동작한다.
- Supervisor memory는 현재 logical Listener 수에만 비례한다.
- Credential을 바꾸려면 새 `ManagedClient`를 만든다.
- `Close`는 connect/backoff를 취소하고 supervisor를 join한다.
