# ADR 001: RelayGate의 역할

## Context

연결 중계의 경계를 정하지 않으면 저장, 재전송, workflow까지 제품 책임이 확장된다.

## Decision

RelayGate는 **주소 가능한 일시적 양방향 Pipe**를 연결하고 중계한다.

- 인증된 namespace 안에서 현재 Listener를 찾는다.
- Pipe를 열고, bounded backpressure로 불투명 payload를 전달하고, 종료를 전파한다.
- SDK session, local Listener binding, Pipe, buffer와 payload는 Gateway process memory에만 둔다.
- Controller가 durable Raft에 보존하는 것은 현재 `GatewaySession`과 exact route로 이루어진 control-plane directory뿐이다. 이 current-only 예외와 복구 경계는 [ADR 002](002-current-state-cluster-and-recovery.md)가 정한다.

Application/Pipe/payload durable storage, message queue, pub/sub, application-level routing, workflow와 application work 또는 Open/Pipe/payload retry, replay, resume은 제공하지 않는다. Control/SDK session의 fresh reconnect는 이 금지와 별개다.
연결이 끊어지면 다음 연결은 새 session, 새 Listener 선언 또는 새 Pipe다.

## Consequences

- RelayGate는 현재 도달 가능성과 연결 상태만 다루며 application outcome이나 payload history를 보존하지 않는다.
- 업무 결과의 저장, 중복 제거와 재시도는 application이 책임진다.
- 새 기능은 Pipe의 탐색, 연결, 전달 또는 종료에 직접 필요할 때만 포함한다.
