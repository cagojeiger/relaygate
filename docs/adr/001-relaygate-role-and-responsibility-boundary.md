# ADR 001: RelayGate의 역할

## Context

연결 중계의 경계를 정하지 않으면 저장, 재전송, workflow까지 제품 책임이 확장된다.

## Decision

RelayGate는 **주소 가능한 일시적 양방향 Pipe**를 연결하고 중계한다.

- 인증된 namespace 안에서 현재 Listener를 찾는다.
- Pipe를 열고, bounded backpressure로 불투명 payload를 전달하고, 종료를 전파한다.
- 연결 상태와 buffer는 process memory에만 둔다.

Durable storage, queue, pub/sub, application routing, workflow, retry, replay와 resume은 제공하지 않는다.
연결이 끊어지면 다음 연결은 새 session, 새 Listener 선언 또는 새 Pipe다.

## Consequences

- RelayGate는 현재 연결 상태만 다룬다.
- 업무 결과의 저장, 중복 제거와 재시도는 application이 책임진다.
- 새 기능은 Pipe의 탐색, 연결, 전달 또는 종료에 직접 필요할 때만 포함한다.
