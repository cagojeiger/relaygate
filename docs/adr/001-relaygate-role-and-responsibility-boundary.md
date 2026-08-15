# ADR 001: RelayGate의 역할과 책임 경계

## Context

RelayGate의 경계가 없으면 연결 중계가 저장, 재전송, workflow까지 확장될 수 있다.

## Decision

RelayGate는 **주소 가능한 일시적 Pipe의 연결과 중계**를 책임진다.

```text
Endpoint = Listener가 연결을 받을 수 있는 논리적 주소
Pipe     = 두 참여자 사이의 일시적 양방향 연결
```

RelayGate는 Endpoint 탐색, Pipe 연결, 인가, backpressure가 있는 불투명 payload 전달과 종료 전파만
담당한다.

Offline storage, durable queue, pub/sub, 재전송, replay, resume, workflow와 application routing은
범위 밖이다. 끊어진 연결은 이어지지 않으며 재연결은 새 Pipe다.

## Consequences

- Buffer와 연결 상태는 일시적이다.
- Application이 retry, resume, deduplication과 업무 결과를 책임진다.
- 새 기능은 Pipe의 탐색·연결·전달·종료에 직접 필요할 때만 포함한다.
