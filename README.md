# RelayGate

RelayGate는 Raft로 조정되는 Gateway cluster에서 일시적인 양방향 Pipe를 연결하고 중계한다.
연결과 payload는 복구하지 않는다.

## 구성

| 구성 | 책임 |
| --- | --- |
| Go Gateway runtime | Process lifecycle, config, gRPC/REST, Gateway relay, Raft integration |
| Go SDK / Rust SDK | 각 언어에 자연스러운 public API |
| protobuf | 두 SDK가 공유하는 wire contract |

- Relay data path는 protobuf gRPC를 사용한다.
- Raft transport는 server 내부 protocol이다.
- REST는 read-only 운영 관찰용이며 relay나 client/key CRUD를 제공하지 않는다.
- 인증된 연결은 하나의 `ClientId`에 격리되고, client와 API key는 external client config가 관리한다.

설계 원칙은 `docs/adr/`, 상태·동작 계약은 `docs/spec/`, 필수 검증 계획은 `docs/test/`에 있다.
장애와 복구 경계는 [SPEC 003](docs/spec/003-failure-and-recovery-model.md), 닫힌 상태 전이표는
[SPEC 004](docs/spec/004-state-transition-model.md), v0 필수 테스트는
[TEST 001](docs/test/001-core-correctness-test-plan.md)에서 정의한다.
