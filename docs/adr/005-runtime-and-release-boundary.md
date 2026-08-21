# ADR 005: Runtime과 release 경계

## 배경

Server control-plane durability, stateless Relay capacity, public SDK compatibility는 서로 다른 축으로 변한다. Runtime role은 Raft/control 내부 구현을 SDK 밖에 두고 Gateway scale-out이 Controller quorum을 바꾸지 않게 해야 한다.

## 결정

RelayGate server는 하나의 Go binary/image와 두 startup role로 배포한다.

| Role | Component |
| --- | --- |
| `controller` | Durable HashiCorp Raft voter/store, current-only FSM, control authority/server, read-only admin |
| `gateway` | Control client, public Relay, internal peer Relay, auth/session/binding/Pipe runtime, read-only admin |

`controller`는 public/peer Relay를 실행하지 않는다. `gateway`는 Raft, durable store, control server, authoritative FSM을 열지 않는다. Role은 startup 때 고정되며 reload로 바꿀 수 없다.

Release unit은 다음 세 개다.

1. Root Go module server image
2. `github.com/cagojeiger/relaygate/sdk/go` Go module
3. `relaygate-sdk` Rust crate

Go/Rust SDK는 `proto/relaygate/relay/v1/relay.proto`만 공유한다. Generated server/control/Raft type은 private이다. Root runtime, Go SDK, Go example은 서로 독립된 module이며 repository workspace file 없이 build/test되어야 한다.

## 결과

- Production Controller와 Gateway runtime은 Go가 소유한다.
- Controller quorum과 Relay throughput은 독립적으로 확장된다.
- Public SDK는 server, control, Raft 구현에 의존하지 않는다.
- 동일 image를 환경별로 승격하고 deployment가 `controller` 또는 `gateway`를 선택한다.
