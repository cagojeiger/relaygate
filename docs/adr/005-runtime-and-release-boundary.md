# ADR 005: Runtime And Release Boundary

## Context

Server control-plane durability, stateless Relay capacity, and public SDK compatibility change on different axes. The runtime roles must keep Raft/control internals out of the SDK and keep Gateway scale-out from changing controller quorum.

## Decision

RelayGate server ships as one Go binary/image with two startup roles.

| Role | Components |
| --- | --- |
| `controller` | Durable HashiCorp Raft voter/store, current-only FSM, control authority/server, read-only admin |
| `gateway` | Control client, public Relay, internal peer Relay, auth/session/binding/Pipe runtime, read-only admin |

The `controller` role does not run public or peer Relay. The `gateway` role does not open Raft, a durable store, a control server, or the authoritative FSM. Role is fixed at startup and cannot be changed by reload.

Release units are:

1. Root Go module server image
2. `github.com/cagojeiger/relaygate/sdk/go` Go module
3. `relaygate-sdk` Rust crate

The Go and Rust SDKs share only `proto/relaygate/relay/v1/relay.proto`. Generated server/control/Raft types remain private. Root `go.work` is a local development convenience; the Go SDK must build/test with `GOWORK=off`.

## Consequences

- Production controller and Gateway runtime are Go-owned.
- Controller quorum and Relay throughput scale independently.
- Public SDKs cannot depend on server, control, or Raft implementation details.
- One image can be promoted through environments while deployment selects `controller` or `gateway`.
