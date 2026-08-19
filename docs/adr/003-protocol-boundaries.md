# ADR 003: Protocol Boundaries

## Context

SDK traffic, Gateway internal communication, Raft, and operational observation have different exposure and trust scopes.

## Decision

| Boundary | Protocol | Exposure |
| --- | --- | --- |
| SDK ↔ Gateway | `relay.proto` gRPC/HTTP2 | Public Go/Rust SDK contract |
| Gateway ↔ authority | `control.proto` gRPC/HTTP2 | Internal |
| Ingress ↔ owner Gateway | `gateway.proto` gRPC/HTTP2 | Internal |
| Raft voter ↔ voter | HashiCorp Raft TCP transport | Internal implementation |
| Controller-local membership | `membership.proto` gRPC over Unix socket | Local operator only |
| Operational observation | Read-only HTTP/JSON | Observation only |

Protocol listeners and protobuf contracts are kept separate. Public SDKs do not expose control, peer, generated server, or Raft types.
REST does not provide relay or client/key mutation. Membership changes are accepted only through a permission-restricted Unix socket bound to the live Controller's Raft data directory; this local operator surface opens no additional TCP or REST port.

Public Relay is served behind TLS termination. mTLS for internal control, peer, and Raft transport is added at the deployment boundary and remains separate from the application protocol.

## Consequences

- Go/Rust SDKs share only one public wire contract.
- Internal protocols can change without a public compatibility promise.
- Transport security can be hardened independently at each trust boundary.
