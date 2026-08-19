# ADR 001: RelayGate Role

## Context

Without a boundary for connection relay, product responsibility expands into storage, redelivery, and workflow.

## Decision

RelayGate connects and relays **addressable, temporary, bidirectional Pipes**.

- It finds the current Listener inside the authenticated namespace.
- It opens a Pipe, forwards opaque payloads with bounded backpressure, and propagates termination.
- SDK sessions, local Listener bindings, Pipes, buffers, and payloads exist only in Gateway process memory.
- The Controller persists only the control-plane directory made of current `GatewaySession` entries and exact routes in durable Raft. [ADR 002](002-current-state-cluster-and-recovery.md) defines this current-only exception and the recovery boundary.

RelayGate does not provide application, Pipe, or payload durable storage; message queues; pub/sub; application-level routing; workflow or application work; or Open, Pipe, or payload retry, replay, or resume. Fresh reconnect for Control/SDK sessions is separate from this prohibition.
When a connection is lost, the next connection is a new session, new Listener declaration, or new Pipe.

## Consequences

- RelayGate handles only current reachability and connection state. It does not preserve application outcomes or payload history.
- The application owns business-result storage, deduplication, and retry.
- New features are included only when directly required for Pipe discovery, connection, forwarding, or termination.
