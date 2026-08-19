# ADR 008: Cross-Gateway Pipe

## Context

Caller ingress and Listener owner can differ, but the system must preserve one temporary Pipe contract. Turning this
into a durable queue or reconnect protocol would exceed RelayGate's responsibility.

## Decision

```text
Caller --public--> Ingress ==internal bidi stream==> Owner --public--> Listener
```

- Owner address exists only in current control session memory.
- Each remote Pipe uses one internal gRPC bidirectional stream.
- Authority issues an expiring single-use Open context bound to ingress, owner, auth, and exact binding.
- Owner revalidates the context and current local binding, then atomically reserves the attempt.
- An already reserved attempt fails closed without replaying a response or `PipeId`.
- Listener accept is the Open linearization point, and Owner creates the `PipeId` there.
- Ingress and Owner each own only their segment of the same logical `PipeId`.
- Each direction is FIFO, and buffers and waits are bounded.
- A public Relay stream that multiplexes multiple Pipes sends control/terminal and payload work on separate bounded
  lanes, so ready control/terminal work bypasses queued payload pressure.
- An internal peer stream that carries one Pipe serializes all sends through one bounded lane. If a blocked send reaches
  timeout or cancellation, it terminalizes that Pipe and cancels the stream; it does not promise a separate priority
  bypass inside a blocked gRPC write.
- The internal hop does not redial, retry, resume, or replay payload.

If the response or hop is lost after Open linearizes, the caller outcome may be `Unknown`. The caller starts a new Open
instead of attaching to the same request.

## Consequences

- Cross-Gateway paths keep the same volatile Pipe semantics as same-Gateway paths.
- Attempt outcomes and payload are not recovered after a Gateway crash.
- Expiring contexts assume bounded deployment clock skew.
- Internal peer transport is limited to trusted local/dev networks until authentication/mTLS is provided.
