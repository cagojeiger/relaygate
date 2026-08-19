# ADR 011: SDK session supervision

## Context

Applications that repeat authentication and Bind directly on every session make transient connection recovery difficult.
If the SDK retries Open, Pipe, or payload state, RelayGate becomes a queue/replay layer.

## Decision

The Go/Rust SDKs provide two layers.

- `Client` owns one authenticated Relay session. Session end is terminal for every child handle.
- Opt-in `ManagedClient` reconnects a fresh `Client` through one SDK-internal supervisor.

`ManagedClient` keeps only current logical Listener declarations in memory. When a session ends, it terminates old
Listeners, Offers, Opens, and Pipes; after bounded backoff, it authenticates a new session and performs fresh Bind for
the current Listeners. It is `Ready` only after all Listeners complete Bind.

`Open` is submitted exactly once to a `Ready` session. During reconnect and rebind, `Open` is rejected as `NotReady`
instead of queued. Open outcomes, Pipes, and payload are not retried, replayed, or resumed in the next session. Permanent
auth, configuration, or protocol errors stop the supervisor.

## Consequences

- SDK reconnect works without a separate daemon or server state.
- Supervisor memory is proportional only to the current logical Listener count.
- Changing credentials requires a new `ManagedClient`.
- `Close` cancels connect/backoff and joins the supervisor.
