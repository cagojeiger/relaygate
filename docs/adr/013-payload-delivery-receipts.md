# ADR 013: End-to-End Payload Delivery Receipts

## Context

Before this decision, `Pipe.Send` proved only a bounded local transport handoff. That was insufficient for a sender that must decide whether
to retry after a relay hop: it cannot distinguish a payload that reached the peer SDK from one that stopped before the peer
receive queue.

RelayGate still must not become a durable broker. It does not own application processing, durable message storage, replay,
or exactly-once effects.

## Decision

- Every public and peer `PipePayload` carries an exact `PayloadId` scoped to its `PipeId` and direction.
- A receiving SDK emits `PipePayloadReceived` only after the exact payload is admitted to its bounded receive queue. This
  queue admission is the payload delivery linearization point.
- The sender reports success only after observing the exact receipt.
- Failure before the payload crosses the local authenticated-stream handoff boundary is `NotSent`. An exact remote refusal is `Rejected`.
  Timeout, Pipe/session loss, or transport loss after that boundary but before the receipt is observed is `Unknown`.
- Receipt and rejection messages carry the exact `PipeId` and `PayloadId`. Unknown, malformed, foreign, duplicate-conflicting,
  or wrong-phase correlation is protocol-fatal.
- `Unknown` is caller-visible and absorbing. A late exact receipt or rejection is a bounded no-op and never revises the result
  already returned to the caller.
- Sender pending state and receiver receipt history are bounded process memory owned by the SDK/Pipe runtime. They are not
  stored in Controller Raft and are not resumed after a Pipe, session, or process ends.
- RelayGate does not automatically retry or replay payload. An application that retries an `Unknown` delivery across a new
  Pipe must supply its own stable message identity and idempotent processing contract.

## Consequences

- `Send` has one precise success meaning: the peer SDK accepted the payload into its receive queue.
- Peer application read, processing, and durable commit remain outside the receipt contract.
- A lost receipt remains fundamentally ambiguous and is surfaced as `Unknown`, never as a stable failure.
- Same-Gateway and cross-Gateway Pipes have the same receipt semantics; each Gateway only relays exact correlated payload
  and receipt frames.
