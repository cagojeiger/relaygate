# ADR 006: Client Isolation and Credentials

## Context

Clients that use the same Endpoint need a namespace that cannot be bypassed and a single credential source.
This final document consolidates and replaces the client isolation and credential verification decisions from the previous ADR 006 and ADR 007. The previous decision records remain in Git history.

## Decision

`ClientId` is a strict namespace determined by authentication. Bindings, routes, and Pipe operations are interpreted only inside that namespace. Cross-client lookup is not allowed.

The source of truth for clients and API keys is external config.

- API keys are limited to operator-generated, high-entropy bearer secrets.
- One `ClientId` can have multiple immutable `ApiKeyId` values for rotation.
- Config stores only `sha256:<64 lowercase hex>` verifiers, not raw keys.
- Gateway compares exact `(ClientId, ApiKeyId)` verifiers in constant time.
- RelayGate does not store credentials in a database or Raft and does not provide a CRUD API.
- Only the first message of public `Relay.Connect` contains the raw key. Later identity comes from the authenticated session.
- Invalid startup config fails closed. An invalid candidate during reload is rejected and the current snapshot is preserved.
  Only valid candidates are applied atomically, and sessions for removed credentials are terminated.

Non-loopback Public Relay is allowed only when TLS is provided.

## Consequences

- RelayGate supports per-client route isolation and uninterrupted key rotation together.
- External config owns the credential lifecycle.
- Raw keys are not recorded in logs, state, Raft, or config.
