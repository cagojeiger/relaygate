# ADR 002: Durable Controller Cohort And Current-State Recovery

## Context

RelayGate needs an authoritative directory for currently reachable Gateway sessions and exact routes. Losing that directory on every controller restart makes normal HA impossible, but retaining route history, tombstones, Pipe state, payload, or replay metadata would turn RelayGate into a durable broker.

This ADR replaces the obsolete volatile-cohort direction. Accepted meaning changes are recorded here rather than silently preserving older text.

## Decision

Production control state is owned by a `controller` role backed by durable embedded HashiCorp Raft.

- Each controller is a persistent Raft voter with a durable `raft.data_dir`.
- The Raft log is protocol history and may be compacted into snapshots.
- The application FSM stores only current `GatewaySession` records and exact routes.
- Snapshots contain only that current FSM. Successful snapshot compaction removes old logical log entries, while the Bolt file may retain reusable high-water pages instead of shrinking immediately.
- Route withdraw, gateway replacement, and gateway removal are true deletes with cascade of owned routes.
- The FSM stores no tombstone, generation history, credential, control-session ID, relay address, Pipe, payload, replay, or resume state.
- The current authority, revalidated control sessions, advertised owner relay addresses, and Open attempts are leader-local volatile state.
- Gateways reconnect to the current leader and send a full current binding snapshot to rebuild leader-local volatile state after authority changes.

Normal recovery stays in the same epoch and same Raft cluster:

| Condition | Required behavior |
| --- | --- |
| Controller restarts with its existing durable store | Reopen the same `NodeId`, log, stable state, and snapshots |
| Leader fails while quorum survives | Elect a same-epoch leader; reset leader-local authority/session/address state; gateways reconnect and revalidate |
| A controller store is lost | Start replacement with a fresh `NodeId`, add it as a voter, let it catch up, then remove the lost server |
| Quorum is unavailable | Fail closed for new authority/control/admission until quorum is restored |

`raft.bootstrap=true` is an external one-shot for initial empty-cluster formation. It is not production recovery. Disaster reset is explicit operator action: fence all old controller/control/gateway paths, choose a new epoch/cohort, and bootstrap a separate cluster from empty current application state.

Normal membership change is submitted to the surviving leader through the controller-local Unix-socket operator API. Exact Add/Remove retries are state-idempotent: they converge to the same committed membership, although Raft protocol log indexes are not an application-level idempotency contract.

Production controllers require durable volumes/PVCs. Compose named volumes are the local durable shape. `emptyDir` is disposable development storage only and is not HA.

## Consequences

- Controller storage is operationally significant and must be backed up, monitored, and replaced through Raft membership.
- Logical application-state cardinality follows current gateways/routes; physical volume sizing also accounts for Raft log bursts, snapshots, and the Bolt high-water mark.
- Storage loss of one controller is recoverable through add/catch-up/remove while surviving quorum remains.
- Quorum loss stops new admissions instead of inventing authority.
- Same-epoch failover preserves committed current `GatewaySession` and route FSM state but discards leader-local control sessions, addresses, and Open attempts.
- Open outcomes, Pipe handles, payload positions, and SDK delivery state remain unrecoverable by design.
- Relay capacity scales with stateless `gateway` replicas, not by mixing Relay into controllers.
