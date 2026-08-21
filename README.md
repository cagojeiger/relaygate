# RelayGate

RelayGate connects authenticated callers to currently reachable Listeners with short-lived bidirectional Pipes. It does not store payloads, replay responses, resume Pipes, or provide durable delivery.

## Architecture

```text
Go/Rust SDK -- public Relay gRPC --> Gateway
                                      |
                                      | control gRPC
                                      v
                  +---------------- Controller quorum ----------------+
                  | durable HashiCorp Raft + current-state FSM         |
                  | control authority + read-only admin                |
                  +----------------------------------------------------+
                                      |
                                      | owner relay address from live control session
                                      v
                                Owner Gateway
```

The server binary has two startup roles.

| Role | Owns | Does not own |
| --- | --- | --- |
| `controller` | Durable embedded HashiCorp Raft voter/store, authoritative current-only `GatewaySession`/exact route FSM, control gRPC, read-only admin | Public Relay, peer Relay, SDK sessions, Pipe payload |
| `gateway` | Public Relay, peer Relay, control client, auth/session/binding/Pipe runtime | Raft node/store, control server, authoritative FSM |

Raft persists term, vote, log, membership, stable state, snapshots, `NodeId`, `ClusterEpoch`, current gateway sessions, and exact routes under `raft.data_dir`. The FSM and snapshots store only current `GatewaySession` records and exact routes. Delete, withdraw, session replacement, and gateway removal are true deletes: no application tombstone, route generation, payload, Pipe state, credential state, or separate route history is retained there. The Raft protocol log remains until snapshot compaction.

Successful snapshots compact old logical Raft log entries and retain the configured snapshot count. The Bolt file may keep previously allocated pages as a reusable disk high-water mark instead of shrinking immediately, so physical volume size is monitored separately from current FSM cardinality.

Controller-leader-local volatile state includes the current `AuthorityId`, control sessions, revalidated gateway mirror, and owner relay addresses. Leadership change resets that authority layer; gateways reconnect and send a full current binding snapshot to rebuild it from live gateway memory and the committed current FSM.

Gateway-local volatile state includes authenticated SDK sessions, local Listener bindings, Open attempt fences, Pipe segments, buffers, and payload. Gateway restart discards that process-local state; SDKs reconnect and fresh-bind their current Listener declarations.

## Repository

```text
cmd/relaygate/                         process entrypoint
internal/
├── app/{relaygate,config,admin}       composition, config, observation
├── raft/{state,node,membership}       durable Raft, current FSM, local operator socket
├── gateway/
│   ├── access/{auth,session,runtime}  authentication and reload retirement
│   ├── control/{model,authority,client,server,transport}
│   ├── routing/{binding,opening}      Open context, Listener, and Pipe lifecycle
│   └── relay/{public,peer}            SDK ingress and Gateway hop
└── gen/{control,gateway,operator,relay}/v1
                                        generated protobuf
proto/relaygate/                       canonical wire schemas
sdk/go                                independent public Go module
sdk/rust/relaygate-sdk                independent public Rust crate
examples/echo/                         isolated echo example
docs/{adr,spec,test}/                  decisions, contracts, evidence
```

## Recovery Model

Normal recovery keeps the same Raft cohort and durable controller stores.

| Scenario | Result |
| --- | --- |
| Controller process restarts with the same volume and `NodeId` | Reopens the durable Raft store and snapshots; no bootstrap is required |
| Same-epoch leader fails while quorum survives | New leader forms; authority state starts empty; gateways reconnect and full-snapshot current bindings |
| Controller store is lost | Do not reuse the erased `NodeId`; add a replacement with a new `NodeId`, let it catch up, then remove the lost member through Raft membership |
| Quorum is lost | Fail closed for new control/admission until quorum is restored |
| Disaster reset | Only after explicitly fencing old controller/control/gateway paths, start a separate new epoch/cohort; this is not automatic production recovery |

`raft.bootstrap=true` is one-shot initial cluster formation for an empty controller store. It is not a production recovery mechanism and must not be used to resurrect a lost member identity.

Membership changes use no REST endpoint or additional TCP port. Run the same binary inside the current leader controller so it reaches that process's protected Unix socket:

```bash
relaygate membership list -config /etc/relaygate/relaygate.yaml
relaygate membership add -node-id controller-4 -raft-address controller-4:27400 -config /etc/relaygate/relaygate.yaml
# Wait until controller-4 is caught up and ready before removing the lost ID.
relaygate membership remove -node-id controller-3 -config /etc/relaygate/relaygate.yaml
```

Start a lost-store replacement first with a fresh `NodeId`, empty data directory, and `bootstrap=false`. Retrying the exact Add or Remove is safe against a lost CLI response and returns the current membership; it does not replay application work.

Production controllers need durable volumes/PVCs. The local Compose file uses named volumes for each controller. `emptyDir` is only acceptable for disposable development environments where losing the cluster is expected and not HA.

## Pipe And SDK Contract

- `ClientId` is set by authentication and is a strict namespace.
- Public Go/Rust SDKs share only `relay.proto`; server, control, and Raft types stay private.
- Open admission requires active client session, current authority, quorum, exact directory route, revalidated owner session, and owner reservation.
- A remote Pipe uses one internal peer gRPC stream. There is no internal redial, retry, resume, or payload replay.
- `Send` success means the peer SDK admitted the exact `PayloadId` to its bounded receive queue and the correlated receipt
  returned. It does not mean peer application processing or durable commit; receipt loss after local transport handoff is `Unknown`.
- On each multiplexed public Relay stream, separate bounded control/terminal and payload lanes let control and terminal work bypass queued payload pressure.
- The one-stream-per-Pipe peer hop serializes sends through one bounded lane. A blocked send that reaches its timeout or cancellation fails the Pipe and cancels the stream; it does not silently drop, retry, or replay the frame.
- `ManagedClient` is the recommended SDK entry point. It reconnects a session and fresh-binds current Listeners. Raw `Client` is the advanced session-owned API. Neither path queues Opens, replays outcomes, resumes Pipes, or replays payload.
- Bind/Unbind validation, capacity, conflict, and availability failures are operation-local; authentication, session, protocol, and transport failures end the Relay stream.
- Any payload rejection ends the SDK's exact Pipe view; the server terminalizes only an exact owned Pipe. Payload is never retried or replayed.

## Install

Release `0.1.0` consists of the server image and the matching Go and Rust SDKs:

```bash
docker pull ghcr.io/cagojeiger/relaygate:v0.1.0
go get github.com/cagojeiger/relaygate/sdk/go@v0.1.0
cargo add relaygate-sdk@0.1.0
```

All three artifacts use the same public Relay protobuf contract. Release mechanics are documented in [RELEASING.md](RELEASING.md).

## Local Configuration

```bash
# First creation of this disposable single-node data directory only:
RELAYGATE_RAFT_BOOTSTRAP=true go run ./cmd/relaygate -config ./configs/relaygate.yaml
curl http://127.0.0.1:27490/healthz/ready
curl http://127.0.0.1:27490/status
```

After the initial store exists, start it with the file's default `bootstrap: false`. Do not reuse the one-shot bootstrap input after deleting or replacing a production member store.

Key ports:

| Port | Purpose |
| --- | --- |
| `27400` | Controller Raft TCP |
| `27410` | Controller control gRPC |
| `27420` | Gateway public Relay gRPC |
| `27430` | Gateway internal owner relay gRPC |
| `27490` | Read-only Admin HTTP |

Compose starts three controllers with named volumes and two gateways that have no durable store. Initial cluster formation sets the command-scoped bootstrap input exactly once:

```bash
RELAYGATE_BOOTSTRAP_ONCE=true docker compose up -d --build
# Recreate only controller-1 with the steady-state default (bootstrap=false).
docker compose up -d --no-build --no-deps --force-recreate controller-1
curl http://127.0.0.1:27591/status
curl http://127.0.0.1:27594/status
docker compose down --remove-orphans
```

Later starts use only `docker compose up -d`. `RELAYGATE_BOOTSTRAP_ONCE` is not a recovery mechanism and must not be reused after a controller volume is erased.

The smoke harness exercises the multi-role local shape and removes the generated project resources after it runs:

```bash
./scripts/compose-smoke.sh
```

## Security And Production Boundary

Current internal control, peer, Raft transport, and Admin REST assume a trusted local/dev network. Production evidence still needs:

- Internal peer/control identity authentication or mTLS
- Raft transport mTLS policy
- `ClockSkewBound < relay.open_timeout` evidence
- Production operator runbook evidence for controller replacement, quorum restoration, and explicit disaster reset fencing

Contracts: [SPEC 001](docs/spec/001-system-model.md), [SPEC 003](docs/spec/003-failure-and-recovery-model.md), [SPEC 004](docs/spec/004-state-transition-model.md). Evidence: [TEST 001](docs/test/001-core-correctness-test-plan.md), [TEST 002](docs/test/002-failure-evidence-matrix.md). Runtime decisions: [ADR 002](docs/adr/002-current-state-cluster-and-recovery.md), [ADR 005](docs/adr/005-runtime-and-release-boundary.md).
