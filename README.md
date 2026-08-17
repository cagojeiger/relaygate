# RelayGate

RelayGate는 작은 Raft-coordinated Gateway cluster에서 **현재 연결 가능한 Listener**를 찾아 일시적 양방향
Pipe를 연결한다. Route history, Pipe와 payload는 복구하지 않는다.

## Architecture

```text
Go/Rust SDK -- public Relay gRPC --> Gateway
                                      |
                    +-----------------+-----------------+
                    |                                   |
             control gRPC                         peer relay gRPC
                    |                                   |
            current Authority                    remote Owner Gateway
                    |
              Raft TCP quorum
```

- Raft는 term/vote/log/membership/snapshot과 `ClusterEpoch`만 저장한다.
- Current authority는 control session과 live route directory를 memory에만 둔다.
- Gateway는 자기 Listener, Pipe segment, bounded buffer와 payload를 소유한다.
- Public Relay, internal control, internal peer relay, Raft TCP와 read-only REST는 분리된 protocol 경계다.
- `ClientId`는 인증으로 정해지는 strict namespace이며 client/API key는 external YAML이 관리한다.
- Retry, response replay, Pipe resume/attach, payload replay와 durable delivery는 지원하지 않는다.

## Repository

```text
cmd/relaygate/                         process entrypoint
internal/
├── app/{relaygate,config,admin}       composition, config, observation
├── raft/{state,node}                  Raft safety/epoch and runtime
├── gateway/
│   ├── access/{auth,session,runtime}  authentication and reload retirement
│   ├── control/{authority,client,server,transport}
│   ├── routing/{binding,opening}      Listener and Pipe lifecycle
│   └── relay/{public,peer}            SDK ingress and Gateway hop
└── gen/{control,gateway,relay}/v1     generated protobuf
proto/relaygate/                       canonical wire schemas
sdk/{go,rust/relaygate-sdk}            public SDKs
examples/echo/                         isolated one-node echo example
docs/{adr,spec,test}/                  decisions, contracts, evidence
```

각 leaf Go package 안은 flat file layout을 유지하고 `internal/` tree가 component ownership을 나타낸다.

## Implemented v0

- HashiCorp Raft single/정적 3-voter bootstrap과 safety-only persistence
- Leader/quorum-confirmed authority, memory-only session/directory, failover clear + full redeclare
- SHA-256 API-key verifier, first-message auth deadline, `SIGHUP` atomic client reload
- Exact literal endpoint + required target Bind/Open, same/cross-Gateway owner routing
- `A ∧ L ∧ Q ∧ D ∧ V ∧ O` admission and bounded single-use forwarded attempt fence
- Listener confirmation ACK, caller `Opened/Failed/Unknown`, participant-owned close
- 1..60 KiB opaque payload, per-direction FIFO, bounded backpressure and terminal priority
- Public Go/Rust SDK and four-language-combination Compose conformance
- Quorum-confirmed read-only status, health/readiness and Prometheus metrics

Wildcard/priority, target selection, `OpenAll`, dynamic membership, automatic retry/resume은 범위 밖이다.

## SDK contract

| SDK | Location | Public handles |
| --- | --- | --- |
| Go | `sdk/go` | `Client`, `Listener`, `Offer`, `Pipe` |
| Rust | `sdk/rust/relaygate-sdk` | `Client`, `Listener`, `Offer`, `Pipe` |

두 SDK는 `relay.proto`만 공유하고 server/control/Raft type을 노출하지 않는다. TLS가 기본이며 plaintext는
명시적인 loopback local-development opt-in만 허용한다. `Send` 성공은 bounded local queue/stream write
성공이지 peer application ACK가 아니다.

직접 실행하는 Go/Rust echo 예제는 [examples/echo](examples/echo/README.md)에 있다.

## Local configuration

```bash
go run ./cmd/relaygate -config ./configs/relaygate.yaml
curl http://127.0.0.1:27490/healthz/ready
curl http://127.0.0.1:27490/status
```

기준 설정은 `configs/relaygate.yaml` 하나다. 배포별 node/address/bootstrap 값은 `RELAYGATE_*` 환경변수로
덮어쓴다. Local port block은 다음과 같다.

| Port | Purpose |
| --- | --- |
| `27400` | Raft TCP |
| `27410` | Internal control gRPC |
| `27420` | Public Relay gRPC |
| `27430` | Internal owner relay gRPC |
| `27490` | Read-only Admin HTTP |

Port와 Raft peer address는 restart-only다. Existing durable peer address를 바꾸면 fresh local volume 또는
명시적 membership migration이 필요하다. 기본 local-development key는
`relaygate-local-development-key`이며 production에서는 반드시 교체한다.

## Local 3-node verification

```bash
./scripts/compose-smoke.sh
```

이 명령은 격리된 3-node cluster에서 same/cross-Gateway relay, 양방향 payload, Go/Rust 네 조합, leader
failover, empty directory와 fresh redeclare를 검증하고 생성한 resource를 정리한다.

상태를 직접 관찰할 때만 수동으로 실행한다.

```bash
docker compose up -d --build
curl http://127.0.0.1:27491/status
curl http://127.0.0.1:27492/status
curl http://127.0.0.1:27493/status
docker compose down -v
```

Compose는 internal owner relay를 host에 publish하지 않는다. Public bearer listener는 loopback만 허용하고,
Compose는 private container network를 위해 명시적으로 bind override한다.

## Security and production boundary

현재 internal control/peer/Raft transport와 Admin REST는 trusted local/dev network 전제다. Shared/untrusted
network에 그대로 노출하지 않는다. Production 전에는 다음이 필요하다.

- Internal peer/control identity authentication 또는 mTLS
- Raft transport mTLS 정책
- `ClockSkewBound < relay.open_timeout` evidence
- Lost voter replacement와 safe old-path fencing operator flow

상세 계약은 [SPEC 001](docs/spec/001-system-model.md), 장애/복구는
[SPEC 003](docs/spec/003-failure-and-recovery-model.md), canonical transition은
[SPEC 004](docs/spec/004-state-transition-model.md), 검증 목록과 현재 evidence는
[TEST 001](docs/test/001-core-correctness-test-plan.md),
[TEST 002](docs/test/002-failure-evidence-matrix.md)를 따른다.
